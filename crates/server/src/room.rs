use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use protocol::Frame;
use tokio::sync::mpsc::error::TrySendError;

use crate::connection::{ConnectionHandle, ConnectionId};

/// Identifier for a chat room.
pub type RoomId = u64;

const SLOW_CONSUMER_TIMEOUT: Duration = Duration::from_millis(100);

/// Summary of a room fan-out operation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BroadcastOutcome {
    /// Number of subscriber queues that accepted the frame.
    pub delivered: usize,
    /// Number of slow subscriber queues that timed out and dropped the frame.
    pub dropped_full: usize,
    /// Number of disconnected subscriber handles pruned during broadcast.
    pub removed_closed: usize,
}

#[derive(Debug, Default)]
struct RoomState {
    rooms: HashMap<RoomId, HashMap<ConnectionId, ConnectionHandle>>,
    subscriptions: HashMap<ConnectionId, HashSet<RoomId>>,
}

/// Manages single-node room memberships and non-blocking frame fan-out.
#[derive(Debug, Clone)]
pub struct RoomManager {
    inner: Arc<RwLock<RoomState>>,
    next_message_id: Arc<AtomicU64>,
}

impl Default for RoomManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RoomManager {
    /// Creates an empty [`RoomManager`].
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(RoomState::default())),
            next_message_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Generates the next monotonically increasing message identifier for acknowledgements.
    pub fn next_message_id(&self) -> u64 {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Subscribes `handle` to `room_id`, returning `true` if newly added or `false` if already subscribed.
    pub fn join_room(&self, room_id: RoomId, handle: ConnectionHandle) -> bool {
        let conn_id = handle.id();
        let mut state = self.inner.write().expect("room manager lock poisoned");
        let is_new = state
            .rooms
            .entry(room_id)
            .or_default()
            .insert(conn_id, handle)
            .is_none();
        state
            .subscriptions
            .entry(conn_id)
            .or_default()
            .insert(room_id);
        is_new
    }

    /// Unsubscribes `conn_id` from `room_id`, pruning empty rooms and empty reverse entries.
    pub fn leave_room(&self, room_id: RoomId, conn_id: ConnectionId) -> bool {
        let mut state = self.inner.write().expect("room manager lock poisoned");
        let removed = match state.rooms.get_mut(&room_id) {
            Some(subscribers) => {
                let existed = subscribers.remove(&conn_id).is_some();
                if subscribers.is_empty() {
                    state.rooms.remove(&room_id);
                }
                existed
            }
            None => false,
        };

        if !removed {
            return false;
        }

        if let Some(conn_rooms) = state.subscriptions.get_mut(&conn_id) {
            conn_rooms.remove(&room_id);
            if conn_rooms.is_empty() {
                state.subscriptions.remove(&conn_id);
            }
        }

        true
    }

    /// Removes `conn_id` from all subscribed rooms, returning how many rooms were left.
    pub fn remove_connection(&self, conn_id: ConnectionId) -> usize {
        let mut state = self.inner.write().expect("room manager lock poisoned");
        let Some(joined_rooms) = state.subscriptions.remove(&conn_id) else {
            return 0;
        };

        let removed_count = joined_rooms.len();
        for room_id in joined_rooms {
            if let Some(subscribers) = state.rooms.get_mut(&room_id) {
                subscribers.remove(&conn_id);
                if subscribers.is_empty() {
                    state.rooms.remove(&room_id);
                }
            }
        }

        removed_count
    }

    /// Returns `true` if `conn_id` is currently subscribed to `room_id`.
    pub fn is_subscribed(&self, room_id: RoomId, conn_id: ConnectionId) -> bool {
        let state = self.inner.read().expect("room manager lock poisoned");
        state
            .rooms
            .get(&room_id)
            .is_some_and(|subscribers| subscribers.contains_key(&conn_id))
    }

    /// Returns the number of active subscribers in `room_id`.
    pub fn subscriber_count(&self, room_id: RoomId) -> usize {
        let state = self.inner.read().expect("room manager lock poisoned");
        state.rooms.get(&room_id).map_or(0, HashMap::len)
    }

    /// Returns the number of non-empty rooms currently tracked.
    pub fn room_count(&self) -> usize {
        let state = self.inner.read().expect("room manager lock poisoned");
        state.rooms.len()
    }

    /// Returns the sorted list of rooms `conn_id` is currently subscribed to.
    pub fn connection_rooms(&self, conn_id: ConnectionId) -> Vec<RoomId> {
        let state = self.inner.read().expect("room manager lock poisoned");
        let mut rooms: Vec<RoomId> = state
            .subscriptions
            .get(&conn_id)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default();
        rooms.sort_unstable();
        rooms
    }

    /// Fans out `frame` to every subscriber in `room_id` except `exclude`, without holding locks across `.await`.
    pub async fn broadcast(
        &self,
        room_id: RoomId,
        frame: Frame,
        exclude: Option<ConnectionId>,
    ) -> BroadcastOutcome {
        let targets: Vec<ConnectionHandle> = {
            let state = self.inner.read().expect("room manager lock poisoned");
            let Some(subscribers) = state.rooms.get(&room_id) else {
                return BroadcastOutcome::default();
            };
            subscribers
                .values()
                .filter(|handle| Some(handle.id()) != exclude)
                .cloned()
                .collect()
        };

        let mut outcome = BroadcastOutcome::default();
        let mut stale_ids = Vec::new();

        for target in targets {
            match target.try_send(frame.clone()) {
                Ok(()) => outcome.delivered += 1,
                Err(TrySendError::Closed(_)) => stale_ids.push(target.id()),
                Err(TrySendError::Full(returned_frame)) => {
                    match tokio::time::timeout(SLOW_CONSUMER_TIMEOUT, target.send(returned_frame))
                        .await
                    {
                        Ok(Ok(())) => outcome.delivered += 1,
                        Ok(Err(_)) => stale_ids.push(target.id()),
                        Err(_) => {
                            outcome.dropped_full += 1;
                            tracing::warn!(
                                room_id,
                                connection_id = %target.id(),
                                "dropping room message for slow consumer"
                            );
                        }
                    }
                }
            }
        }

        for stale_id in stale_ids {
            if self.remove_connection(stale_id) > 0 {
                outcome.removed_closed += 1;
            }
        }

        outcome
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use protocol::{Frame, Message, RoomMessagePayload};

    use super::{BroadcastOutcome, RoomManager};
    use crate::connection::{ConnectionHandle, ConnectionId};

    fn test_handle(id: u64) -> (ConnectionHandle, tokio::sync::mpsc::Receiver<Frame>) {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40000 + id as u16);
        ConnectionHandle::new_for_test(ConnectionId(id), addr, 16)
    }

    fn room_frame(room_id: u64, text: &str) -> Frame {
        Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id,
            message: text.to_string(),
        }))
    }

    #[test]
    fn join_and_leave_room_updates_counts_and_prunes_empty_room() {
        let manager = RoomManager::new();
        let (h1, _rx1) = test_handle(1);
        let (h2, _rx2) = test_handle(2);

        assert!(manager.join_room(10, h1));
        assert!(manager.join_room(10, h2));
        assert_eq!(manager.room_count(), 1);
        assert_eq!(manager.subscriber_count(10), 2);
        assert!(manager.is_subscribed(10, ConnectionId(1)));
        assert_eq!(manager.connection_rooms(ConnectionId(1)), vec![10]);

        assert!(manager.leave_room(10, ConnectionId(1)));
        assert_eq!(manager.subscriber_count(10), 1);
        assert!(!manager.is_subscribed(10, ConnectionId(1)));
        assert!(manager.connection_rooms(ConnectionId(1)).is_empty());

        assert!(manager.leave_room(10, ConnectionId(2)));
        assert_eq!(manager.subscriber_count(10), 0);
        assert_eq!(manager.room_count(), 0);
    }

    #[test]
    fn duplicate_join_is_idempotent() {
        let manager = RoomManager::new();
        let (h1_a, _rx_a) = test_handle(1);
        let (h1_b, mut rx_b) = test_handle(1);

        assert!(manager.join_room(5, h1_a));
        assert!(!manager.join_room(5, h1_b));
        assert_eq!(manager.subscriber_count(5), 1);
        assert_eq!(manager.connection_rooms(ConnectionId(1)), vec![5]);

        // Verify the refreshed handle is used on subsequent delivery.
        let frame = room_frame(5, "refresh");
        let rt = tokio::runtime::Runtime::new().unwrap();
        let outcome = rt.block_on(manager.broadcast(5, frame.clone(), None));
        assert_eq!(
            outcome,
            BroadcastOutcome {
                delivered: 1,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert_eq!(rx_b.try_recv().unwrap(), frame);
    }

    #[test]
    fn leave_unsubscribed_room_returns_false() {
        let manager = RoomManager::new();
        let (h1, _rx1) = test_handle(1);

        assert!(!manager.leave_room(99, ConnectionId(1)));
        manager.join_room(1, h1);
        assert!(!manager.leave_room(99, ConnectionId(1)));
        assert_eq!(manager.subscriber_count(1), 1);
    }

    #[tokio::test]
    async fn broadcast_delivers_to_subscribers_excluding_sender() {
        let manager = RoomManager::new();
        let (h1, mut rx1) = test_handle(1);
        let (h2, mut rx2) = test_handle(2);
        let (h3, mut rx3) = test_handle(3);

        manager.join_room(7, h1);
        manager.join_room(7, h2);
        manager.join_room(7, h3);

        let frame = room_frame(7, "hello room");
        let outcome = manager
            .broadcast(7, frame.clone(), Some(ConnectionId(1)))
            .await;

        assert_eq!(
            outcome,
            BroadcastOutcome {
                delivered: 2,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert!(rx1.try_recv().is_err());
        assert_eq!(rx2.try_recv().unwrap(), frame);
        assert_eq!(rx3.try_recv().unwrap(), frame);
    }

    #[tokio::test]
    async fn broadcast_without_exclusion_delivers_to_all_subscribers() {
        let manager = RoomManager::new();
        let (h1, mut rx1) = test_handle(1);
        let (h2, mut rx2) = test_handle(2);

        manager.join_room(8, h1);
        manager.join_room(8, h2);

        let frame = room_frame(8, "system announcement");
        let outcome = manager.broadcast(8, frame.clone(), None).await;

        assert_eq!(
            outcome,
            BroadcastOutcome {
                delivered: 2,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert_eq!(rx1.try_recv().unwrap(), frame);
        assert_eq!(rx2.try_recv().unwrap(), frame);
    }

    #[test]
    fn remove_connection_cleans_all_joined_rooms_and_prunes_empty() {
        let manager = RoomManager::new();
        let (h1, _rx1) = test_handle(1);
        let (h2, _rx2) = test_handle(2);

        manager.join_room(1, h1.clone());
        manager.join_room(2, h1.clone());
        manager.join_room(3, h1);
        manager.join_room(2, h2);

        assert_eq!(manager.room_count(), 3);
        assert_eq!(manager.connection_rooms(ConnectionId(1)), vec![1, 2, 3]);

        assert_eq!(manager.remove_connection(ConnectionId(1)), 3);
        assert_eq!(manager.remove_connection(ConnectionId(1)), 0);

        assert_eq!(manager.room_count(), 1);
        assert_eq!(manager.subscriber_count(1), 0);
        assert_eq!(manager.subscriber_count(2), 1);
        assert_eq!(manager.subscriber_count(3), 0);
        assert!(manager.connection_rooms(ConnectionId(1)).is_empty());
    }

    #[tokio::test]
    async fn broadcast_prunes_closed_handles_automatically() {
        let manager = RoomManager::new();
        let (h1, rx1) = test_handle(1);
        let (h2, mut rx2) = test_handle(2);

        manager.join_room(12, h1);
        manager.join_room(12, h2);
        drop(rx1);

        let frame = room_frame(12, "prune stale");
        let outcome = manager.broadcast(12, frame.clone(), None).await;

        assert_eq!(
            outcome,
            BroadcastOutcome {
                delivered: 1,
                dropped_full: 0,
                removed_closed: 1,
            }
        );
        assert_eq!(rx2.try_recv().unwrap(), frame);
        assert_eq!(manager.subscriber_count(12), 1);
        assert!(!manager.is_subscribed(12, ConnectionId(1)));
    }

    #[tokio::test]
    async fn room_isolation_prevents_cross_room_leakage() {
        let manager = RoomManager::new();
        let (h1, mut rx1) = test_handle(1);
        let (h2, mut rx2) = test_handle(2);

        manager.join_room(100, h1);
        manager.join_room(200, h2);

        let frame_100 = room_frame(100, "only room 100");
        let outcome = manager.broadcast(100, frame_100.clone(), None).await;

        assert_eq!(outcome.delivered, 1);
        assert_eq!(rx1.try_recv().unwrap(), frame_100);
        assert!(rx2.try_recv().is_err());
    }
}
