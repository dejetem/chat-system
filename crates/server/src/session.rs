use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use protocol::Frame;
use tokio::sync::mpsc::error::TrySendError;

use crate::connection::{ConnectionHandle, ConnectionId};

const SLOW_CONSUMER_TIMEOUT: Duration = Duration::from_millis(100);

/// Result of registering an authenticated connection in [`SessionRegistry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisterOutcome {
    /// `true` when this connection became the user's first active local session.
    pub first_local_session: bool,
    /// Total active local sessions now registered for this user.
    pub active_sessions: usize,
}

/// Result of removing a connection from [`SessionRegistry`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnregisterOutcome {
    /// Authenticated username associated with the removed connection.
    pub username: String,
    /// Remaining active local sessions for `username` after removal.
    pub remaining_local_sessions: usize,
}

/// Outcome of routing a direct frame to a recipient user on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirectDelivery {
    /// Recipient had at least one local session (or matched only the excluded self-connection).
    DeliveredLocal {
        delivered: usize,
        dropped_full: usize,
        removed_closed: usize,
    },
    /// Recipient has no active local sessions on this node.
    RecipientNotLocal,
}

#[derive(Debug, Default)]
struct SessionState {
    user_sessions: HashMap<String, HashMap<ConnectionId, ConnectionHandle>>,
    connection_users: HashMap<ConnectionId, String>,
}

/// Tracks active authenticated user sessions on a single chat node for direct message routing.
#[derive(Debug, Clone)]
pub struct SessionRegistry {
    node_id: Arc<str>,
    inner: Arc<RwLock<SessionState>>,
    next_message_id: Arc<AtomicU64>,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionRegistry {
    /// Creates a [`SessionRegistry`] tagged with `"local"`.
    pub fn new() -> Self {
        Self::for_node("local")
    }

    /// Creates a [`SessionRegistry`] associated with `node_id`.
    pub fn for_node(node_id: impl Into<String>) -> Self {
        Self {
            node_id: Arc::from(node_id.into()),
            inner: Arc::new(RwLock::new(SessionState::default())),
            next_message_id: Arc::new(AtomicU64::new(1)),
        }
    }

    /// Returns the cluster node identifier associated with this registry.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Generates the next monotonically increasing message identifier for acknowledgements.
    pub fn next_message_id(&self) -> u64 {
        self.next_message_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Registers `handle` under `username`, supporting concurrent multi-device sessions.
    pub fn register(
        &self,
        username: impl Into<String>,
        handle: ConnectionHandle,
    ) -> RegisterOutcome {
        let username = username.into();
        let conn_id = handle.id();

        let mut state = self.inner.write().expect("session registry lock poisoned");

        // Clean up any stale username mapping if this connection ID was previously registered.
        if let Some(previous_user) = state.connection_users.insert(conn_id, username.clone())
            && previous_user != username
            && let Some(prev_sessions) = state.user_sessions.get_mut(&previous_user)
        {
            prev_sessions.remove(&conn_id);
            if prev_sessions.is_empty() {
                state.user_sessions.remove(&previous_user);
            }
        }

        let sessions = state.user_sessions.entry(username).or_default();
        sessions.insert(conn_id, handle);
        let active_sessions = sessions.len();

        RegisterOutcome {
            first_local_session: active_sessions == 1,
            active_sessions,
        }
    }

    /// Unregisters `conn_id`, returning its username and remaining session count if present.
    pub fn remove_connection(&self, conn_id: ConnectionId) -> Option<UnregisterOutcome> {
        let mut state = self.inner.write().expect("session registry lock poisoned");
        let username = state.connection_users.remove(&conn_id)?;

        let remaining_local_sessions = match state.user_sessions.get_mut(&username) {
            Some(sessions) => {
                sessions.remove(&conn_id);
                let remaining = sessions.len();
                if remaining == 0 {
                    state.user_sessions.remove(&username);
                }
                remaining
            }
            None => 0,
        };

        Some(UnregisterOutcome {
            username,
            remaining_local_sessions,
        })
    }

    /// Returns `true` if `username` has at least one active local session.
    pub fn is_online(&self, username: &str) -> bool {
        let state = self.inner.read().expect("session registry lock poisoned");
        state
            .user_sessions
            .get(username)
            .is_some_and(|sessions| !sessions.is_empty())
    }

    /// Returns the number of active local sessions for `username`.
    pub fn session_count_for(&self, username: &str) -> usize {
        let state = self.inner.read().expect("session registry lock poisoned");
        state.user_sessions.get(username).map_or(0, HashMap::len)
    }

    /// Returns the number of distinct authenticated users with active local sessions.
    pub fn online_user_count(&self) -> usize {
        let state = self.inner.read().expect("session registry lock poisoned");
        state.user_sessions.len()
    }

    /// Returns the total number of active local sessions across all users.
    pub fn total_session_count(&self) -> usize {
        let state = self.inner.read().expect("session registry lock poisoned");
        state.connection_users.len()
    }

    /// Delivers `frame` to all active local sessions for `recipient` except `exclude`.
    pub async fn send_to_user(
        &self,
        recipient: &str,
        frame: Frame,
        exclude: Option<ConnectionId>,
    ) -> DirectDelivery {
        let (targets, matched_excluded): (Vec<ConnectionHandle>, bool) = {
            let state = self.inner.read().expect("session registry lock poisoned");
            let Some(sessions) = state.user_sessions.get(recipient) else {
                return DirectDelivery::RecipientNotLocal;
            };
            if sessions.is_empty() {
                return DirectDelivery::RecipientNotLocal;
            }
            let matched_excluded = exclude.is_some_and(|ex| sessions.contains_key(&ex));
            let targets = sessions
                .values()
                .filter(|handle| Some(handle.id()) != exclude)
                .cloned()
                .collect();
            (targets, matched_excluded)
        };

        let mut delivered = 0usize;
        let mut dropped_full = 0usize;
        let mut stale_ids = Vec::new();

        for target in targets {
            match target.try_send(frame.clone()) {
                Ok(()) => delivered += 1,
                Err(TrySendError::Closed(_)) => stale_ids.push(target.id()),
                Err(TrySendError::Full(returned_frame)) => {
                    match tokio::time::timeout(SLOW_CONSUMER_TIMEOUT, target.send(returned_frame))
                        .await
                    {
                        Ok(Ok(())) => delivered += 1,
                        Ok(Err(_)) => stale_ids.push(target.id()),
                        Err(_) => {
                            dropped_full += 1;
                            tracing::warn!(
                                %recipient,
                                connection_id = %target.id(),
                                "dropping private message for slow consumer"
                            );
                        }
                    }
                }
            }
        }

        let mut removed_closed = 0usize;
        for stale_id in stale_ids {
            if self.remove_connection(stale_id).is_some() {
                removed_closed += 1;
            }
        }

        if delivered == 0 && dropped_full == 0 && !matched_excluded {
            return DirectDelivery::RecipientNotLocal;
        }

        DirectDelivery::DeliveredLocal {
            delivered,
            dropped_full,
            removed_closed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use protocol::{Frame, Message, PrivateMessagePayload};

    use super::{DirectDelivery, RegisterOutcome, SessionRegistry, UnregisterOutcome};
    use crate::connection::{ConnectionHandle, ConnectionId};

    fn test_handle(id: u64) -> (ConnectionHandle, tokio::sync::mpsc::Receiver<Frame>) {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 41000 + id as u16);
        ConnectionHandle::new_for_test(ConnectionId(id), addr, 16)
    }

    fn dm_frame(recipient: &str, text: &str) -> Frame {
        Frame::new(Message::PrivateMessage(PrivateMessagePayload {
            recipient: recipient.to_string(),
            message: text.to_string(),
        }))
    }

    #[test]
    fn register_and_remove_connection_tracks_online_users_and_node_id() {
        let registry = SessionRegistry::for_node("node-east-1");
        assert_eq!(registry.node_id(), "node-east-1");

        let (h1, _rx1) = test_handle(1);
        let outcome = registry.register("alice", h1);
        assert_eq!(
            outcome,
            RegisterOutcome {
                first_local_session: true,
                active_sessions: 1,
            }
        );
        assert!(registry.is_online("alice"));
        assert_eq!(registry.session_count_for("alice"), 1);
        assert_eq!(registry.online_user_count(), 1);
        assert_eq!(registry.total_session_count(), 1);

        let removed = registry.remove_connection(ConnectionId(1));
        assert_eq!(
            removed,
            Some(UnregisterOutcome {
                username: "alice".to_string(),
                remaining_local_sessions: 0,
            })
        );
        assert_eq!(registry.remove_connection(ConnectionId(1)), None);
        assert!(!registry.is_online("alice"));
        assert_eq!(registry.online_user_count(), 0);
        assert_eq!(registry.total_session_count(), 0);
    }

    #[tokio::test]
    async fn multi_device_same_username_delivers_to_all_other_sessions() {
        let registry = SessionRegistry::new();
        let (laptop, mut laptop_rx) = test_handle(1);
        let (phone, mut phone_rx) = test_handle(2);
        let (tablet, mut tablet_rx) = test_handle(3);

        assert_eq!(
            registry.register("alice", laptop),
            RegisterOutcome {
                first_local_session: true,
                active_sessions: 1,
            }
        );
        assert_eq!(
            registry.register("alice", phone),
            RegisterOutcome {
                first_local_session: false,
                active_sessions: 2,
            }
        );
        assert_eq!(
            registry.register("alice", tablet),
            RegisterOutcome {
                first_local_session: false,
                active_sessions: 3,
            }
        );

        let frame = dm_frame("alice", "sync across devices");
        let delivery = registry
            .send_to_user("alice", frame.clone(), Some(ConnectionId(1)))
            .await;

        assert_eq!(
            delivery,
            DirectDelivery::DeliveredLocal {
                delivered: 2,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert!(laptop_rx.try_recv().is_err());
        assert_eq!(phone_rx.try_recv().unwrap(), frame);
        assert_eq!(tablet_rx.try_recv().unwrap(), frame);
    }

    #[tokio::test]
    async fn reconnect_race_removing_old_connection_preserves_new_connection() {
        let registry = SessionRegistry::new();
        let (old_conn, _old_rx) = test_handle(10);
        let (new_conn, mut new_rx) = test_handle(11);

        registry.register("alice", old_conn);
        registry.register("alice", new_conn);
        assert_eq!(registry.session_count_for("alice"), 2);

        let removed = registry.remove_connection(ConnectionId(10)).unwrap();
        assert_eq!(
            removed,
            UnregisterOutcome {
                username: "alice".to_string(),
                remaining_local_sessions: 1,
            }
        );
        assert!(registry.is_online("alice"));

        let frame = dm_frame("alice", "still reachable");
        let delivery = registry.send_to_user("alice", frame.clone(), None).await;
        assert_eq!(
            delivery,
            DirectDelivery::DeliveredLocal {
                delivered: 1,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert_eq!(new_rx.try_recv().unwrap(), frame);
    }

    #[tokio::test]
    async fn send_to_offline_user_returns_recipient_not_local() {
        let registry = SessionRegistry::new();
        let frame = dm_frame("ghost", "anyone there?");
        let delivery = registry
            .send_to_user("ghost", frame, Some(ConnectionId(1)))
            .await;
        assert_eq!(delivery, DirectDelivery::RecipientNotLocal);
    }

    #[tokio::test]
    async fn send_to_self_single_device_acknowledges_as_local_without_self_echo() {
        let registry = SessionRegistry::new();
        let (h1, mut rx1) = test_handle(1);
        registry.register("alice", h1);

        let frame = dm_frame("alice", "note to self");
        let delivery = registry
            .send_to_user("alice", frame, Some(ConnectionId(1)))
            .await;

        assert_eq!(
            delivery,
            DirectDelivery::DeliveredLocal {
                delivered: 0,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert!(rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn send_without_exclusion_delivers_to_all_user_sessions() {
        let registry = SessionRegistry::new();
        let (h1, mut rx1) = test_handle(1);
        let (h2, mut rx2) = test_handle(2);
        registry.register("alice", h1);
        registry.register("alice", h2);

        let frame = dm_frame("alice", "remote node forward");
        let delivery = registry.send_to_user("alice", frame.clone(), None).await;

        assert_eq!(
            delivery,
            DirectDelivery::DeliveredLocal {
                delivered: 2,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert_eq!(rx1.try_recv().unwrap(), frame);
        assert_eq!(rx2.try_recv().unwrap(), frame);
    }

    #[tokio::test]
    async fn send_prunes_closed_handles_and_reports_not_local_when_all_stale() {
        let registry = SessionRegistry::new();
        let (stale_handle, stale_rx) = test_handle(5);
        registry.register("bob", stale_handle);
        drop(stale_rx);

        let frame = dm_frame("bob", "hello");
        let delivery = registry
            .send_to_user("bob", frame, Some(ConnectionId(1)))
            .await;

        assert_eq!(delivery, DirectDelivery::RecipientNotLocal);
        assert!(!registry.is_online("bob"));
        assert_eq!(registry.online_user_count(), 0);
        assert_eq!(registry.total_session_count(), 0);
    }

    #[test]
    fn duplicate_register_same_connection_id_is_idempotent() {
        let registry = SessionRegistry::new();
        let (h1_a, _rx_a) = test_handle(1);
        let (h1_b, mut rx_b) = test_handle(1);

        assert_eq!(
            registry.register("alice", h1_a),
            RegisterOutcome {
                first_local_session: true,
                active_sessions: 1,
            }
        );
        assert_eq!(
            registry.register("alice", h1_b),
            RegisterOutcome {
                first_local_session: true,
                active_sessions: 1,
            }
        );
        assert_eq!(registry.total_session_count(), 1);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let frame = dm_frame("alice", "refreshed");
        let delivery = rt.block_on(registry.send_to_user("alice", frame.clone(), None));
        assert_eq!(
            delivery,
            DirectDelivery::DeliveredLocal {
                delivered: 1,
                dropped_full: 0,
                removed_closed: 0,
            }
        );
        assert_eq!(rx_b.try_recv().unwrap(), frame);
    }
}
