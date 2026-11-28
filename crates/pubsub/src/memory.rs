use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::broker::PubSubBackend;
use crate::error::PubSubError;
use crate::keys::{KeySpace, validate_segment};
use crate::message::PubSubEnvelope;

#[derive(Debug)]
struct NodeEntry {
    event_tx: broadcast::Sender<PubSubEnvelope>,
    active_rooms: HashSet<u64>,
}

#[derive(Debug)]
struct HubState {
    channel_capacity: usize,
    nodes: HashMap<String, NodeEntry>,
    user_nodes: HashMap<String, HashSet<String>>,
}

impl HubState {
    fn new(channel_capacity: usize) -> Self {
        Self {
            channel_capacity,
            nodes: HashMap::new(),
            user_nodes: HashMap::new(),
        }
    }
}

/// Shared in-memory cluster bus for multi-node tests without a live Redis server.
#[derive(Debug, Clone)]
pub struct InMemoryPubSubHub {
    keyspace: KeySpace,
    inner: Arc<RwLock<HubState>>,
}

impl Default for InMemoryPubSubHub {
    fn default() -> Self {
        Self::new("chat", 1024).expect("default KeySpace and capacity are valid")
    }
}

impl InMemoryPubSubHub {
    /// Creates an in-memory cluster hub using `key_prefix` and per-node broadcast buffer capacity.
    pub fn new(prefix: impl AsRef<str>, channel_capacity: usize) -> Result<Self, PubSubError> {
        if channel_capacity == 0 {
            return Err(PubSubError::Validation(
                "channel_capacity must be greater than 0".to_string(),
            ));
        }
        Ok(Self {
            keyspace: KeySpace::new(prefix)?,
            inner: Arc::new(RwLock::new(HubState::new(channel_capacity))),
        })
    }

    /// Returns the hub's [`KeySpace`].
    pub fn keyspace(&self) -> &KeySpace {
        &self.keyspace
    }

    /// Registers and returns a [`PubSubBackend`] handle representing `node_id`.
    pub fn node(&self, node_id: impl Into<String>) -> InMemoryPubSub {
        let node_id_clean =
            validate_segment(&node_id.into(), "node_id").expect("node_id must be valid");
        let event_tx = {
            let mut state = self.inner.write().expect("in-memory hub lock poisoned");
            if let Some(existing) = state.nodes.get(&node_id_clean) {
                existing.event_tx.clone()
            } else {
                let (event_tx, _rx) = broadcast::channel(state.channel_capacity);
                state.nodes.insert(
                    node_id_clean.clone(),
                    NodeEntry {
                        event_tx: event_tx.clone(),
                        active_rooms: HashSet::new(),
                    },
                );
                event_tx
            }
        };

        InMemoryPubSub {
            node_id: Arc::from(node_id_clean),
            hub: self.clone(),
            event_tx,
            room_counts: Arc::new(Mutex::new(HashMap::new())),
            user_counts: Arc::new(Mutex::new(HashMap::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Returns whether `node_id` currently maintains an active cluster subscription for `room_id`.
    pub fn is_node_subscribed_to_room(&self, node_id: &str, room_id: u64) -> bool {
        let state = self.inner.read().expect("in-memory hub lock poisoned");
        state
            .nodes
            .get(node_id)
            .is_some_and(|entry| entry.active_rooms.contains(&room_id))
    }

    /// Injects a stale node ID for `username` to test lazy pruning when a node disappears uncleanly.
    pub fn insert_stale_user_node_for_test(&self, username: &str, stale_node_id: &str) {
        let mut state = self.inner.write().expect("in-memory hub lock poisoned");
        state
            .user_nodes
            .entry(username.to_string())
            .or_default()
            .insert(stale_node_id.to_string());
    }
}

/// Per-node handle into [`InMemoryPubSubHub`] matching [`crate::RedisPubSub`] behavior.
#[derive(Debug, Clone)]
pub struct InMemoryPubSub {
    node_id: Arc<str>,
    hub: InMemoryPubSubHub,
    event_tx: broadcast::Sender<PubSubEnvelope>,
    room_counts: Arc<Mutex<HashMap<u64, usize>>>,
    user_counts: Arc<Mutex<HashMap<String, usize>>>,
    shutting_down: Arc<AtomicBool>,
}

impl InMemoryPubSub {
    fn ensure_open(&self) -> Result<(), PubSubError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(PubSubError::Closed);
        }
        Ok(())
    }
}

#[async_trait]
impl PubSubBackend for InMemoryPubSub {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn subscribe_events(&self) -> broadcast::Receiver<PubSubEnvelope> {
        self.event_tx.subscribe()
    }

    async fn register_user_session(&self, username: &str) -> Result<bool, PubSubError> {
        self.ensure_open()?;
        let clean_user = validate_segment(username, "username")?;

        let mut users = self.user_counts.lock().expect("user_counts mutex poisoned");
        let count = users.entry(clean_user.clone()).or_insert(0);
        let newly_registered = *count == 0;
        if newly_registered {
            let mut state = self.hub.inner.write().expect("in-memory hub lock poisoned");
            state
                .user_nodes
                .entry(clean_user)
                .or_default()
                .insert(self.node_id.to_string());
        }
        *count += 1;
        Ok(newly_registered)
    }

    async fn unregister_user_session(&self, username: &str) -> Result<bool, PubSubError> {
        let clean_user = validate_segment(username, "username")?;

        let mut users = self.user_counts.lock().expect("user_counts mutex poisoned");
        let current = users.get(&clean_user).copied().unwrap_or(0);
        match current {
            0 => Ok(false),
            1 => {
                users.remove(&clean_user);
                let mut state = self.hub.inner.write().expect("in-memory hub lock poisoned");
                if let Some(nodes) = state.user_nodes.get_mut(&clean_user) {
                    nodes.remove(self.node_id.as_ref());
                    if nodes.is_empty() {
                        state.user_nodes.remove(&clean_user);
                    }
                }
                Ok(true)
            }
            n => {
                users.insert(clean_user, n - 1);
                Ok(false)
            }
        }
    }

    async fn lookup_user_nodes(&self, username: &str) -> Result<Vec<String>, PubSubError> {
        let clean_user = validate_segment(username, "username")?;
        let state = self.hub.inner.read().expect("in-memory hub lock poisoned");
        let mut nodes: Vec<String> = state
            .user_nodes
            .get(&clean_user)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default();
        nodes.sort_unstable();
        Ok(nodes)
    }

    async fn prune_stale_user_node(
        &self,
        username: &str,
        stale_node_id: &str,
    ) -> Result<(), PubSubError> {
        let clean_user = validate_segment(username, "username")?;
        let clean_node = validate_segment(stale_node_id, "stale_node_id")?;
        let mut state = self.hub.inner.write().expect("in-memory hub lock poisoned");
        if let Some(nodes) = state.user_nodes.get_mut(&clean_user) {
            nodes.remove(&clean_node);
            if nodes.is_empty() {
                state.user_nodes.remove(&clean_user);
            }
        }
        Ok(())
    }

    async fn subscribe_room(&self, room_id: u64) -> Result<bool, PubSubError> {
        self.ensure_open()?;
        let mut rooms = self.room_counts.lock().expect("room_counts mutex poisoned");
        let count = rooms.entry(room_id).or_insert(0);
        let newly_subscribed = *count == 0;
        if newly_subscribed {
            let mut state = self.hub.inner.write().expect("in-memory hub lock poisoned");
            if let Some(node_entry) = state.nodes.get_mut(self.node_id.as_ref()) {
                node_entry.active_rooms.insert(room_id);
            }
        }
        *count += 1;
        Ok(newly_subscribed)
    }

    async fn unsubscribe_room(&self, room_id: u64) -> Result<bool, PubSubError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut rooms = self.room_counts.lock().expect("room_counts mutex poisoned");
        let current = rooms.get(&room_id).copied().unwrap_or(0);
        match current {
            0 => Ok(false),
            1 => {
                rooms.remove(&room_id);
                let mut state = self.hub.inner.write().expect("in-memory hub lock poisoned");
                if let Some(node_entry) = state.nodes.get_mut(self.node_id.as_ref()) {
                    node_entry.active_rooms.remove(&room_id);
                }
                Ok(true)
            }
            n => {
                rooms.insert(room_id, n - 1);
                Ok(false)
            }
        }
    }

    async fn publish_room(&self, envelope: &PubSubEnvelope) -> Result<usize, PubSubError> {
        self.ensure_open()?;
        let room_id = match envelope {
            PubSubEnvelope::RoomBroadcast { room_id, .. } => *room_id,
            PubSubEnvelope::DirectMessage { .. } => {
                return Err(PubSubError::Validation(
                    "publish_room requires RoomBroadcast envelope".to_string(),
                ));
            }
        };

        let senders: Vec<broadcast::Sender<PubSubEnvelope>> = {
            let state = self.hub.inner.read().expect("in-memory hub lock poisoned");
            state
                .nodes
                .values()
                .filter(|entry| entry.active_rooms.contains(&room_id))
                .map(|entry| entry.event_tx.clone())
                .collect()
        };

        let mut delivered_subscribers = 0usize;
        for tx in senders {
            if let Ok(receivers) = tx.send(envelope.clone()) {
                delivered_subscribers += receivers;
            }
        }
        Ok(delivered_subscribers)
    }

    async fn publish_direct(
        &self,
        target_node_id: &str,
        envelope: &PubSubEnvelope,
    ) -> Result<usize, PubSubError> {
        self.ensure_open()?;
        if !matches!(envelope, PubSubEnvelope::DirectMessage { .. }) {
            return Err(PubSubError::Validation(
                "publish_direct requires DirectMessage envelope".to_string(),
            ));
        }
        let target_clean = validate_segment(target_node_id, "target_node_id")?;

        let tx = {
            let state = self.hub.inner.read().expect("in-memory hub lock poisoned");
            state
                .nodes
                .get(&target_clean)
                .map(|entry| entry.event_tx.clone())
        };

        Ok(match tx {
            Some(tx) => tx.send(envelope.clone()).unwrap_or(0),
            None => 0,
        })
    }

    async fn shutdown(&self) -> Result<(), PubSubError> {
        if self.shutting_down.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        let remaining_users: Vec<String> = {
            let mut users = self.user_counts.lock().expect("user_counts mutex poisoned");
            users.drain().map(|(username, _)| username).collect()
        };
        {
            let mut rooms = self.room_counts.lock().expect("room_counts mutex poisoned");
            rooms.clear();
        }

        let mut state = self.hub.inner.write().expect("in-memory hub lock poisoned");
        state.nodes.remove(self.node_id.as_ref());
        for username in remaining_users {
            if let Some(nodes) = state.user_nodes.get_mut(&username) {
                nodes.remove(self.node_id.as_ref());
                if nodes.is_empty() {
                    state.user_nodes.remove(&username);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryPubSubHub;
    use crate::broker::PubSubBackend;
    use crate::message::PubSubEnvelope;

    #[tokio::test]
    async fn room_subscription_ref_counting_and_cross_node_delivery() {
        let hub = InMemoryPubSubHub::default();
        let node1 = hub.node("node-1");
        let node2 = hub.node("node-2");
        let node3 = hub.node("node-3");

        let mut rx1 = node1.subscribe_events();
        let mut rx2 = node2.subscribe_events();
        let mut rx3 = node3.subscribe_events();

        // node-1 joins room 10 twice (two local connections).
        assert!(node1.subscribe_room(10).await.unwrap());
        assert!(!node1.subscribe_room(10).await.unwrap());
        // node-2 joins room 10 once; node-3 does not join room 10.
        assert!(node2.subscribe_room(10).await.unwrap());
        assert!(hub.is_node_subscribed_to_room("node-1", 10));
        assert!(hub.is_node_subscribed_to_room("node-2", 10));
        assert!(!hub.is_node_subscribed_to_room("node-3", 10));

        let env = PubSubEnvelope::RoomBroadcast {
            origin_node_id: "node-1".to_string(),
            room_id: 10,
            sender_username: "alice".to_string(),
            message_id: 1,
            message: "cipher-room-10".to_string(),
        };
        let sent = node1.publish_room(&env).await.unwrap();
        assert_eq!(sent, 2);
        assert_eq!(rx1.try_recv().unwrap(), env);
        assert_eq!(rx2.try_recv().unwrap(), env);
        assert!(rx3.try_recv().is_err());

        // First leave on node-1 keeps subscription alive; second leave removes it.
        assert!(!node1.unsubscribe_room(10).await.unwrap());
        assert!(hub.is_node_subscribed_to_room("node-1", 10));
        assert!(node1.unsubscribe_room(10).await.unwrap());
        assert!(!hub.is_node_subscribed_to_room("node-1", 10));
        assert!(!node1.unsubscribe_room(10).await.unwrap());
    }

    #[tokio::test]
    async fn multi_device_user_nodes_direct_routing_stale_pruning_and_shutdown() {
        let hub = InMemoryPubSubHub::default();
        let node1 = hub.node("node-1");
        let node2 = hub.node("node-2");

        let mut rx2 = node2.subscribe_events();

        // Bob connects twice on node-2 and once on node-1.
        assert!(node2.register_user_session("bob").await.unwrap());
        assert!(!node2.register_user_session("bob").await.unwrap());
        assert!(node1.register_user_session("bob").await.unwrap());
        assert_eq!(
            node1.lookup_user_nodes("bob").await.unwrap(),
            vec!["node-1".to_string(), "node-2".to_string()]
        );

        // Inject a stale dead node ID and verify publish_direct returns 0 and prune removes it.
        hub.insert_stale_user_node_for_test("bob", "node-dead");
        let dm = PubSubEnvelope::DirectMessage {
            origin_node_id: "node-1".to_string(),
            target_node_id: "node-dead".to_string(),
            sender_username: "alice".to_string(),
            recipient_username: "bob".to_string(),
            message_id: Some(7),
            message: "hi bob".to_string(),
        };
        assert_eq!(node1.publish_direct("node-dead", &dm).await.unwrap(), 0);
        node1
            .prune_stale_user_node("bob", "node-dead")
            .await
            .unwrap();

        let live_dm = PubSubEnvelope::DirectMessage {
            origin_node_id: "node-1".to_string(),
            target_node_id: "node-2".to_string(),
            sender_username: "alice".to_string(),
            recipient_username: "bob".to_string(),
            message_id: Some(8),
            message: "hi bob live".to_string(),
        };
        assert_eq!(node1.publish_direct("node-2", &live_dm).await.unwrap(), 1);
        assert_eq!(rx2.try_recv().unwrap(), live_dm);

        // Unregister one Bob connection on node-2: still stays registered on node-2.
        assert!(!node2.unregister_user_session("bob").await.unwrap());
        assert_eq!(
            node1.lookup_user_nodes("bob").await.unwrap(),
            vec!["node-1".to_string(), "node-2".to_string()]
        );

        // Shutting down node-2 automatically drains Bob's remaining session on node-2.
        node2.shutdown().await.unwrap();
        assert_eq!(
            node1.lookup_user_nodes("bob").await.unwrap(),
            vec!["node-1".to_string()]
        );
    }
}
