use async_trait::async_trait;
use tokio::sync::broadcast;

use crate::error::PubSubError;
use crate::message::PubSubEnvelope;

/// Distributed cluster Pub/Sub and user-to-node routing contract.
#[async_trait]
pub trait PubSubBackend: Send + Sync {
    /// Returns the local chat node identifier owning this broker handle.
    fn node_id(&self) -> &str;

    /// Returns a broadcast receiver yielding inbound cross-node envelopes delivered to this node.
    fn subscribe_events(&self) -> broadcast::Receiver<PubSubEnvelope>;

    /// Increments the local session ref-count for `username`, registering this node in the cluster map on `0 -> 1`.
    ///
    /// Returns `true` when this call registered `username` on this node for the first time.
    async fn register_user_session(&self, username: &str) -> Result<bool, PubSubError>;

    /// Decrements the local session ref-count for `username`, removing this node from the cluster map on `1 -> 0`.
    ///
    /// Returns `true` when `username` now has zero active sessions on this node.
    async fn unregister_user_session(&self, username: &str) -> Result<bool, PubSubError>;

    /// Lists cluster `node_id`s currently holding active sessions for `username`.
    async fn lookup_user_nodes(&self, username: &str) -> Result<Vec<String>, PubSubError>;

    /// Lazily removes a dead or unreachable `stale_node_id` entry for `username`.
    async fn prune_stale_user_node(
        &self,
        username: &str,
        stale_node_id: &str,
    ) -> Result<(), PubSubError>;

    /// Increments the local subscription ref-count for `room_id`, subscribing to its Redis channel on `0 -> 1`.
    ///
    /// Returns `true` when a new cluster subscription was established.
    async fn subscribe_room(&self, room_id: u64) -> Result<bool, PubSubError>;

    /// Decrements the local subscription ref-count for `room_id`, unsubscribing from its Redis channel on `1 -> 0`.
    ///
    /// Returns `true` when the cluster subscription was removed.
    async fn unsubscribe_room(&self, room_id: u64) -> Result<bool, PubSubError>;

    /// Publishes a [`PubSubEnvelope::RoomBroadcast`] to the target room's cluster channel, returning subscriber count.
    async fn publish_room(&self, envelope: &PubSubEnvelope) -> Result<usize, PubSubError>;

    /// Publishes a [`PubSubEnvelope::DirectMessage`] directly to `target_node_id`'s channel, returning subscriber count.
    async fn publish_direct(
        &self,
        target_node_id: &str,
        envelope: &PubSubEnvelope,
    ) -> Result<usize, PubSubError>;

    /// Unregisters all remaining local user sessions from the cluster map and stops background listener tasks.
    async fn shutdown(&self) -> Result<(), PubSubError>;
}
