use async_trait::async_trait;

use crate::error::StorageError;
use crate::models::{NewPrivateMessage, NewRoomMessage, StoredMessage, StoredRoom, StoredUser};

/// Durable persistence contract for users, rooms, encrypted message history, and offline queues.
///
/// All message persistence methods store and return opaque ciphertext payloads only.
/// Server-side plaintext or body search methods are intentionally excluded.
#[async_trait]
pub trait ChatStorage: Send + Sync {
    /// Creates or updates a user's metadata record (username + role).
    async fn upsert_user(&self, username: &str, role: &str) -> Result<StoredUser, StorageError>;

    /// Updates a user's public E2EE key bundle.
    async fn set_user_public_key(
        &self,
        username: &str,
        public_key: Vec<u8>,
    ) -> Result<StoredUser, StorageError>;

    /// Fetches a user record by username, if present.
    async fn get_user_by_username(
        &self,
        username: &str,
    ) -> Result<Option<StoredUser>, StorageError>;

    /// Ensures a room row exists for `room_id`, returning its metadata.
    async fn ensure_room(
        &self,
        room_id: u64,
        name: Option<&str>,
        created_by: Option<i64>,
    ) -> Result<StoredRoom, StorageError>;

    /// Persists an encrypted room message blob and returns the assigned metadata and ID.
    async fn store_room_message(&self, msg: NewRoomMessage) -> Result<StoredMessage, StorageError>;

    /// Fetches up to `limit` of the most recent encrypted messages in `room_id` in ascending order.
    async fn fetch_room_history(
        &self,
        room_id: u64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StorageError>;

    /// Persists an encrypted direct message, optionally enqueueing it for offline delivery.
    async fn store_private_message(
        &self,
        msg: NewPrivateMessage,
    ) -> Result<StoredMessage, StorageError>;

    /// Atomically drains and returns all queued offline encrypted messages for `recipient_username` in FIFO order.
    async fn flush_offline_messages(
        &self,
        recipient_username: &str,
    ) -> Result<Vec<StoredMessage>, StorageError>;

    /// Returns the number of queued offline messages currently pending for `recipient_username`.
    async fn offline_message_count(&self, recipient_username: &str) -> Result<usize, StorageError>;
}
