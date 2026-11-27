use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::Utc;

use crate::error::StorageError;
use crate::models::{
    NewPrivateMessage, NewRoomMessage, StoredMessage, StoredRoom, StoredUser, clamp_history_limit,
    default_room_name, validate_ciphertext, validate_optional_media_key, validate_role,
    validate_room_id, validate_room_name, validate_username,
};
use crate::repository::ChatStorage;

#[derive(Debug)]
struct MemoryState {
    next_user_id: i64,
    next_message_id: u64,
    users: HashMap<String, StoredUser>,
    rooms: HashMap<u64, StoredRoom>,
    messages: Vec<StoredMessage>,
    room_message_indices: HashMap<u64, Vec<usize>>,
    offline_queues: HashMap<String, Vec<usize>>,
}

impl Default for MemoryState {
    fn default() -> Self {
        Self {
            next_user_id: 1,
            next_message_id: 1,
            users: HashMap::new(),
            rooms: HashMap::new(),
            messages: Vec::new(),
            room_message_indices: HashMap::new(),
            offline_queues: HashMap::new(),
        }
    }
}

impl MemoryState {
    fn ensure_user(&mut self, username: &str, role: &str, overwrite_role: bool) -> StoredUser {
        if let Some(existing) = self.users.get_mut(username) {
            if overwrite_role {
                existing.role = role.to_string();
            }
            return existing.clone();
        }

        let id = self.next_user_id;
        self.next_user_id += 1;
        let user = StoredUser {
            id,
            username: username.to_string(),
            role: role.to_string(),
            public_key: None,
            created_at: Utc::now(),
        };
        self.users.insert(username.to_string(), user.clone());
        user
    }

    fn ensure_room(
        &mut self,
        room_id: u64,
        resolved_name: &str,
        explicit_name: bool,
        created_by: Option<i64>,
    ) -> StoredRoom {
        if let Some(existing) = self.rooms.get_mut(&room_id) {
            if explicit_name && existing.name != resolved_name {
                existing.name = resolved_name.to_string();
            }
            return existing.clone();
        }

        let room = StoredRoom {
            id: room_id,
            name: resolved_name.to_string(),
            created_by,
            created_at: Utc::now(),
        };
        self.rooms.insert(room_id, room.clone());
        room
    }
}

/// Thread-safe in-memory [`ChatStorage`] backend for unit and integration tests without PostgreSQL.
#[derive(Debug, Clone, Default)]
pub struct InMemoryStorage {
    inner: Arc<RwLock<MemoryState>>,
}

impl InMemoryStorage {
    /// Creates an empty [`InMemoryStorage`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of all persisted messages in insertion order (test inspection helper).
    pub fn all_messages(&self) -> Vec<StoredMessage> {
        let state = self.inner.read().expect("in-memory storage lock poisoned");
        state.messages.clone()
    }
}

#[async_trait]
impl ChatStorage for InMemoryStorage {
    async fn upsert_user(&self, username: &str, role: &str) -> Result<StoredUser, StorageError> {
        let username = validate_username(username)?;
        let role = validate_role(role)?;
        let mut state = self.inner.write().expect("in-memory storage lock poisoned");
        Ok(state.ensure_user(&username, &role, true))
    }

    async fn set_user_public_key(
        &self,
        username: &str,
        public_key: Vec<u8>,
    ) -> Result<StoredUser, StorageError> {
        let username = validate_username(username)?;
        if public_key.is_empty() {
            return Err(StorageError::Validation(
                "public_key cannot be empty".to_string(),
            ));
        }

        let mut state = self.inner.write().expect("in-memory storage lock poisoned");
        let user = state
            .users
            .get_mut(&username)
            .ok_or_else(|| StorageError::NotFound(format!("user '{username}' not found")))?;
        user.public_key = Some(public_key);
        Ok(user.clone())
    }

    async fn get_user_by_username(
        &self,
        username: &str,
    ) -> Result<Option<StoredUser>, StorageError> {
        let username = validate_username(username)?;
        let state = self.inner.read().expect("in-memory storage lock poisoned");
        Ok(state.users.get(&username).cloned())
    }

    async fn ensure_room(
        &self,
        room_id: u64,
        name: Option<&str>,
        created_by: Option<i64>,
    ) -> Result<StoredRoom, StorageError> {
        validate_room_id(room_id)?;
        let explicit_name = name.is_some();
        let resolved_name = match name {
            Some(raw) => validate_room_name(raw)?,
            None => default_room_name(room_id),
        };

        let mut state = self.inner.write().expect("in-memory storage lock poisoned");
        Ok(state.ensure_room(room_id, &resolved_name, explicit_name, created_by))
    }

    async fn store_room_message(&self, msg: NewRoomMessage) -> Result<StoredMessage, StorageError> {
        let sender_username = validate_username(&msg.sender_username)?;
        validate_room_id(msg.room_id)?;
        validate_ciphertext(&msg.ciphertext)?;
        let media_key = validate_optional_media_key(msg.media_key.as_deref())?;
        let fallback_room_name = default_room_name(msg.room_id);

        let mut state = self.inner.write().expect("in-memory storage lock poisoned");
        let sender = state.ensure_user(&sender_username, "user", false);
        state.ensure_room(msg.room_id, &fallback_room_name, false, Some(sender.id));

        let message_id = state.next_message_id;
        state.next_message_id += 1;

        let stored = StoredMessage {
            message_id,
            sender_id: sender.id,
            sender_username,
            room_id: Some(msg.room_id),
            recipient_id: None,
            recipient_username: None,
            ciphertext: msg.ciphertext,
            media_key,
            created_at: Utc::now(),
        };

        let idx = state.messages.len();
        state.messages.push(stored.clone());
        state
            .room_message_indices
            .entry(msg.room_id)
            .or_default()
            .push(idx);

        Ok(stored)
    }

    async fn fetch_room_history(
        &self,
        room_id: u64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        validate_room_id(room_id)?;
        let capped = clamp_history_limit(limit);
        if capped == 0 {
            return Ok(Vec::new());
        }

        let state = self.inner.read().expect("in-memory storage lock poisoned");
        let Some(indices) = state.room_message_indices.get(&room_id) else {
            return Ok(Vec::new());
        };

        let start = indices.len().saturating_sub(capped);
        Ok(indices[start..]
            .iter()
            .map(|&idx| state.messages[idx].clone())
            .collect())
    }

    async fn store_private_message(
        &self,
        msg: NewPrivateMessage,
    ) -> Result<StoredMessage, StorageError> {
        let sender_username = validate_username(&msg.sender_username)?;
        let recipient_username = validate_username(&msg.recipient_username)?;
        validate_ciphertext(&msg.ciphertext)?;
        let media_key = validate_optional_media_key(msg.media_key.as_deref())?;

        let mut state = self.inner.write().expect("in-memory storage lock poisoned");
        let sender = state.ensure_user(&sender_username, "user", false);
        let recipient = state.ensure_user(&recipient_username, "user", false);

        let message_id = state.next_message_id;
        state.next_message_id += 1;

        let stored = StoredMessage {
            message_id,
            sender_id: sender.id,
            sender_username,
            room_id: None,
            recipient_id: Some(recipient.id),
            recipient_username: Some(recipient_username.clone()),
            ciphertext: msg.ciphertext,
            media_key,
            created_at: Utc::now(),
        };

        let idx = state.messages.len();
        state.messages.push(stored.clone());
        if msg.queue_offline {
            state
                .offline_queues
                .entry(recipient_username)
                .or_default()
                .push(idx);
        }

        Ok(stored)
    }

    async fn flush_offline_messages(
        &self,
        recipient_username: &str,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        let recipient_username = validate_username(recipient_username)?;
        let mut state = self.inner.write().expect("in-memory storage lock poisoned");
        let Some(indices) = state.offline_queues.remove(&recipient_username) else {
            return Ok(Vec::new());
        };
        Ok(indices
            .into_iter()
            .map(|idx| state.messages[idx].clone())
            .collect())
    }

    async fn offline_message_count(&self, recipient_username: &str) -> Result<usize, StorageError> {
        let recipient_username = validate_username(recipient_username)?;
        let state = self.inner.read().expect("in-memory storage lock poisoned");
        Ok(state
            .offline_queues
            .get(&recipient_username)
            .map_or(0, Vec::len))
    }
}

#[cfg(test)]
mod tests {
    use super::InMemoryStorage;
    use crate::models::{MAX_HISTORY_LIMIT, NewPrivateMessage, NewRoomMessage};
    use crate::repository::ChatStorage;

    #[tokio::test]
    async fn user_upsert_and_public_key_lifecycle() {
        let store = InMemoryStorage::new();

        let created = store.upsert_user("alice", "user").await.unwrap();
        assert_eq!(created.username, "alice");
        assert_eq!(created.role, "user");
        assert_eq!(created.public_key, None);

        let updated_role = store.upsert_user("alice", "moderator").await.unwrap();
        assert_eq!(updated_role.id, created.id);
        assert_eq!(updated_role.role, "moderator");

        let with_key = store
            .set_user_public_key("alice", vec![1, 2, 3, 4])
            .await
            .unwrap();
        assert_eq!(with_key.public_key.as_deref(), Some(&[1, 2, 3, 4][..]));

        assert!(store.set_user_public_key("missing", vec![9]).await.is_err());
        assert!(
            store
                .set_user_public_key("alice", Vec::new())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn room_history_stores_opaque_ciphertext_and_returns_latest_chronologically() {
        let store = InMemoryStorage::new();

        for i in 1..=5 {
            store
                .store_room_message(NewRoomMessage {
                    sender_username: "alice".to_string(),
                    room_id: 42,
                    ciphertext: format!("cipher-{i}").into_bytes(),
                    media_key: if i == 5 {
                        Some("attachments/photo.bin".to_string())
                    } else {
                        None
                    },
                })
                .await
                .unwrap();
        }

        let latest_two = store.fetch_room_history(42, 2).await.unwrap();
        assert_eq!(latest_two.len(), 2);
        assert_eq!(latest_two[0].ciphertext, b"cipher-4");
        assert_eq!(latest_two[1].ciphertext, b"cipher-5");
        assert_eq!(
            latest_two[1].media_key.as_deref(),
            Some("attachments/photo.bin")
        );

        assert!(store.fetch_room_history(42, 0).await.unwrap().is_empty());
        assert!(store.fetch_room_history(999, 10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn room_history_respects_max_limit() {
        let store = InMemoryStorage::new();
        for i in 0..(MAX_HISTORY_LIMIT + 25) {
            store
                .store_room_message(NewRoomMessage {
                    sender_username: "alice".to_string(),
                    room_id: 7,
                    ciphertext: format!("msg-{i}").into_bytes(),
                    media_key: None,
                })
                .await
                .unwrap();
        }

        let fetched = store.fetch_room_history(7, 10_000).await.unwrap();
        assert_eq!(fetched.len(), MAX_HISTORY_LIMIT);
        assert_eq!(fetched[0].ciphertext, b"msg-25");
        assert_eq!(
            fetched[MAX_HISTORY_LIMIT - 1].ciphertext,
            format!("msg-{}", MAX_HISTORY_LIMIT + 24).as_bytes()
        );
    }

    #[tokio::test]
    async fn private_message_offline_buffer_flushes_atomically_once_in_order() {
        let store = InMemoryStorage::new();

        // Direct online delivery (not queued offline).
        let online_dm = store
            .store_private_message(NewPrivateMessage {
                sender_username: "alice".to_string(),
                recipient_username: "bob".to_string(),
                ciphertext: b"delivered-live".to_vec(),
                media_key: None,
                queue_offline: false,
            })
            .await
            .unwrap();
        assert_eq!(online_dm.recipient_username.as_deref(), Some("bob"));
        assert_eq!(store.offline_message_count("bob").await.unwrap(), 0);

        // Two messages queued while bob is offline.
        store
            .store_private_message(NewPrivateMessage {
                sender_username: "alice".to_string(),
                recipient_username: "bob".to_string(),
                ciphertext: b"offline-1".to_vec(),
                media_key: None,
                queue_offline: true,
            })
            .await
            .unwrap();
        store
            .store_private_message(NewPrivateMessage {
                sender_username: "carol".to_string(),
                recipient_username: "bob".to_string(),
                ciphertext: b"offline-2".to_vec(),
                media_key: Some("attachments/voice.bin".to_string()),
                queue_offline: true,
            })
            .await
            .unwrap();

        assert_eq!(store.offline_message_count("bob").await.unwrap(), 2);

        let flushed = store.flush_offline_messages("bob").await.unwrap();
        assert_eq!(flushed.len(), 2);
        assert_eq!(flushed[0].ciphertext, b"offline-1");
        assert_eq!(flushed[0].sender_username, "alice");
        assert_eq!(flushed[1].ciphertext, b"offline-2");
        assert_eq!(flushed[1].sender_username, "carol");
        assert_eq!(
            flushed[1].media_key.as_deref(),
            Some("attachments/voice.bin")
        );

        assert_eq!(store.offline_message_count("bob").await.unwrap(), 0);
        assert!(
            store
                .flush_offline_messages("bob")
                .await
                .unwrap()
                .is_empty()
        );
    }
}
