use chrono::{DateTime, Utc};

use crate::error::StorageError;

/// Default number of historical room messages returned upon joining a room.
pub const DEFAULT_HISTORY_LIMIT: usize = 50;

/// Maximum number of historical room messages allowed in a single query.
pub const MAX_HISTORY_LIMIT: usize = 500;

/// Maximum length (in Unicode scalar values) for usernames and room names.
pub const MAX_IDENTIFIER_LEN: usize = 128;

/// Maximum byte length for an S3 media object key.
pub const MAX_MEDIA_KEY_LEN: usize = 512;

/// Persisted user metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredUser {
    pub id: i64,
    pub username: String,
    pub role: String,
    pub public_key: Option<Vec<u8>>,
    pub created_at: DateTime<Utc>,
}

/// Persisted chat room metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRoom {
    pub id: u64,
    pub name: String,
    pub created_by: Option<i64>,
    pub created_at: DateTime<Utc>,
}

/// Persisted chat message record.
///
/// Under the locked E2EE architecture, `ciphertext` stores opaque encrypted bytes
/// only; queryable fields are restricted to message metadata and optional media keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessage {
    pub message_id: u64,
    pub sender_id: i64,
    pub sender_username: String,
    pub room_id: Option<u64>,
    pub recipient_id: Option<i64>,
    pub recipient_username: Option<String>,
    pub ciphertext: Vec<u8>,
    pub media_key: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// Input parameters for storing a new room message ciphertext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewRoomMessage {
    pub sender_username: String,
    pub room_id: u64,
    pub ciphertext: Vec<u8>,
    pub media_key: Option<String>,
}

/// Input parameters for storing a new private message ciphertext.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPrivateMessage {
    pub sender_username: String,
    pub recipient_username: String,
    pub ciphertext: Vec<u8>,
    pub media_key: Option<String>,
    pub queue_offline: bool,
}

/// Returns the deterministic fallback name assigned to auto-provisioned rooms.
pub fn default_room_name(room_id: u64) -> String {
    format!("room-{room_id}")
}

fn validate_identifier(raw: &str, field: &str) -> Result<String, StorageError> {
    if raw.chars().any(char::is_control) {
        return Err(StorageError::Validation(format!(
            "{field} cannot contain control characters"
        )));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(StorageError::Validation(format!("{field} cannot be empty")));
    }
    if trimmed.chars().count() > MAX_IDENTIFIER_LEN {
        return Err(StorageError::Validation(format!(
            "{field} exceeds {MAX_IDENTIFIER_LEN} characters"
        )));
    }
    Ok(trimmed.to_string())
}

/// Validates and trims a username against storage constraints.
pub fn validate_username(username: &str) -> Result<String, StorageError> {
    validate_identifier(username, "username")
}

/// Validates an RBAC role string accepted by PostgreSQL constraints.
pub fn validate_role(role: &str) -> Result<String, StorageError> {
    let normalized = role.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "user" | "moderator" | "admin" => Ok(normalized),
        _ => Err(StorageError::Validation(format!(
            "unsupported role '{role}'"
        ))),
    }
}

/// Validates and trims a room name against storage constraints.
pub fn validate_room_name(name: &str) -> Result<String, StorageError> {
    validate_identifier(name, "room name")
}

/// Converts a protocol `u64` room identifier to a non-negative PostgreSQL `BIGINT`.
pub fn validate_room_id(room_id: u64) -> Result<i64, StorageError> {
    i64::try_from(room_id).map_err(|_| {
        StorageError::Validation(format!("room_id {room_id} exceeds signed 64-bit range"))
    })
}

/// Converts a PostgreSQL `BIGINT`/`BIGSERIAL` identifier into a `u64`.
pub fn to_u64_id(id: i64, field: &str) -> Result<u64, StorageError> {
    u64::try_from(id)
        .map_err(|_| StorageError::Validation(format!("invalid negative {field} value: {id}")))
}

/// Ensures persisted ciphertext payloads are non-empty.
pub fn validate_ciphertext(ciphertext: &[u8]) -> Result<(), StorageError> {
    if ciphertext.is_empty() {
        return Err(StorageError::Validation(
            "message ciphertext cannot be empty".to_string(),
        ));
    }
    Ok(())
}

/// Validates an object storage key, rejecting traversal segments, leading slashes, and control chars.
pub fn validate_media_key(key: &str) -> Result<String, StorageError> {
    if key != key.trim() || key.is_empty() {
        return Err(StorageError::Validation(
            "media_key must be non-empty without surrounding whitespace".to_string(),
        ));
    }
    if key.len() > MAX_MEDIA_KEY_LEN {
        return Err(StorageError::Validation(format!(
            "media_key exceeds {MAX_MEDIA_KEY_LEN} bytes"
        )));
    }
    if key.starts_with('/') || key.ends_with('/') || key.contains('\\') {
        return Err(StorageError::Validation(
            "media_key contains invalid path separators".to_string(),
        ));
    }
    if key.chars().any(char::is_control) {
        return Err(StorageError::Validation(
            "media_key cannot contain control characters".to_string(),
        ));
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(StorageError::Validation(
            "media_key contains invalid or traversal path segment".to_string(),
        ));
    }
    Ok(key.to_string())
}

/// Validates an optional object storage key reference.
pub fn validate_optional_media_key(
    media_key: Option<&str>,
) -> Result<Option<String>, StorageError> {
    media_key.map(validate_media_key).transpose()
}

/// Caps room history query limits to [`MAX_HISTORY_LIMIT`].
pub fn clamp_history_limit(limit: usize) -> usize {
    limit.min(MAX_HISTORY_LIMIT)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_HISTORY_LIMIT, MAX_IDENTIFIER_LEN, MAX_MEDIA_KEY_LEN, clamp_history_limit,
        validate_ciphertext, validate_media_key, validate_role, validate_room_id,
        validate_username,
    };

    #[test]
    fn validation_rejects_empty_and_oversized_inputs() {
        assert!(validate_username("").is_err());
        assert!(validate_username("   ").is_err());
        assert!(validate_username("alice\n").is_err());
        assert!(validate_username(&"a".repeat(MAX_IDENTIFIER_LEN + 1)).is_err());
        assert_eq!(validate_username(" alice ").unwrap(), "alice");

        assert!(validate_role("superadmin").is_err());
        assert_eq!(validate_role("Moderator").unwrap(), "moderator");

        assert!(validate_room_id(u64::MAX).is_err());
        assert_eq!(validate_room_id(42).unwrap(), 42);

        assert!(validate_ciphertext(&[]).is_err());
        assert!(validate_ciphertext(b"cipher").is_ok());

        assert_eq!(clamp_history_limit(10_000), MAX_HISTORY_LIMIT);
    }

    #[test]
    fn media_key_validation_blocks_traversal_and_malformed_paths() {
        assert!(validate_media_key("").is_err());
        assert!(validate_media_key(" ../secret.bin").is_err());
        assert!(validate_media_key("../secret.bin").is_err());
        assert!(validate_media_key("attachments/../secret.bin").is_err());
        assert!(validate_media_key("/leading/slash.bin").is_err());
        assert!(validate_media_key("trailing/").is_err());
        assert!(validate_media_key("double//slash.bin").is_err());
        assert!(validate_media_key("win\\path.bin").is_err());
        assert!(validate_media_key("nul\0byte.bin").is_err());
        assert!(validate_media_key(&"k".repeat(MAX_MEDIA_KEY_LEN + 1)).is_err());

        assert_eq!(
            validate_media_key("attachments/room-1/file_01.bin").unwrap(),
            "attachments/room-1/file_01.bin"
        );
    }
}
