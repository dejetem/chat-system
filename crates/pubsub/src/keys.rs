use crate::error::PubSubError;

/// Canonical Redis key and channel naming helper isolated by a configurable prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySpace {
    prefix: String,
}

impl KeySpace {
    /// Creates a [`KeySpace`] after validating that `prefix` is non-empty without control chars.
    pub fn new(prefix: impl AsRef<str>) -> Result<Self, PubSubError> {
        let prefix = validate_segment(prefix.as_ref(), "key_prefix")?;
        Ok(Self { prefix })
    }

    /// Returns the configured namespace prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Redis Pub/Sub channel carrying broadcasts for `room_id`.
    pub fn room_channel(&self, room_id: u64) -> String {
        format!("{}:room:{room_id}", self.prefix)
    }

    /// Dedicated Redis Pub/Sub channel carrying direct messages targeted at `node_id`.
    pub fn node_channel(&self, node_id: &str) -> Result<String, PubSubError> {
        let node_id = validate_segment(node_id, "node_id")?;
        Ok(format!("{}:node:{node_id}", self.prefix))
    }

    /// Redis Set key tracking cluster `node_id`s currently hosting active sessions for `username`.
    pub fn user_nodes_key(&self, username: &str) -> Result<String, PubSubError> {
        let username = validate_segment(username, "username")?;
        Ok(format!("{}:user_nodes:{username}", self.prefix))
    }
}

pub(crate) fn validate_segment(raw: &str, field: &str) -> Result<String, PubSubError> {
    if raw.chars().any(char::is_control) {
        return Err(PubSubError::Validation(format!(
            "{field} cannot contain control characters"
        )));
    }
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(PubSubError::Validation(format!("{field} cannot be empty")));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::KeySpace;

    #[test]
    fn keyspace_formats_channels_and_rejects_invalid_segments() {
        let ks = KeySpace::new(" chat ").unwrap();
        assert_eq!(ks.prefix(), "chat");
        assert_eq!(ks.room_channel(42), "chat:room:42");
        assert_eq!(
            ks.node_channel("node-east-1").unwrap(),
            "chat:node:node-east-1"
        );
        assert_eq!(ks.user_nodes_key("alice").unwrap(), "chat:user_nodes:alice");

        assert!(KeySpace::new("").is_err());
        assert!(KeySpace::new("chat\n").is_err());
        assert!(ks.node_channel("   ").is_err());
        assert!(ks.user_nodes_key("alice\0").is_err());
    }
}
