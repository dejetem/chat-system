use serde::{Deserialize, Serialize};

use crate::error::PubSubError;

/// Version-flexible JSON envelope exchanged over Redis Pub/Sub channels between chat nodes.
///
/// Under the locked E2EE architecture, `message` carries opaque ciphertext verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PubSubEnvelope {
    /// Cross-node broadcast for a chat room.
    RoomBroadcast {
        origin_node_id: String,
        room_id: u64,
        sender_username: String,
        message_id: u64,
        message: String,
    },
    /// Cross-node direct message routed to all active sessions of `recipient_username` on `target_node_id`.
    DirectMessage {
        origin_node_id: String,
        target_node_id: String,
        sender_username: String,
        recipient_username: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_id: Option<u64>,
        message: String,
    },
}

impl PubSubEnvelope {
    /// Returns the cluster node ID that originally published this envelope.
    pub fn origin_node_id(&self) -> &str {
        match self {
            PubSubEnvelope::RoomBroadcast { origin_node_id, .. }
            | PubSubEnvelope::DirectMessage { origin_node_id, .. } => origin_node_id,
        }
    }

    /// Serializes the envelope into compact JSON for Redis `PUBLISH`.
    pub fn encode_json(&self) -> Result<String, PubSubError> {
        Ok(serde_json::to_string(self)?)
    }

    /// Deserializes a JSON payload received from a Redis Pub/Sub channel.
    pub fn decode_json(raw: &str) -> Result<Self, PubSubError> {
        Ok(serde_json::from_str(raw)?)
    }
}

#[cfg(test)]
mod tests {
    use super::PubSubEnvelope;

    #[test]
    fn envelope_round_trip_preserves_ciphertext_and_metadata() {
        let room_env = PubSubEnvelope::RoomBroadcast {
            origin_node_id: "node-1".to_string(),
            room_id: 77,
            sender_username: "alice".to_string(),
            message_id: 9001,
            message: "AES-GCM:dGVzdC1jaXBoZXJ0ZXh0".to_string(),
        };
        let encoded = room_env.encode_json().unwrap();
        assert_eq!(PubSubEnvelope::decode_json(&encoded).unwrap(), room_env);
        assert_eq!(room_env.origin_node_id(), "node-1");

        let dm_env = PubSubEnvelope::DirectMessage {
            origin_node_id: "node-1".to_string(),
            target_node_id: "node-2".to_string(),
            sender_username: "alice".to_string(),
            recipient_username: "bob".to_string(),
            message_id: Some(42),
            message: "secret-cipher-blob".to_string(),
        };
        let dm_encoded = dm_env.encode_json().unwrap();
        assert_eq!(PubSubEnvelope::decode_json(&dm_encoded).unwrap(), dm_env);
        assert_eq!(dm_env.origin_node_id(), "node-1");

        assert!(PubSubEnvelope::decode_json("{not valid json").is_err());
    }
}

