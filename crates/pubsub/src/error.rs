use thiserror::Error;

/// Errors returned by Redis and in-memory cluster Pub/Sub backends.
#[derive(Debug, Error)]
pub enum PubSubError {
    /// Caller supplied an invalid channel identifier, prefix, or envelope payload.
    #[error("validation error: {0}")]
    Validation(String),

    /// Underlying Redis client, connection manager, or Pub/Sub stream failure.
    #[error("redis error: {0}")]
    Redis(#[from] redis::RedisError),

    /// JSON serialization or deserialization failure for cross-node envelopes.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Broker has been shut down and can no longer accept commands.
    #[error("pubsub broker has been shut down")]
    Closed,
}

