use std::fmt;

/// Redis client and Pub/Sub channel configuration.
#[derive(Clone)]
pub struct RedisConfig {
    pub url: String,
    pub key_prefix: String,
    pub channel_capacity: usize,
}

impl RedisConfig {
    /// Creates a [`RedisConfig`] with default key prefix and broadcast buffer size for `url`.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            key_prefix: "chat".to_string(),
            channel_capacity: 1024,
        }
    }
}

impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("url", &"[REDACTED]")
            .field("key_prefix", &self.key_prefix)
            .field("channel_capacity", &self.channel_capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::RedisConfig;

    #[test]
    fn debug_redacts_redis_url() {
        let cfg = RedisConfig {
            url: "redis://default:super-secret@redis.internal:6379/0".to_string(),
            key_prefix: "chat".to_string(),
            channel_capacity: 1024,
        };

        let formatted = format!("{cfg:?}");
        assert!(!formatted.contains("super-secret"));
        assert!(!formatted.contains("redis://"));
        assert!(formatted.contains("[REDACTED]"));
        assert!(formatted.contains("key_prefix: \"chat\""));
        assert!(formatted.contains("channel_capacity: 1024"));
    }
}
