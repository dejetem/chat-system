use std::fmt;

/// PostgreSQL connection pool and room history configuration.
#[derive(Clone)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub acquire_timeout_secs: u64,
    pub room_history_limit: usize,
}

impl DatabaseConfig {
    /// Creates a [`DatabaseConfig`] with default pool and history settings for `url`.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            max_connections: 20,
            acquire_timeout_secs: 5,
            room_history_limit: 50,
        }
    }
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("url", &"[REDACTED]")
            .field("max_connections", &self.max_connections)
            .field("acquire_timeout_secs", &self.acquire_timeout_secs)
            .field("room_history_limit", &self.room_history_limit)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::DatabaseConfig;

    #[test]
    fn debug_redacts_database_url() {
        let cfg = DatabaseConfig {
            url: "postgres://postgres:super-secret-pw@db.internal:5432/chat".to_string(),
            max_connections: 20,
            acquire_timeout_secs: 5,
            room_history_limit: 50,
        };

        let formatted = format!("{cfg:?}");
        assert!(!formatted.contains("super-secret-pw"));
        assert!(!formatted.contains("postgres://"));
        assert!(formatted.contains("[REDACTED]"));
        assert!(formatted.contains("max_connections: 20"));
        assert!(formatted.contains("acquire_timeout_secs: 5"));
        assert!(formatted.contains("room_history_limit: 50"));
    }
}
