use std::fmt;

/// Runtime authentication settings loaded from environment variables.
#[derive(Clone)]
pub struct AuthConfig {
    pub jwt_secret: String,
    pub jwt_issuer: Option<String>,
    pub auth_timeout_secs: u64,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field("jwt_secret", &"[REDACTED]")
            .field("jwt_issuer", &self.jwt_issuer)
            .field("auth_timeout_secs", &self.auth_timeout_secs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::AuthConfig;

    #[test]
    fn debug_redacts_jwt_secret() {
        let auth_cfg = AuthConfig {
            jwt_secret: "actual-secret-value".to_string(),
            jwt_issuer: Some("chat-system".to_string()),
            auth_timeout_secs: 10,
        };

        let formatted = format!("{auth_cfg:?}");
        assert!(!formatted.contains("actual-secret"));
        assert!(formatted.contains("[REDACTED]"));
        assert!(formatted.contains("chat-system"));
    }
}
