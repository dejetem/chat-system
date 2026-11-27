use std::fmt;

/// S3-compatible object storage configuration for encrypted media attachments.
#[derive(Clone)]
pub struct MediaConfig {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub force_path_style: bool,
    pub presign_ttl_secs: u64,
}

impl MediaConfig {
    /// Returns `true` when S3 access credentials are configured.
    pub fn is_configured(&self) -> bool {
        self.access_key_id.is_some() && self.secret_access_key.is_some()
    }
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            endpoint: None,
            region: "us-east-1".to_string(),
            bucket: "chat-media".to_string(),
            access_key_id: None,
            secret_access_key: None,
            force_path_style: false,
            presign_ttl_secs: 900,
        }
    }
}

impl fmt::Debug for MediaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let redacted_secret = self.secret_access_key.as_ref().map(|_| "[REDACTED]");
        f.debug_struct("MediaConfig")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &redacted_secret)
            .field("force_path_style", &self.force_path_style)
            .field("presign_ttl_secs", &self.presign_ttl_secs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::MediaConfig;

    #[test]
    fn debug_redacts_s3_secret_access_key() {
        let cfg = MediaConfig {
            endpoint: Some("http://minio:9000".to_string()),
            region: "us-east-1".to_string(),
            bucket: "chat-media".to_string(),
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("very-secret-key".to_string()),
            force_path_style: true,
            presign_ttl_secs: 900,
        };

        let formatted = format!("{cfg:?}");
        assert!(!formatted.contains("very-secret-key"));
        assert!(formatted.contains("Some(\"[REDACTED]\")"));
        assert!(formatted.contains("minioadmin"));
        assert!(formatted.contains("chat-media"));
    }

    #[test]
    fn is_configured_checks_credentials() {
        let default_cfg = MediaConfig::default();
        assert!(!default_cfg.is_configured());

        let only_key = MediaConfig {
            access_key_id: Some("key".to_string()),
            ..MediaConfig::default()
        };
        assert!(!only_key.is_configured());

        let both = MediaConfig {
            access_key_id: Some("key".to_string()),
            secret_access_key: Some("secret".to_string()),
            ..MediaConfig::default()
        };
        assert!(both.is_configured());
    }
}
