use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use config::MediaConfig;
use hmac::{Hmac, Mac};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;
use uuid::Uuid;

use crate::error::StorageError;
use crate::models::{validate_ciphertext, validate_media_key};

type HmacSha256 = Hmac<Sha256>;

const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
const EMPTY_PAYLOAD_SHA256: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const MAX_PRESIGN_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Metadata for an encrypted media object stored in S3-compatible object storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaObjectMeta {
    pub key: String,
    pub url: String,
    pub content_length: u64,
    pub content_type: String,
    pub etag: Option<String>,
    pub sha256_hex: Option<String>,
}

/// Encrypted media payload paired with its object storage metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaObject {
    pub meta: MediaObjectMeta,
    pub ciphertext: Vec<u8>,
}

/// Reference embedded in message payloads linking to encrypted S3 media objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaRef {
    pub media_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ciphertext_sha256: Option<String>,
}

impl MediaRef {
    /// Generates a unique, safe object key under `prefix` (defaulting to `"attachments"`).
    pub fn generate_media_key(prefix: Option<&str>) -> String {
        let base = prefix
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .and_then(|p| validate_media_key(p).ok())
            .unwrap_or_else(|| "attachments".to_string());
        format!("{base}/{}.bin", Uuid::new_v4())
    }

    /// Best-effort extraction of a valid media key reference from a message payload without interpreting ciphertext.
    ///
    /// Recognizes either `"s3://<bucket>/<key>"` URIs or JSON objects containing a valid `"media_key"` field.
    pub fn extract_from_payload(payload: &str) -> Option<String> {
        let trimmed = payload.trim();
        if let Some(rest) = trimmed.strip_prefix("s3://") {
            let (_bucket, raw_key) = rest.split_once('/')?;
            return validate_media_key(raw_key).ok();
        }
        if trimmed.starts_with('{')
            && let Ok(serde_json::Value::Object(map)) =
                serde_json::from_str::<serde_json::Value>(trimmed)
            && let Some(serde_json::Value::String(raw_key)) = map.get("media_key")
        {
            return validate_media_key(raw_key).ok();
        }
        None
    }
}

/// Contract for encrypted media blob storage in S3-compatible object stores.
#[async_trait]
pub trait MediaStorage: Send + Sync {
    /// Uploads encrypted media bytes under `key`.
    async fn put_encrypted_object(
        &self,
        key: &str,
        ciphertext: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<MediaObjectMeta, StorageError>;

    /// Downloads encrypted media bytes stored at `key`.
    async fn get_encrypted_object(&self, key: &str) -> Result<MediaObject, StorageError>;

    /// Queries object metadata via `HEAD`, returning `None` if `key` does not exist.
    async fn head_object(&self, key: &str) -> Result<Option<MediaObjectMeta>, StorageError>;

    /// Deletes the object stored at `key`, returning `true` if deleted or `false` if absent.
    async fn delete_object(&self, key: &str) -> Result<bool, StorageError>;

    /// Computes the canonical URL for addressing `key`.
    fn object_url(&self, key: &str) -> Result<String, StorageError>;
}

/// Validated configuration for S3-compatible object storage signed with AWS SigV4.
#[derive(Clone)]
pub struct S3Config {
    pub endpoint: Option<String>,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub force_path_style: bool,
    pub default_presign_ttl_secs: u64,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[REDACTED]")
            .field("force_path_style", &self.force_path_style)
            .field("default_presign_ttl_secs", &self.default_presign_ttl_secs)
            .finish()
    }
}

impl S3Config {
    /// Validates S3 settings from [`MediaConfig`]. Returns `Ok(None)` if S3 credentials are absent.
    pub fn from_media_config(cfg: &MediaConfig) -> Result<Option<Self>, StorageError> {
        let (access_key_id, secret_access_key) = match (
            cfg.access_key_id
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
            cfg.secret_access_key
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty()),
        ) {
            (None, None) => return Ok(None),
            (Some(_), None) | (None, Some(_)) => {
                return Err(StorageError::MediaConfig(
                    "both S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY must be provided together"
                        .to_string(),
                ));
            }
            (Some(ak), Some(sk)) => (ak.to_string(), sk.to_string()),
        };

        let bucket = validate_bucket(&cfg.bucket)?;
        let region = validate_region(&cfg.region)?;
        let endpoint = cfg
            .endpoint
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(validate_endpoint)
            .transpose()?;

        validate_presign_ttl(cfg.presign_ttl_secs)?;

        Ok(Some(Self {
            endpoint,
            region,
            bucket,
            access_key_id,
            secret_access_key,
            force_path_style: cfg.force_path_style,
            default_presign_ttl_secs: cfg.presign_ttl_secs,
        }))
    }
}

fn validate_bucket(bucket: &str) -> Result<String, StorageError> {
    let trimmed = bucket.trim();
    if trimmed.len() < 3
        || trimmed.len() > 63
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
    {
        return Err(StorageError::MediaConfig(format!(
            "invalid S3 bucket name '{bucket}'"
        )));
    }
    Ok(trimmed.to_string())
}

fn validate_region(region: &str) -> Result<String, StorageError> {
    let trimmed = region.trim();
    if trimmed.is_empty()
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
    {
        return Err(StorageError::MediaConfig(format!(
            "invalid S3 region '{region}'"
        )));
    }
    Ok(trimmed.to_string())
}

fn validate_endpoint(endpoint: &str) -> Result<String, StorageError> {
    let parsed = Url::parse(endpoint)
        .map_err(|err| StorageError::MediaConfig(format!("invalid S3 endpoint URL: {err}")))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(StorageError::MediaConfig(format!(
            "S3 endpoint '{endpoint}' must use http or https with a valid host"
        )));
    }
    Ok(endpoint.trim_end_matches('/').to_string())
}

fn validate_presign_ttl(ttl_secs: u64) -> Result<(), StorageError> {
    if ttl_secs == 0 || ttl_secs > MAX_PRESIGN_TTL_SECS {
        return Err(StorageError::Validation(format!(
            "presign TTL must be between 1 and {MAX_PRESIGN_TTL_SECS} seconds"
        )));
    }
    Ok(())
}

/// Async S3-compatible object storage client using `reqwest` (`rustls`) and AWS SigV4 signing.
#[derive(Debug, Clone)]
pub struct S3MediaStorage {
    config: S3Config,
    client: reqwest::Client,
}

impl S3MediaStorage {
    /// Constructs a new [`S3MediaStorage`] client from validated configuration.
    pub fn new(config: S3Config) -> Result<Self, StorageError> {
        let _ = validate_bucket(&config.bucket)?;
        let _ = validate_region(&config.region)?;
        if let Some(ep) = &config.endpoint {
            let _ = validate_endpoint(ep)?;
        }
        validate_presign_ttl(config.default_presign_ttl_secs)?;

        let client = reqwest::Client::builder().build()?;
        Ok(Self { config, client })
    }

    /// Returns a reference to the validated S3 configuration.
    pub fn config(&self) -> &S3Config {
        &self.config
    }

    /// Generates a presigned `PUT` URL so clients can upload ciphertext directly to S3.
    pub fn presign_put_url(&self, key: &str, expires_secs: u64) -> Result<String, StorageError> {
        self.presign_url(Method::PUT, key, expires_secs, Utc::now())
    }

    /// Generates a presigned `GET` URL so clients can download ciphertext directly from S3.
    pub fn presign_get_url(&self, key: &str, expires_secs: u64) -> Result<String, StorageError> {
        self.presign_url(Method::GET, key, expires_secs, Utc::now())
    }

    fn presign_url(
        &self,
        method: Method,
        key: &str,
        expires_secs: u64,
        now: DateTime<Utc>,
    ) -> Result<String, StorageError> {
        validate_presign_ttl(expires_secs)?;
        let key = validate_media_key(key)?;
        let target = resolve_object_target(&self.config, &key)?;
        Ok(build_presigned_url(
            &self.config,
            method,
            &target,
            expires_secs,
            now,
        ))
    }

    async fn send_signed(
        &self,
        method: Method,
        key: &str,
        body: Option<Vec<u8>>,
        content_type: Option<&str>,
    ) -> Result<(reqwest::Response, ObjectTarget, String), StorageError> {
        let key = validate_media_key(key)?;
        let target = resolve_object_target(&self.config, &key)?;
        let payload_bytes = body.as_deref().unwrap_or(&[]);
        let payload_hash = sha256_hex(payload_bytes);
        let sig_headers = build_sigv4_headers(
            &self.config,
            method.as_str(),
            &target,
            &payload_hash,
            Utc::now(),
        );

        let mut req = self
            .client
            .request(method, &target.full_url)
            .header("host", &target.host_header)
            .header("x-amz-date", &sig_headers.amz_date)
            .header("x-amz-content-sha256", &sig_headers.payload_sha256)
            .header("authorization", &sig_headers.authorization);

        if let Some(ct) = content_type {
            req = req.header("content-type", ct);
        }
        if let Some(bytes) = body {
            req = req.body(bytes);
        }

        let resp = req.send().await?;
        Ok((resp, target, payload_hash))
    }
}

#[async_trait]
impl MediaStorage for S3MediaStorage {
    async fn put_encrypted_object(
        &self,
        key: &str,
        ciphertext: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<MediaObjectMeta, StorageError> {
        validate_ciphertext(&ciphertext)?;
        let clean_key = validate_media_key(key)?;
        let resolved_ct = content_type
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_CONTENT_TYPE)
            .to_string();
        let content_length = u64::try_from(ciphertext.len())
            .map_err(|_| StorageError::Validation("media payload too large".to_string()))?;

        let (resp, target, payload_hash) = self
            .send_signed(
                Method::PUT,
                &clean_key,
                Some(ciphertext),
                Some(&resolved_ct),
            )
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let msg = resp.text().await.unwrap_or_default();
            return Err(StorageError::MediaStatus {
                status: status.as_u16(),
                message: msg,
            });
        }

        let etag = extract_etag(resp.headers());
        Ok(MediaObjectMeta {
            key: clean_key,
            url: target.full_url,
            content_length,
            content_type: resolved_ct,
            etag,
            sha256_hex: Some(payload_hash),
        })
    }

    async fn get_encrypted_object(&self, key: &str) -> Result<MediaObject, StorageError> {
        let clean_key = validate_media_key(key)?;
        let (resp, target, _) = self
            .send_signed(Method::GET, &clean_key, None, None)
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(StorageError::NotFound(format!(
                "media object '{clean_key}' not found"
            )));
        }
        if !status.is_success() {
            let msg = resp.text().await.unwrap_or_default();
            return Err(StorageError::MediaStatus {
                status: status.as_u16(),
                message: msg,
            });
        }

        let content_type = extract_header_string(resp.headers(), "content-type")
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string());
        let etag = extract_etag(resp.headers());
        let bytes = resp.bytes().await?.to_vec();
        let content_length = u64::try_from(bytes.len())
            .map_err(|_| StorageError::Validation("media payload too large".to_string()))?;
        let digest = sha256_hex(&bytes);

        Ok(MediaObject {
            meta: MediaObjectMeta {
                key: clean_key,
                url: target.full_url,
                content_length,
                content_type,
                etag,
                sha256_hex: Some(digest),
            },
            ciphertext: bytes,
        })
    }

    async fn head_object(&self, key: &str) -> Result<Option<MediaObjectMeta>, StorageError> {
        let clean_key = validate_media_key(key)?;
        let (resp, target, _) = self
            .send_signed(Method::HEAD, &clean_key, None, None)
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            let msg = resp.text().await.unwrap_or_default();
            return Err(StorageError::MediaStatus {
                status: status.as_u16(),
                message: msg,
            });
        }

        let content_length = extract_header_string(resp.headers(), "content-length")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let content_type = extract_header_string(resp.headers(), "content-type")
            .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_string());
        let etag = extract_etag(resp.headers());

        Ok(Some(MediaObjectMeta {
            key: clean_key,
            url: target.full_url,
            content_length,
            content_type,
            etag,
            sha256_hex: None,
        }))
    }

    async fn delete_object(&self, key: &str) -> Result<bool, StorageError> {
        let clean_key = validate_media_key(key)?;
        let (resp, _, _) = self
            .send_signed(Method::DELETE, &clean_key, None, None)
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if !status.is_success() {
            let msg = resp.text().await.unwrap_or_default();
            return Err(StorageError::MediaStatus {
                status: status.as_u16(),
                message: msg,
            });
        }
        Ok(true)
    }

    fn object_url(&self, key: &str) -> Result<String, StorageError> {
        let clean_key = validate_media_key(key)?;
        Ok(resolve_object_target(&self.config, &clean_key)?.full_url)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectTarget {
    host_header: String,
    canonical_uri: String,
    full_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SigV4Headers {
    amz_date: String,
    payload_sha256: String,
    authorization: String,
}

fn resolve_object_target(cfg: &S3Config, key: &str) -> Result<ObjectTarget, StorageError> {
    let encoded_key = encode_uri_path(key);
    let encoded_bucket = uri_encode(&cfg.bucket, true);

    if let Some(ep) = &cfg.endpoint {
        let parsed = Url::parse(ep)
            .map_err(|err| StorageError::MediaConfig(format!("invalid S3 endpoint URL: {err}")))?;
        let scheme = parsed.scheme();
        let host = parsed
            .host_str()
            .ok_or_else(|| StorageError::MediaConfig("S3 endpoint missing host".to_string()))?;
        let base_host = format_host_header(host, parsed.port());
        let base_prefix = parsed.path().trim_matches('/');

        if cfg.force_path_style {
            let canonical_uri = if base_prefix.is_empty() {
                format!("/{encoded_bucket}/{encoded_key}")
            } else {
                format!(
                    "/{}/{encoded_bucket}/{encoded_key}",
                    encode_uri_path(base_prefix)
                )
            };
            let full_url = format!("{scheme}://{base_host}{canonical_uri}");
            Ok(ObjectTarget {
                host_header: base_host,
                canonical_uri,
                full_url,
            })
        } else {
            let virtual_host = match parsed.port() {
                Some(port) => format!("{}.{host}:{port}", cfg.bucket),
                None => format!("{}.{host}", cfg.bucket),
            };
            let canonical_uri = if base_prefix.is_empty() {
                format!("/{encoded_key}")
            } else {
                format!("/{}/{encoded_key}", encode_uri_path(base_prefix))
            };
            let full_url = format!("{scheme}://{virtual_host}{canonical_uri}");
            Ok(ObjectTarget {
                host_header: virtual_host,
                canonical_uri,
                full_url,
            })
        }
    } else if cfg.force_path_style {
        let host_header = format!("s3.{}.amazonaws.com", cfg.region);
        let canonical_uri = format!("/{encoded_bucket}/{encoded_key}");
        let full_url = format!("https://{host_header}{canonical_uri}");
        Ok(ObjectTarget {
            host_header,
            canonical_uri,
            full_url,
        })
    } else {
        let host_header = format!("{}.s3.{}.amazonaws.com", cfg.bucket, cfg.region);
        let canonical_uri = format!("/{encoded_key}");
        let full_url = format!("https://{host_header}{canonical_uri}");
        Ok(ObjectTarget {
            host_header,
            canonical_uri,
            full_url,
        })
    }
}

fn format_host_header(host: &str, explicit_port: Option<u16>) -> String {
    match explicit_port {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

fn build_sigv4_headers(
    cfg: &S3Config,
    method: &str,
    target: &ObjectTarget,
    payload_sha256: &str,
    now: DateTime<Utc>,
) -> SigV4Headers {
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let credential_scope = format!("{date_stamp}/{}/s3/aws4_request", cfg.region);

    let canonical_headers = format!(
        "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
        target.host_header, payload_sha256, amz_date
    );
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";

    let canonical_request = format!(
        "{method}\n{}\n\n{canonical_headers}\n{signed_headers}\n{payload_sha256}",
        target.canonical_uri
    );
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key(&cfg.secret_access_key, &date_stamp, &cfg.region);
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}",
        cfg.access_key_id
    );

    SigV4Headers {
        amz_date,
        payload_sha256: payload_sha256.to_string(),
        authorization,
    }
}

fn build_presigned_url(
    cfg: &S3Config,
    method: Method,
    target: &ObjectTarget,
    expires_secs: u64,
    now: DateTime<Utc>,
) -> String {
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let date_stamp = now.format("%Y%m%d").to_string();
    let credential_scope = format!("{date_stamp}/{}/s3/aws4_request", cfg.region);
    let credential = format!("{}/{credential_scope}", cfg.access_key_id);

    // Parameters already ordered lexicographically by name per SigV4 spec.
    let canonical_query = format!(
        "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={}&X-Amz-Date={}&X-Amz-Expires={}&X-Amz-SignedHeaders=host",
        uri_encode(&credential, true),
        uri_encode(&amz_date, true),
        expires_secs
    );

    let canonical_headers = format!("host:{}\n", target.host_header);
    let canonical_request = format!(
        "{}\n{}\n{canonical_query}\n{canonical_headers}\nhost\nUNSIGNED-PAYLOAD",
        method.as_str(),
        target.canonical_uri
    );

    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );
    let signing_key = derive_signing_key(&cfg.secret_access_key, &date_stamp, &cfg.region);
    let signature = hex::encode(hmac_sha256(&signing_key, string_to_sign.as_bytes()));

    format!(
        "{}?{canonical_query}&X-Amz-Signature={signature}",
        target.full_url
    )
}

fn derive_signing_key(secret: &str, date_stamp: &str, region: &str) -> [u8; 32] {
    let k_secret = format!("AWS4{secret}");
    let k_date = hmac_sha256(k_secret.as_bytes(), date_stamp.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, b"s3");
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC takes keys of any size");
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    bytes.into()
}

fn sha256_hex(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return EMPTY_PAYLOAD_SHA256.to_string();
    }
    hex::encode(Sha256::digest(bytes))
}

fn encode_uri_path(path: &str) -> String {
    path.split('/')
        .map(|segment| uri_encode(segment, true))
        .collect::<Vec<_>>()
        .join("/")
}

fn uri_encode(input: &str, encode_slash: bool) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(input.len());
    for byte in input.bytes() {
        let is_unreserved = matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~'
        );
        if is_unreserved || (!encode_slash && byte == b'/') {
            out.push(byte as char);
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

fn extract_header_string(headers: &reqwest::header::HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn extract_etag(headers: &reqwest::header::HeaderMap) -> Option<String> {
    extract_header_string(headers, "etag").map(|raw| raw.trim_matches('"').to_string())
}

/// Thread-safe in-memory [`MediaStorage`] backend for unit and server tests without S3/MinIO.
#[derive(Debug, Clone)]
pub struct InMemoryMediaStorage {
    bucket: String,
    objects: Arc<RwLock<HashMap<String, MediaObject>>>,
}

impl Default for InMemoryMediaStorage {
    fn default() -> Self {
        Self::new("chat-media")
    }
}

impl InMemoryMediaStorage {
    /// Creates an in-memory object store tagged with `bucket`.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            objects: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl MediaStorage for InMemoryMediaStorage {
    async fn put_encrypted_object(
        &self,
        key: &str,
        ciphertext: Vec<u8>,
        content_type: Option<&str>,
    ) -> Result<MediaObjectMeta, StorageError> {
        validate_ciphertext(&ciphertext)?;
        let clean_key = validate_media_key(key)?;
        let ct = content_type
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_CONTENT_TYPE)
            .to_string();
        let digest = sha256_hex(&ciphertext);
        let url = self.object_url(&clean_key)?;
        let meta = MediaObjectMeta {
            key: clean_key.clone(),
            url,
            content_length: ciphertext.len() as u64,
            content_type: ct,
            etag: Some(digest[..16].to_string()),
            sha256_hex: Some(digest),
        };
        let obj = MediaObject {
            meta: meta.clone(),
            ciphertext,
        };
        self.objects
            .write()
            .expect("in-memory media store lock poisoned")
            .insert(clean_key, obj);
        Ok(meta)
    }

    async fn get_encrypted_object(&self, key: &str) -> Result<MediaObject, StorageError> {
        let clean_key = validate_media_key(key)?;
        self.objects
            .read()
            .expect("in-memory media store lock poisoned")
            .get(&clean_key)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(format!("media object '{clean_key}' not found")))
    }

    async fn head_object(&self, key: &str) -> Result<Option<MediaObjectMeta>, StorageError> {
        let clean_key = validate_media_key(key)?;
        Ok(self
            .objects
            .read()
            .expect("in-memory media store lock poisoned")
            .get(&clean_key)
            .map(|obj| obj.meta.clone()))
    }

    async fn delete_object(&self, key: &str) -> Result<bool, StorageError> {
        let clean_key = validate_media_key(key)?;
        Ok(self
            .objects
            .write()
            .expect("in-memory media store lock poisoned")
            .remove(&clean_key)
            .is_some())
    }

    fn object_url(&self, key: &str) -> Result<String, StorageError> {
        let clean_key = validate_media_key(key)?;
        Ok(format!("s3://{}/{}", self.bucket, clean_key))
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use config::MediaConfig;
    use reqwest::Method;

    use super::{
        InMemoryMediaStorage, MediaRef, MediaStorage, S3Config, S3MediaStorage,
        build_presigned_url, build_sigv4_headers, resolve_object_target, sha256_hex,
    };

    fn sample_s3_config(path_style: bool, endpoint: Option<&str>) -> S3Config {
        S3Config {
            endpoint: endpoint.map(str::to_string),
            region: "us-east-1".to_string(),
            bucket: "chat-media".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            force_path_style: path_style,
            default_presign_ttl_secs: 900,
        }
    }

    #[test]
    fn media_ref_extracts_valid_references_and_ignores_opaque_ciphertext() {
        assert_eq!(MediaRef::extract_from_payload("aGVsbG8td29ybGQ="), None);
        assert_eq!(
            MediaRef::extract_from_payload("plain encrypted text without metadata"),
            None
        );
        assert_eq!(
            MediaRef::extract_from_payload(r#"{"ciphertext":"abc","nonce":"123"}"#),
            None
        );
        assert_eq!(
            MediaRef::extract_from_payload("s3://chat-media/attachments/photo.bin").as_deref(),
            Some("attachments/photo.bin")
        );
        assert_eq!(
            MediaRef::extract_from_payload(
                r#"{"media_key":"attachments/room-1/clip.enc","ciphertext":"abc"}"#
            )
            .as_deref(),
            Some("attachments/room-1/clip.enc")
        );
        // Path traversal inside reference is rejected.
        assert_eq!(
            MediaRef::extract_from_payload("s3://chat-media/../etc/passwd"),
            None
        );
        assert_eq!(
            MediaRef::extract_from_payload(r#"{"media_key":"../secret.bin"}"#),
            None
        );

        let key = MediaRef::generate_media_key(Some("room-42"));
        assert!(key.starts_with("room-42/"));
        assert!(key.ends_with(".bin"));
    }

    #[test]
    fn s3_config_validation_redacts_secret_and_handles_missing_credentials() {
        let unconfigured = MediaConfig::default();
        assert!(
            S3Config::from_media_config(&unconfigured)
                .unwrap()
                .is_none()
        );

        let half = MediaConfig {
            access_key_id: Some("minioadmin".to_string()),
            ..MediaConfig::default()
        };
        assert!(S3Config::from_media_config(&half).is_err());

        let valid = MediaConfig {
            endpoint: Some("http://minio:9000/".to_string()),
            region: "us-east-1".to_string(),
            bucket: "chat-media".to_string(),
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("supersecret".to_string()),
            force_path_style: true,
            presign_ttl_secs: 900,
        };
        let parsed = S3Config::from_media_config(&valid).unwrap().unwrap();
        assert_eq!(parsed.endpoint.as_deref(), Some("http://minio:9000"));
        let debug = format!("{parsed:?}");
        assert!(!debug.contains("supersecret"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn s3_target_resolution_and_sigv4_headers_are_deterministic() {
        let minio_cfg = sample_s3_config(true, Some("http://minio:9000"));
        let target = resolve_object_target(&minio_cfg, "attachments/hello world.bin").unwrap();
        assert_eq!(target.host_header, "minio:9000");
        assert_eq!(
            target.canonical_uri,
            "/chat-media/attachments/hello%20world.bin"
        );
        assert_eq!(
            target.full_url,
            "http://minio:9000/chat-media/attachments/hello%20world.bin"
        );

        let aws_cfg = sample_s3_config(false, None);
        let aws_target = resolve_object_target(&aws_cfg, "attachments/pic.bin").unwrap();
        assert_eq!(
            aws_target.host_header,
            "chat-media.s3.us-east-1.amazonaws.com"
        );
        assert_eq!(
            aws_target.full_url,
            "https://chat-media.s3.us-east-1.amazonaws.com/attachments/pic.bin"
        );

        let fixed_time = chrono::Utc
            .with_ymd_and_hms(2026, 11, 27, 12, 0, 0)
            .unwrap();
        let headers = build_sigv4_headers(
            &minio_cfg,
            "PUT",
            &target,
            &sha256_hex(b"ciphertext"),
            fixed_time,
        );
        assert_eq!(headers.amz_date, "20261127T120000Z");
        assert!(headers.authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20261127/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="
        ));

        let presigned = build_presigned_url(&minio_cfg, Method::GET, &target, 300, fixed_time);
        assert!(presigned.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(presigned.contains("X-Amz-Expires=300"));
        assert!(presigned.contains("X-Amz-SignedHeaders=host"));
        assert!(presigned.contains("X-Amz-Signature="));

        let client = S3MediaStorage::new(minio_cfg).unwrap();
        assert!(client.presign_put_url("ok/file.bin", 0).is_err());
        assert!(client.presign_get_url("ok/file.bin", 604_801).is_err());
        assert!(client.presign_get_url("ok/file.bin", 60).is_ok());
    }

    #[tokio::test]
    async fn in_memory_media_storage_crud_round_trip() {
        let media = InMemoryMediaStorage::new("chat-media");
        assert!(
            media
                .head_object("attachments/missing.bin")
                .await
                .unwrap()
                .is_none()
        );

        assert!(
            media
                .put_encrypted_object("attachments/empty.bin", Vec::new(), None)
                .await
                .is_err()
        );

        let meta = media
            .put_encrypted_object(
                "attachments/cipher.bin",
                b"opaque-encrypted-image-bytes".to_vec(),
                Some("image/png"),
            )
            .await
            .unwrap();
        assert_eq!(meta.content_type, "image/png");
        assert_eq!(meta.content_length, 28);
        assert_eq!(meta.url, "s3://chat-media/attachments/cipher.bin");

        assert!(
            media
                .head_object("attachments/cipher.bin")
                .await
                .unwrap()
                .is_some()
        );
        let fetched = media
            .get_encrypted_object("attachments/cipher.bin")
            .await
            .unwrap();
        assert_eq!(fetched.ciphertext, b"opaque-encrypted-image-bytes");

        assert!(media.delete_object("attachments/cipher.bin").await.unwrap());
        assert!(!media.delete_object("attachments/cipher.bin").await.unwrap());
        assert!(
            media
                .get_encrypted_object("attachments/cipher.bin")
                .await
                .is_err()
        );
    }
}
