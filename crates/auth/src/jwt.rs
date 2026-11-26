use std::fmt;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};

use crate::claims::{AuthenticatedUser, Claims, Role};
use crate::error::AuthError;

/// Verifies client credentials during connection authentication.
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticates `username` against `token` and returns the verified user identity.
    fn authenticate(&self, username: &str, token: &str) -> Result<AuthenticatedUser, AuthError>;
}

/// Stateless JWT issuer and validator supporting HS256 shared secrets and RS256 PEM keys.
pub struct JwtAuthenticator {
    encoding_key: Option<EncodingKey>,
    decoding_key: DecodingKey,
    algorithm: Algorithm,
    issuer: Option<String>,
    leeway_secs: u64,
}

impl fmt::Debug for JwtAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JwtAuthenticator")
            .field("algorithm", &self.algorithm)
            .field("has_signing_key", &self.encoding_key.is_some())
            .field("issuer", &self.issuer)
            .field("leeway_secs", &self.leeway_secs)
            .finish()
    }
}

impl JwtAuthenticator {
    /// Creates an `HS256` authenticator that can both issue and verify tokens using `secret`.
    pub fn from_secret(secret: impl AsRef<str>) -> Result<Self, AuthError> {
        let secret = secret.as_ref();
        if secret.trim().is_empty() {
            return Err(AuthError::InvalidKey(
                "JWT secret must not be empty".to_string(),
            ));
        }

        let bytes = secret.as_bytes();
        Ok(Self {
            encoding_key: Some(EncodingKey::from_secret(bytes)),
            decoding_key: DecodingKey::from_secret(bytes),
            algorithm: Algorithm::HS256,
            issuer: None,
            leeway_secs: 0,
        })
    }

    /// Creates an `RS256` verification-only authenticator from an RSA public key PEM.
    pub fn from_rsa_public_pem(public_pem: &[u8]) -> Result<Self, AuthError> {
        let decoding_key = DecodingKey::from_rsa_pem(public_pem)?;
        Ok(Self {
            encoding_key: None,
            decoding_key,
            algorithm: Algorithm::RS256,
            issuer: None,
            leeway_secs: 0,
        })
    }

    /// Creates an `RS256` authenticator that can issue and verify tokens from an RSA PEM key pair.
    pub fn from_rsa_pem_pair(private_pem: &[u8], public_pem: &[u8]) -> Result<Self, AuthError> {
        let encoding_key = EncodingKey::from_rsa_pem(private_pem)?;
        let decoding_key = DecodingKey::from_rsa_pem(public_pem)?;
        Ok(Self {
            encoding_key: Some(encoding_key),
            decoding_key,
            algorithm: Algorithm::RS256,
            issuer: None,
            leeway_secs: 0,
        })
    }

    /// Enforces a required `iss` claim during validation and stamps it on issued tokens.
    pub fn with_issuer(mut self, issuer: impl Into<String>) -> Self {
        let trimmed = issuer.into().trim().to_string();
        self.issuer = (!trimmed.is_empty()).then_some(trimmed);
        self
    }

    /// Sets clock-skew leeway in seconds applied to `exp` validation (default `0`).
    pub fn with_leeway(mut self, leeway_secs: u64) -> Self {
        self.leeway_secs = leeway_secs;
        self
    }

    /// Signs arbitrary [`Claims`] using the configured signing key.
    pub fn encode_claims(&self, claims: &Claims) -> Result<String, AuthError> {
        let key = self
            .encoding_key
            .as_ref()
            .ok_or_else(|| AuthError::InvalidKey("signing key not configured".to_string()))?;

        jsonwebtoken::encode(&Header::new(self.algorithm), claims, key)
            .map_err(|err| AuthError::TokenCreation(err.to_string()))
    }

    /// Issues a signed JWT for `username` with `role` expiring after `ttl`.
    pub fn issue_token(
        &self,
        username: &str,
        role: Role,
        ttl: Duration,
    ) -> Result<String, AuthError> {
        if username.trim().is_empty() {
            return Err(AuthError::MissingCredentials);
        }

        let now = current_unix_timestamp();
        let claims = Claims {
            sub: username.to_string(),
            exp: now.saturating_add(ttl.as_secs()),
            iat: Some(now),
            iss: self.issuer.clone(),
            role,
        };

        self.encode_claims(&claims)
    }

    /// Validates `token` signature, algorithm, expiration, and optional issuer, returning its [`Claims`].
    pub fn verify_token(&self, token: &str) -> Result<Claims, AuthError> {
        if token.trim().is_empty() {
            return Err(AuthError::MissingCredentials);
        }

        // Override jsonwebtoken's default 60s leeway so expiration checks obey `self.leeway_secs`.
        let mut validation = Validation::new(self.algorithm);
        validation.validate_exp = true;
        validation.leeway = self.leeway_secs;

        if let Some(iss) = &self.issuer {
            validation.set_issuer(&[iss]);
            validation.required_spec_claims.insert("iss".to_string());
        }

        let token_data = jsonwebtoken::decode::<Claims>(token, &self.decoding_key, &validation)?;
        if token_data.claims.sub.trim().is_empty() {
            return Err(AuthError::InvalidToken(
                "token subject (sub) is empty".to_string(),
            ));
        }

        Ok(token_data.claims)
    }
}

impl Authenticator for JwtAuthenticator {
    fn authenticate(&self, username: &str, token: &str) -> Result<AuthenticatedUser, AuthError> {
        if username.trim().is_empty() || token.trim().is_empty() {
            return Err(AuthError::MissingCredentials);
        }

        let claims = self.verify_token(token)?;
        if claims.sub != username {
            return Err(AuthError::UsernameMismatch);
        }

        Ok(AuthenticatedUser {
            username: claims.sub,
            role: claims.role,
            expires_at: claims.exp,
        })
    }
}

pub(crate) fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
