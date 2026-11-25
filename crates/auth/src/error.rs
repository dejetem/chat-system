use jsonwebtoken::errors::{Error as JwtError, ErrorKind};
use thiserror::Error;

/// Errors returned during JWT issuance, validation, and authentication.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthError {
    /// Username or token was missing or blank.
    #[error("missing username or token")]
    MissingCredentials,

    /// Signing or verification key was invalid or unconfigured.
    #[error("invalid authentication key: {0}")]
    InvalidKey(String),

    /// Signing a JWT failed.
    #[error("token creation failed: {0}")]
    TokenCreation(String),

    /// Token `exp` claim is in the past.
    #[error("token has expired")]
    ExpiredToken,

    /// Token signature, structure, algorithm, or claims failed validation.
    #[error("invalid token: {0}")]
    InvalidToken(String),

    /// Authenticated token `sub` claim did not match the requested username.
    #[error("token subject does not match requested username")]
    UsernameMismatch,
}

impl From<JwtError> for AuthError {
    fn from(err: JwtError) -> Self {
        match err.kind() {
            ErrorKind::ExpiredSignature => AuthError::ExpiredToken,
            ErrorKind::InvalidKeyFormat
            | ErrorKind::InvalidEcdsaKey
            | ErrorKind::InvalidRsaKey(_) => AuthError::InvalidKey(err.to_string()),
            _ => AuthError::InvalidToken(err.to_string()),
        }
    }
}
