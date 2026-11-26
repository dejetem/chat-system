use std::fmt;

use serde::{Deserialize, Serialize};

/// Role assigned to an authenticated user for RBAC checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    #[default]
    User,
    Moderator,
    Admin,
}

impl fmt::Display for Role {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Role::User => "user",
            Role::Moderator => "moderator",
            Role::Admin => "admin",
        };
        f.write_str(label)
    }
}

/// JWT claims payload issued and verified for chat clients.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    /// Subject (authenticated username).
    pub sub: String,
    /// Expiration timestamp (Unix epoch seconds).
    pub exp: u64,
    /// Issued-at timestamp (Unix epoch seconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>,
    /// Optional token issuer identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
    /// User role used for authorization.
    #[serde(default)]
    pub role: Role,
}

/// Verified user identity extracted from a validated authentication token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedUser {
    pub username: String,
    pub role: Role,
    pub expires_at: u64,
}
