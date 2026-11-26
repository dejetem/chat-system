pub mod claims;
pub mod error;
pub mod jwt;

pub use claims::{AuthenticatedUser, Claims, Role};
pub use error::AuthError;
pub use jwt::{Authenticator, JwtAuthenticator};

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde::Serialize;

    use super::*;
    use crate::jwt::current_unix_timestamp;

    fn test_auth() -> JwtAuthenticator {
        JwtAuthenticator::from_secret("super-secret-signing-key").unwrap()
    }

    #[test]
    fn valid_hs256_round_trip_preserves_role() {
        let auth = test_auth();

        for role in [Role::User, Role::Moderator, Role::Admin] {
            let token = auth
                .issue_token("alice", role, Duration::from_secs(300))
                .unwrap();
            let user = auth.authenticate("alice", &token).unwrap();

            assert_eq!(user.username, "alice");
            assert_eq!(user.role, role);
            assert!(user.expires_at > current_unix_timestamp());
        }
    }

    #[test]
    fn external_jwt_without_role_defaults_to_user() {
        #[derive(Serialize)]
        struct MinimalClaims {
            sub: String,
            exp: u64,
        }

        let secret = "super-secret-signing-key";
        let claims = MinimalClaims {
            sub: "alice".to_string(),
            exp: current_unix_timestamp() + 300,
        };
        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();

        let auth = JwtAuthenticator::from_secret(secret).unwrap();
        let user = auth.authenticate("alice", &token).unwrap();
        assert_eq!(user.username, "alice");
        assert_eq!(user.role, Role::User);
    }

    #[test]
    fn expired_token_is_rejected() {
        let auth = test_auth();
        let claims = Claims {
            sub: "alice".to_string(),
            exp: current_unix_timestamp().saturating_sub(1),
            iat: None,
            iss: None,
            role: Role::User,
        };
        let token = auth.encode_claims(&claims).unwrap();

        let err = auth.authenticate("alice", &token).unwrap_err();
        assert_eq!(err, AuthError::ExpiredToken);
    }

    #[test]
    fn wrong_secret_and_tampered_payload_are_rejected() {
        let issuer = JwtAuthenticator::from_secret("issuer-secret").unwrap();
        let verifier = JwtAuthenticator::from_secret("different-secret").unwrap();

        let token = issuer
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();

        assert!(matches!(
            verifier.authenticate("alice", &token),
            Err(AuthError::InvalidToken(_))
        ));

        let mut parts: Vec<String> = token.split('.').map(str::to_string).collect();
        assert_eq!(parts.len(), 3);
        let tampered_char = if parts[1].starts_with('a') { 'b' } else { 'a' };
        parts[1].replace_range(0..1, &tampered_char.to_string());
        let tampered = parts.join(".");

        assert!(matches!(
            issuer.authenticate("alice", &tampered),
            Err(AuthError::InvalidToken(_))
        ));
    }

    #[test]
    fn unsigned_alg_none_token_is_rejected() {
        let auth = test_auth();
        // Header {"alg":"none","typ":"JWT"}, payload {"sub":"alice","exp":9999999999}, empty sig.
        let unsigned =
            "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJhbGljZSIsImV4cCI6OTk5OTk5OTk5OX0.";

        assert!(matches!(
            auth.authenticate("alice", unsigned),
            Err(AuthError::InvalidToken(_))
        ));
    }

    #[test]
    fn subject_mismatch_is_rejected() {
        let auth = test_auth();
        let alice_token = auth
            .issue_token("alice", Role::Admin, Duration::from_secs(300))
            .unwrap();

        let err = auth.authenticate("bob", &alice_token).unwrap_err();
        assert_eq!(err, AuthError::UsernameMismatch);
    }

    #[test]
    fn issuer_validation_accepts_match_and_rejects_mismatch_or_missing() {
        let required_iss = test_auth().with_issuer("chat-system");
        let valid_token = required_iss
            .issue_token("alice", Role::User, Duration::from_secs(300))
            .unwrap();
        assert_eq!(
            required_iss
                .authenticate("alice", &valid_token)
                .unwrap()
                .username,
            "alice"
        );

        let no_iss = test_auth()
            .issue_token("alice", Role::User, Duration::from_secs(300))
            .unwrap();
        assert!(matches!(
            required_iss.authenticate("alice", &no_iss),
            Err(AuthError::InvalidToken(_))
        ));

        let wrong_iss = test_auth()
            .with_issuer("other-service")
            .issue_token("alice", Role::User, Duration::from_secs(300))
            .unwrap();
        assert!(matches!(
            required_iss.authenticate("alice", &wrong_iss),
            Err(AuthError::InvalidToken(_))
        ));
    }

    #[test]
    fn empty_secret_is_rejected() {
        let err = JwtAuthenticator::from_secret("   ").unwrap_err();
        assert!(matches!(err, AuthError::InvalidKey(_)));
    }

    #[test]
    fn blank_username_or_token_returns_missing_credentials() {
        let auth = test_auth();
        let token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();

        assert_eq!(
            auth.authenticate("   ", &token).unwrap_err(),
            AuthError::MissingCredentials
        );
        assert_eq!(
            auth.authenticate("alice", "  ").unwrap_err(),
            AuthError::MissingCredentials
        );
    }
}
