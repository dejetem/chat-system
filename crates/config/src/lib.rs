pub mod app;
pub mod auth;
pub mod cluster;
pub mod database;
pub mod redis;
pub mod server;

pub use app::Config;
pub use auth::AuthConfig;
pub use cluster::ClusterConfig;
pub use database::DatabaseConfig;
pub use redis::RedisConfig;
pub use server::ServerConfig;

use std::env;
use uuid::Uuid;

impl Config {
    pub fn from_env() -> Self {
        Self {
            server: ServerConfig {
                host: env::var("HOST").unwrap_or_else(|_| "0.0.0.0".into()),
                port: env::var("PORT")
                    .unwrap_or_else(|_| "8080".into())
                    .parse()
                    .expect("Invalid PORT"),
            },

            database: DatabaseConfig {
                url: env::var("DATABASE_URL").expect("DATABASE_URL is missing"),
            },

            redis: RedisConfig {
                url: env::var("REDIS_URL").expect("REDIS_URL is missing"),
            },

            cluster: ClusterConfig {
                node_id: env::var("NODE_ID").unwrap_or_else(|_| Uuid::new_v4().to_string()),
                region: env::var("REGION").unwrap_or_else(|_| "local".into()),
            },

            auth: AuthConfig {
                jwt_secret: env::var("JWT_SECRET").expect("JWT_SECRET is missing"),
                jwt_issuer: env::var("JWT_ISSUER")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                auth_timeout_secs: env::var("AUTH_TIMEOUT_SECS")
                    .unwrap_or_else(|_| "10".into())
                    .parse()
                    .expect("Invalid AUTH_TIMEOUT_SECS"),
            },
        }
    }
}
