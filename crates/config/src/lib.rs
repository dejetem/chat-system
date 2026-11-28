pub mod app;
pub mod auth;
pub mod cluster;
pub mod database;
pub mod media;
pub mod redis;
pub mod server;

pub use app::Config;
pub use auth::AuthConfig;
pub use cluster::ClusterConfig;
pub use database::DatabaseConfig;
pub use media::MediaConfig;
pub use redis::RedisConfig;
pub use server::ServerConfig;

use std::env;
use uuid::Uuid;

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn parse_bool_env(name: &str) -> Option<bool> {
    non_empty_env(name).map(|value| match value.to_ascii_lowercase().as_str() {
        "true" | "1" => true,
        "false" | "0" => false,
        _ => panic!("Invalid {name}: expected true/false or 1/0"),
    })
}

impl Config {
    pub fn from_env() -> Self {
        let s3_endpoint = non_empty_env("S3_ENDPOINT");
        let s3_force_path_style =
            parse_bool_env("S3_FORCE_PATH_STYLE").unwrap_or_else(|| s3_endpoint.is_some());

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
                max_connections: env::var("DATABASE_MAX_CONNECTIONS")
                    .unwrap_or_else(|_| "20".into())
                    .parse()
                    .expect("Invalid DATABASE_MAX_CONNECTIONS"),
                acquire_timeout_secs: env::var("DATABASE_ACQUIRE_TIMEOUT_SECS")
                    .unwrap_or_else(|_| "5".into())
                    .parse()
                    .expect("Invalid DATABASE_ACQUIRE_TIMEOUT_SECS"),
                room_history_limit: env::var("ROOM_HISTORY_LIMIT")
                    .unwrap_or_else(|_| "50".into())
                    .parse()
                    .expect("Invalid ROOM_HISTORY_LIMIT"),
            },

            redis: RedisConfig {
                url: env::var("REDIS_URL").expect("REDIS_URL is missing"),
                key_prefix: non_empty_env("REDIS_KEY_PREFIX").unwrap_or_else(|| "chat".into()),
                channel_capacity: env::var("REDIS_CHANNEL_CAPACITY")
                    .unwrap_or_else(|_| "1024".into())
                    .parse()
                    .expect("Invalid REDIS_CHANNEL_CAPACITY"),
            },

            cluster: ClusterConfig {
                node_id: env::var("NODE_ID").unwrap_or_else(|_| Uuid::new_v4().to_string()),
                region: env::var("REGION").unwrap_or_else(|_| "local".into()),
            },

            auth: AuthConfig {
                jwt_secret: env::var("JWT_SECRET").expect("JWT_SECRET is missing"),
                jwt_issuer: non_empty_env("JWT_ISSUER"),
                auth_timeout_secs: env::var("AUTH_TIMEOUT_SECS")
                    .unwrap_or_else(|_| "10".into())
                    .parse()
                    .expect("Invalid AUTH_TIMEOUT_SECS"),
            },

            media: MediaConfig {
                endpoint: s3_endpoint,
                region: non_empty_env("S3_REGION").unwrap_or_else(|| "us-east-1".into()),
                bucket: non_empty_env("S3_BUCKET").unwrap_or_else(|| "chat-media".into()),
                access_key_id: non_empty_env("S3_ACCESS_KEY_ID"),
                secret_access_key: non_empty_env("S3_SECRET_ACCESS_KEY"),
                force_path_style: s3_force_path_style,
                presign_ttl_secs: env::var("S3_PRESIGN_TTL_SECS")
                    .unwrap_or_else(|_| "900".into())
                    .parse()
                    .expect("Invalid S3_PRESIGN_TTL_SECS"),
            },
        }
    }
}
