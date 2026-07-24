pub mod app;
pub mod cluster;
pub mod database;
pub mod redis;
pub mod server;

pub use app::Config;
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
        }
    }
}
