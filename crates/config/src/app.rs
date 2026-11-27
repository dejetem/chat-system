use crate::{
    auth::AuthConfig, cluster::ClusterConfig, database::DatabaseConfig, media::MediaConfig,
    redis::RedisConfig, server::ServerConfig,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub cluster: ClusterConfig,
    pub auth: AuthConfig,
    pub media: MediaConfig,
}
