use crate::{
    cluster::ClusterConfig, database::DatabaseConfig, redis::RedisConfig, server::ServerConfig,
};

#[derive(Debug, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub redis: RedisConfig,
    pub cluster: ClusterConfig,
}
