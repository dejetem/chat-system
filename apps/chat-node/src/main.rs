use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use auth::JwtAuthenticator;
use cluster::NodeInfo;
use config::Config;
use server::Server;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    // Load configuration
    let config = Config::from_env();

    // Create this chat node
    let node = NodeInfo::new(
        config.cluster.node_id.clone(),
        config.cluster.region.clone(),
        config.server.port,
    );

    tracing::info!("Node: {:?}", node);

    // Initialize JWT authenticator
    let mut jwt_auth = JwtAuthenticator::from_secret(&config.auth.jwt_secret)?;
    if let Some(issuer) = &config.auth.jwt_issuer {
        jwt_auth = jwt_auth.with_issuer(issuer.clone());
    }
    let authenticator = Arc::new(jwt_auth);

    // TODO: Connect PostgreSQL

    // TODO: Connect Redis

    // TODO: Start Cluster

    let server = Server::new(config.server, node, authenticator)
        .with_auth_timeout(Duration::from_secs(config.auth.auth_timeout_secs));
    server.run().await?;

    Ok(())
}
