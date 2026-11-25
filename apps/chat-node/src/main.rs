use anyhow::Result;
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

    // TODO: Connect PostgreSQL

    // TODO: Connect Redis

    // TODO: Start Cluster

    let server = Server::new(config.server, node);
    server.run().await?;

    Ok(())
}
