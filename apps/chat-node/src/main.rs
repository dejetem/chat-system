// use std::{
//     env,
//     time::{SystemTime, UNIX_EPOCH},
// };

// use cluster::NodeInfo;
// use server::Server;

// fn current_unix_timestamp() -> u64 {
//     SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .unwrap()
//         .as_secs()
// }

// #[tokio::main]
// async fn main() -> anyhow::Result<()> {
//     // -------------------------------------------------------
//     // Initialize logging
//     // -------------------------------------------------------
//     tracing_subscriber::fmt()
//         .with_target(false)
//         .compact()
//         .init();

//     tracing::info!("Starting Chat Node...");

//     // -------------------------------------------------------
//     // Load configuration
//     // -------------------------------------------------------
//     let port = env::var("PORT")
//         .unwrap_or_else(|_| "8080".into())
//         .parse::<u16>()?;

//     let redis_url = env::var("REDIS_URL").expect("REDIS_URL environment variable is missing");

//     let database_url =
//         env::var("DATABASE_URL").expect("DATABASE_URL environment variable is missing");

//     let region = env::var("REGION").unwrap_or_else(|_| "local".to_string());

//     let node = NodeInfo {
//         node_id: uuid::Uuid::new_v4().to_string(),
//         region,
//         port,
//         timestamp: current_unix_timestamp(),
//     };

//     tracing::info!("Node ID      : {}", node.node_id);
//     tracing::info!("Region       : {}", node.region);
//     tracing::info!("Listening on : {}", node.port);
//     tracing::info!("Timestamp    : {}", node.timestamp);

//     // -------------------------------------------------------
//     // Initialize infrastructure
//     // -------------------------------------------------------

//     // Connect PostgreSQL
//     // let database = storage::Database::connect(&database_url).await?;

//     // Connect Redis
//     // let pubsub = pubsub::RedisPubSub::connect(&redis_url).await?;

//     // Start cluster heartbeat
//     // cluster::start(node.clone(), pubsub.clone()).await?;

//     // -------------------------------------------------------
//     // Start Chat Server
//     // -------------------------------------------------------

//     let server = Server::new(node, database_url, redis_url);

//     server.run().await?;

//     Ok(())
// }

use anyhow::Result;
use cluster::NodeInfo;
use config::Config;

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

    // TODO: Start Chat Server

    Ok(())
}
