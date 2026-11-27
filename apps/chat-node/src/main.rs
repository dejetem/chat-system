use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use auth::JwtAuthenticator;
use cluster::NodeInfo;
use config::Config;
use server::Server;
use storage::{PgStorage, S3Config, S3MediaStorage};

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

    // Connect PostgreSQL and run schema migrations
    let pg_storage = PgStorage::connect_with_config(&config.database)
        .await
        .context("failed to connect PostgreSQL storage")?;
    pg_storage
        .migrate()
        .await
        .context("failed to run PostgreSQL schema migrations")?;
    tracing::info!("PostgreSQL storage connected and migrations verified");

    // Initialize S3-compatible media storage configuration if credentials are provided
    if let Some(s3_config) = S3Config::from_media_config(&config.media)
        .context("failed to validate S3 media storage configuration")?
    {
        let _media_storage = S3MediaStorage::new(s3_config.clone())
            .context("failed to initialize S3 media storage client")?;
        tracing::info!(
            bucket = %s3_config.bucket,
            region = %s3_config.region,
            endpoint = ?s3_config.endpoint,
            "S3-compatible media storage initialized"
        );
    } else {
        tracing::info!("S3 media storage credentials not configured; object client disabled");
    }

    // TODO: Connect Redis

    // TODO: Start Cluster

    let server = Server::new(config.server, node, authenticator)
        .with_storage(Arc::new(pg_storage))
        .with_room_history_limit(config.database.room_history_limit)
        .with_auth_timeout(Duration::from_secs(config.auth.auth_timeout_secs));
    server.run().await?;

    Ok(())
}
