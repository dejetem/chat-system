use cluster::NodeInfo;

pub struct Server {
    node: NodeInfo,
    database_url: String,
    redis_url: String,
}

impl Server {
    pub fn new(node: NodeInfo, database_url: String, redis_url: String) -> Self {
        Self {
            node,
            database_url,
            redis_url,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        // Start TCP listener
        // Accept connections
        // Spawn connection tasks
        println!(
            "Server running for node: {} in region: {} on port: {}",
            self.node.node_id, self.node.region, self.node.port
        );
        println!("Connecting database at: {}", self.database_url);
        println!("Connecting redis at: {}", self.redis_url);

        Ok(())
    }
}
