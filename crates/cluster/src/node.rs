use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub region: String,
    pub port: u16,
    pub started_at: u64,
}

impl NodeInfo {
    pub fn new(node_id: String, region: String, port: u16) -> Self {
        Self {
            node_id,
            region,
            port,
            started_at: current_unix_timestamp(),
        }
    }
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
