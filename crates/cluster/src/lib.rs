// use std::env;

// #[derive(Clone, Debug)]
// pub struct NodeInfo {
//     pub node_id: String,
//     pub region: String,
//     pub port: u16,
//     pub timestamp: u64,
// }

// impl NodeInfo {
//     pub fn new(port: u16, node_id: String, region: String, timestamp: u64) -> Self {
//         Self {
//             node_id,
//             region,
//             port,
//             timestamp,
//         }
//     }
// }

pub mod node;
pub use node::NodeInfo;
