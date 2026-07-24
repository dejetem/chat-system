use crate::{constants::MAGIC, message_type::MessageType, version::Version};

#[derive(Debug, Clone, PartialEq)]
pub struct Header {
    pub magic: u16,
    pub version: Version,
    pub message_type: MessageType,
    pub payload_length: u32,
}

impl Header {
    pub fn new(version: Version, message_type: MessageType, payload_length: u32) -> Self {
        Self {
            magic: MAGIC,
            version,
            message_type,
            payload_length,
        }
    }
}
