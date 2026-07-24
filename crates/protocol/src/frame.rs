// The frame does not store raw bytes.
// The encoder/decoder handle bytes.
// The frame is the in-memory representation.

use crate::{Header, Message, Version};

#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    pub header: Header,
    pub message: Message,
}

impl Frame {
    pub fn new(message: Message) -> Self {
        let mut payload = Vec::new();
        message.encode(&mut payload);
        let payload_length = payload.len() as u32;

        Self {
            header: Header::new(Version::V1, message.message_type(), payload_length),
            message,
        }
    }
}
