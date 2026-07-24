use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateMessagePayload {
    pub recipient: String,
    pub message: String,
}

impl BinarySerializable for PrivateMessagePayload {
    fn encode(&self, buffer: &mut Vec<u8>) {
        crate::io::write_string(buffer, &self.recipient);
        crate::io::write_string(buffer, &self.message);
    }

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        let recipient = crate::io::read_string(cursor)?;
        let message = crate::io::read_string(cursor)?;
        Ok(Self { recipient, message })
    }
}

impl ProtocolMessage for PrivateMessagePayload {
    const TYPE: MessageType = MessageType::PrivateMessage;
}
