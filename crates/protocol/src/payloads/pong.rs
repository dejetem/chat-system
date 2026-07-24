use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PongPayload;

impl BinarySerializable for PongPayload {
    fn encode(&self, _buffer: &mut Vec<u8>) {}

    fn decode(_cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

impl ProtocolMessage for PongPayload {
    const TYPE: MessageType = MessageType::Pong;
}
