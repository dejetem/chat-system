use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PingPayload;

impl BinarySerializable for PingPayload {
    fn encode(&self, _buffer: &mut Vec<u8>) {}

    fn decode(_cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

impl ProtocolMessage for PingPayload {
    const TYPE: MessageType = MessageType::Ping;
}
