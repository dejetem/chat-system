use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthFailedPayload;

impl BinarySerializable for AuthFailedPayload {
    fn encode(&self, _buffer: &mut Vec<u8>) {}

    fn decode(_cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        Ok(Self)
    }
}

impl ProtocolMessage for AuthFailedPayload {
    const TYPE: MessageType = MessageType::AuthFailed;
}
