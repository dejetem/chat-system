use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckPayload {
    pub message_id: u64,
}

impl BinarySerializable for AckPayload {
    fn encode(&self, buffer: &mut Vec<u8>) {
        crate::io::write_u64(buffer, self.message_id);
    }

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        let message_id = crate::io::read_u64(cursor)?;
        Ok(Self { message_id })
    }
}

impl ProtocolMessage for AckPayload {
    const TYPE: MessageType = MessageType::Ack;
}
