use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorPayload {
    pub code: u16,
    pub message: String,
}

impl BinarySerializable for ErrorPayload {
    fn encode(&self, buffer: &mut Vec<u8>) {
        crate::io::write_u16(buffer, self.code);
        crate::io::write_string(buffer, &self.message);
    }

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        let code = crate::io::read_u16(cursor)?;
        let message = crate::io::read_string(cursor)?;
        Ok(Self { code, message })
    }
}

impl ProtocolMessage for ErrorPayload {
    const TYPE: MessageType = MessageType::Error;
}
