use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthPayload {
    pub username: String,
    pub token: String,
}

impl BinarySerializable for AuthPayload {
    fn encode(&self, buffer: &mut Vec<u8>) {
        crate::io::write_string(buffer, &self.username);
        crate::io::write_string(buffer, &self.token);
    }

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        let username = crate::io::read_string(cursor)?;
        let token = crate::io::read_string(cursor)?;
        Ok(Self { username, token })
    }
}

impl ProtocolMessage for AuthPayload {
    const TYPE: MessageType = MessageType::Auth;
}
