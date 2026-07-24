use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoomMessagePayload {
    pub room_id: u64,
    pub message: String,
}

impl BinarySerializable for RoomMessagePayload {
    fn encode(&self, buffer: &mut Vec<u8>) {
        crate::io::write_u64(buffer, self.room_id);
        crate::io::write_string(buffer, &self.message);
    }

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        let room_id = crate::io::read_u64(cursor)?;
        let message = crate::io::read_string(cursor)?;
        Ok(Self { room_id, message })
    }
}

impl ProtocolMessage for RoomMessagePayload {
    const TYPE: MessageType = MessageType::RoomMessage;
}
