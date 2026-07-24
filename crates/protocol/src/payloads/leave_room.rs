use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::error::ProtocolError;
use crate::message_type::MessageType;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveRoomPayload {
    pub room_id: u64,
}

impl BinarySerializable for LeaveRoomPayload {
    fn encode(&self, buffer: &mut Vec<u8>) {
        crate::io::write_u64(buffer, self.room_id);
    }

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        let room_id = crate::io::read_u64(cursor)?;
        Ok(Self { room_id })
    }
}

impl ProtocolMessage for LeaveRoomPayload {
    const TYPE: MessageType = MessageType::LeaveRoom;
}
