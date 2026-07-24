use crate::error::ProtocolError;
use crate::message_type::MessageType;

// This trait will be implemented by every payload.
pub trait BinarySerializable: Sized {
    fn encode(&self, buffer: &mut Vec<u8>);

    fn decode(cursor: &mut &[u8]) -> Result<Self, ProtocolError>;
}

pub trait ProtocolMessage: BinarySerializable {
    const TYPE: MessageType;
}
