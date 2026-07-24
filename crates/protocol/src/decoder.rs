// Validate: Magic, Version, Length, Message type
// Reject malformed frames.

use crate::{
    constants::{MAGIC, MAX_FRAME_SIZE},
    error::ProtocolError,
    frame::Frame,
    header::Header,
    io::{read_u8, read_u16, read_u32},
    message::Message,
    message_type::MessageType,
    version::Version,
};

impl Header {
    pub fn from_wire(
        magic: u16,
        version: Version,
        message_type: MessageType,
        payload_length: u32,
    ) -> Self {
        Self {
            magic,
            version,
            message_type,
            payload_length,
        }
    }
}

pub struct Decoder;

impl Decoder {
    pub fn decode(bytes: &[u8]) -> Result<Frame, ProtocolError> {
        let mut cursor = bytes;

        // Read header
        let magic = read_u16(&mut cursor)?;

        if magic != MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }

        let version = Version::try_from(read_u8(&mut cursor)?)?;

        let message_type = MessageType::try_from(read_u8(&mut cursor)?)?;

        let payload_length = read_u32(&mut cursor)?;

        if payload_length as usize > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge);
        }

        if cursor.len() < payload_length as usize {
            return Err(ProtocolError::UnexpectedEof);
        }

        if cursor.len() > payload_length as usize {
            return Err(ProtocolError::TrailingBytes);
        }

        let mut payload = cursor;

        let message = Message::decode(message_type, &mut payload)?;

        let header = Header::from_wire(magic, version, message_type, payload_length);

        Ok(Frame { header, message })
    }
}
