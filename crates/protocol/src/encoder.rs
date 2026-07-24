use crate::{
    constants::{MAGIC, MAX_FRAME_SIZE},
    error::ProtocolError,
    frame::Frame,
};

pub struct Encoder;

impl Encoder {
    pub fn encode(frame: &Frame) -> Result<Vec<u8>, ProtocolError> {
        // 1. Encode payload
        let mut payload = Vec::new();
        frame.message.encode(&mut payload);

        // 2. Validate payload size
        let payload_len = payload.len();
        if payload_len > MAX_FRAME_SIZE {
            return Err(ProtocolError::FrameTooLarge);
        }

        // 3. Build final buffer
        let mut buffer = Vec::with_capacity(8 + payload_len);

        // 4. Encode header
        crate::io::write_u16(&mut buffer, MAGIC);
        crate::io::write_u8(&mut buffer, frame.header.version.as_u8());
        crate::io::write_u8(&mut buffer, frame.header.message_type as u8);
        crate::io::write_u32(&mut buffer, payload_len as u32);

        // 5. Append payload
        buffer.extend_from_slice(&payload);

        // 6. Return final buffer
        Ok(buffer)
    }
}
