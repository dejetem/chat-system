// Encodes a frame into bytes.
// Decodes bytes into a frame.
// The server never manually parses bytes.

use crate::{decoder::Decoder, encoder::Encoder, error::ProtocolError, frame::Frame};
pub struct Codec;
impl Codec {
    pub fn encode(frame: &Frame) -> Result<Vec<u8>, ProtocolError> {
        Encoder::encode(frame)
    }

    pub fn decode(bytes: &[u8]) -> Result<Frame, ProtocolError> {
        Decoder::decode(bytes)
    }
}
