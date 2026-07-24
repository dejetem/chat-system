use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid magic")]
    InvalidMagic,

    #[error("invalid version")]
    InvalidVersion,

    #[error("invalid frame length")]
    InvalidLength,

    #[error("frame exceeds maximum size")]
    FrameTooLarge,

    #[error("unknown message type")]
    UnknownMessage,

    #[error("invalid payload")]
    InvalidPayload,

    #[error("unexpected end of file")]
    UnexpectedEof,

    #[error("trailing bytes")]
    TrailingBytes,

    #[error("invalid UTF-8 string")]
    InvalidUtf8,
}
