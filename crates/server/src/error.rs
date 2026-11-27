use thiserror::Error;

/// Wire error code for malformed or unexpected client requests.
pub const ERR_BAD_REQUEST: u16 = 400;

/// Wire error code when an unauthenticated connection sends a restricted message.
pub const ERR_UNAUTHORIZED: u16 = 401;

/// Wire error code when an authenticated client attempts an action in a room it has not joined.
pub const ERR_FORBIDDEN: u16 = 403;

/// Wire error code when a target resource or recipient user is not found or offline.
pub const ERR_NOT_FOUND: u16 = 404;

/// Wire error code when a frame violates the binary protocol specification.
pub const ERR_PROTOCOL_ERROR: u16 = 422;

/// Wire error code for protocol messages whose backend handlers belong to later phases.
pub const ERR_NOT_IMPLEMENTED: u16 = 501;

/// Errors that can occur while reading, writing, or handling a client connection.
#[derive(Debug, Error)]
pub enum ConnectionError {
    /// Underlying socket or stream I/O failure.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Binary wire protocol framing or decoding failure.
    #[error("protocol error: {0}")]
    Protocol(#[from] protocol::ProtocolError),
}
