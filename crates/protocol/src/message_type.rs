// This identifies packet types.

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Ping = 1,
    Pong = 2,

    Auth = 3,
    AuthOk = 4,
    AuthFailed = 5,

    JoinRoom = 6,
    LeaveRoom = 7,

    RoomMessage = 8,
    PrivateMessage = 9,

    Ack = 10,

    Error = 11,
}

impl TryFrom<u8> for MessageType {
    type Error = crate::error::ProtocolError;

    fn try_from(value: u8) -> Result<Self, crate::error::ProtocolError> {
        match value {
            1 => Ok(MessageType::Ping),
            2 => Ok(MessageType::Pong),
            3 => Ok(MessageType::Auth),
            4 => Ok(MessageType::AuthOk),
            5 => Ok(MessageType::AuthFailed),
            6 => Ok(MessageType::JoinRoom),
            7 => Ok(MessageType::LeaveRoom),
            8 => Ok(MessageType::RoomMessage),
            9 => Ok(MessageType::PrivateMessage),
            10 => Ok(MessageType::Ack),
            11 => Ok(MessageType::Error),
            _ => Err(crate::error::ProtocolError::UnknownMessage),
        }
    }
}
