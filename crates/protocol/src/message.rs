// This becomes the central protocol abstraction.
use crate::binary::{BinarySerializable, ProtocolMessage};
use crate::payloads::*;
use crate::{MessageType, ProtocolError};

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    Ping(PingPayload),

    Pong(PongPayload),

    Auth(AuthPayload),

    AuthOk(AuthOkPayload),

    AuthFailed(AuthFailedPayload),

    JoinRoom(JoinRoomPayload),

    LeaveRoom(LeaveRoomPayload),

    RoomMessage(RoomMessagePayload),

    PrivateMessage(PrivateMessagePayload),

    Ack(AckPayload),

    Error(ErrorPayload),
}

impl Message {
    pub fn message_type(&self) -> MessageType {
        match self {
            Message::Ping(_) => PingPayload::TYPE,

            Message::Pong(_) => PongPayload::TYPE,

            Message::Auth(_) => AuthPayload::TYPE,

            Message::AuthOk(_) => AuthOkPayload::TYPE,

            Message::AuthFailed(_) => AuthFailedPayload::TYPE,

            Message::JoinRoom(_) => JoinRoomPayload::TYPE,

            Message::LeaveRoom(_) => LeaveRoomPayload::TYPE,

            Message::RoomMessage(_) => RoomMessagePayload::TYPE,

            Message::PrivateMessage(_) => PrivateMessagePayload::TYPE,

            Message::Ack(_) => AckPayload::TYPE,

            Message::Error(_) => ErrorPayload::TYPE,
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) {
        match self {
            Message::Ping(payload) => payload.encode(buffer),

            Message::Pong(payload) => payload.encode(buffer),

            Message::Auth(payload) => payload.encode(buffer),

            Message::AuthOk(payload) => payload.encode(buffer),

            Message::AuthFailed(payload) => payload.encode(buffer),

            Message::JoinRoom(payload) => payload.encode(buffer),

            Message::LeaveRoom(payload) => payload.encode(buffer),

            Message::RoomMessage(payload) => payload.encode(buffer),

            Message::PrivateMessage(payload) => payload.encode(buffer),

            Message::Ack(payload) => payload.encode(buffer),

            Message::Error(payload) => payload.encode(buffer),
        }
    }

    pub fn decode(message_type: MessageType, cursor: &mut &[u8]) -> Result<Self, ProtocolError> {
        match message_type {
            MessageType::Ping => Ok(Message::Ping(PingPayload::decode(cursor)?)),

            MessageType::Pong => Ok(Message::Pong(PongPayload::decode(cursor)?)),

            MessageType::Auth => Ok(Message::Auth(AuthPayload::decode(cursor)?)),

            MessageType::AuthOk => Ok(Message::AuthOk(AuthOkPayload::decode(cursor)?)),

            MessageType::AuthFailed => Ok(Message::AuthFailed(AuthFailedPayload::decode(cursor)?)),

            MessageType::JoinRoom => Ok(Message::JoinRoom(JoinRoomPayload::decode(cursor)?)),

            MessageType::LeaveRoom => Ok(Message::LeaveRoom(LeaveRoomPayload::decode(cursor)?)),

            MessageType::RoomMessage => {
                Ok(Message::RoomMessage(RoomMessagePayload::decode(cursor)?))
            }

            MessageType::PrivateMessage => Ok(Message::PrivateMessage(
                PrivateMessagePayload::decode(cursor)?,
            )),
            MessageType::Ack => Ok(Message::Ack(AckPayload::decode(cursor)?)),

            MessageType::Error => Ok(Message::Error(ErrorPayload::decode(cursor)?)),
        }
    }
}
