use protocol::{Codec, Frame, Message, payloads::JoinRoomPayload};

#[test]
fn malformed_payload_is_rejected() {
    let frame = Frame::new(Message::JoinRoom(JoinRoomPayload { room_id: 42 }));

    let mut bytes = Codec::encode(&frame).unwrap();

    // Remove one payload byte
    bytes.pop();

    assert!(Codec::decode(&bytes).is_err());
}
