mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::RoomMessagePayload};

#[test]
fn room_message_round_trip() {
    let frame = Frame::new(Message::RoomMessage(RoomMessagePayload {
        room_id: 10,
        message: "Hello Rust!".into(),
    }));

    assert_round_trip(frame);
}
