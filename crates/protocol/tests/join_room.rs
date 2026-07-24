mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::JoinRoomPayload};

#[test]
fn join_room_round_trip() {
    let frame = Frame::new(Message::JoinRoom(JoinRoomPayload { room_id: 42 }));

    assert_round_trip(frame);
}
