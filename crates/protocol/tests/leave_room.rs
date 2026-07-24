mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::LeaveRoomPayload};

#[test]
fn leave_room_round_trip() {
    let frame = Frame::new(Message::LeaveRoom(LeaveRoomPayload { room_id: 7 }));

    assert_round_trip(frame);
}
