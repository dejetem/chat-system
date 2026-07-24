mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::PongPayload};

#[test]
fn pong_round_trip() {
    let frame = Frame::new(Message::Pong(PongPayload));

    assert_round_trip(frame);
}
