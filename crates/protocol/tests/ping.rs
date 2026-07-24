mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::PingPayload};

#[test]
fn ping_round_trip() {
    let frame = Frame::new(Message::Ping(PingPayload));

    assert_round_trip(frame);
}
