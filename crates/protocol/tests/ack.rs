mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::AckPayload};

#[test]
fn ack_round_trip() {
    let frame = Frame::new(Message::Ack(AckPayload { message_id: 15 }));

    assert_round_trip(frame);
}
