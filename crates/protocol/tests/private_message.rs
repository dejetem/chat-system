mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::PrivateMessagePayload};

#[test]
fn private_message_round_trip() {
    let frame = Frame::new(Message::PrivateMessage(PrivateMessagePayload {
        recipient: "alice".into(),
        message: "Secret".into(),
    }));

    assert_round_trip(frame);
}
