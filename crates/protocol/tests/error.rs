mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::ErrorPayload};

#[test]
fn error_round_trip() {
    let frame = Frame::new(Message::Error(ErrorPayload {
        code: 500,
        message: "Internal Server Error".into(),
    }));

    assert_round_trip(frame);
}
