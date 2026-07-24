mod common;

use common::assert_round_trip;

use protocol::{Frame, Message, payloads::AuthPayload};

#[test]
fn auth_round_trip() {
    let frame = Frame::new(Message::Auth(AuthPayload {
        username: "simon".into(),
        token: "jwt-token".into(),
    }));

    assert_round_trip(frame);
}
