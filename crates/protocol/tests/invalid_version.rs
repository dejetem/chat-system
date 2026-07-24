use protocol::{Codec, Frame, Message, payloads::PingPayload};

#[test]
fn invalid_version_is_rejected() {
    let frame = Frame::new(Message::Ping(PingPayload));

    let mut bytes = Codec::encode(&frame).unwrap();

    bytes[2] = 99;

    assert!(Codec::decode(&bytes).is_err());
}
