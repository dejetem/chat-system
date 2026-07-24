use protocol::{Codec, Frame, Message, payloads::PingPayload};

#[test]
fn invalid_magic_is_rejected() {
    let frame = Frame::new(Message::Ping(PingPayload));

    let mut bytes = Codec::encode(&frame).unwrap();

    bytes[0] = 0xAA;
    bytes[1] = 0xBB;

    assert!(Codec::decode(&bytes).is_err());
}
