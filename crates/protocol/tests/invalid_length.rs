use protocol::{Codec, Frame, Message, payloads::PingPayload};

#[test]
fn invalid_length_is_rejected() {
    let frame = Frame::new(Message::Ping(PingPayload));

    let mut bytes = Codec::encode(&frame).unwrap();

    // Corrupt payload length
    bytes[4] = 0x00;
    bytes[5] = 0x00;
    bytes[6] = 0x00;
    bytes[7] = 0x10;

    assert!(Codec::decode(&bytes).is_err());
}
