use protocol::{Codec, Frame, Message, payloads::PingPayload};

#[test]
fn oversized_frame_is_rejected() {
    let frame = Frame::new(Message::Ping(PingPayload));

    let mut bytes = Codec::encode(&frame).unwrap();

    bytes[4] = 0xFF;
    bytes[5] = 0xFF;
    bytes[6] = 0xFF;
    bytes[7] = 0xFF;

    assert!(Codec::decode(&bytes).is_err());
}
