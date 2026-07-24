use protocol::{Codec, Frame};

pub fn assert_round_trip(frame: Frame) {
    let bytes = Codec::encode(&frame).unwrap();

    let decoded = Codec::decode(&bytes).unwrap();

    assert_eq!(frame, decoded);
}
