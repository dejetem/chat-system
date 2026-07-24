use protocol::io::read_string;

#[test]
fn invalid_utf8_is_rejected() {
    let bytes = [
        0x00, 0x02, // string length = 2
        0xFF, 0xFF, // invalid UTF-8
    ];

    let mut cursor = bytes.as_slice();

    assert!(read_string(&mut cursor).is_err());
}
