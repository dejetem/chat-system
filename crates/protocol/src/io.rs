// This module should be the only place that knows how to read and write primitive binary types.

use crate::error::ProtocolError;

impl From<std::string::FromUtf8Error> for ProtocolError {
    fn from(_: std::string::FromUtf8Error) -> Self {
        ProtocolError::InvalidUtf8
    }
}

pub fn write_u8(buffer: &mut Vec<u8>, value: u8) {
    buffer.push(value);
}

pub fn write_u16(buffer: &mut Vec<u8>, value: u16) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

pub fn write_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

pub fn write_u64(buffer: &mut Vec<u8>, value: u64) {
    buffer.extend_from_slice(&value.to_be_bytes());
}

fn take<'a>(cursor: &mut &'a [u8], count: usize) -> Result<&'a [u8], ProtocolError> {
    if cursor.len() < count {
        return Err(ProtocolError::UnexpectedEof);
    }

    let (head, tail) = cursor.split_at(count);

    *cursor = tail;

    Ok(head)
}

pub fn read_u8(cursor: &mut &[u8]) -> Result<u8, ProtocolError> {
    Ok(take(cursor, 1)?[0])
}

pub fn read_u16(cursor: &mut &[u8]) -> Result<u16, ProtocolError> {
    Ok(u16::from_be_bytes(take(cursor, 2)?.try_into().unwrap()))
}

pub fn read_u32(cursor: &mut &[u8]) -> Result<u32, ProtocolError> {
    Ok(u32::from_be_bytes(take(cursor, 4)?.try_into().unwrap()))
}

pub fn read_u64(cursor: &mut &[u8]) -> Result<u64, ProtocolError> {
    Ok(u64::from_be_bytes(take(cursor, 8)?.try_into().unwrap()))
}

// Strings should have an explicit length prefix.
pub fn write_string(buffer: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();

    write_u16(buffer, bytes.len() as u16);

    buffer.extend_from_slice(bytes);
}

pub fn read_string(cursor: &mut &[u8]) -> Result<String, ProtocolError> {
    let len = read_u16(cursor)? as usize;

    let bytes = take(cursor, len)?;

    Ok(String::from_utf8(bytes.to_vec())?)
}
