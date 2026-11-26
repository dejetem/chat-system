use std::io;

use protocol::{
    Codec, Frame, HEADER_SIZE, MAGIC, MAX_FRAME_SIZE, MessageType, ProtocolError, Version,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

use crate::error::ConnectionError;

const READER_BUFFER_CAPACITY: usize = 8 * 1024;
const SHRINK_THRESHOLD_CAPACITY: usize = 64 * 1024;

/// Reads length-prefixed binary protocol [`Frame`] values from an async byte stream.
pub struct FrameReader<R> {
    reader: BufReader<R>,
    frame_buf: Vec<u8>,
}

impl<R> FrameReader<R>
where
    R: AsyncRead + Unpin,
{
    /// Creates a new buffered [`FrameReader`] over `reader`.
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::with_capacity(READER_BUFFER_CAPACITY, reader),
            frame_buf: Vec::with_capacity(READER_BUFFER_CAPACITY),
        }
    }

    /// Reads and decodes the next [`Frame`], returning `Ok(None)` on a clean EOF at a frame boundary.
    pub async fn read_frame(&mut self) -> Result<Option<Frame>, ConnectionError> {
        let mut header = [0u8; HEADER_SIZE];
        let mut read = 0;

        while read < HEADER_SIZE {
            let n = self.reader.read(&mut header[read..]).await?;
            if n == 0 {
                return if read == 0 {
                    Ok(None)
                } else {
                    Err(ConnectionError::Protocol(ProtocolError::UnexpectedEof))
                };
            }
            read += n;
        }

        // Validate all header fields before allocating or waiting on the payload bytes.
        let magic = u16::from_be_bytes([header[0], header[1]]);
        if magic != MAGIC {
            return Err(ConnectionError::Protocol(ProtocolError::InvalidMagic));
        }

        Version::try_from(header[2])?;
        MessageType::try_from(header[3])?;

        let payload_len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;
        if payload_len > MAX_FRAME_SIZE {
            return Err(ConnectionError::Protocol(ProtocolError::FrameTooLarge));
        }

        let total_len = HEADER_SIZE + payload_len;
        self.frame_buf.resize(total_len, 0);
        self.frame_buf[..HEADER_SIZE].copy_from_slice(&header);

        if payload_len > 0
            && let Err(err) = self
                .reader
                .read_exact(&mut self.frame_buf[HEADER_SIZE..])
                .await
        {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                return Err(ConnectionError::Protocol(ProtocolError::UnexpectedEof));
            }
            return Err(ConnectionError::Io(err));
        }

        let frame = Codec::decode(&self.frame_buf)?;

        // Reclaim buffer memory after an unusually large frame.
        if self.frame_buf.capacity() > SHRINK_THRESHOLD_CAPACITY
            && self.frame_buf.len() < READER_BUFFER_CAPACITY
        {
            self.frame_buf.shrink_to(READER_BUFFER_CAPACITY);
        }

        Ok(Some(frame))
    }
}

/// Encodes and writes binary protocol [`Frame`] values to an async byte stream.
pub struct FrameWriter<W> {
    writer: W,
}

impl<W> FrameWriter<W>
where
    W: AsyncWrite + Unpin,
{
    /// Creates a new [`FrameWriter`] over `writer`.
    pub fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Encodes `frame` with [`Codec::encode`], writes all bytes, and flushes the stream.
    pub async fn write_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        let bytes = Codec::encode(frame)?;
        self.writer.write_all(&bytes).await?;
        self.writer.flush().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use protocol::{
        Codec, Frame, HEADER_SIZE, MAGIC, MAX_FRAME_SIZE, Message, MessageType, PingPayload,
        ProtocolError, RoomMessagePayload, Version,
    };
    use tokio::io::{AsyncWriteExt, duplex};

    use super::{FrameReader, FrameWriter};
    use crate::error::ConnectionError;

    #[tokio::test]
    async fn single_frame_round_trip() {
        let (client, server) = duplex(1024);
        let mut writer = FrameWriter::new(client);
        let mut reader = FrameReader::new(server);

        let ping = Frame::new(Message::Ping(PingPayload));
        writer.write_frame(&ping).await.unwrap();
        assert_eq!(reader.read_frame().await.unwrap(), Some(ping));

        let room_msg = Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 42,
            message: "hello binary stream".to_string(),
        }));
        writer.write_frame(&room_msg).await.unwrap();
        assert_eq!(reader.read_frame().await.unwrap(), Some(room_msg));
    }

    #[tokio::test]
    async fn pipelined_frames_decode_in_order() {
        let (mut client, server) = duplex(4096);
        let mut reader = FrameReader::new(server);

        let f1 = Frame::new(Message::Ping(PingPayload));
        let f2 = Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 7,
            message: "first".to_string(),
        }));
        let f3 = Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 9,
            message: "second".to_string(),
        }));

        let mut combined = Codec::encode(&f1).unwrap();
        combined.extend_from_slice(&Codec::encode(&f2).unwrap());
        combined.extend_from_slice(&Codec::encode(&f3).unwrap());
        client.write_all(&combined).await.unwrap();
        client.flush().await.unwrap();

        assert_eq!(reader.read_frame().await.unwrap(), Some(f1));
        assert_eq!(reader.read_frame().await.unwrap(), Some(f2));
        assert_eq!(reader.read_frame().await.unwrap(), Some(f3));
    }

    #[tokio::test]
    async fn fragmented_byte_by_byte_stream() {
        let (mut client, server) = duplex(1024);
        let mut reader = FrameReader::new(server);

        let frame = Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 99,
            message: "fragmented frame payload".to_string(),
        }));
        let bytes = Codec::encode(&frame).unwrap();

        let writer_task = tokio::spawn(async move {
            for chunk in bytes.chunks(1) {
                client.write_all(chunk).await.unwrap();
                client.flush().await.unwrap();
                tokio::task::yield_now().await;
            }
        });

        let decoded = reader.read_frame().await.unwrap();
        assert_eq!(decoded, Some(frame));
        writer_task.await.unwrap();
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let (client, server) = duplex(128);
        drop(client);
        let mut reader = FrameReader::new(server);

        assert_eq!(reader.read_frame().await.unwrap(), None);
    }

    #[tokio::test]
    async fn mid_header_eof_returns_unexpected_eof() {
        let (mut client, server) = duplex(128);
        let mut reader = FrameReader::new(server);

        client.write_all(&MAGIC.to_be_bytes()).await.unwrap();
        client.shutdown().await.unwrap();

        assert!(matches!(
            reader.read_frame().await,
            Err(ConnectionError::Protocol(ProtocolError::UnexpectedEof))
        ));
    }

    #[tokio::test]
    async fn mid_payload_eof_returns_unexpected_eof() {
        let (mut client, server) = duplex(128);
        let mut reader = FrameReader::new(server);

        let frame = Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 1,
            message: "truncated".to_string(),
        }));
        let bytes = Codec::encode(&frame).unwrap();
        client.write_all(&bytes[..HEADER_SIZE + 2]).await.unwrap();
        client.shutdown().await.unwrap();

        assert!(matches!(
            reader.read_frame().await,
            Err(ConnectionError::Protocol(ProtocolError::UnexpectedEof))
        ));
    }

    #[tokio::test]
    async fn invalid_magic_rejected_immediately_even_with_open_writer() {
        let (mut client, server) = duplex(128);
        let mut reader = FrameReader::new(server);

        // Bad magic + nonzero payload length while writer stays open and sends no payload.
        let mut bad_header = [0u8; HEADER_SIZE];
        bad_header[0] = 0xDE;
        bad_header[1] = 0xAD;
        bad_header[2] = Version::V1.as_u8();
        bad_header[3] = MessageType::RoomMessage as u8;
        bad_header[4..8].copy_from_slice(&500u32.to_be_bytes());

        client.write_all(&bad_header).await.unwrap();
        client.flush().await.unwrap();

        let err = reader.read_frame().await.unwrap_err();
        assert!(matches!(
            err,
            ConnectionError::Protocol(ProtocolError::InvalidMagic)
        ));
    }

    #[tokio::test]
    async fn oversized_payload_length_rejected_without_hanging() {
        let (mut client, server) = duplex(128);
        let mut reader = FrameReader::new(server);

        let mut header = [0u8; HEADER_SIZE];
        header[0..2].copy_from_slice(&MAGIC.to_be_bytes());
        header[2] = Version::V1.as_u8();
        header[3] = MessageType::RoomMessage as u8;
        let too_large = (MAX_FRAME_SIZE as u32) + 1;
        header[4..8].copy_from_slice(&too_large.to_be_bytes());

        client.write_all(&header).await.unwrap();
        client.flush().await.unwrap();

        let err = reader.read_frame().await.unwrap_err();
        assert!(matches!(
            err,
            ConnectionError::Protocol(ProtocolError::FrameTooLarge)
        ));
    }
}
