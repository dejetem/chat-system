use std::fmt;
use std::net::SocketAddr;

use protocol::{
    AuthFailedPayload, AuthOkPayload, ErrorPayload, Frame, Message, PongPayload, ProtocolError,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};

use crate::error::{
    ConnectionError, ERR_BAD_REQUEST, ERR_NOT_IMPLEMENTED, ERR_PROTOCOL_ERROR, ERR_UNAUTHORIZED,
};
use crate::framed::{FrameReader, FrameWriter};

const OUTBOUND_CHANNEL_CAPACITY: usize = 64;

/// Unique per-node identifier assigned to an accepted client connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub u64);

impl fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Lifecycle authentication state of a client connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection has not yet completed authentication.
    Unauthenticated,
    /// Connection authenticated under `username`.
    Authenticated { username: String },
}

/// Clonable handle for pushing outbound frames to a running connection task.
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    id: ConnectionId,
    peer_addr: SocketAddr,
    outbound_tx: mpsc::Sender<Frame>,
}

impl ConnectionHandle {
    /// Returns the connection identifier.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the remote socket address of the peer.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Enqueues an outbound [`Frame`] to be written to the connection stream.
    pub async fn send(&self, frame: Frame) -> Result<(), mpsc::error::SendError<Frame>> {
        self.outbound_tx.send(frame).await
    }
}

/// Owns the framed reader, writer, state machine, and outbound queue for a single TCP connection.
pub struct Connection<R, W> {
    id: ConnectionId,
    peer_addr: SocketAddr,
    state: ConnectionState,
    reader: FrameReader<R>,
    writer: FrameWriter<W>,
    outbound_rx: mpsc::Receiver<Frame>,
    // Retained internally so `outbound_rx.recv()` suspends even before external registries clone a handle.
    handle: ConnectionHandle,
}

impl<R, W> Connection<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// Creates a new unauthenticated [`Connection`] over split async reader/writer halves.
    pub fn new(id: ConnectionId, peer_addr: SocketAddr, reader: R, writer: W) -> Self {
        let (outbound_tx, outbound_rx) = mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let handle = ConnectionHandle {
            id,
            peer_addr,
            outbound_tx,
        };

        Self {
            id,
            peer_addr,
            state: ConnectionState::Unauthenticated,
            reader: FrameReader::new(reader),
            writer: FrameWriter::new(writer),
            outbound_rx,
            handle,
        }
    }

    /// Returns a clonable [`ConnectionHandle`] for delivering outbound frames.
    pub fn handle(&self) -> ConnectionHandle {
        self.handle.clone()
    }

    /// Returns the current [`ConnectionState`].
    pub fn state(&self) -> &ConnectionState {
        &self.state
    }

    /// Runs the connection event loop until the peer disconnects, errors, or shutdown is signaled.
    pub async fn run(
        &mut self,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), ConnectionError> {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        tracing::info!(connection_id = %self.id, peer = %self.peer_addr, "client connected");

        loop {
            tokio::select! {
                changed_res = shutdown_rx.changed() => {
                    match changed_res {
                        Ok(()) if *shutdown_rx.borrow() => {
                            tracing::debug!(
                                connection_id = %self.id,
                                peer = %self.peer_addr,
                                "connection closing on server shutdown"
                            );
                            break;
                        }
                        Ok(()) => {}
                        Err(_) => break,
                    }
                }
                Some(frame) = self.outbound_rx.recv() => {
                    self.writer.write_frame(&frame).await?;
                }
                read_res = self.reader.read_frame() => {
                    match read_res {
                        Ok(None) => {
                            tracing::info!(
                                connection_id = %self.id,
                                peer = %self.peer_addr,
                                "client disconnected"
                            );
                            break;
                        }
                        Ok(Some(frame)) => {
                            if !self.handle_frame(frame).await? {
                                break;
                            }
                        }
                        Err(ConnectionError::Protocol(proto_err)) => {
                            tracing::warn!(
                                connection_id = %self.id,
                                peer = %self.peer_addr,
                                error = %proto_err,
                                "protocol error on connection"
                            );
                            if !matches!(proto_err, ProtocolError::UnexpectedEof) {
                                let err_frame = Frame::new(Message::Error(ErrorPayload {
                                    code: ERR_PROTOCOL_ERROR,
                                    message: proto_err.to_string(),
                                }));
                                let _ = self.writer.write_frame(&err_frame).await;
                            }
                            return Err(ConnectionError::Protocol(proto_err));
                        }
                        Err(ConnectionError::Io(io_err)) => {
                            return Err(ConnectionError::Io(io_err));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn send_message(&mut self, message: Message) -> Result<(), ConnectionError> {
        self.writer.write_frame(&Frame::new(message)).await
    }

    async fn send_error(
        &mut self,
        code: u16,
        message: impl Into<String>,
    ) -> Result<(), ConnectionError> {
        self.send_message(Message::Error(ErrorPayload {
            code,
            message: message.into(),
        }))
        .await
    }

    async fn handle_frame(&mut self, frame: Frame) -> Result<bool, ConnectionError> {
        match frame.message {
            Message::Ping(_) => {
                self.send_message(Message::Pong(PongPayload)).await?;
                Ok(true)
            }
            Message::Pong(_) => {
                tracing::trace!(connection_id = %self.id, peer = %self.peer_addr, "received pong");
                Ok(true)
            }
            Message::Auth(payload) => {
                if matches!(self.state, ConnectionState::Authenticated { .. }) {
                    self.send_error(ERR_BAD_REQUEST, "already authenticated")
                        .await?;
                    return Ok(true);
                }

                // TODO(Phase 3): Replace with auth crate JWT validation.
                if payload.username.trim().is_empty() || payload.token.trim().is_empty() {
                    tracing::warn!(
                        connection_id = %self.id,
                        peer = %self.peer_addr,
                        "authentication failed: empty username or token"
                    );
                    self.send_message(Message::AuthFailed(AuthFailedPayload))
                        .await?;
                    return Ok(false);
                }

                self.state = ConnectionState::Authenticated {
                    username: payload.username.clone(),
                };
                tracing::info!(
                    connection_id = %self.id,
                    peer = %self.peer_addr,
                    username = %payload.username,
                    "client authenticated"
                );
                self.send_message(Message::AuthOk(AuthOkPayload)).await?;
                Ok(true)
            }
            Message::JoinRoom(_)
            | Message::LeaveRoom(_)
            | Message::RoomMessage(_)
            | Message::PrivateMessage(_) => {
                match &self.state {
                    ConnectionState::Unauthenticated => {
                        self.send_error(ERR_UNAUTHORIZED, "authentication required")
                            .await?;
                    }
                    ConnectionState::Authenticated { .. } => {
                        self.send_error(ERR_NOT_IMPLEMENTED, "feature not yet implemented")
                            .await?;
                    }
                }
                Ok(true)
            }
            Message::AuthOk(_) | Message::AuthFailed(_) | Message::Ack(_) | Message::Error(_) => {
                self.send_error(ERR_BAD_REQUEST, "unexpected client message type")
                    .await?;
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use protocol::{
        AuthFailedPayload, AuthOkPayload, AuthPayload, ErrorPayload, Frame, JoinRoomPayload, MAGIC,
        Message, MessageType, PingPayload, PongPayload, PrivateMessagePayload, ProtocolError,
        RoomMessagePayload, Version,
    };
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::sync::watch;

    use super::{Connection, ConnectionId, ConnectionState};
    use crate::error::{
        ConnectionError, ERR_BAD_REQUEST, ERR_NOT_IMPLEMENTED, ERR_PROTOCOL_ERROR, ERR_UNAUTHORIZED,
    };
    use crate::framed::{FrameReader, FrameWriter};

    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40000)
    }

    struct TestConnection {
        writer: FrameWriter<tokio::io::WriteHalf<tokio::io::DuplexStream>>,
        reader: FrameReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        handle: super::ConnectionHandle,
        shutdown_tx: watch::Sender<bool>,
        join: tokio::task::JoinHandle<Result<ConnectionState, ConnectionError>>,
    }

    fn spawn_connection() -> TestConnection {
        let (client_stream, server_stream) = duplex(4096);
        let (client_read, client_write) = tokio::io::split(client_stream);
        let (server_read, server_write) = tokio::io::split(server_stream);

        let mut conn = Connection::new(ConnectionId(1), test_addr(), server_read, server_write);
        let handle = conn.handle();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let join = tokio::spawn(async move {
            conn.run(shutdown_rx).await?;
            Ok(conn.state().clone())
        });

        TestConnection {
            writer: FrameWriter::new(client_write),
            reader: FrameReader::new(client_read),
            handle,
            shutdown_tx,
            join,
        }
    }

    #[tokio::test]
    async fn ping_pong_before_and_after_auth() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        writer
            .write_frame(&Frame::new(Message::Ping(PingPayload)))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Pong(PongPayload)
        );

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: "valid-token".to_string(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        writer
            .write_frame(&Frame::new(Message::Ping(PingPayload)))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Pong(PongPayload)
        );

        drop(writer);
        drop(reader);
        let final_state = join.await.unwrap().unwrap();
        assert_eq!(
            final_state,
            ConnectionState::Authenticated {
                username: "alice".to_string()
            }
        );
    }

    #[tokio::test]
    async fn unauthenticated_restricted_messages_return_401() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        for msg in [
            Message::JoinRoom(JoinRoomPayload { room_id: 1 }),
            Message::RoomMessage(RoomMessagePayload {
                room_id: 1,
                message: "hi".to_string(),
            }),
            Message::PrivateMessage(PrivateMessagePayload {
                recipient: "bob".to_string(),
                message: "secret".to_string(),
            }),
        ] {
            writer.write_frame(&Frame::new(msg)).await.unwrap();
            let reply = reader.read_frame().await.unwrap().unwrap();
            assert_eq!(
                reply.message,
                Message::Error(ErrorPayload {
                    code: ERR_UNAUTHORIZED,
                    message: "authentication required".to_string(),
                })
            );
        }

        drop(writer);
        drop(reader);
        let final_state = join.await.unwrap().unwrap();
        assert_eq!(final_state, ConnectionState::Unauthenticated);
    }

    #[tokio::test]
    async fn valid_auth_duplicate_auth_and_authenticated_room_message() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: "tok".to_string(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: "tok".to_string(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_BAD_REQUEST,
                message: "already authenticated".to_string(),
            })
        );

        writer
            .write_frame(&Frame::new(Message::RoomMessage(RoomMessagePayload {
                room_id: 5,
                message: "ciphertext".to_string(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_NOT_IMPLEMENTED,
                message: "feature not yet implemented".to_string(),
            })
        );

        drop(writer);
        drop(reader);
        join.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn empty_credentials_send_auth_failed_and_close_connection() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "   ".to_string(),
                token: "valid-token".to_string(),
            })))
            .await
            .unwrap();

        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthFailed(AuthFailedPayload)
        );
        assert_eq!(reader.read_frame().await.unwrap(), None);

        let final_state = join.await.unwrap().unwrap();
        assert_eq!(final_state, ConnectionState::Unauthenticated);
    }

    #[tokio::test]
    async fn handle_push_delivers_frame_to_client() {
        let TestConnection {
            writer,
            mut reader,
            handle,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let pushed = Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 10,
            message: "fanout".to_string(),
        }));
        handle.send(pushed.clone()).await.unwrap();

        assert_eq!(reader.read_frame().await.unwrap(), Some(pushed));

        drop(writer);
        drop(reader);
        join.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn malformed_frame_sends_422_error_and_closes_connection() {
        let (client_stream, server_stream) = duplex(4096);
        let (client_read, mut client_write) = tokio::io::split(client_stream);
        let (server_read, server_write) = tokio::io::split(server_stream);

        let mut conn = Connection::new(ConnectionId(2), test_addr(), server_read, server_write);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let join = tokio::spawn(async move { conn.run(shutdown_rx).await });

        let mut bad_header = [0u8; 8];
        bad_header[0..2].copy_from_slice(&(MAGIC ^ 0xFFFF).to_be_bytes());
        bad_header[2] = Version::V1.as_u8();
        bad_header[3] = MessageType::Ping as u8;
        client_write.write_all(&bad_header).await.unwrap();
        client_write.flush().await.unwrap();

        let mut reader = FrameReader::new(client_read);
        let reply = reader.read_frame().await.unwrap().unwrap();
        match reply.message {
            Message::Error(err) => assert_eq!(err.code, ERR_PROTOCOL_ERROR),
            other => panic!("expected Error frame, got {other:?}"),
        }
        assert_eq!(reader.read_frame().await.unwrap(), None);

        let conn_err = join.await.unwrap().unwrap_err();
        assert!(matches!(
            conn_err,
            ConnectionError::Protocol(ProtocolError::InvalidMagic)
        ));
    }

    #[tokio::test]
    async fn shutdown_signal_exits_connection_cleanly() {
        let TestConnection {
            writer: _writer,
            mut reader,
            handle: _handle,
            shutdown_tx,
            join,
        } = spawn_connection();

        shutdown_tx.send(true).unwrap();

        assert_eq!(reader.read_frame().await.unwrap(), None);
        let state = join.await.unwrap().unwrap();
        assert_eq!(state, ConnectionState::Unauthenticated);
    }
}
