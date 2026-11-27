use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use auth::{AuthenticatedUser, Authenticator};
use protocol::{
    AckPayload, AuthFailedPayload, AuthOkPayload, ErrorPayload, Frame, Message, PongPayload,
    PrivateMessagePayload, ProtocolError, RoomMessagePayload,
};
use storage::{
    ChatStorage, DEFAULT_HISTORY_LIMIT, MediaRef, NewPrivateMessage, NewRoomMessage, StorageError,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::{SendError, TrySendError};
use tokio::sync::{mpsc, watch};

use crate::error::{
    ConnectionError, ERR_BAD_REQUEST, ERR_FORBIDDEN, ERR_INTERNAL_ERROR, ERR_NOT_FOUND,
    ERR_PROTOCOL_ERROR, ERR_UNAUTHORIZED,
};
use crate::framed::{FrameReader, FrameWriter};
use crate::room::RoomManager;
use crate::session::{DirectDelivery, SessionRegistry};

const OUTBOUND_CHANNEL_CAPACITY: usize = 256;

/// Default deadline for a newly accepted connection to complete authentication.
pub const DEFAULT_AUTH_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// Connection authenticated as the verified user.
    Authenticated(AuthenticatedUser),
}

impl ConnectionState {
    /// Returns `true` if the connection has completed authentication.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, ConnectionState::Authenticated(_))
    }

    /// Returns the authenticated user identity, if authenticated.
    pub fn user(&self) -> Option<&AuthenticatedUser> {
        match self {
            ConnectionState::Unauthenticated => None,
            ConnectionState::Authenticated(user) => Some(user),
        }
    }

    /// Returns the authenticated username, if authenticated.
    pub fn username(&self) -> Option<&str> {
        self.user().map(|user| user.username.as_str())
    }
}

/// Clonable handle for pushing outbound frames to a running connection task.
#[derive(Debug, Clone)]
pub struct ConnectionHandle {
    id: ConnectionId,
    peer_addr: SocketAddr,
    outbound_tx: mpsc::Sender<Frame>,
}

impl ConnectionHandle {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        id: ConnectionId,
        peer_addr: SocketAddr,
        capacity: usize,
    ) -> (Self, mpsc::Receiver<Frame>) {
        let (outbound_tx, outbound_rx) = mpsc::channel(capacity);
        let handle = Self {
            id,
            peer_addr,
            outbound_tx,
        };
        (handle, outbound_rx)
    }

    /// Returns the connection identifier.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Returns the remote socket address of the peer.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Attempts to enqueue `frame` without waiting for queue capacity.
    pub fn try_send(&self, frame: Frame) -> Result<(), TrySendError<Frame>> {
        self.outbound_tx.try_send(frame)
    }

    /// Returns `true` if the owning connection's outbound receiver has been closed.
    pub fn is_closed(&self) -> bool {
        self.outbound_tx.is_closed()
    }

    /// Enqueues an outbound [`Frame`] to be written to the connection stream.
    pub async fn send(&self, frame: Frame) -> Result<(), SendError<Frame>> {
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
    authenticator: Arc<dyn Authenticator>,
    room_manager: RoomManager,
    session_registry: SessionRegistry,
    storage: Option<Arc<dyn ChatStorage>>,
    room_history_limit: usize,
    auth_timeout: Duration,
}

impl<R, W> Drop for Connection<R, W> {
    fn drop(&mut self) {
        self.room_manager.remove_connection(self.id);
        self.session_registry.remove_connection(self.id);
    }
}

impl<R, W> Connection<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    /// Creates a new unauthenticated [`Connection`] over split async reader/writer halves.
    pub fn new(
        id: ConnectionId,
        peer_addr: SocketAddr,
        reader: R,
        writer: W,
        authenticator: Arc<dyn Authenticator>,
        room_manager: RoomManager,
        session_registry: SessionRegistry,
    ) -> Self {
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
            authenticator,
            room_manager,
            session_registry,
            storage: None,
            room_history_limit: DEFAULT_HISTORY_LIMIT,
            auth_timeout: DEFAULT_AUTH_TIMEOUT,
        }
    }

    /// Overrides the pre-authentication handshake timeout for this connection.
    pub fn with_auth_timeout(mut self, auth_timeout: Duration) -> Self {
        self.auth_timeout = auth_timeout;
        self
    }

    /// Attaches a persistent [`ChatStorage`] backend to this connection.
    pub fn with_storage(mut self, storage: Arc<dyn ChatStorage>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Overrides the maximum number of historical messages replayed on `JoinRoom`.
    pub fn with_room_history_limit(mut self, room_history_limit: usize) -> Self {
        self.room_history_limit = room_history_limit;
        self
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
    pub async fn run(&mut self, shutdown_rx: watch::Receiver<bool>) -> Result<(), ConnectionError> {
        let result = self.run_loop(shutdown_rx).await;
        let rooms_cleaned = self.room_manager.remove_connection(self.id);
        let session_removed = self.session_registry.remove_connection(self.id);
        if rooms_cleaned > 0 {
            tracing::debug!(
                connection_id = %self.id,
                peer = %self.peer_addr,
                rooms_cleaned,
                "cleaned up room subscriptions on disconnect"
            );
        }
        if let Some(unreg) = session_removed {
            tracing::debug!(
                connection_id = %self.id,
                peer = %self.peer_addr,
                username = %unreg.username,
                remaining_sessions = unreg.remaining_local_sessions,
                "unregistered session on disconnect"
            );
        }
        result
    }

    async fn run_loop(
        &mut self,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> Result<(), ConnectionError> {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        tracing::info!(connection_id = %self.id, peer = %self.peer_addr, "client connected");

        // Compute the handshake deadline once so pre-auth keepalive pings cannot extend it.
        let auth_deadline = tokio::time::Instant::now() + self.auth_timeout;

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
                _ = tokio::time::sleep_until(auth_deadline), if !self.state.is_authenticated() => {
                    tracing::warn!(
                        connection_id = %self.id,
                        peer = %self.peer_addr,
                        timeout_ms = self.auth_timeout.as_millis(),
                        "closing unauthenticated connection after auth timeout"
                    );
                    let _ = self.send_message(Message::AuthFailed(AuthFailedPayload)).await;
                    break;
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

    async fn send_storage_error(
        &mut self,
        action: &str,
        err: StorageError,
    ) -> Result<(), ConnectionError> {
        match err {
            StorageError::Validation(msg) => self.send_error(ERR_BAD_REQUEST, msg).await,
            StorageError::NotFound(msg) => self.send_error(ERR_NOT_FOUND, msg).await,
            other => {
                tracing::error!(
                    connection_id = %self.id,
                    peer = %self.peer_addr,
                    action,
                    error = %other,
                    "storage operation failed"
                );
                self.send_error(ERR_INTERNAL_ERROR, "internal storage error")
                    .await
            }
        }
    }

    fn decode_utf8_payload(&self, message_id: u64, bytes: Vec<u8>) -> Option<String> {
        match String::from_utf8(bytes) {
            Ok(text) => Some(text),
            Err(err) => {
                tracing::warn!(
                    connection_id = %self.id,
                    message_id,
                    error = %err,
                    "skipping non-UTF-8 stored message payload during TCP replay"
                );
                None
            }
        }
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
                if self.state.is_authenticated() {
                    self.send_error(ERR_BAD_REQUEST, "already authenticated")
                        .await?;
                    return Ok(true);
                }

                match self
                    .authenticator
                    .authenticate(&payload.username, &payload.token)
                {
                    Ok(user) => {
                        if let Some(storage) = self.storage.clone()
                            && let Err(err) = storage
                                .upsert_user(&user.username, &user.role.to_string())
                                .await
                        {
                            self.send_storage_error("upsert_user", err).await?;
                            return Ok(true);
                        }

                        let reg = self
                            .session_registry
                            .register(user.username.clone(), self.handle());
                        let username = user.username.clone();
                        tracing::info!(
                            connection_id = %self.id,
                            peer = %self.peer_addr,
                            %username,
                            role = %user.role,
                            node_id = %self.session_registry.node_id(),
                            active_sessions = reg.active_sessions,
                            "client authenticated"
                        );
                        self.state = ConnectionState::Authenticated(user);
                        self.send_message(Message::AuthOk(AuthOkPayload)).await?;

                        if let Some(storage) = self.storage.clone() {
                            match storage.flush_offline_messages(&username).await {
                                Ok(queued) => {
                                    for record in queued {
                                        if let Some(message) = self.decode_utf8_payload(
                                            record.message_id,
                                            record.ciphertext,
                                        ) {
                                            self.send_message(Message::PrivateMessage(
                                                PrivateMessagePayload {
                                                    recipient: username.clone(),
                                                    message,
                                                },
                                            ))
                                            .await?;
                                        }
                                    }
                                }
                                Err(err) => {
                                    tracing::error!(
                                        connection_id = %self.id,
                                        %username,
                                        error = %err,
                                        "failed to flush offline messages on login"
                                    );
                                }
                            }
                        }

                        Ok(true)
                    }
                    Err(err) => {
                        tracing::warn!(
                            connection_id = %self.id,
                            peer = %self.peer_addr,
                            username = %payload.username,
                            error = %err,
                            "authentication failed"
                        );
                        self.send_message(Message::AuthFailed(AuthFailedPayload))
                            .await?;
                        Ok(false)
                    }
                }
            }
            Message::JoinRoom(payload) => {
                if !self.state.is_authenticated() {
                    self.send_error(ERR_UNAUTHORIZED, "authentication required")
                        .await?;
                    return Ok(true);
                }

                let history = if let Some(storage) = self.storage.clone() {
                    if let Err(err) = storage.ensure_room(payload.room_id, None, None).await {
                        self.send_storage_error("ensure_room", err).await?;
                        return Ok(true);
                    }
                    match storage
                        .fetch_room_history(payload.room_id, self.room_history_limit)
                        .await
                    {
                        Ok(records) => records,
                        Err(err) => {
                            self.send_storage_error("fetch_room_history", err).await?;
                            return Ok(true);
                        }
                    }
                } else {
                    Vec::new()
                };

                self.room_manager.join_room(payload.room_id, self.handle());
                tracing::debug!(
                    connection_id = %self.id,
                    room_id = payload.room_id,
                    history_count = history.len(),
                    "joined room"
                );
                self.send_message(Message::Ack(AckPayload {
                    message_id: payload.room_id,
                }))
                .await?;

                for record in history {
                    if let Some(message) =
                        self.decode_utf8_payload(record.message_id, record.ciphertext)
                    {
                        self.send_message(Message::RoomMessage(RoomMessagePayload {
                            room_id: payload.room_id,
                            message,
                        }))
                        .await?;
                    }
                }

                Ok(true)
            }
            Message::LeaveRoom(payload) => {
                if !self.state.is_authenticated() {
                    self.send_error(ERR_UNAUTHORIZED, "authentication required")
                        .await?;
                    return Ok(true);
                }

                if !self.room_manager.leave_room(payload.room_id, self.id) {
                    self.send_error(
                        ERR_BAD_REQUEST,
                        format!("not subscribed to room {}", payload.room_id),
                    )
                    .await?;
                    return Ok(true);
                }

                tracing::debug!(
                    connection_id = %self.id,
                    room_id = payload.room_id,
                    "left room"
                );
                self.send_message(Message::Ack(AckPayload {
                    message_id: payload.room_id,
                }))
                .await?;
                Ok(true)
            }
            Message::RoomMessage(payload) => {
                if !self.state.is_authenticated() {
                    self.send_error(ERR_UNAUTHORIZED, "authentication required")
                        .await?;
                    return Ok(true);
                }

                if payload.message.is_empty() {
                    self.send_error(ERR_BAD_REQUEST, "room message cannot be empty")
                        .await?;
                    return Ok(true);
                }

                if !self.room_manager.is_subscribed(payload.room_id, self.id) {
                    self.send_error(
                        ERR_FORBIDDEN,
                        format!("not subscribed to room {}", payload.room_id),
                    )
                    .await?;
                    return Ok(true);
                }

                let room_id = payload.room_id;
                let message_id = if let Some(storage) = self.storage.clone() {
                    let sender_username = self.state.username().unwrap_or_default().to_string();
                    let media_key = MediaRef::extract_from_payload(&payload.message);
                    match storage
                        .store_room_message(NewRoomMessage {
                            sender_username,
                            room_id,
                            ciphertext: payload.message.as_bytes().to_vec(),
                            media_key,
                        })
                        .await
                    {
                        Ok(stored) => stored.message_id,
                        Err(err) => {
                            self.send_storage_error("store_room_message", err).await?;
                            return Ok(true);
                        }
                    }
                } else {
                    self.room_manager.next_message_id()
                };

                let broadcast_frame = Frame::new(Message::RoomMessage(payload));
                self.room_manager
                    .broadcast(room_id, broadcast_frame, Some(self.id))
                    .await;
                self.send_message(Message::Ack(AckPayload { message_id }))
                    .await?;
                Ok(true)
            }
            Message::PrivateMessage(payload) => {
                if !self.state.is_authenticated() {
                    self.send_error(ERR_UNAUTHORIZED, "authentication required")
                        .await?;
                    return Ok(true);
                }

                if payload.recipient.trim().is_empty() {
                    self.send_error(ERR_BAD_REQUEST, "recipient username cannot be empty")
                        .await?;
                    return Ok(true);
                }

                if payload.message.is_empty() {
                    self.send_error(ERR_BAD_REQUEST, "private message cannot be empty")
                        .await?;
                    return Ok(true);
                }

                let sender_username = self.state.username().unwrap_or_default().to_string();
                let recipient = payload.recipient.clone();
                let raw_ciphertext = payload.message.as_bytes().to_vec();
                let media_key = MediaRef::extract_from_payload(&payload.message);
                let dm_frame = Frame::new(Message::PrivateMessage(payload));

                let delivery = self
                    .session_registry
                    .send_to_user(&recipient, dm_frame, Some(self.id))
                    .await;

                match (delivery, self.storage.clone()) {
                    (DirectDelivery::DeliveredLocal { delivered, .. }, Some(storage)) => {
                        match storage
                            .store_private_message(NewPrivateMessage {
                                sender_username,
                                recipient_username: recipient.clone(),
                                ciphertext: raw_ciphertext,
                                media_key,
                                queue_offline: false,
                            })
                            .await
                        {
                            Ok(stored) => {
                                let message_id = stored.message_id;
                                tracing::debug!(
                                    connection_id = %self.id,
                                    %recipient,
                                    message_id,
                                    delivered,
                                    "delivered and persisted private message"
                                );
                                self.send_message(Message::Ack(AckPayload { message_id }))
                                    .await?;
                            }
                            Err(err) => {
                                self.send_storage_error("store_private_message", err)
                                    .await?;
                                return Ok(true);
                            }
                        }
                    }
                    (DirectDelivery::DeliveredLocal { delivered, .. }, None) => {
                        let message_id = self.session_registry.next_message_id();
                        tracing::debug!(
                            connection_id = %self.id,
                            %recipient,
                            message_id,
                            delivered,
                            "delivered private message"
                        );
                        self.send_message(Message::Ack(AckPayload { message_id }))
                            .await?;
                    }
                    (DirectDelivery::RecipientNotLocal, Some(storage)) => {
                        // TODO(Phase 7): Query Redis session registry and publish to remote node channel before buffering offline.
                        match storage
                            .store_private_message(NewPrivateMessage {
                                sender_username,
                                recipient_username: recipient.clone(),
                                ciphertext: raw_ciphertext,
                                media_key,
                                queue_offline: true,
                            })
                            .await
                        {
                            Ok(stored) => {
                                let message_id = stored.message_id;
                                tracing::debug!(
                                    connection_id = %self.id,
                                    %recipient,
                                    message_id,
                                    "buffered offline private message"
                                );
                                self.send_message(Message::Ack(AckPayload { message_id }))
                                    .await?;

                                if self.session_registry.is_online(&recipient)
                                    && let Ok(queued) =
                                        storage.flush_offline_messages(&recipient).await
                                {
                                    for record in queued {
                                        if let Some(msg) = self.decode_utf8_payload(
                                            record.message_id,
                                            record.ciphertext,
                                        ) {
                                            let frame = Frame::new(Message::PrivateMessage(
                                                PrivateMessagePayload {
                                                    recipient: recipient.clone(),
                                                    message: msg,
                                                },
                                            ));
                                            let _ = self
                                                .session_registry
                                                .send_to_user(&recipient, frame, None)
                                                .await;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                self.send_storage_error("store_offline_private_message", err)
                                    .await?;
                                return Ok(true);
                            }
                        }
                    }
                    (DirectDelivery::RecipientNotLocal, None) => {
                        // TODO(Phase 7): Query Redis session registry and publish to remote node channel if online elsewhere.
                        self.send_error(
                            ERR_NOT_FOUND,
                            format!("recipient '{recipient}' is not online"),
                        )
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
    use std::sync::Arc;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use auth::{Authenticator, Claims, JwtAuthenticator, Role};
    use protocol::{
        AckPayload, AuthFailedPayload, AuthOkPayload, AuthPayload, ErrorPayload, Frame,
        JoinRoomPayload, LeaveRoomPayload, MAGIC, Message, MessageType, PingPayload, PongPayload,
        PrivateMessagePayload, ProtocolError, RoomMessagePayload, Version,
    };
    use storage::{ChatStorage, InMemoryStorage};
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio::sync::watch;

    use super::{Connection, ConnectionId, ConnectionState, DEFAULT_AUTH_TIMEOUT};
    use crate::error::{
        ConnectionError, ERR_BAD_REQUEST, ERR_FORBIDDEN, ERR_NOT_FOUND, ERR_PROTOCOL_ERROR,
        ERR_UNAUTHORIZED,
    };
    use crate::framed::{FrameReader, FrameWriter};
    use crate::room::RoomManager;
    use crate::session::SessionRegistry;

    fn test_addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40000)
    }

    fn test_authenticator() -> Arc<JwtAuthenticator> {
        Arc::new(JwtAuthenticator::from_secret("test-secret").unwrap())
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    struct TestConnection {
        writer: FrameWriter<tokio::io::WriteHalf<tokio::io::DuplexStream>>,
        reader: FrameReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
        handle: super::ConnectionHandle,
        auth: Arc<JwtAuthenticator>,
        shutdown_tx: watch::Sender<bool>,
        join: tokio::task::JoinHandle<Result<ConnectionState, ConnectionError>>,
    }

    fn spawn_connection() -> TestConnection {
        spawn_connection_with(
            ConnectionId(1),
            RoomManager::new(),
            SessionRegistry::new(),
            test_authenticator(),
            DEFAULT_AUTH_TIMEOUT,
        )
    }

    fn spawn_connection_with_timeout(auth_timeout: Duration) -> TestConnection {
        spawn_connection_with(
            ConnectionId(1),
            RoomManager::new(),
            SessionRegistry::new(),
            test_authenticator(),
            auth_timeout,
        )
    }

    fn spawn_connection_with(
        id: ConnectionId,
        room_manager: RoomManager,
        session_registry: SessionRegistry,
        auth: Arc<JwtAuthenticator>,
        auth_timeout: Duration,
    ) -> TestConnection {
        spawn_connection_full(
            id,
            room_manager,
            session_registry,
            auth,
            auth_timeout,
            None,
            storage::DEFAULT_HISTORY_LIMIT,
        )
    }

    fn spawn_connection_with_storage(
        id: ConnectionId,
        room_manager: RoomManager,
        session_registry: SessionRegistry,
        auth: Arc<JwtAuthenticator>,
        storage: Arc<dyn ChatStorage>,
        room_history_limit: usize,
    ) -> TestConnection {
        spawn_connection_full(
            id,
            room_manager,
            session_registry,
            auth,
            DEFAULT_AUTH_TIMEOUT,
            Some(storage),
            room_history_limit,
        )
    }

    fn spawn_connection_full(
        id: ConnectionId,
        room_manager: RoomManager,
        session_registry: SessionRegistry,
        auth: Arc<JwtAuthenticator>,
        auth_timeout: Duration,
        storage: Option<Arc<dyn ChatStorage>>,
        room_history_limit: usize,
    ) -> TestConnection {
        let (client_stream, server_stream) = duplex(4096);
        let (client_read, client_write) = tokio::io::split(client_stream);
        let (server_read, server_write) = tokio::io::split(server_stream);

        let mut conn = Connection::new(
            id,
            test_addr(),
            server_read,
            server_write,
            Arc::clone(&auth) as Arc<dyn Authenticator>,
            room_manager,
            session_registry,
        )
        .with_auth_timeout(auth_timeout)
        .with_room_history_limit(room_history_limit);
        if let Some(storage) = storage {
            conn = conn.with_storage(storage);
        }
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
            auth,
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
            auth,
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

        let token = auth
            .issue_token("alice", Role::Moderator, Duration::from_secs(60))
            .unwrap();
        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token,
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
        assert_eq!(final_state.username(), Some("alice"));
        assert_eq!(final_state.user().unwrap().role, Role::Moderator);
    }

    #[tokio::test]
    async fn unauthenticated_restricted_messages_return_401() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth: _auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        for msg in [
            Message::JoinRoom(JoinRoomPayload { room_id: 1 }),
            Message::LeaveRoom(LeaveRoomPayload { room_id: 1 }),
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
    async fn valid_auth_duplicate_auth_and_room_lifecycle() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: token.clone(),
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
                token,
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

        // Sending to a room without joining returns 403 Forbidden.
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
                code: ERR_FORBIDDEN,
                message: "not subscribed to room 5".to_string(),
            })
        );

        // Join room 5 -> Ack(5).
        writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
                room_id: 5,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 5 })
        );

        // Subscribed RoomMessage -> Ack(message_id).
        writer
            .write_frame(&Frame::new(Message::RoomMessage(RoomMessagePayload {
                room_id: 5,
                message: "ciphertext".to_string(),
            })))
            .await
            .unwrap();
        match reader.read_frame().await.unwrap().unwrap().message {
            Message::Ack(ack) => assert!(ack.message_id >= 1),
            other => panic!("expected Ack, got {other:?}"),
        }

        // Leave room 5 -> Ack(5).
        writer
            .write_frame(&Frame::new(Message::LeaveRoom(LeaveRoomPayload {
                room_id: 5,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 5 })
        );

        // Duplicate leave returns 400 Bad Request.
        writer
            .write_frame(&Frame::new(Message::LeaveRoom(LeaveRoomPayload {
                room_id: 5,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_BAD_REQUEST,
                message: "not subscribed to room 5".to_string(),
            })
        );

        // Private messages to offline users return 404 Not Found.
        writer
            .write_frame(&Frame::new(Message::PrivateMessage(
                PrivateMessagePayload {
                    recipient: "bob".to_string(),
                    message: "ciphertext".to_string(),
                },
            )))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_NOT_FOUND,
                message: "recipient 'bob' is not online".to_string(),
            })
        );

        drop(writer);
        drop(reader);
        join.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn two_connections_exchange_room_messages_and_cleanup_on_disconnect() {
        let room_manager = RoomManager::new();
        let session_registry = SessionRegistry::new();
        let auth = test_authenticator();

        let alice = spawn_connection_with(
            ConnectionId(1),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );
        let bob = spawn_connection_with(
            ConnectionId(2),
            room_manager.clone(),
            session_registry,
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );

        let mut alice_writer = alice.writer;
        let mut alice_reader = alice.reader;
        let mut bob_writer = bob.writer;
        let mut bob_reader = bob.reader;

        let alice_tok = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        let bob_tok = auth
            .issue_token("bob", Role::User, Duration::from_secs(60))
            .unwrap();

        alice_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: alice_tok,
            })))
            .await
            .unwrap();
        assert_eq!(
            alice_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        bob_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "bob".to_string(),
                token: bob_tok,
            })))
            .await
            .unwrap();
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        alice_writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
                room_id: 42,
            })))
            .await
            .unwrap();
        assert_eq!(
            alice_reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 42 })
        );

        bob_writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
                room_id: 42,
            })))
            .await
            .unwrap();
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 42 })
        );
        assert_eq!(room_manager.subscriber_count(42), 2);

        let msg = RoomMessagePayload {
            room_id: 42,
            message: "hello bob".to_string(),
        };
        alice_writer
            .write_frame(&Frame::new(Message::RoomMessage(msg.clone())))
            .await
            .unwrap();

        match alice_reader.read_frame().await.unwrap().unwrap().message {
            Message::Ack(ack) => assert_eq!(ack.message_id, 1),
            other => panic!("expected Ack, got {other:?}"),
        }
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::RoomMessage(msg)
        );

        drop(bob_writer);
        drop(bob_reader);
        bob.join.await.unwrap().unwrap();
        assert_eq!(room_manager.subscriber_count(42), 1);

        drop(alice_writer);
        drop(alice_reader);
        alice.join.await.unwrap().unwrap();
        assert_eq!(room_manager.room_count(), 0);
    }

    #[tokio::test]
    async fn empty_room_message_returns_400() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
                room_id: 9,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 9 })
        );

        writer
            .write_frame(&Frame::new(Message::RoomMessage(RoomMessagePayload {
                room_id: 9,
                message: String::new(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_BAD_REQUEST,
                message: "room message cannot be empty".to_string(),
            })
        );

        drop(writer);
        drop(reader);
        join.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn two_connections_exchange_private_messages_and_cleanup_on_disconnect() {
        let room_manager = RoomManager::new();
        let session_registry = SessionRegistry::new();
        let auth = test_authenticator();

        let alice = spawn_connection_with(
            ConnectionId(1),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );
        let bob = spawn_connection_with(
            ConnectionId(2),
            room_manager,
            session_registry.clone(),
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );

        let mut alice_writer = alice.writer;
        let mut alice_reader = alice.reader;
        let mut bob_writer = bob.writer;
        let mut bob_reader = bob.reader;

        for (username, writer, reader) in [
            ("alice", &mut alice_writer, &mut alice_reader),
            ("bob", &mut bob_writer, &mut bob_reader),
        ] {
            let token = auth
                .issue_token(username, Role::User, Duration::from_secs(60))
                .unwrap();
            writer
                .write_frame(&Frame::new(Message::Auth(AuthPayload {
                    username: username.to_string(),
                    token,
                })))
                .await
                .unwrap();
            assert_eq!(
                reader.read_frame().await.unwrap().unwrap().message,
                Message::AuthOk(AuthOkPayload)
            );
        }

        assert_eq!(session_registry.online_user_count(), 2);

        let dm = PrivateMessagePayload {
            recipient: "bob".to_string(),
            message: "secret ciphertext".to_string(),
        };
        alice_writer
            .write_frame(&Frame::new(Message::PrivateMessage(dm.clone())))
            .await
            .unwrap();
        match alice_reader.read_frame().await.unwrap().unwrap().message {
            Message::Ack(ack) => assert_eq!(ack.message_id, 1),
            other => panic!("expected Ack, got {other:?}"),
        }
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::PrivateMessage(dm)
        );

        drop(bob_writer);
        drop(bob_reader);
        bob.join.await.unwrap().unwrap();
        assert!(!session_registry.is_online("bob"));
        assert!(session_registry.is_online("alice"));

        alice_writer
            .write_frame(&Frame::new(Message::PrivateMessage(
                PrivateMessagePayload {
                    recipient: "bob".to_string(),
                    message: "after disconnect".to_string(),
                },
            )))
            .await
            .unwrap();
        assert_eq!(
            alice_reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_NOT_FOUND,
                message: "recipient 'bob' is not online".to_string(),
            })
        );

        drop(alice_writer);
        drop(alice_reader);
        alice.join.await.unwrap().unwrap();
        assert_eq!(session_registry.online_user_count(), 0);
    }

    #[tokio::test]
    async fn multi_device_user_receives_private_message_on_all_active_connections() {
        let room_manager = RoomManager::new();
        let session_registry = SessionRegistry::new();
        let auth = test_authenticator();

        let alice_laptop = spawn_connection_with(
            ConnectionId(1),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );
        let alice_phone = spawn_connection_with(
            ConnectionId(2),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );
        let bob = spawn_connection_with(
            ConnectionId(3),
            room_manager,
            session_registry.clone(),
            Arc::clone(&auth),
            DEFAULT_AUTH_TIMEOUT,
        );

        let mut laptop_writer = alice_laptop.writer;
        let mut laptop_reader = alice_laptop.reader;
        let mut phone_writer = alice_phone.writer;
        let mut phone_reader = alice_phone.reader;
        let mut bob_writer = bob.writer;
        let mut bob_reader = bob.reader;

        for (username, writer, reader) in [
            ("alice", &mut laptop_writer, &mut laptop_reader),
            ("alice", &mut phone_writer, &mut phone_reader),
            ("bob", &mut bob_writer, &mut bob_reader),
        ] {
            let token = auth
                .issue_token(username, Role::User, Duration::from_secs(60))
                .unwrap();
            writer
                .write_frame(&Frame::new(Message::Auth(AuthPayload {
                    username: username.to_string(),
                    token,
                })))
                .await
                .unwrap();
            assert_eq!(
                reader.read_frame().await.unwrap().unwrap().message,
                Message::AuthOk(AuthOkPayload)
            );
        }

        assert_eq!(session_registry.session_count_for("alice"), 2);

        let dm = PrivateMessagePayload {
            recipient: "alice".to_string(),
            message: "for both devices".to_string(),
        };
        bob_writer
            .write_frame(&Frame::new(Message::PrivateMessage(dm.clone())))
            .await
            .unwrap();
        match bob_reader.read_frame().await.unwrap().unwrap().message {
            Message::Ack(ack) => assert!(ack.message_id >= 1),
            other => panic!("expected Ack, got {other:?}"),
        }
        assert_eq!(
            laptop_reader.read_frame().await.unwrap().unwrap().message,
            Message::PrivateMessage(dm.clone())
        );
        assert_eq!(
            phone_reader.read_frame().await.unwrap().unwrap().message,
            Message::PrivateMessage(dm)
        );

        drop((
            laptop_writer,
            laptop_reader,
            phone_writer,
            phone_reader,
            bob_writer,
            bob_reader,
        ));
        alice_laptop.join.await.unwrap().unwrap();
        alice_phone.join.await.unwrap().unwrap();
        bob.join.await.unwrap().unwrap();
        assert_eq!(session_registry.online_user_count(), 0);
    }

    #[tokio::test]
    async fn invalid_private_message_empty_recipient_or_body_returns_400() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        writer
            .write_frame(&Frame::new(Message::PrivateMessage(
                PrivateMessagePayload {
                    recipient: "   ".to_string(),
                    message: "hi".to_string(),
                },
            )))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_BAD_REQUEST,
                message: "recipient username cannot be empty".to_string(),
            })
        );

        writer
            .write_frame(&Frame::new(Message::PrivateMessage(
                PrivateMessagePayload {
                    recipient: "bob".to_string(),
                    message: String::new(),
                },
            )))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::Error(ErrorPayload {
                code: ERR_BAD_REQUEST,
                message: "private message cannot be empty".to_string(),
            })
        );

        drop(writer);
        drop(reader);
        join.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn invalid_signature_jwt_sends_auth_failed_and_closes_connection() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth: _auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let wrong_issuer = JwtAuthenticator::from_secret("wrong-secret").unwrap();
        let bad_token = wrong_issuer
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: bad_token,
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
    async fn expired_jwt_sends_auth_failed_and_closes_connection() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let expired_token = auth
            .encode_claims(&Claims {
                sub: "alice".to_string(),
                exp: now_secs().saturating_sub(1),
                iat: None,
                iss: None,
                role: Role::User,
            })
            .unwrap();

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: expired_token,
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
    async fn subject_mismatch_sends_auth_failed_and_closes_connection() {
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection();

        let alice_token = auth
            .issue_token("alice", Role::Admin, Duration::from_secs(60))
            .unwrap();

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "bob".to_string(),
                token: alice_token,
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
    async fn unauthenticated_timeout_closes_idle_client_but_authenticated_client_stays_open() {
        // 1. Unauthenticated client exceeds timeout and is disconnected.
        let TestConnection {
            writer: _idle_writer,
            reader: mut idle_reader,
            handle: _idle_handle,
            auth: _idle_auth,
            shutdown_tx: _idle_shutdown,
            join: idle_join,
        } = spawn_connection_with_timeout(Duration::from_millis(40));

        assert_eq!(
            idle_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthFailed(AuthFailedPayload)
        );
        assert_eq!(idle_reader.read_frame().await.unwrap(), None);
        assert_eq!(
            idle_join.await.unwrap().unwrap(),
            ConnectionState::Unauthenticated
        );

        // 2. Authenticated client remains connected past the 40ms auth timeout.
        let TestConnection {
            mut writer,
            mut reader,
            handle: _handle,
            auth,
            shutdown_tx: _shutdown_tx,
            join,
        } = spawn_connection_with_timeout(Duration::from_millis(40));

        let token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token,
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        tokio::time::sleep(Duration::from_millis(70)).await;
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
        assert_eq!(join.await.unwrap().unwrap().username(), Some("alice"));
    }

    #[tokio::test]
    async fn handle_push_delivers_frame_to_client() {
        let TestConnection {
            writer,
            mut reader,
            handle,
            auth: _auth,
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

        let mut conn = Connection::new(
            ConnectionId(2),
            test_addr(),
            server_read,
            server_write,
            test_authenticator(),
            RoomManager::new(),
            SessionRegistry::new(),
        );
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
            auth: _auth,
            shutdown_tx,
            join,
        } = spawn_connection();

        shutdown_tx.send(true).unwrap();

        assert_eq!(reader.read_frame().await.unwrap(), None);
        let state = join.await.unwrap().unwrap();
        assert_eq!(state, ConnectionState::Unauthenticated);
    }

    #[tokio::test]
    async fn join_room_replays_persisted_ciphertext_history_in_order() {
        let room_manager = RoomManager::new();
        let session_registry = SessionRegistry::new();
        let auth = test_authenticator();
        let storage = Arc::new(InMemoryStorage::new());
        let storage_trait: Arc<dyn ChatStorage> = storage.clone();

        let alice = spawn_connection_with_storage(
            ConnectionId(1),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            Arc::clone(&storage_trait),
            50,
        );
        let mut alice_writer = alice.writer;
        let mut alice_reader = alice.reader;

        let alice_token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        alice_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: alice_token,
            })))
            .await
            .unwrap();
        assert_eq!(
            alice_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        alice_writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
                room_id: 10,
            })))
            .await
            .unwrap();
        assert_eq!(
            alice_reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 10 })
        );

        let ciphertexts = ["cipher-msg-1", "cipher-msg-2", "cipher-msg-3"];
        for (idx, body) in ciphertexts.iter().enumerate() {
            alice_writer
                .write_frame(&Frame::new(Message::RoomMessage(RoomMessagePayload {
                    room_id: 10,
                    message: (*body).to_string(),
                })))
                .await
                .unwrap();
            match alice_reader.read_frame().await.unwrap().unwrap().message {
                Message::Ack(ack) => assert_eq!(ack.message_id, (idx + 1) as u64),
                other => panic!("expected Ack, got {other:?}"),
            }
        }

        // Verify opaque ciphertext bytes persisted inside storage.
        let persisted = storage.all_messages();
        assert_eq!(persisted.len(), 3);
        assert_eq!(persisted[0].ciphertext, b"cipher-msg-1");
        assert_eq!(persisted[2].ciphertext, b"cipher-msg-3");

        // Bob connects later and joins room 10 -> gets Ack(10) followed by chronological history.
        let bob = spawn_connection_with_storage(
            ConnectionId(2),
            room_manager,
            session_registry,
            Arc::clone(&auth),
            storage_trait,
            50,
        );
        let mut bob_writer = bob.writer;
        let mut bob_reader = bob.reader;

        let bob_token = auth
            .issue_token("bob", Role::User, Duration::from_secs(60))
            .unwrap();
        bob_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "bob".to_string(),
                token: bob_token,
            })))
            .await
            .unwrap();
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        bob_writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
                room_id: 10,
            })))
            .await
            .unwrap();
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 10 })
        );

        for expected in ciphertexts {
            assert_eq!(
                bob_reader.read_frame().await.unwrap().unwrap().message,
                Message::RoomMessage(RoomMessagePayload {
                    room_id: 10,
                    message: expected.to_string(),
                })
            );
        }

        drop((alice_writer, alice_reader, bob_writer, bob_reader));
        alice.join.await.unwrap().unwrap();
        bob.join.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn offline_private_messages_buffered_and_flushed_on_reconnect() {
        let room_manager = RoomManager::new();
        let session_registry = SessionRegistry::new();
        let auth = test_authenticator();
        let storage = Arc::new(InMemoryStorage::new());
        let storage_trait: Arc<dyn ChatStorage> = storage.clone();

        let alice = spawn_connection_with_storage(
            ConnectionId(1),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            Arc::clone(&storage_trait),
            50,
        );
        let mut alice_writer = alice.writer;
        let mut alice_reader = alice.reader;

        let alice_token = auth
            .issue_token("alice", Role::User, Duration::from_secs(60))
            .unwrap();
        alice_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: alice_token,
            })))
            .await
            .unwrap();
        assert_eq!(
            alice_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        // Bob is offline: Alice receives Acks and messages queue in offline_messages.
        for (idx, body) in ["offline-cipher-1", "offline-cipher-2"].iter().enumerate() {
            alice_writer
                .write_frame(&Frame::new(Message::PrivateMessage(
                    PrivateMessagePayload {
                        recipient: "bob".to_string(),
                        message: (*body).to_string(),
                    },
                )))
                .await
                .unwrap();
            match alice_reader.read_frame().await.unwrap().unwrap().message {
                Message::Ack(ack) => assert_eq!(ack.message_id, (idx + 1) as u64),
                other => panic!("expected Ack, got {other:?}"),
            }
        }
        assert_eq!(storage.offline_message_count("bob").await.unwrap(), 2);

        // Bob connects and authenticates -> receives AuthOk then both buffered DMs in order.
        let bob = spawn_connection_with_storage(
            ConnectionId(2),
            room_manager.clone(),
            session_registry.clone(),
            Arc::clone(&auth),
            Arc::clone(&storage_trait),
            50,
        );
        let mut bob_writer = bob.writer;
        let mut bob_reader = bob.reader;

        let bob_token = auth
            .issue_token("bob", Role::User, Duration::from_secs(60))
            .unwrap();
        bob_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "bob".to_string(),
                token: bob_token.clone(),
            })))
            .await
            .unwrap();
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );

        for expected in ["offline-cipher-1", "offline-cipher-2"] {
            assert_eq!(
                bob_reader.read_frame().await.unwrap().unwrap().message,
                Message::PrivateMessage(PrivateMessagePayload {
                    recipient: "bob".to_string(),
                    message: expected.to_string(),
                })
            );
        }
        assert_eq!(storage.offline_message_count("bob").await.unwrap(), 0);

        drop((bob_writer, bob_reader));
        bob.join.await.unwrap().unwrap();

        // Subsequent reconnect gets 0 duplicate replays.
        let bob_reconnect = spawn_connection_with_storage(
            ConnectionId(3),
            room_manager,
            session_registry,
            Arc::clone(&auth),
            storage_trait,
            50,
        );
        let mut bob_r2_writer = bob_reconnect.writer;
        let mut bob_r2_reader = bob_reconnect.reader;

        bob_r2_writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "bob".to_string(),
                token: bob_token,
            })))
            .await
            .unwrap();
        assert_eq!(
            bob_r2_reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthOk(AuthOkPayload)
        );
        bob_r2_writer
            .write_frame(&Frame::new(Message::Ping(PingPayload)))
            .await
            .unwrap();
        assert_eq!(
            bob_r2_reader.read_frame().await.unwrap().unwrap().message,
            Message::Pong(PongPayload)
        );

        drop((alice_writer, alice_reader, bob_r2_writer, bob_r2_reader));
        alice.join.await.unwrap().unwrap();
        bob_reconnect.join.await.unwrap().unwrap();
    }
}
