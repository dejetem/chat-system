use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use auth::{Claims, JwtAuthenticator, Role};
use cluster::NodeInfo;
use config::ServerConfig;
use protocol::{
    AuthFailedPayload, AuthOkPayload, AuthPayload, Frame, HEADER_SIZE, JoinRoomPayload, MAGIC,
    Message, MessageType, PingPayload, PongPayload, Version,
};
use server::{ERR_PROTOCOL_ERROR, ERR_UNAUTHORIZED, FrameReader, FrameWriter, Server};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

struct TestServer {
    addr: SocketAddr,
    auth: Arc<JwtAuthenticator>,
    active_connections: Arc<AtomicUsize>,
    shutdown_tx: oneshot::Sender<()>,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn spawn_test_server() -> TestServer {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
    };
    let node = NodeInfo::new("test-node".to_string(), "local".to_string(), 0);
    let auth = Arc::new(JwtAuthenticator::from_secret("integration-test-secret").unwrap());
    let server = Server::new(
        config,
        node,
        Arc::clone(&auth) as Arc<dyn auth::Authenticator>,
    );
    let listener = server.bind().await.unwrap();
    let addr = listener.local_addr().unwrap();
    let active_connections = server.active_connections_counter();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let server_handle = tokio::spawn(async move {
        server
            .run_with_listener(listener, async move {
                let _ = shutdown_rx.await;
            })
            .await
    });

    TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
    }
}

async fn wait_for_zero_connections(counter: &AtomicUsize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while counter.load(Ordering::Relaxed) != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for active_connections to return to 0");
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn tcp_ping_pong_and_auth_lifecycle() {
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
    } = spawn_test_server().await;

    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    let mut reader = FrameReader::new(read_half);
    let mut writer = FrameWriter::new(write_half);

    writer
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        reader.read_frame().await.unwrap().unwrap().message,
        Message::Pong(PongPayload)
    );
    assert_eq!(active_connections.load(Ordering::Relaxed), 1);

    writer
        .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
            room_id: 42,
        })))
        .await
        .unwrap();
    match reader.read_frame().await.unwrap().unwrap().message {
        Message::Error(err) => assert_eq!(err.code, ERR_UNAUTHORIZED),
        other => panic!("expected Error(401), got {other:?}"),
    }

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
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        reader.read_frame().await.unwrap().unwrap().message,
        Message::Pong(PongPayload)
    );

    drop(writer);
    drop(reader);
    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_multiple_concurrent_clients() {
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
    } = spawn_test_server().await;

    let mut clients = Vec::with_capacity(10);
    for idx in 0..10usize {
        let auth = Arc::clone(&auth);
        clients.push(tokio::spawn(async move {
            let stream = TcpStream::connect(addr).await.unwrap();
            let (read_half, write_half) = stream.into_split();
            let mut reader = FrameReader::new(read_half);
            let mut writer = FrameWriter::new(write_half);

            let username = format!("user-{idx}");
            let token = auth
                .issue_token(&username, Role::User, Duration::from_secs(60))
                .unwrap();
            writer
                .write_frame(&Frame::new(Message::Auth(AuthPayload { username, token })))
                .await
                .unwrap();
            assert_eq!(
                reader.read_frame().await.unwrap().unwrap().message,
                Message::AuthOk(AuthOkPayload)
            );

            for _ in 0..3 {
                writer
                    .write_frame(&Frame::new(Message::Ping(PingPayload)))
                    .await
                    .unwrap();
                assert_eq!(
                    reader.read_frame().await.unwrap().unwrap().message,
                    Message::Pong(PongPayload)
                );
            }
        }));
    }

    for client in clients {
        client.await.unwrap();
    }

    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_malformed_client_isolated_from_healthy_clients() {
    let TestServer {
        addr,
        auth: _auth,
        active_connections,
        shutdown_tx,
        server_handle,
    } = spawn_test_server().await;

    let bad_stream = TcpStream::connect(addr).await.unwrap();
    let (bad_read, mut bad_write) = bad_stream.into_split();
    let mut bad_reader = FrameReader::new(bad_read);

    let mut bad_header = [0u8; HEADER_SIZE];
    bad_header[0..2].copy_from_slice(&(MAGIC ^ 0xFFFF).to_be_bytes());
    bad_header[2] = Version::V1.as_u8();
    bad_header[3] = MessageType::Ping as u8;
    bad_write.write_all(&bad_header).await.unwrap();
    bad_write.flush().await.unwrap();

    match bad_reader.read_frame().await.unwrap().unwrap().message {
        Message::Error(err) => assert_eq!(err.code, ERR_PROTOCOL_ERROR),
        other => panic!("expected Error(422), got {other:?}"),
    }
    assert_eq!(bad_reader.read_frame().await.unwrap(), None);
    drop(bad_write);
    drop(bad_reader);

    let healthy_stream = TcpStream::connect(addr).await.unwrap();
    let (healthy_read, healthy_write) = healthy_stream.into_split();
    let mut healthy_reader = FrameReader::new(healthy_read);
    let mut healthy_writer = FrameWriter::new(healthy_write);

    healthy_writer
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        healthy_reader.read_frame().await.unwrap().unwrap().message,
        Message::Pong(PongPayload)
    );

    drop(healthy_writer);
    drop(healthy_reader);
    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_invalid_and_expired_jwt_rejected_and_dropped() {
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
    } = spawn_test_server().await;

    // 1. Forged / garbage token -> AuthFailed + EOF.
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let mut reader = FrameReader::new(read_half);
        let mut writer = FrameWriter::new(write_half);

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: "not.a.valid.jwt".to_string(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthFailed(AuthFailedPayload)
        );
        assert_eq!(reader.read_frame().await.unwrap(), None);
    }

    // 2. Expired JWT -> AuthFailed + EOF.
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let mut reader = FrameReader::new(read_half);
        let mut writer = FrameWriter::new(write_half);

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
    }

    // 3. Username mismatch ("mallory" using "alice"'s token) -> AuthFailed + EOF.
    let valid_alice_token = auth
        .issue_token("alice", Role::Admin, Duration::from_secs(60))
        .unwrap();
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let mut reader = FrameReader::new(read_half);
        let mut writer = FrameWriter::new(write_half);

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "mallory".to_string(),
                token: valid_alice_token.clone(),
            })))
            .await
            .unwrap();
        assert_eq!(
            reader.read_frame().await.unwrap().unwrap().message,
            Message::AuthFailed(AuthFailedPayload)
        );
        assert_eq!(reader.read_frame().await.unwrap(), None);
    }

    // 4. Valid signed JWT for "alice" -> AuthOk and stays connected.
    {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (read_half, write_half) = stream.into_split();
        let mut reader = FrameReader::new(read_half);
        let mut writer = FrameWriter::new(write_half);

        writer
            .write_frame(&Frame::new(Message::Auth(AuthPayload {
                username: "alice".to_string(),
                token: valid_alice_token,
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
    }

    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}
