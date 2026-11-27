use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use auth::{Claims, JwtAuthenticator, Role};
use cluster::NodeInfo;
use config::ServerConfig;
use protocol::{
    AckPayload, AuthFailedPayload, AuthOkPayload, AuthPayload, Frame, HEADER_SIZE, JoinRoomPayload,
    LeaveRoomPayload, MAGIC, Message, MessageType, PingPayload, PongPayload, PrivateMessagePayload,
    RoomMessagePayload, Version,
};
use server::{
    ERR_NOT_FOUND, ERR_PROTOCOL_ERROR, ERR_UNAUTHORIZED, FrameReader, FrameWriter, RoomManager,
    Server, SessionRegistry,
};
use storage::{ChatStorage, InMemoryStorage};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;

struct TestServer {
    addr: SocketAddr,
    auth: Arc<JwtAuthenticator>,
    room_manager: RoomManager,
    session_registry: SessionRegistry,
    active_connections: Arc<AtomicUsize>,
    shutdown_tx: oneshot::Sender<()>,
    server_handle: tokio::task::JoinHandle<anyhow::Result<()>>,
}

async fn spawn_test_server() -> TestServer {
    spawn_test_server_internal(None, storage::DEFAULT_HISTORY_LIMIT).await
}

async fn spawn_test_server_with_storage(
    storage: Arc<dyn ChatStorage>,
    room_history_limit: usize,
) -> TestServer {
    spawn_test_server_internal(Some(storage), room_history_limit).await
}

async fn spawn_test_server_internal(
    storage: Option<Arc<dyn ChatStorage>>,
    room_history_limit: usize,
) -> TestServer {
    let config = ServerConfig {
        host: "127.0.0.1".to_string(),
        port: 0,
    };
    let node = NodeInfo::new("test-node".to_string(), "local".to_string(), 0);
    let auth = Arc::new(JwtAuthenticator::from_secret("integration-test-secret").unwrap());
    let mut server = Server::new(
        config,
        node,
        Arc::clone(&auth) as Arc<dyn auth::Authenticator>,
    )
    .with_room_history_limit(room_history_limit);
    if let Some(storage) = storage {
        server = server.with_storage(storage);
    }
    let listener = server.bind().await.unwrap();
    let addr = listener.local_addr().unwrap();
    let room_manager = server.room_manager().clone();
    let session_registry = server.session_registry().clone();
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
        room_manager,
        session_registry,
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

async fn connect_authenticated_client(
    addr: SocketAddr,
    auth: &JwtAuthenticator,
    username: &str,
) -> (
    FrameReader<tokio::net::tcp::OwnedReadHalf>,
    FrameWriter<tokio::net::tcp::OwnedWriteHalf>,
) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (read_half, write_half) = stream.into_split();
    let mut reader = FrameReader::new(read_half);
    let mut writer = FrameWriter::new(write_half);

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

    (reader, writer)
}

#[tokio::test]
async fn tcp_ping_pong_and_auth_lifecycle() {
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
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
        ..
    } = spawn_test_server().await;

    let mut clients = Vec::with_capacity(10);
    for idx in 0..10usize {
        let auth = Arc::clone(&auth);
        clients.push(tokio::spawn(async move {
            let username = format!("user-{idx}");
            let (mut reader, mut writer) =
                connect_authenticated_client(addr, &auth, &username).await;

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
        active_connections,
        shutdown_tx,
        server_handle,
        ..
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
        ..
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

#[tokio::test]
async fn tcp_room_broadcast_multi_room_isolation_and_leave() {
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
    } = spawn_test_server().await;

    let (mut alice_reader, mut alice_writer) =
        connect_authenticated_client(addr, &auth, "alice").await;
    let (mut bob_reader, mut bob_writer) = connect_authenticated_client(addr, &auth, "bob").await;
    let (mut carol_reader, mut carol_writer) =
        connect_authenticated_client(addr, &auth, "carol").await;

    // Alice joins room 1; Bob joins rooms 1 and 2; Carol joins room 2.
    alice_writer
        .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
            room_id: 1,
        })))
        .await
        .unwrap();
    assert_eq!(
        alice_reader.read_frame().await.unwrap().unwrap().message,
        Message::Ack(AckPayload { message_id: 1 })
    );

    for room_id in [1u64, 2u64] {
        bob_writer
            .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload { room_id })))
            .await
            .unwrap();
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload {
                message_id: room_id
            })
        );
    }

    carol_writer
        .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
            room_id: 2,
        })))
        .await
        .unwrap();
    assert_eq!(
        carol_reader.read_frame().await.unwrap().unwrap().message,
        Message::Ack(AckPayload { message_id: 2 })
    );

    // Alice broadcasts to room 1 -> Bob receives it; Carol does not.
    let room1_msg = RoomMessagePayload {
        room_id: 1,
        message: "room 1 hello".to_string(),
    };
    alice_writer
        .write_frame(&Frame::new(Message::RoomMessage(room1_msg.clone())))
        .await
        .unwrap();
    match alice_reader.read_frame().await.unwrap().unwrap().message {
        Message::Ack(ack) => assert!(ack.message_id >= 1),
        other => panic!("expected Ack, got {other:?}"),
    }
    assert_eq!(
        bob_reader.read_frame().await.unwrap().unwrap().message,
        Message::RoomMessage(room1_msg)
    );

    // Carol broadcasts to room 2 -> Bob receives it; verify Carol received no room 1 leak first.
    let room2_msg = RoomMessagePayload {
        room_id: 2,
        message: "room 2 hello".to_string(),
    };
    carol_writer
        .write_frame(&Frame::new(Message::RoomMessage(room2_msg.clone())))
        .await
        .unwrap();
    match carol_reader.read_frame().await.unwrap().unwrap().message {
        Message::Ack(ack) => assert!(ack.message_id >= 1),
        other => panic!("expected Ack, got {other:?}"),
    }
    assert_eq!(
        bob_reader.read_frame().await.unwrap().unwrap().message,
        Message::RoomMessage(room2_msg)
    );

    // Bob leaves room 1.
    bob_writer
        .write_frame(&Frame::new(Message::LeaveRoom(LeaveRoomPayload {
            room_id: 1,
        })))
        .await
        .unwrap();
    assert_eq!(
        bob_reader.read_frame().await.unwrap().unwrap().message,
        Message::Ack(AckPayload { message_id: 1 })
    );

    // Alice sends another message to room 1 -> Bob should not receive it.
    alice_writer
        .write_frame(&Frame::new(Message::RoomMessage(RoomMessagePayload {
            room_id: 1,
            message: "after bob left".to_string(),
        })))
        .await
        .unwrap();
    match alice_reader.read_frame().await.unwrap().unwrap().message {
        Message::Ack(ack) => assert!(ack.message_id >= 1),
        other => panic!("expected Ack, got {other:?}"),
    }

    bob_writer
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        bob_reader.read_frame().await.unwrap().unwrap().message,
        Message::Pong(PongPayload)
    );

    drop((
        alice_writer,
        alice_reader,
        bob_writer,
        bob_reader,
        carol_writer,
        carol_reader,
    ));
    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_disconnect_cleans_up_room_subscriptions() {
    let TestServer {
        addr,
        auth,
        room_manager,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
    } = spawn_test_server().await;

    let (mut r1, mut w1) = connect_authenticated_client(addr, &auth, "alice").await;
    let (mut r2, mut w2) = connect_authenticated_client(addr, &auth, "bob").await;

    for (r, w) in [(&mut r1, &mut w1), (&mut r2, &mut w2)] {
        w.write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
            room_id: 77,
        })))
        .await
        .unwrap();
        assert_eq!(
            r.read_frame().await.unwrap().unwrap().message,
            Message::Ack(AckPayload { message_id: 77 })
        );
    }
    assert_eq!(room_manager.subscriber_count(77), 2);
    assert_eq!(room_manager.room_count(), 1);

    drop((w1, r1));
    tokio::time::timeout(Duration::from_secs(2), async {
        while room_manager.subscriber_count(77) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for first client's room cleanup");

    drop((w2, r2));
    tokio::time::timeout(Duration::from_secs(2), async {
        while room_manager.room_count() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for second client's room cleanup");

    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_private_message_routing_multi_device_and_offline_404() {
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
    } = spawn_test_server().await;

    let (mut alice_laptop_reader, mut alice_laptop_writer) =
        connect_authenticated_client(addr, &auth, "alice").await;
    let (mut alice_phone_reader, alice_phone_writer) =
        connect_authenticated_client(addr, &auth, "alice").await;
    let (mut bob_reader, mut bob_writer) = connect_authenticated_client(addr, &auth, "bob").await;
    let (mut carol_reader, mut carol_writer) =
        connect_authenticated_client(addr, &auth, "carol").await;

    // 1. Bob sends a DM to Alice -> Bob gets Ack, both Alice devices receive DM, Carol receives nothing.
    let bob_to_alice = PrivateMessagePayload {
        recipient: "alice".to_string(),
        message: "hi alice on both devices".to_string(),
    };
    bob_writer
        .write_frame(&Frame::new(Message::PrivateMessage(bob_to_alice.clone())))
        .await
        .unwrap();
    match bob_reader.read_frame().await.unwrap().unwrap().message {
        Message::Ack(ack) => assert!(ack.message_id >= 1),
        other => panic!("expected Ack, got {other:?}"),
    }
    assert_eq!(
        alice_laptop_reader
            .read_frame()
            .await
            .unwrap()
            .unwrap()
            .message,
        Message::PrivateMessage(bob_to_alice.clone())
    );
    assert_eq!(
        alice_phone_reader
            .read_frame()
            .await
            .unwrap()
            .unwrap()
            .message,
        Message::PrivateMessage(bob_to_alice)
    );

    carol_writer
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        carol_reader.read_frame().await.unwrap().unwrap().message,
        Message::Pong(PongPayload)
    );

    // 2. Alice laptop sends a DM to Alice -> laptop gets Ack (no self-echo), phone receives DM.
    let laptop_to_phone = PrivateMessagePayload {
        recipient: "alice".to_string(),
        message: "sync note".to_string(),
    };
    alice_laptop_writer
        .write_frame(&Frame::new(Message::PrivateMessage(
            laptop_to_phone.clone(),
        )))
        .await
        .unwrap();
    match alice_laptop_reader
        .read_frame()
        .await
        .unwrap()
        .unwrap()
        .message
    {
        Message::Ack(ack) => assert!(ack.message_id >= 1),
        other => panic!("expected Ack, got {other:?}"),
    }
    assert_eq!(
        alice_phone_reader
            .read_frame()
            .await
            .unwrap()
            .unwrap()
            .message,
        Message::PrivateMessage(laptop_to_phone)
    );

    alice_laptop_writer
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        alice_laptop_reader
            .read_frame()
            .await
            .unwrap()
            .unwrap()
            .message,
        Message::Pong(PongPayload)
    );

    // 3. Bob sends a DM to offline user -> Error(404).
    bob_writer
        .write_frame(&Frame::new(Message::PrivateMessage(
            PrivateMessagePayload {
                recipient: "offline-user".to_string(),
                message: "hello?".to_string(),
            },
        )))
        .await
        .unwrap();
    match bob_reader.read_frame().await.unwrap().unwrap().message {
        Message::Error(err) => assert_eq!(err.code, ERR_NOT_FOUND),
        other => panic!("expected Error(404), got {other:?}"),
    }

    drop((
        alice_laptop_writer,
        alice_laptop_reader,
        alice_phone_writer,
        alice_phone_reader,
        bob_writer,
        bob_reader,
        carol_writer,
        carol_reader,
    ));
    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_disconnect_cleans_up_session_registry_preserving_other_devices() {
    let TestServer {
        addr,
        auth,
        session_registry,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
    } = spawn_test_server().await;

    let (r1, w1) = connect_authenticated_client(addr, &auth, "alice").await;
    let (r2, w2) = connect_authenticated_client(addr, &auth, "alice").await;

    assert_eq!(session_registry.session_count_for("alice"), 2);
    assert_eq!(session_registry.online_user_count(), 1);

    drop((w1, r1));
    tokio::time::timeout(Duration::from_secs(2), async {
        while session_registry.session_count_for("alice") != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for first device cleanup");
    assert!(session_registry.is_online("alice"));

    drop((w2, r2));
    tokio::time::timeout(Duration::from_secs(2), async {
        while session_registry.is_online("alice") {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for second device cleanup");
    assert_eq!(session_registry.online_user_count(), 0);

    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_room_join_replays_persisted_ciphertext_history() {
    let storage = Arc::new(InMemoryStorage::new());
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
    } = spawn_test_server_with_storage(storage.clone() as Arc<dyn ChatStorage>, 2).await;

    let (mut alice_reader, mut alice_writer) =
        connect_authenticated_client(addr, &auth, "alice").await;

    alice_writer
        .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
            room_id: 55,
        })))
        .await
        .unwrap();
    assert_eq!(
        alice_reader.read_frame().await.unwrap().unwrap().message,
        Message::Ack(AckPayload { message_id: 55 })
    );

    // Send 3 ciphertexts while room_history_limit is 2.
    for body in ["cipher-old", "cipher-mid", "cipher-new"] {
        alice_writer
            .write_frame(&Frame::new(Message::RoomMessage(RoomMessagePayload {
                room_id: 55,
                message: body.to_string(),
            })))
            .await
            .unwrap();
        match alice_reader.read_frame().await.unwrap().unwrap().message {
            Message::Ack(ack) => assert!(ack.message_id >= 1),
            other => panic!("expected Ack, got {other:?}"),
        }
    }

    assert_eq!(storage.all_messages().len(), 3);

    // Bob joins late -> receives Ack(55) followed by latest 2 messages in chronological order.
    let (mut bob_reader, mut bob_writer) = connect_authenticated_client(addr, &auth, "bob").await;
    bob_writer
        .write_frame(&Frame::new(Message::JoinRoom(JoinRoomPayload {
            room_id: 55,
        })))
        .await
        .unwrap();
    assert_eq!(
        bob_reader.read_frame().await.unwrap().unwrap().message,
        Message::Ack(AckPayload { message_id: 55 })
    );

    for expected in ["cipher-mid", "cipher-new"] {
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::RoomMessage(RoomMessagePayload {
                room_id: 55,
                message: expected.to_string(),
            })
        );
    }

    drop((alice_writer, alice_reader, bob_writer, bob_reader));
    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}

#[tokio::test]
async fn tcp_offline_private_messages_flushed_on_reconnect() {
    let storage = Arc::new(InMemoryStorage::new());
    let TestServer {
        addr,
        auth,
        active_connections,
        shutdown_tx,
        server_handle,
        ..
    } = spawn_test_server_with_storage(storage.clone() as Arc<dyn ChatStorage>, 50).await;

    let (mut alice_reader, mut alice_writer) =
        connect_authenticated_client(addr, &auth, "alice").await;

    for body in ["queued-cipher-1", "queued-cipher-2"] {
        alice_writer
            .write_frame(&Frame::new(Message::PrivateMessage(
                PrivateMessagePayload {
                    recipient: "bob".to_string(),
                    message: body.to_string(),
                },
            )))
            .await
            .unwrap();
        match alice_reader.read_frame().await.unwrap().unwrap().message {
            Message::Ack(ack) => assert!(ack.message_id >= 1),
            other => panic!("expected Ack, got {other:?}"),
        }
    }
    assert_eq!(storage.offline_message_count("bob").await.unwrap(), 2);

    // Bob connects and authenticates -> immediately receives queued DMs after AuthOk.
    let (mut bob_reader, bob_writer) = connect_authenticated_client(addr, &auth, "bob").await;
    for expected in ["queued-cipher-1", "queued-cipher-2"] {
        assert_eq!(
            bob_reader.read_frame().await.unwrap().unwrap().message,
            Message::PrivateMessage(PrivateMessagePayload {
                recipient: "bob".to_string(),
                message: expected.to_string(),
            })
        );
    }
    assert_eq!(storage.offline_message_count("bob").await.unwrap(), 0);

    // Reconnecting again yields no duplicate replays.
    drop((bob_writer, bob_reader));
    let (mut bob2_reader, mut bob2_writer) = connect_authenticated_client(addr, &auth, "bob").await;
    bob2_writer
        .write_frame(&Frame::new(Message::Ping(PingPayload)))
        .await
        .unwrap();
    assert_eq!(
        bob2_reader.read_frame().await.unwrap().unwrap().message,
        Message::Pong(PongPayload)
    );

    drop((alice_writer, alice_reader, bob2_writer, bob2_reader));
    wait_for_zero_connections(&active_connections).await;

    shutdown_tx.send(()).unwrap();
    server_handle.await.unwrap().unwrap();
}
