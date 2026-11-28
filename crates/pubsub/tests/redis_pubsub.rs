use config::RedisConfig;
use pubsub::{PubSubBackend, PubSubEnvelope, RedisPubSub};
use tokio::time::{Duration, timeout};

fn test_redis_url() -> Option<String> {
    if let Ok(url) = std::env::var("TEST_REDIS_URL") {
        let trimmed = url.trim().to_string();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    if std::env::var("RUN_REDIS_TESTS").as_deref() == Ok("1")
        && let Ok(url) = std::env::var("REDIS_URL")
    {
        let trimmed = url.trim().to_string();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    None
}

#[tokio::test]
async fn redis_cross_node_room_and_direct_pubsub_and_shutdown_cleanup() {
    let Some(redis_url) = test_redis_url() else {
        return;
    };

    let prefix = format!(
        "chat-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let mut cfg = RedisConfig::from_url(redis_url);
    cfg.key_prefix = prefix;

    let node_a = RedisPubSub::connect(&cfg, "node-a").await.unwrap();
    let node_b = RedisPubSub::connect(&cfg, "node-b").await.unwrap();
    let mut rx_b = node_b.subscribe_events();

    assert!(node_b.subscribe_room(88).await.unwrap());
    let room_msg = PubSubEnvelope::RoomBroadcast {
        origin_node_id: "node-a".to_string(),
        room_id: 88,
        sender_username: "alice".to_string(),
        message_id: 1,
        message: "cipher-cross-node-room".to_string(),
    };
    let receivers = node_a.publish_room(&room_msg).await.unwrap();
    assert!(receivers >= 1);
    let got_room = timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got_room, room_msg);

    // Register bob on node-b and route a direct message from node-a -> node-b.
    assert!(node_b.register_user_session("bob").await.unwrap());
    assert_eq!(
        node_a.lookup_user_nodes("bob").await.unwrap(),
        vec!["node-b".to_string()]
    );
    let dm_msg = PubSubEnvelope::DirectMessage {
        origin_node_id: "node-a".to_string(),
        target_node_id: "node-b".to_string(),
        sender_username: "alice".to_string(),
        recipient_username: "bob".to_string(),
        message_id: Some(2),
        message: "cipher-cross-node-dm".to_string(),
    };
    let dm_receivers = node_a.publish_direct("node-b", &dm_msg).await.unwrap();
    assert!(dm_receivers >= 1);
    let got_dm = timeout(Duration::from_secs(2), rx_b.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got_dm, dm_msg);

    node_b.shutdown().await.unwrap();
    assert!(node_a.lookup_user_nodes("bob").await.unwrap().is_empty());
    node_a.shutdown().await.unwrap();
}
