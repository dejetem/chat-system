use config::DatabaseConfig;
use storage::{ChatStorage, NewPrivateMessage, NewRoomMessage, PgStorage};
use uuid::Uuid;

fn test_database_url() -> Option<String> {
    if let Ok(url) = std::env::var("TEST_DATABASE_URL") {
        let trimmed = url.trim().to_string();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    if std::env::var("RUN_POSTGRES_TESTS").as_deref() == Ok("1")
        && let Ok(url) = std::env::var("DATABASE_URL")
    {
        let trimmed = url.trim().to_string();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    None
}

#[tokio::test]
async fn postgres_migrations_room_ciphertext_history_and_offline_flush() {
    let Some(database_url) = test_database_url() else {
        return;
    };

    let storage = PgStorage::connect_with_config(&DatabaseConfig::from_url(database_url))
        .await
        .expect("failed to connect to test Postgres database");
    storage.migrate().await.expect("failed to run migrations");

    let tag = Uuid::new_v4().simple().to_string();
    let alice = format!("alice-{tag}");
    let bob = format!("bob-{tag}");
    let room_id =
        900_000_000 + u64::from(u32::from_be_bytes(tag.as_bytes()[..4].try_into().unwrap()));

    let stored_alice = storage.upsert_user(&alice, "user").await.unwrap();
    let stored_bob = storage.upsert_user(&bob, "moderator").await.unwrap();
    assert_eq!(stored_bob.role, "moderator");

    storage
        .ensure_room(room_id, Some("encrypted-test-room"), Some(stored_alice.id))
        .await
        .unwrap();

    // Verify arbitrary opaque non-UTF8 ciphertext round-trips untouched via BYTEA.
    let raw_cipher_1 = vec![0xca, 0xfe, 0x00, 0x01, 0xff];
    let raw_cipher_2 = vec![0xde, 0xad, 0xbe, 0xef];
    storage
        .store_room_message(NewRoomMessage {
            sender_username: alice.clone(),
            room_id,
            ciphertext: raw_cipher_1.clone(),
            media_key: None,
        })
        .await
        .unwrap();
    storage
        .store_room_message(NewRoomMessage {
            sender_username: bob.clone(),
            room_id,
            ciphertext: raw_cipher_2.clone(),
            media_key: Some("attachments/room-blob.bin".to_string()),
        })
        .await
        .unwrap();

    let history = storage.fetch_room_history(room_id, 10).await.unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].ciphertext, raw_cipher_1);
    assert_eq!(history[1].ciphertext, raw_cipher_2);
    assert_eq!(
        history[1].media_key.as_deref(),
        Some("attachments/room-blob.bin")
    );

    storage
        .store_private_message(NewPrivateMessage {
            sender_username: alice.clone(),
            recipient_username: bob.clone(),
            ciphertext: b"offline-dm-cipher".to_vec(),
            media_key: None,
            queue_offline: true,
        })
        .await
        .unwrap();
    assert_eq!(storage.offline_message_count(&bob).await.unwrap(), 1);

    let flushed = storage.flush_offline_messages(&bob).await.unwrap();
    assert_eq!(flushed.len(), 1);
    assert_eq!(flushed[0].ciphertext, b"offline-dm-cipher");
    assert_eq!(storage.offline_message_count(&bob).await.unwrap(), 0);

    // Clean up test rows.
    let room_id_i64 = i64::try_from(room_id).unwrap();
    sqlx::query("DELETE FROM rooms WHERE id = $1")
        .bind(room_id_i64)
        .execute(storage.pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM users WHERE username IN ($1, $2)")
        .bind(&alice)
        .bind(&bob)
        .execute(storage.pool())
        .await
        .unwrap();
}
