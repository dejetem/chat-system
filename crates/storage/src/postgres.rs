use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use config::DatabaseConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::{FromRow, PgConnection, PgPool};

use crate::error::StorageError;
use crate::models::{
    NewPrivateMessage, NewRoomMessage, StoredMessage, StoredRoom, StoredUser, clamp_history_limit,
    default_room_name, to_u64_id, validate_ciphertext, validate_optional_media_key, validate_role,
    validate_room_id, validate_room_name, validate_username,
};
use crate::repository::ChatStorage;

#[derive(Debug, FromRow)]
struct UserRow {
    id: i64,
    username: String,
    role: String,
    public_key: Option<Vec<u8>>,
    created_at: DateTime<Utc>,
}

impl From<UserRow> for StoredUser {
    fn from(row: UserRow) -> Self {
        Self {
            id: row.id,
            username: row.username,
            role: row.role,
            public_key: row.public_key,
            created_at: row.created_at,
        }
    }
}

#[derive(Debug, FromRow)]
struct RoomRow {
    id: i64,
    name: String,
    created_by: Option<i64>,
    created_at: DateTime<Utc>,
}

impl TryFrom<RoomRow> for StoredRoom {
    type Error = StorageError;

    fn try_from(row: RoomRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: to_u64_id(row.id, "rooms.id")?,
            name: row.name,
            created_by: row.created_by,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct MessageRow {
    message_id: i64,
    sender_id: i64,
    sender_username: String,
    room_id: Option<i64>,
    recipient_id: Option<i64>,
    recipient_username: Option<String>,
    body: Vec<u8>,
    media_key: Option<String>,
    created_at: DateTime<Utc>,
}

impl TryFrom<MessageRow> for StoredMessage {
    type Error = StorageError;

    fn try_from(row: MessageRow) -> Result<Self, Self::Error> {
        Ok(Self {
            message_id: to_u64_id(row.message_id, "messages.message_id")?,
            sender_id: row.sender_id,
            sender_username: row.sender_username,
            room_id: row
                .room_id
                .map(|id| to_u64_id(id, "messages.room_id"))
                .transpose()?,
            recipient_id: row.recipient_id,
            recipient_username: row.recipient_username,
            ciphertext: row.body,
            media_key: row.media_key,
            created_at: row.created_at,
        })
    }
}

/// PostgreSQL-backed implementation of [`ChatStorage`] using SQLx runtime queries.
#[derive(Debug, Clone)]
pub struct PgStorage {
    pool: PgPool,
}

impl PgStorage {
    /// Connects a PostgreSQL pool to `database_url` using default pool settings.
    pub async fn connect(database_url: &str) -> Result<Self, StorageError> {
        let cfg = DatabaseConfig::from_url(database_url);
        Self::connect_with_config(&cfg).await
    }

    /// Connects a PostgreSQL pool configured by [`DatabaseConfig`].
    pub async fn connect_with_config(cfg: &DatabaseConfig) -> Result<Self, StorageError> {
        let pool = PgPoolOptions::new()
            .max_connections(cfg.max_connections)
            .acquire_timeout(Duration::from_secs(cfg.acquire_timeout_secs))
            .connect(&cfg.url)
            .await?;

        Ok(Self { pool })
    }

    /// Wraps an existing [`PgPool`].
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Returns a reference to the underlying [`PgPool`].
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs embedded SQLx schema migrations from `crates/storage/migrations`.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }
}

async fn select_user_by_username(
    conn: &mut PgConnection,
    username: &str,
) -> Result<Option<UserRow>, StorageError> {
    let row = sqlx::query_as::<_, UserRow>(
        "SELECT id, username, role, public_key, created_at FROM users WHERE username = $1",
    )
    .bind(username)
    .fetch_optional(&mut *conn)
    .await?;
    Ok(row)
}

/// Resolves a user ID using a read-first pattern to avoid write locks on hot paths.
async fn ensure_user_id(conn: &mut PgConnection, username: &str) -> Result<i64, StorageError> {
    if let Some(id) = sqlx::query_scalar::<_, i64>("SELECT id FROM users WHERE username = $1")
        .bind(username)
        .fetch_optional(&mut *conn)
        .await?
    {
        return Ok(id);
    }

    if let Some(id) = sqlx::query_scalar::<_, i64>(
        "INSERT INTO users (username, role) VALUES ($1, 'user') \
         ON CONFLICT (username) DO NOTHING \
         RETURNING id",
    )
    .bind(username)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(id);
    }

    let id = sqlx::query_scalar::<_, i64>("SELECT id FROM users WHERE username = $1")
        .bind(username)
        .fetch_one(&mut *conn)
        .await?;
    Ok(id)
}

async fn ensure_room_row(
    conn: &mut PgConnection,
    room_id: i64,
    name: &str,
    explicit_name: bool,
    created_by: Option<i64>,
) -> Result<RoomRow, StorageError> {
    if let Some(existing) = sqlx::query_as::<_, RoomRow>(
        "SELECT id, name, created_by, created_at FROM rooms WHERE id = $1",
    )
    .bind(room_id)
    .fetch_optional(&mut *conn)
    .await?
    {
        if explicit_name && existing.name != name {
            let updated = sqlx::query_as::<_, RoomRow>(
                "UPDATE rooms SET name = $2 WHERE id = $1 RETURNING id, name, created_by, created_at",
            )
            .bind(room_id)
            .bind(name)
            .fetch_one(&mut *conn)
            .await?;
            return Ok(updated);
        }
        return Ok(existing);
    }

    if let Some(inserted) = sqlx::query_as::<_, RoomRow>(
        "INSERT INTO rooms (id, name, created_by) VALUES ($1, $2, $3) \
         ON CONFLICT (id) DO NOTHING \
         RETURNING id, name, created_by, created_at",
    )
    .bind(room_id)
    .bind(name)
    .bind(created_by)
    .fetch_optional(&mut *conn)
    .await?
    {
        return Ok(inserted);
    }

    let existing = sqlx::query_as::<_, RoomRow>(
        "SELECT id, name, created_by, created_at FROM rooms WHERE id = $1",
    )
    .bind(room_id)
    .fetch_one(&mut *conn)
    .await?;
    Ok(existing)
}

#[async_trait]
impl ChatStorage for PgStorage {
    async fn upsert_user(&self, username: &str, role: &str) -> Result<StoredUser, StorageError> {
        let username = validate_username(username)?;
        let role = validate_role(role)?;

        let mut conn = self.pool.acquire().await?;
        if let Some(existing) = select_user_by_username(&mut conn, &username).await?
            && existing.role == role
        {
            return Ok(existing.into());
        }

        let row = sqlx::query_as::<_, UserRow>(
            "INSERT INTO users (username, role) VALUES ($1, $2) \
             ON CONFLICT (username) DO UPDATE SET role = EXCLUDED.role \
             RETURNING id, username, role, public_key, created_at",
        )
        .bind(&username)
        .bind(&role)
        .fetch_one(&mut *conn)
        .await?;

        Ok(row.into())
    }

    async fn set_user_public_key(
        &self,
        username: &str,
        public_key: Vec<u8>,
    ) -> Result<StoredUser, StorageError> {
        let username = validate_username(username)?;
        if public_key.is_empty() {
            return Err(StorageError::Validation(
                "public_key cannot be empty".to_string(),
            ));
        }

        let row = sqlx::query_as::<_, UserRow>(
            "UPDATE users SET public_key = $2 WHERE username = $1 \
             RETURNING id, username, role, public_key, created_at",
        )
        .bind(&username)
        .bind(&public_key)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StorageError::NotFound(format!("user '{username}' not found")))?;

        Ok(row.into())
    }

    async fn get_user_by_username(
        &self,
        username: &str,
    ) -> Result<Option<StoredUser>, StorageError> {
        let username = validate_username(username)?;
        let mut conn = self.pool.acquire().await?;
        let row = select_user_by_username(&mut conn, &username).await?;
        Ok(row.map(Into::into))
    }

    async fn ensure_room(
        &self,
        room_id: u64,
        name: Option<&str>,
        created_by: Option<i64>,
    ) -> Result<StoredRoom, StorageError> {
        let room_id_i64 = validate_room_id(room_id)?;
        let explicit_name = name.is_some();
        let resolved_name = match name {
            Some(raw) => validate_room_name(raw)?,
            None => default_room_name(room_id),
        };

        let mut conn = self.pool.acquire().await?;
        let row = ensure_room_row(
            &mut conn,
            room_id_i64,
            &resolved_name,
            explicit_name,
            created_by,
        )
        .await?;
        row.try_into()
    }

    async fn store_room_message(&self, msg: NewRoomMessage) -> Result<StoredMessage, StorageError> {
        let sender_username = validate_username(&msg.sender_username)?;
        let room_id_i64 = validate_room_id(msg.room_id)?;
        validate_ciphertext(&msg.ciphertext)?;
        let media_key = validate_optional_media_key(msg.media_key.as_deref())?;
        let fallback_room_name = default_room_name(msg.room_id);

        let mut tx = self.pool.begin().await?;
        let sender_id = ensure_user_id(&mut tx, &sender_username).await?;
        ensure_room_row(
            &mut tx,
            room_id_i64,
            &fallback_room_name,
            false,
            Some(sender_id),
        )
        .await?;

        let (message_id, created_at) = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
            "INSERT INTO messages (sender_id, room_id, body, media_key) \
             VALUES ($1, $2, $3, $4) \
             RETURNING message_id, created_at",
        )
        .bind(sender_id)
        .bind(room_id_i64)
        .bind(&msg.ciphertext)
        .bind(&media_key)
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(StoredMessage {
            message_id: to_u64_id(message_id, "messages.message_id")?,
            sender_id,
            sender_username,
            room_id: Some(msg.room_id),
            recipient_id: None,
            recipient_username: None,
            ciphertext: msg.ciphertext,
            media_key,
            created_at,
        })
    }

    async fn fetch_room_history(
        &self,
        room_id: u64,
        limit: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        let room_id_i64 = validate_room_id(room_id)?;
        let capped = clamp_history_limit(limit);
        if capped == 0 {
            return Ok(Vec::new());
        }
        let limit_i64 = i64::try_from(capped)
            .map_err(|_| StorageError::Validation("history limit out of range".to_string()))?;

        let rows = sqlx::query_as::<_, MessageRow>(
            "SELECT message_id, sender_id, sender_username, room_id, recipient_id, \
                    recipient_username, body, media_key, created_at \
             FROM ( \
                 SELECT \
                     m.message_id, \
                     m.sender_id, \
                     su.username AS sender_username, \
                     m.room_id, \
                     m.recipient_id, \
                     CAST(NULL AS TEXT) AS recipient_username, \
                     m.body, \
                     m.media_key, \
                     m.created_at \
                 FROM messages m \
                 INNER JOIN users su ON su.id = m.sender_id \
                 WHERE m.room_id = $1 \
                 ORDER BY m.message_id DESC \
                 LIMIT $2 \
             ) sub \
             ORDER BY sub.message_id ASC",
        )
        .bind(room_id_i64)
        .bind(limit_i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn store_private_message(
        &self,
        msg: NewPrivateMessage,
    ) -> Result<StoredMessage, StorageError> {
        let sender_username = validate_username(&msg.sender_username)?;
        let recipient_username = validate_username(&msg.recipient_username)?;
        validate_ciphertext(&msg.ciphertext)?;
        let media_key = validate_optional_media_key(msg.media_key.as_deref())?;

        let mut tx = self.pool.begin().await?;
        let sender_id = ensure_user_id(&mut tx, &sender_username).await?;
        let recipient_id = ensure_user_id(&mut tx, &recipient_username).await?;

        let (message_id, created_at) = sqlx::query_as::<_, (i64, DateTime<Utc>)>(
            "INSERT INTO messages (sender_id, recipient_id, body, media_key) \
             VALUES ($1, $2, $3, $4) \
             RETURNING message_id, created_at",
        )
        .bind(sender_id)
        .bind(recipient_id)
        .bind(&msg.ciphertext)
        .bind(&media_key)
        .fetch_one(&mut *tx)
        .await?;

        if msg.queue_offline {
            sqlx::query(
                "INSERT INTO offline_messages (recipient_id, message_id) \
                 VALUES ($1, $2) \
                 ON CONFLICT (recipient_id, message_id) DO NOTHING",
            )
            .bind(recipient_id)
            .bind(message_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(StoredMessage {
            message_id: to_u64_id(message_id, "messages.message_id")?,
            sender_id,
            sender_username,
            room_id: None,
            recipient_id: Some(recipient_id),
            recipient_username: Some(recipient_username),
            ciphertext: msg.ciphertext,
            media_key,
            created_at,
        })
    }

    async fn flush_offline_messages(
        &self,
        recipient_username: &str,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        let recipient_username = validate_username(recipient_username)?;

        let rows = sqlx::query_as::<_, MessageRow>(
            "WITH popped AS ( \
                 DELETE FROM offline_messages om \
                 USING users ru \
                 WHERE om.recipient_id = ru.id \
                   AND ru.username = $1 \
                 RETURNING om.message_id, ru.username AS recipient_username \
             ) \
             SELECT \
                 m.message_id, \
                 m.sender_id, \
                 su.username AS sender_username, \
                 m.room_id, \
                 m.recipient_id, \
                 p.recipient_username, \
                 m.body, \
                 m.media_key, \
                 m.created_at \
             FROM popped p \
             INNER JOIN messages m ON m.message_id = p.message_id \
             INNER JOIN users su ON su.id = m.sender_id \
             ORDER BY m.message_id ASC",
        )
        .bind(&recipient_username)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    async fn offline_message_count(&self, recipient_username: &str) -> Result<usize, StorageError> {
        let recipient_username = validate_username(recipient_username)?;
        let count = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)::BIGINT \
             FROM offline_messages om \
             INNER JOIN users u ON u.id = om.recipient_id \
             WHERE u.username = $1",
        )
        .bind(&recipient_username)
        .fetch_one(&self.pool)
        .await?;

        usize::try_from(count)
            .map_err(|_| StorageError::Validation(format!("invalid negative count: {count}")))
    }
}
