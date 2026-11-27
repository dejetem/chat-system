-- Phase 6: Initial persistent storage schema.
-- Invariants:
--   1. E2EE: `messages.body` stores opaque ciphertext bytes (BYTEA) only — never plaintext.
--   2. Client-side search only: no GIN/tsvector/trigram/text indexes on `messages.body`.
--   3. Media in S3: `messages.media_key` holds only object keys; media bytes never enter PostgreSQL.

CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE CHECK (char_length(btrim(username)) BETWEEN 1 AND 128),
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('user', 'moderator', 'admin')),
    public_key BYTEA,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS rooms (
    id BIGINT PRIMARY KEY CHECK (id >= 0),
    name TEXT NOT NULL CHECK (char_length(btrim(name)) BETWEEN 1 AND 128),
    created_by BIGINT REFERENCES users(id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS messages (
    message_id BIGSERIAL PRIMARY KEY,
    sender_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    room_id BIGINT REFERENCES rooms(id) ON DELETE CASCADE,
    recipient_id BIGINT REFERENCES users(id) ON DELETE CASCADE,
    body BYTEA NOT NULL CHECK (octet_length(body) > 0),
    media_key TEXT CHECK (media_key IS NULL OR char_length(btrim(media_key)) BETWEEN 1 AND 512),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT messages_target_exclusive CHECK (
        (room_id IS NOT NULL AND recipient_id IS NULL)
        OR (room_id IS NULL AND recipient_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_messages_room_history
    ON messages (room_id, message_id DESC)
    WHERE room_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_messages_direct_history
    ON messages (recipient_id, message_id DESC)
    WHERE recipient_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_messages_sender_created
    ON messages (sender_id, created_at DESC);

CREATE TABLE IF NOT EXISTS offline_messages (
    id BIGSERIAL PRIMARY KEY,
    recipient_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    message_id BIGINT NOT NULL REFERENCES messages(message_id) ON DELETE CASCADE,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (recipient_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_offline_messages_recipient_order
    ON offline_messages (recipient_id, message_id ASC);

