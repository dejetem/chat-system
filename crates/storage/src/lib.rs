pub mod error;
pub mod media;
pub mod memory;
pub mod models;
pub mod postgres;
pub mod repository;

pub use error::StorageError;
pub use media::{
    InMemoryMediaStorage, MediaObject, MediaObjectMeta, MediaRef, MediaStorage, S3Config,
    S3MediaStorage,
};
pub use memory::InMemoryStorage;
pub use models::{
    DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT, MAX_IDENTIFIER_LEN, MAX_MEDIA_KEY_LEN,
    NewPrivateMessage, NewRoomMessage, StoredMessage, StoredRoom, StoredUser, validate_ciphertext,
    validate_media_key, validate_optional_media_key, validate_role, validate_room_id,
    validate_room_name, validate_username,
};
pub use postgres::PgStorage;
pub use repository::ChatStorage;
