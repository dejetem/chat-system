use thiserror::Error;

/// Errors returned by PostgreSQL, in-memory, and S3-compatible storage backends.
#[derive(Debug, Error)]
pub enum StorageError {
    /// Input parameter or persisted record violated storage domain constraints.
    #[error("validation error: {0}")]
    Validation(String),

    /// Requested entity or object key does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// PostgreSQL driver or query execution error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Database schema migration error.
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// HTTP transport error while communicating with S3-compatible object storage.
    #[error("media http error: {0}")]
    MediaHttp(#[from] reqwest::Error),

    /// Non-success HTTP response returned by S3-compatible object storage.
    #[error("object storage returned HTTP {status}: {message}")]
    MediaStatus { status: u16, message: String },

    /// Invalid or incomplete S3/object storage configuration.
    #[error("media storage configuration error: {0}")]
    MediaConfig(String),
}
