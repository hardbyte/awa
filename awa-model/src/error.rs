use thiserror::Error;

#[derive(Debug, Error)]
pub enum AwaError {
    #[error("job not found: {id}")]
    JobNotFound { id: i64 },

    #[error("callback not found: {callback_id}")]
    CallbackNotFound { callback_id: String },

    #[error("unique conflict")]
    UniqueConflict { constraint: Option<String> },

    #[error("schema not migrated: expected version {expected}, found {found}")]
    SchemaNotMigrated { expected: i32, found: i32 },

    #[error("unknown job kind: {kind}")]
    UnknownJobKind { kind: String },

    #[error("validation error: {0}")]
    Validation(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("database error: {0}")]
    Database(#[source] sqlx::Error),

    #[cfg(feature = "tokio-postgres")]
    #[error("tokio-postgres error: {0}")]
    TokioPg(#[source] tokio_postgres::Error),
}

impl From<sqlx::Error> for AwaError {
    fn from(err: sqlx::Error) -> Self {
        AwaError::Database(err)
    }
}

/// The single Rust-side mapping from a raw `sqlx::Error` to an [`AwaError`],
/// translating a Postgres unique violation (SQLSTATE 23505) into
/// [`AwaError::UniqueConflict`] with the offending constraint name.
///
/// Insert paths call this explicitly rather than relying on the blanket
/// `From<sqlx::Error>` (which preserves the raw error for general `?` use).
/// Database adapters running Awa's insert SQL through another driver should
/// route the underlying sqlx error through here so their errors match the
/// native path.
pub fn map_sqlx_error(err: sqlx::Error) -> AwaError {
    if let sqlx::Error::Database(ref db_err) = err {
        if db_err.code().as_deref() == Some("23505") {
            return AwaError::UniqueConflict {
                constraint: db_err.constraint().map(|c| c.to_string()),
            };
        }
    }
    AwaError::Database(err)
}
