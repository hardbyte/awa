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

/// Map SQLx database errors into Awa's public error surface.
///
/// Keep this helper as the single Rust-side place that turns Postgres unique
/// violations into [`AwaError::UniqueConflict`].
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
