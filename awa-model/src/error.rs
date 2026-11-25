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

    #[cfg(feature = "sqlx09")]
    #[error("sqlx 0.9 error: {0}")]
    Sqlx09(#[source] sqlx09::Error),
}

impl AwaError {
    /// Postgres aborted this transaction to break a lock cycle (SQLSTATE
    /// `40P01`). Nothing the transaction did was applied, so a caller whose
    /// statements are idempotent may simply run it again.
    pub fn is_deadlock(&self) -> bool {
        match self {
            AwaError::Database(sqlx::Error::Database(database)) => {
                database.code().as_deref() == Some("40P01")
            }
            #[cfg(feature = "sqlx09")]
            AwaError::Sqlx09(sqlx09::Error::Database(database)) => {
                database.code().as_deref() == Some("40P01")
            }
            _ => false,
        }
    }

    /// A low-cardinality class of this error, for the `error.type` attribute
    /// of a metric: the variant, or for a database error the class
    /// [`sqlx_error_type`] gives. Never the message.
    pub fn error_type(&self) -> &'static str {
        match self {
            AwaError::JobNotFound { .. } => "job_not_found",
            AwaError::CallbackNotFound { .. } => "callback_not_found",
            AwaError::UniqueConflict { .. } => "unique_conflict",
            AwaError::SchemaNotMigrated { .. } => "schema_not_migrated",
            AwaError::UnknownJobKind { .. } => "unknown_job_kind",
            AwaError::Validation(_) => "validation",
            AwaError::Serialization(_) => "serialization",
            AwaError::Database(err) => sqlx_error_type(err),
            #[cfg(feature = "tokio-postgres")]
            AwaError::TokioPg(err) => match err.code().map(|code| code.code()) {
                Some(code) => sqlstate_error_type(code),
                None if err.is_closed() => "connection",
                None => "database",
            },
            #[cfg(feature = "sqlx09")]
            AwaError::Sqlx09(err) => match err {
                sqlx09::Error::Database(database) => database
                    .code()
                    .as_deref()
                    .map_or("database", sqlstate_error_type),
                sqlx09::Error::PoolTimedOut => "timeout",
                sqlx09::Error::Io(_) | sqlx09::Error::Tls(_) | sqlx09::Error::PoolClosed => {
                    "connection"
                }
                sqlx09::Error::Protocol(message) if message.contains("too many arguments") => {
                    "bind_limit"
                }
                _ => "database",
            },
        }
    }
}

/// A low-cardinality class of a `sqlx` error, for the `error.type` attribute
/// of a metric:
///
/// - `bind_limit`: a statement with more bind parameters than Postgres
///   accepts, refused before it is sent.
/// - `timeout`: a statement cancelled by `statement_timeout` or a cancel
///   request (SQLSTATE `57014`), or no pool connection in time.
/// - `lock_not_available`: `lock_timeout` or `NOWAIT` (SQLSTATE `55P03`).
/// - `deadlock`, `serialization_failure`: the transaction was aborted and
///   can be retried.
/// - `connection`: the connection or pool is gone.
/// - `database`: any other error Postgres or the driver reported.
pub fn sqlx_error_type(err: &sqlx::Error) -> &'static str {
    match err {
        sqlx::Error::Database(database) => database
            .code()
            .as_deref()
            .map_or("database", sqlstate_error_type),
        sqlx::Error::PoolTimedOut => "timeout",
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => "connection",
        sqlx::Error::Protocol(message) if message.contains("too many arguments") => "bind_limit",
        _ => "database",
    }
}

fn sqlstate_error_type(code: &str) -> &'static str {
    match code {
        "57014" => "timeout",
        "55P03" => "lock_not_available",
        "40P01" => "deadlock",
        "40001" => "serialization_failure",
        code if code.starts_with("08") || code.starts_with("57P") => "connection",
        _ => "database",
    }
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

#[cfg(test)]
mod error_type_tests {
    use super::{sqlx_error_type, AwaError};
    use sqlx::PgPool;

    #[test]
    fn client_side_errors_have_fixed_classes() {
        assert_eq!(sqlx_error_type(&sqlx::Error::PoolTimedOut), "timeout");
        assert_eq!(sqlx_error_type(&sqlx::Error::PoolClosed), "connection");
        assert_eq!(
            sqlx_error_type(&sqlx::Error::Protocol(
                "PgConnection::run(): too many arguments for query: 70000".into()
            )),
            "bind_limit"
        );
        assert_eq!(sqlx_error_type(&sqlx::Error::RowNotFound), "database");
        assert_eq!(
            AwaError::Validation("bad".into()).error_type(),
            "validation"
        );
        assert_eq!(
            AwaError::UniqueConflict { constraint: None }.error_type(),
            "unique_conflict"
        );
    }

    /// Each class from the error Postgres or the driver really raises.
    #[sqlx::test(migrations = false)]
    async fn database_errors_are_classed_by_sqlstate(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        sqlx::query("SET statement_timeout = '10ms'")
            .execute(&mut *conn)
            .await
            .unwrap();
        let timeout = sqlx::query("SELECT pg_sleep(1)")
            .execute(&mut *conn)
            .await
            .unwrap_err();
        assert_eq!(sqlx_error_type(&timeout), "timeout");
        sqlx::query("RESET statement_timeout")
            .execute(&mut *conn)
            .await
            .unwrap();

        sqlx::query("CREATE TABLE error_type_locked (id int)")
            .execute(&pool)
            .await
            .unwrap();
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("LOCK TABLE error_type_locked IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *holder)
            .await
            .unwrap();
        let mut waiter = pool.begin().await.unwrap();
        let busy = sqlx::query("LOCK TABLE error_type_locked IN ACCESS SHARE MODE NOWAIT")
            .execute(&mut *waiter)
            .await
            .unwrap_err();
        assert_eq!(sqlx_error_type(&busy), "lock_not_available");
        waiter.rollback().await.unwrap();
        holder.rollback().await.unwrap();

        let placeholders: Vec<String> = (1..=70_000).map(|n| format!("${n}")).collect();
        let sql = format!("SELECT 1 WHERE 1 IN ({})", placeholders.join(", "));
        let mut query = sqlx::query(&sql);
        for n in 0..70_000 {
            query = query.bind(n);
        }
        let too_many = query.execute(&mut *conn).await.unwrap_err();
        assert_eq!(sqlx_error_type(&too_many), "bind_limit");

        let undefined = sqlx::query("SELECT * FROM error_type_missing")
            .execute(&mut *conn)
            .await
            .unwrap_err();
        assert_eq!(sqlx_error_type(&undefined), "database");
        assert_eq!(AwaError::Database(undefined).error_type(), "database");
    }
}
