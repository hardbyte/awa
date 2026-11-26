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

    #[error(
        "schema is at v{found} but this binary supports up to v{supported} — upgrade the awa \
         binaries (see docs/upgrade-0.6-to-0.7.md); refusing to modify a newer schema"
    )]
    SchemaNewerThanBinary { found: i32, supported: i32 },

    #[error(
        "migration v{migration_version} requires no live runtimes: {count} live \
         runtime{plural} detected (newest heartbeat {newest_secs}s ago). A pre-migration \
         binary cannot operate against the post-migration schema, so this migration needs a \
         stop-the-world window — stop or drain the workers first, or pass \
         --allow-live-runtimes to override.{instances}"
    )]
    LiveRuntimesRequireExclusiveWindow {
        migration_version: i32,
        count: i64,
        plural: &'static str,
        newest_secs: i64,
        instances: String,
    },

    #[error(
        "migration v{migration_version} requires all live runtimes to be version \
         {minimum_version} or newer; incompatible live runtime count: {count}. A reported \
         version below the floor or an unparseable version blocks migration. Roll the fleet \
         to {minimum_version} first, or \
         pass --allow-live-runtimes to override.{instances}"
    )]
    RuntimeVersionFloorNotMet {
        migration_version: i32,
        minimum_version: &'static str,
        count: i64,
        instances: String,
    },

    #[error(
        "storage transition not finalized (state: {state}): awa 0.7 refuses to apply \
         migrations while the deprecated canonical engine can still hold work (ADR-037). \
         Finalize the queue-storage transition on awa 0.6 first — `awa storage prepare \
         --engine queue_storage`, then `awa storage enter-mixed-transition`, then \
         `awa storage finalize --wait` — and re-run `awa migrate`. Fresh installs (no jobs, \
         no recently-live runtimes) are exempt. See docs/upgrade-0.6-to-0.7.md and \
         docs/upgrade-0.5-to-0.6.md."
    )]
    StorageNotFinalized { state: String },

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

impl AwaError {
    /// Postgres aborted this transaction to break a lock cycle (SQLSTATE
    /// `40P01`). Nothing the transaction did was applied, so a caller whose
    /// statements are idempotent may simply run it again.
    pub fn is_deadlock(&self) -> bool {
        matches!(self, AwaError::Database(err) if pg_session_lock::is_deadlock(err))
    }

    /// Postgres aborted a `REPEATABLE READ` or `SERIALIZABLE` transaction
    /// because it could not be serialized against a concurrent one (SQLSTATE
    /// `40001`). Nothing the transaction did was applied; run the whole
    /// transaction again. Under these isolation levels a race on a unique key
    /// can also surface as `40001` rather than a unique violation.
    pub fn is_serialization_failure(&self) -> bool {
        matches!(self, AwaError::Database(err) if pg_session_lock::is_serialization_failure(err))
    }

    /// Postgres cancelled the statement at its `statement_timeout` (SQLSTATE
    /// `57014`).
    pub fn is_statement_timeout(&self) -> bool {
        matches!(self, AwaError::Database(err) if pg_session_lock::is_query_canceled(err))
    }

    /// The statement named a column the table does not have (SQLSTATE 42703),
    /// as when a schema loses a migration's columns under a running process.
    pub fn is_undefined_column(&self) -> bool {
        matches!(
            self,
            AwaError::Database(sqlx::Error::Database(database))
                if database.code().as_deref() == Some("42703")
        )
    }

    /// Database writes are unavailable because of a transport, TLS, protocol,
    /// pool-acquire, server-availability or read-only-session error.
    ///
    /// A retry may recover, but the transaction may already have committed:
    /// callers must establish replay safety independently before retrying it.
    pub fn is_connection_lost(&self) -> bool {
        match self {
            AwaError::Database(sqlx::Error::Io(_))
            | AwaError::Database(sqlx::Error::PoolTimedOut)
            | AwaError::Database(sqlx::Error::Protocol(_))
            | AwaError::Database(sqlx::Error::Tls(_)) => true,
            AwaError::Database(sqlx::Error::Database(database)) => {
                database.code().is_some_and(|code| {
                    code.starts_with("08")
                        || matches!(code.as_ref(), "57P01" | "57P02" | "57P03" | "25006")
                })
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
            AwaError::SchemaNewerThanBinary { .. } => "schema_newer_than_binary",
            AwaError::LiveRuntimesRequireExclusiveWindow { .. } => {
                "live_runtimes_require_exclusive_window"
            }
            AwaError::RuntimeVersionFloorNotMet { .. } => "runtime_version_floor_not_met",
            AwaError::StorageNotFinalized { .. } => "storage_not_finalized",
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
mod tests {
    use super::AwaError;
    use std::borrow::Cow;

    #[derive(Debug)]
    struct StubDatabaseError(&'static str);

    impl std::fmt::Display for StubDatabaseError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "sqlstate {}", self.0)
        }
    }

    impl std::error::Error for StubDatabaseError {}

    impl sqlx::error::DatabaseError for StubDatabaseError {
        fn message(&self) -> &str {
            "stub"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn sqlstate(code: &'static str) -> AwaError {
        AwaError::Database(sqlx::Error::Database(Box::new(StubDatabaseError(code))))
    }

    #[test]
    fn connection_lost_covers_transport_pool_and_server_shutdown_errors() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert!(AwaError::Database(sqlx::Error::Io(io)).is_connection_lost());
        assert!(AwaError::Database(sqlx::Error::PoolTimedOut).is_connection_lost());
        assert!(
            AwaError::Database(sqlx::Error::Tls(Box::new(std::io::Error::other(
                "TLS failure"
            ))))
            .is_connection_lost()
        );
        assert!(
            AwaError::Database(sqlx::Error::Protocol("unexpected response".into()))
                .is_connection_lost()
        );
        for code in [
            "08000", "08003", "08006", "57P01", "57P02", "57P03", "25006",
        ] {
            assert!(sqlstate(code).is_connection_lost(), "{code}");
        }
    }

    #[test]
    fn connection_lost_excludes_statement_and_pool_shutdown_errors() {
        assert!(!AwaError::Database(sqlx::Error::PoolClosed).is_connection_lost());
        assert!(!AwaError::Database(sqlx::Error::RowNotFound).is_connection_lost());
        assert!(!AwaError::Validation("completion batch flush failed".into()).is_connection_lost());
        for code in ["40P01", "23505", "42P01", "57014"] {
            assert!(!sqlstate(code).is_connection_lost(), "{code}");
        }
        assert!(sqlstate("40P01").is_deadlock());
    }

    #[test]
    fn serialization_failure_is_sqlstate_40001_only() {
        assert!(sqlstate("40001").is_serialization_failure());
        for code in ["40P01", "23505", "55P03"] {
            assert!(!sqlstate(code).is_serialization_failure(), "{code}");
        }
        assert!(!AwaError::Validation("40001".into()).is_serialization_failure());
    }
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
        let mut query = sqlx::query(crate::audited_sql(sql));
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
