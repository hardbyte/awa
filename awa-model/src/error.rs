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
        matches!(
            self,
            AwaError::Database(sqlx::Error::Database(database))
                if database.code().as_deref() == Some("40P01")
        )
    }

    /// The statement never reached a healthy server: the socket dropped, the
    /// pool could not hand out a connection in time, the server rejected or
    /// closed the session (SQLSTATE class `08`), or it was shutting down or
    /// still starting up (`57P01`..`57P03`). A lease-guarded statement can be
    /// run again once the server is back.
    pub fn is_connection_lost(&self) -> bool {
        match self {
            AwaError::Database(sqlx::Error::Io(_))
            | AwaError::Database(sqlx::Error::PoolTimedOut)
            | AwaError::Database(sqlx::Error::Protocol(_))
            | AwaError::Database(sqlx::Error::Tls(_)) => true,
            AwaError::Database(sqlx::Error::Database(database)) => {
                database.code().is_some_and(|code| {
                    code.starts_with("08") || matches!(code.as_ref(), "57P01" | "57P02" | "57P03")
                })
            }
            _ => false,
        }
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
            AwaError::Database(sqlx::Error::Protocol("unexpected response".into()))
                .is_connection_lost()
        );
        for code in ["08000", "08003", "08006", "57P01", "57P02", "57P03"] {
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
}
