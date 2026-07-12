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
