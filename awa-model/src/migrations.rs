use crate::error::AwaError;
use sqlx::PgPool;
use tracing::info;

/// Current schema version.
pub const CURRENT_VERSION: i32 = 1;

/// The initial migration SQL.
const V1_UP: &str = r#"
-- Awa schema v1: Initial schema

CREATE SCHEMA IF NOT EXISTS awa;

CREATE TYPE awa.job_state AS ENUM (
    'scheduled', 'available', 'running',
    'completed', 'retryable', 'failed', 'cancelled'
);

CREATE TABLE awa.jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind            TEXT        NOT NULL,
    queue           TEXT        NOT NULL DEFAULT 'default',
    args            JSONB       NOT NULL DEFAULT '{}',
    state           awa.job_state NOT NULL DEFAULT 'available',
    priority        SMALLINT    NOT NULL DEFAULT 2,
    attempt         SMALLINT    NOT NULL DEFAULT 0,
    max_attempts    SMALLINT    NOT NULL DEFAULT 25,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,
    attempted_at    TIMESTAMPTZ,
    finalized_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    errors          JSONB[]     DEFAULT '{}',
    metadata        JSONB       NOT NULL DEFAULT '{}',
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    unique_key      BYTEA,
    unique_states   BIT(8),

    CONSTRAINT priority_in_range CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT max_attempts_range CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT queue_name_length CHECK (length(queue) <= 200),
    CONSTRAINT kind_length CHECK (length(kind) <= 200),
    CONSTRAINT tags_count CHECK (cardinality(tags) <= 20)
);

CREATE TABLE awa.queue_meta (
    queue       TEXT PRIMARY KEY,
    paused      BOOLEAN NOT NULL DEFAULT FALSE,
    paused_at   TIMESTAMPTZ,
    paused_by   TEXT
);

CREATE TABLE awa.schema_version (
    version     INT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Functions (must be created before indexes that reference them)

CREATE FUNCTION awa.job_state_in_bitmask(bitmask BIT(8), state awa.job_state)
RETURNS BOOLEAN AS $$
    SELECT CASE state
        WHEN 'scheduled'  THEN get_bit(bitmask, 0) = 1
        WHEN 'available'  THEN get_bit(bitmask, 1) = 1
        WHEN 'running'    THEN get_bit(bitmask, 2) = 1
        WHEN 'completed'  THEN get_bit(bitmask, 3) = 1
        WHEN 'retryable'  THEN get_bit(bitmask, 4) = 1
        WHEN 'failed'     THEN get_bit(bitmask, 5) = 1
        WHEN 'cancelled'  THEN get_bit(bitmask, 6) = 1
        ELSE FALSE
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE FUNCTION awa.backoff_duration(attempt SMALLINT, max_attempts SMALLINT)
RETURNS interval AS $$
    SELECT LEAST(
        (power(2, attempt)::int || ' seconds')::interval
            + (random() * power(2, attempt) * 0.25 || ' seconds')::interval,
        interval '24 hours'
    );
$$ LANGUAGE sql VOLATILE;

CREATE FUNCTION awa.notify_new_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('awa:' || NEW.queue, NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_awa_notify
    AFTER INSERT ON awa.jobs
    FOR EACH ROW
    WHEN (NEW.state = 'available' AND NEW.run_at <= now())
    EXECUTE FUNCTION awa.notify_new_job();

-- Indexes

-- Dequeue hot path
CREATE INDEX idx_awa_jobs_dequeue
    ON awa.jobs (queue, priority, run_at, id)
    WHERE state = 'available';

-- Heartbeat staleness (crash detection)
CREATE INDEX idx_awa_jobs_heartbeat
    ON awa.jobs (heartbeat_at)
    WHERE state = 'running';

-- Hard deadline (runaway protection)
CREATE INDEX idx_awa_jobs_deadline
    ON awa.jobs (deadline_at)
    WHERE state = 'running' AND deadline_at IS NOT NULL;

-- Uniqueness enforcement
CREATE UNIQUE INDEX idx_awa_jobs_unique
    ON awa.jobs (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND awa.job_state_in_bitmask(unique_states, state);

-- Kind-based lookups (admin, monitoring)
CREATE INDEX idx_awa_jobs_kind_state
    ON awa.jobs (kind, state);

-- Record version
INSERT INTO awa.schema_version (version, description) VALUES (1, 'Initial schema');
"#;

/// Run all pending migrations against the database.
///
/// Uses an advisory lock to prevent concurrent migration attempts.
pub async fn run(pool: &PgPool) -> Result<(), AwaError> {
    // Acquire advisory lock to serialize migration (key chosen to not collide with maintenance)
    let lock_key: i64 = 0x4157_415f_4d49_4752; // "AWA_MIGR"
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(pool)
        .await?;

    let result = run_inner(pool).await;

    // Always release the lock
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(pool)
        .await;

    result
}

async fn run_inner(pool: &PgPool) -> Result<(), AwaError> {
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(pool)
            .await?;

    if !has_schema {
        info!("Running initial migration (v1)");
        sqlx::raw_sql(V1_UP).execute(pool).await?;
        info!("Migration v1 applied successfully");
        return Ok(());
    }

    // Check if schema_version table exists
    let has_version_table: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'schema_version')",
    )
    .fetch_one(pool)
    .await?;

    if !has_version_table {
        info!("Running initial migration (v1)");
        sqlx::raw_sql(V1_UP).execute(pool).await?;
        info!("Migration v1 applied successfully");
        return Ok(());
    }

    let current: Option<i32> = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(pool)
        .await?;

    let current_version = current.unwrap_or(0);

    if current_version >= CURRENT_VERSION {
        info!(version = current_version, "Schema is up to date");
        return Ok(());
    }

    if current_version == 0 {
        info!("Running initial migration (v1)");
        sqlx::raw_sql(V1_UP).execute(pool).await?;
        info!("Migration v1 applied successfully");
    }

    Ok(())
}

/// Get the current schema version.
pub async fn current_version(pool: &PgPool) -> Result<i32, AwaError> {
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(pool)
            .await?;

    if !has_schema {
        return Ok(0);
    }

    let has_table: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'schema_version')",
    )
    .fetch_one(pool)
    .await?;

    if !has_table {
        return Ok(0);
    }

    let version: Option<i32> = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(pool)
        .await?;

    Ok(version.unwrap_or(0))
}

/// Get the raw SQL for all migrations (for extraction / external tooling).
pub fn migration_sql() -> Vec<(i32, &'static str, &'static str)> {
    vec![(1, "Initial schema", V1_UP)]
}
