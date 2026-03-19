use crate::error::AwaError;
use sqlx::postgres::PgConnection;
use sqlx::PgPool;
use tracing::info;

/// Current schema version.
pub const CURRENT_VERSION: i32 = 1;

/// All migrations in order.
const MIGRATIONS: &[(i32, &str, &[&str])] = &[(1, "Canonical schema", &[V1_UP])];

/// The canonical schema.
const V1_UP: &str = r#"
-- Awa schema v1: Canonical hot/deferred schema

CREATE SCHEMA IF NOT EXISTS awa;

CREATE TYPE awa.job_state AS ENUM (
    'scheduled', 'available', 'running',
    'completed', 'retryable', 'failed', 'cancelled', 'waiting_external'
);

CREATE SEQUENCE awa.jobs_id_seq;

CREATE TABLE awa.jobs_hot (
    id                  BIGINT      NOT NULL DEFAULT nextval('awa.jobs_id_seq') PRIMARY KEY,
    kind                TEXT        NOT NULL,
    queue               TEXT        NOT NULL DEFAULT 'default',
    args                JSONB       NOT NULL DEFAULT '{}',
    state               awa.job_state NOT NULL DEFAULT 'available',
    priority            SMALLINT    NOT NULL DEFAULT 2,
    attempt             SMALLINT    NOT NULL DEFAULT 0,
    max_attempts        SMALLINT    NOT NULL DEFAULT 25,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at        TIMESTAMPTZ,
    deadline_at         TIMESTAMPTZ,
    attempted_at        TIMESTAMPTZ,
    finalized_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    errors              JSONB[]     DEFAULT '{}',
    metadata            JSONB       NOT NULL DEFAULT '{}',
    tags                TEXT[]      NOT NULL DEFAULT '{}',
    unique_key          BYTEA,
    unique_states       BIT(8),
    callback_id         UUID,
    callback_timeout_at TIMESTAMPTZ,
    callback_filter     TEXT,
    callback_on_complete TEXT,
    callback_on_fail    TEXT,
    callback_transform  TEXT,
    run_lease           BIGINT      NOT NULL DEFAULT 0,

    CONSTRAINT jobs_hot_state_check CHECK (state NOT IN ('scheduled', 'retryable')),
    CONSTRAINT jobs_hot_priority_in_range CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT jobs_hot_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT jobs_hot_queue_name_length CHECK (length(queue) <= 200),
    CONSTRAINT jobs_hot_kind_length CHECK (length(kind) <= 200),
    CONSTRAINT jobs_hot_tags_count CHECK (cardinality(tags) <= 20)
);

CREATE TABLE awa.scheduled_jobs (
    id                  BIGINT      NOT NULL DEFAULT nextval('awa.jobs_id_seq') PRIMARY KEY,
    kind                TEXT        NOT NULL,
    queue               TEXT        NOT NULL DEFAULT 'default',
    args                JSONB       NOT NULL DEFAULT '{}',
    state               awa.job_state NOT NULL DEFAULT 'scheduled',
    priority            SMALLINT    NOT NULL DEFAULT 2,
    attempt             SMALLINT    NOT NULL DEFAULT 0,
    max_attempts        SMALLINT    NOT NULL DEFAULT 25,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at        TIMESTAMPTZ,
    deadline_at         TIMESTAMPTZ,
    attempted_at        TIMESTAMPTZ,
    finalized_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    errors              JSONB[]     DEFAULT '{}',
    metadata            JSONB       NOT NULL DEFAULT '{}',
    tags                TEXT[]      NOT NULL DEFAULT '{}',
    unique_key          BYTEA,
    unique_states       BIT(8),
    callback_id         UUID,
    callback_timeout_at TIMESTAMPTZ,
    callback_filter     TEXT,
    callback_on_complete TEXT,
    callback_on_fail    TEXT,
    callback_transform  TEXT,
    run_lease           BIGINT      NOT NULL DEFAULT 0,

    CONSTRAINT scheduled_jobs_state_check CHECK (state IN ('scheduled', 'retryable')),
    CONSTRAINT scheduled_jobs_priority_in_range CHECK (priority BETWEEN 1 AND 4),
    CONSTRAINT scheduled_jobs_max_attempts_range CHECK (max_attempts BETWEEN 1 AND 1000),
    CONSTRAINT scheduled_jobs_queue_name_length CHECK (length(queue) <= 200),
    CONSTRAINT scheduled_jobs_kind_length CHECK (length(kind) <= 200),
    CONSTRAINT scheduled_jobs_tags_count CHECK (cardinality(tags) <= 20)
);

CREATE TABLE awa.queue_meta (
    queue       TEXT PRIMARY KEY,
    paused      BOOLEAN NOT NULL DEFAULT FALSE,
    paused_at   TIMESTAMPTZ,
    paused_by   TEXT
);

CREATE TABLE awa.job_unique_claims (
    unique_key  BYTEA NOT NULL,
    job_id      BIGINT NOT NULL
);

CREATE TABLE awa.cron_jobs (
    name             TEXT PRIMARY KEY,
    cron_expr        TEXT        NOT NULL,
    timezone         TEXT        NOT NULL DEFAULT 'UTC',
    kind             TEXT        NOT NULL,
    queue            TEXT        NOT NULL DEFAULT 'default',
    args             JSONB       NOT NULL DEFAULT '{}',
    priority         SMALLINT    NOT NULL DEFAULT 2,
    max_attempts     SMALLINT    NOT NULL DEFAULT 25,
    tags             TEXT[]      NOT NULL DEFAULT '{}',
    metadata         JSONB       NOT NULL DEFAULT '{}',
    last_enqueued_at TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE awa.schema_version (
    version     INT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE FUNCTION awa.job_state_in_bitmask(bitmask BIT(8), state awa.job_state)
RETURNS BOOLEAN AS $$
    SELECT CASE state
        WHEN 'scheduled'         THEN get_bit(bitmask, 0) = 1
        WHEN 'available'         THEN get_bit(bitmask, 1) = 1
        WHEN 'running'           THEN get_bit(bitmask, 2) = 1
        WHEN 'completed'         THEN get_bit(bitmask, 3) = 1
        WHEN 'retryable'         THEN get_bit(bitmask, 4) = 1
        WHEN 'failed'            THEN get_bit(bitmask, 5) = 1
        WHEN 'cancelled'         THEN get_bit(bitmask, 6) = 1
        WHEN 'waiting_external'  THEN get_bit(bitmask, 7) = 1
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

CREATE OR REPLACE FUNCTION awa.notify_new_job() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('awa:' || NEW.queue, '');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION awa.sync_job_unique_claims() RETURNS trigger AS $$
DECLARE
    old_claim BOOLEAN := FALSE;
    new_claim BOOLEAN := FALSE;
    existing_job_id BIGINT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        old_claim := OLD.unique_key IS NOT NULL
            AND OLD.unique_states IS NOT NULL
            AND awa.job_state_in_bitmask(OLD.unique_states, OLD.state);
    END IF;

    IF TG_OP <> 'DELETE' THEN
        new_claim := NEW.unique_key IS NOT NULL
            AND NEW.unique_states IS NOT NULL
            AND awa.job_state_in_bitmask(NEW.unique_states, NEW.state);
    END IF;

    IF old_claim AND (
        NOT new_claim
        OR OLD.unique_key IS DISTINCT FROM NEW.unique_key
        OR OLD.id IS DISTINCT FROM NEW.id
    ) THEN
        DELETE FROM awa.job_unique_claims
        WHERE unique_key = OLD.unique_key
          AND job_id = OLD.id;
    END IF;

    IF new_claim AND (
        NOT old_claim
        OR OLD.unique_key IS DISTINCT FROM NEW.unique_key
        OR OLD.id IS DISTINCT FROM NEW.id
    ) THEN
        BEGIN
            INSERT INTO awa.job_unique_claims (unique_key, job_id)
            VALUES (NEW.unique_key, NEW.id);
        EXCEPTION
            WHEN unique_violation THEN
                SELECT job_id
                INTO existing_job_id
                FROM awa.job_unique_claims
                WHERE unique_key = NEW.unique_key;

                IF existing_job_id IS DISTINCT FROM NEW.id THEN
                    RAISE unique_violation
                        USING CONSTRAINT = 'idx_awa_jobs_unique',
                              MESSAGE = 'duplicate key value violates unique constraint "idx_awa_jobs_unique"';
                END IF;
        END;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE VIEW awa.jobs AS
SELECT * FROM awa.jobs_hot
UNION ALL
SELECT * FROM awa.scheduled_jobs;

CREATE OR REPLACE FUNCTION awa.write_jobs_view() RETURNS trigger AS $$
DECLARE
    target_table TEXT;
    source_table TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.id := COALESCE(NEW.id, nextval('awa.jobs_id_seq'));
        NEW.queue := COALESCE(NEW.queue, 'default');
        NEW.args := COALESCE(NEW.args, '{}'::jsonb);
        NEW.state := COALESCE(NEW.state, 'available'::awa.job_state);
        NEW.priority := COALESCE(NEW.priority, 2);
        NEW.attempt := COALESCE(NEW.attempt, 0);
        NEW.max_attempts := COALESCE(NEW.max_attempts, 25);
        NEW.run_at := COALESCE(NEW.run_at, now());
        NEW.created_at := COALESCE(NEW.created_at, now());
        NEW.errors := COALESCE(NEW.errors, '{}'::jsonb[]);
        NEW.metadata := COALESCE(NEW.metadata, '{}'::jsonb);
        NEW.tags := COALESCE(NEW.tags, '{}'::text[]);
        NEW.run_lease := COALESCE(NEW.run_lease, 0);
    END IF;

    IF TG_OP = 'DELETE' THEN
        source_table := CASE
            WHEN OLD.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
                THEN 'awa.scheduled_jobs'
            ELSE 'awa.jobs_hot'
        END;
        EXECUTE format('DELETE FROM %s WHERE id = $1', source_table) USING OLD.id;
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        source_table := CASE
            WHEN OLD.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
                THEN 'awa.scheduled_jobs'
            ELSE 'awa.jobs_hot'
        END;
        EXECUTE format('DELETE FROM %s WHERE id = $1', source_table) USING OLD.id;
    END IF;

    target_table := CASE
        WHEN NEW.state IN ('scheduled'::awa.job_state, 'retryable'::awa.job_state)
            THEN 'awa.scheduled_jobs'
        ELSE 'awa.jobs_hot'
    END;

    EXECUTE format(
        'INSERT INTO %s (
            id, kind, queue, args, state, priority, attempt, max_attempts,
            run_at, heartbeat_at, deadline_at, attempted_at, finalized_at,
            created_at, errors, metadata, tags, unique_key, unique_states,
            callback_id, callback_timeout_at, callback_filter, callback_on_complete,
            callback_on_fail, callback_transform, run_lease
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8,
            $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23,
            $24, $25, $26
        )',
        target_table
    )
    USING
        NEW.id, NEW.kind, NEW.queue, NEW.args, NEW.state, NEW.priority, NEW.attempt,
        NEW.max_attempts, NEW.run_at, NEW.heartbeat_at, NEW.deadline_at, NEW.attempted_at,
        NEW.finalized_at, NEW.created_at, NEW.errors, NEW.metadata, NEW.tags,
        NEW.unique_key, NEW.unique_states, NEW.callback_id, NEW.callback_timeout_at,
        NEW.callback_filter, NEW.callback_on_complete, NEW.callback_on_fail,
        NEW.callback_transform, NEW.run_lease;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_awa_notify
    AFTER INSERT ON awa.jobs_hot
    FOR EACH ROW
    WHEN (NEW.state = 'available' AND NEW.run_at <= now())
    EXECUTE FUNCTION awa.notify_new_job();

CREATE TRIGGER trg_jobs_hot_unique_claims_insert
    AFTER INSERT ON awa.jobs_hot
    FOR EACH ROW
    WHEN (NEW.unique_key IS NOT NULL AND NEW.unique_states IS NOT NULL)
    EXECUTE FUNCTION awa.sync_job_unique_claims();

CREATE TRIGGER trg_jobs_hot_unique_claims_update
    AFTER UPDATE ON awa.jobs_hot
    FOR EACH ROW
    WHEN (
        (OLD.unique_key IS NOT NULL AND OLD.unique_states IS NOT NULL)
        OR (NEW.unique_key IS NOT NULL AND NEW.unique_states IS NOT NULL)
    )
    EXECUTE FUNCTION awa.sync_job_unique_claims();

CREATE TRIGGER trg_jobs_hot_unique_claims_delete
    AFTER DELETE ON awa.jobs_hot
    FOR EACH ROW
    WHEN (OLD.unique_key IS NOT NULL AND OLD.unique_states IS NOT NULL)
    EXECUTE FUNCTION awa.sync_job_unique_claims();

CREATE TRIGGER trg_scheduled_jobs_unique_claims_insert
    AFTER INSERT ON awa.scheduled_jobs
    FOR EACH ROW
    WHEN (NEW.unique_key IS NOT NULL AND NEW.unique_states IS NOT NULL)
    EXECUTE FUNCTION awa.sync_job_unique_claims();

CREATE TRIGGER trg_scheduled_jobs_unique_claims_update
    AFTER UPDATE ON awa.scheduled_jobs
    FOR EACH ROW
    WHEN (
        (OLD.unique_key IS NOT NULL AND OLD.unique_states IS NOT NULL)
        OR (NEW.unique_key IS NOT NULL AND NEW.unique_states IS NOT NULL)
    )
    EXECUTE FUNCTION awa.sync_job_unique_claims();

CREATE TRIGGER trg_scheduled_jobs_unique_claims_delete
    AFTER DELETE ON awa.scheduled_jobs
    FOR EACH ROW
    WHEN (OLD.unique_key IS NOT NULL AND OLD.unique_states IS NOT NULL)
    EXECUTE FUNCTION awa.sync_job_unique_claims();

CREATE TRIGGER trg_awa_jobs_view_insert
    INSTEAD OF INSERT ON awa.jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.write_jobs_view();

CREATE TRIGGER trg_awa_jobs_view_update
    INSTEAD OF UPDATE ON awa.jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.write_jobs_view();

CREATE TRIGGER trg_awa_jobs_view_delete
    INSTEAD OF DELETE ON awa.jobs
    FOR EACH ROW
    EXECUTE FUNCTION awa.write_jobs_view();

CREATE INDEX idx_awa_jobs_hot_dequeue
    ON awa.jobs_hot (queue, priority, run_at, id)
    WHERE state = 'available';

CREATE INDEX idx_awa_jobs_hot_heartbeat
    ON awa.jobs_hot (heartbeat_at)
    WHERE state = 'running';

CREATE INDEX idx_awa_jobs_hot_deadline
    ON awa.jobs_hot (deadline_at)
    WHERE state = 'running' AND deadline_at IS NOT NULL;

CREATE INDEX idx_awa_jobs_hot_kind_state
    ON awa.jobs_hot (kind, state);

CREATE UNIQUE INDEX idx_awa_jobs_hot_callback_id
    ON awa.jobs_hot (callback_id)
    WHERE callback_id IS NOT NULL;

CREATE INDEX idx_awa_jobs_hot_callback_timeout
    ON awa.jobs_hot (callback_timeout_at)
    WHERE state = 'waiting_external' AND callback_timeout_at IS NOT NULL;

CREATE INDEX idx_awa_scheduled_jobs_run_at_scheduled
    ON awa.scheduled_jobs (run_at, id, queue)
    WHERE state = 'scheduled';

CREATE INDEX idx_awa_scheduled_jobs_run_at_retryable
    ON awa.scheduled_jobs (run_at, id, queue)
    WHERE state = 'retryable';

CREATE INDEX idx_awa_scheduled_jobs_kind_state
    ON awa.scheduled_jobs (kind, state);

CREATE UNIQUE INDEX idx_awa_jobs_unique
    ON awa.job_unique_claims (unique_key);

INSERT INTO awa.schema_version (version, description)
VALUES (1, 'Canonical schema');
"#;

/// Run all pending migrations against the database.
///
/// Because Awa does not have external users yet, any pre-canonical `awa`
/// schema is replaced with the canonical schema rather than upgraded through a
/// historical chain.
///
/// Takes `&PgPool` for ergonomic use from Rust. For a `Send`-safe variant
/// that takes the pool by value, see [`run_owned`].
pub async fn run(pool: &PgPool) -> Result<(), AwaError> {
    let lock_key: i64 = 0x4157_415f_4d49_4752; // "AWA_MIGR"
    let mut conn = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await?;

    let result = run_inner(&mut conn).await;

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(lock_key)
        .execute(&mut *conn)
        .await;

    result
}

async fn run_inner(conn: &mut PgConnection) -> Result<(), AwaError> {
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(&mut *conn)
            .await?;

    let current = if has_schema {
        current_version_conn(conn).await?
    } else {
        0
    };

    if has_schema && current == CURRENT_VERSION {
        info!(version = current, "Schema is up to date");
        return Ok(());
    }

    if has_schema {
        info!(
            existing_version = current,
            "Replacing existing awa schema with canonical schema"
        );
        sqlx::raw_sql("DROP SCHEMA awa CASCADE")
            .execute(&mut *conn)
            .await?;
    }

    for &(version, description, steps) in MIGRATIONS {
        info!(version, description, "Applying migration");
        for step in steps {
            sqlx::raw_sql(step).execute(&mut *conn).await?;
        }
        info!(version, "Migration applied");
    }

    Ok(())
}

/// Get the current schema version.
pub async fn current_version(pool: &PgPool) -> Result<i32, AwaError> {
    let mut conn = pool.acquire().await?;
    current_version_conn(&mut conn).await
}

async fn current_version_conn(conn: &mut PgConnection) -> Result<i32, AwaError> {
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(&mut *conn)
            .await?;

    if !has_schema {
        return Ok(0);
    }

    let has_table: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'schema_version')",
    )
    .fetch_one(&mut *conn)
    .await?;

    if !has_table {
        return Ok(0);
    }

    let version: Option<i32> = sqlx::query_scalar("SELECT MAX(version) FROM awa.schema_version")
        .fetch_one(&mut *conn)
        .await?;

    Ok(version.unwrap_or(0))
}

/// Get the raw SQL for all migrations (for extraction / external tooling).
pub fn migration_sql() -> Vec<(i32, &'static str, String)> {
    MIGRATIONS
        .iter()
        .map(|&(v, d, steps)| (v, d, steps.join("\n")))
        .collect()
}
