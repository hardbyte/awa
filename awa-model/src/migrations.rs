use crate::error::AwaError;
use sqlx::postgres::PgConnection;
use sqlx::{Connection, PgPool};
use tracing::info;

/// Current schema version.
pub const CURRENT_VERSION: i32 = 42;

/// All migrations in order. SQL lives in `awa-model/migrations/*.sql`
/// for easy inspection by users who run their own migration tooling.
///
/// ## Migration policy
///
/// Migrations MUST be **additive only**:
/// - Add tables, columns (with defaults), indexes, functions
/// - Never drop columns, change types, or tighten constraints
///
/// This ensures running workers are not broken by a schema upgrade.
/// For breaking schema changes, bump the major version and document
/// the required stop-the-world upgrade procedure.
///
/// **Documented exception — v042 (#371):** the ring-state cursor columns
/// (`current_slot` / `generation`) are dropped from the `{ring}_ring_state`
/// singletons after being seeded into the append-only rotation ledgers.
/// A pre-v042 binary reading a stale frozen cursor would silently misroute
/// writes; the missing column makes it fail loudly instead. Binaries and
/// migration move together across v042 — no mixed fleet on one database
/// (see the 0.7 upgrade notes in CHANGELOG.md and ADR-040).
const MIGRATIONS: &[(i32, &str, &[&str])] = &[
    (1, "Canonical schema with UI indexes", &[V1_UP]),
    (2, "Runtime observability snapshots", &[V2_UP]),
    (3, "Maintenance loop health in runtime snapshots", &[V3_UP]),
    (4, "Admin metadata cache tables", &[V4_UP]),
    (5, "Statement-level admin metadata triggers", &[V5_UP]),
    (
        6,
        "Dirty-key statement triggers for deadlock-free admin metadata",
        &[V6_UP],
    ),
    (
        7,
        "Backoff interval creation avoids scientific-notation parse failures",
        &[V7_UP],
    ),
    // v008 is reserved for the dead-letter-queue migration on a parallel
    // branch; leave the slot open so both PRs can land without renumbering.
    (9, "Queue and job-kind descriptor catalogs", &[V9_UP]),
    (
        10,
        "Storage transition metadata and canonical compat routing",
        &[V10_UP],
    ),
    (
        11,
        "Storage transition self-heal: NULL-safe engine resolution and singleton re-seed",
        &[V11_UP],
    ),
    (
        12,
        "Queue storage compatibility layer and active backend selection",
        &[V12_UP],
    ),
    (
        13,
        "Storage auto-finalize and queue-storage count maintenance",
        &[V13_UP],
    ),
    (
        14,
        "Storage transition role tracking and tightened mixed-transition gate",
        &[V14_UP],
    ),
    (15, "Cron missed-fire policy", &[V15_UP]),
    (
        16,
        "Drop redundant queue_lanes.available_count cache; reader derives from heads",
        &[V16_UP],
    ),
    (
        17,
        "Shard queue_enqueue_heads/queue_claim_heads/ready_entries by enqueue_shard",
        &[V17_UP],
    ),
    (
        18,
        "Thread ordering_key through insert_job_compat for queue-storage producers",
        &[V18_UP],
    ),
    (
        19,
        "Make queue-storage jobs compatibility view shard-aware",
        &[V19_UP],
    ),
    (
        20,
        "Derive active queue-storage schema from transition state",
        &[V20_UP],
    ),
    (
        21,
        "Shard-aware lane indexes on ready_entries/done_entries/leases",
        &[V21_UP],
    ),
    (
        22,
        "delete_job_compat decrements queue_terminal_live_counts for done_entries deletes",
        &[V22_UP],
    ),
    (
        23,
        "Install default awa queue-storage substrate via SQL helper",
        &[V23_UP],
    ),
    (
        24,
        "Lower fillfactor to 50 on leases and lease_claims partitions",
        &[V24_UP],
    ),
    (
        25,
        "Drop idx_<schema>_leases_<slot>_state_hb on all AWA substrates",
        &[V25_UP],
    ),
    (
        26,
        "Add paused_at + paused_by to cron_jobs for per-schedule pause",
        &[V26_UP],
    ),
    (
        27,
        "Move lane cursors to sequences and stripe terminal counters",
        &[V23_UP, V22_UP, V27_UP],
    ),
    (
        28,
        "Add ready_tombstones ledger and compatibility filters (#295)",
        &[V23_UP, V22_UP, V28_UP],
    ),
    (29, "Add durable batch operations control table", &[V29_UP]),
    (
        30,
        "Add terminal-count delta ledger and async rollup",
        &[V23_UP, V30_UP],
    ),
    (
        31,
        "Backfill failed done-entry metric indexes for queue storage",
        &[V31_UP],
    ),
    (
        32,
        "Add pruned_failed_count to queue_terminal_rollups for the failed terminal retention floor",
        &[V32_UP],
    ),
    (
        33,
        "Add per-slot receipt-rescue cursors for queue storage",
        &[V23_UP, V33_UP],
    ),
    (
        34,
        "Materialize receipt closures before terminal compatibility deletes",
        &[V34_UP],
    ),
    (
        35,
        "Add per-slot receipt deadline-rescue cursors for queue storage",
        &[V35_UP],
    ),
    (
        36,
        "Compact successful receipt completions into batch terminal history",
        &[V18_UP, V23_UP, V36_UP],
    ),
    (
        37,
        "Add ready_segments claim-routing ledger for queue storage",
        &[V18_UP, V23_UP, V37_UP],
    ),
    (
        38,
        "Add compact receipt claim batch ledger for queue storage",
        &[V23_UP, V38_UP],
    ),
    (
        39,
        "Refresh claim_ready_runtime to cache-free ready-segment routing",
        &[V23_UP, V39_UP],
    ),
    (40, "Per-queue runtime overrides on queue_meta", &[V40_UP]),
    (
        41,
        "Compact deadline receipt claims and batch deadline-rescue cursors (#246)",
        &[V23_UP, V41_UP],
    ),
    (
        42,
        "Append-only ring-rotation ledgers and terminal-rollup deltas (#371)",
        &[V18_UP, V23_UP, V42_UP],
    ),
];

const V1_UP: &str = include_str!("../migrations/v001_canonical_schema.sql");
const V2_UP: &str = include_str!("../migrations/v002_runtime_instances.sql");
const V3_UP: &str = include_str!("../migrations/v003_maintenance_health.sql");
const V4_UP: &str = include_str!("../migrations/v004_admin_metadata.sql");
const V5_UP: &str = include_str!("../migrations/v005_admin_metadata_stmt_triggers.sql");
const V6_UP: &str = include_str!("../migrations/v006_remove_hot_table_triggers.sql");
const V7_UP: &str = include_str!("../migrations/v007_backoff_interval_fix.sql");
const V9_UP: &str = include_str!("../migrations/v009_descriptors.sql");
const V10_UP: &str = include_str!("../migrations/v010_storage_transition_prep.sql");
const V11_UP: &str = include_str!("../migrations/v011_storage_transition_self_heal.sql");
const V12_UP: &str = include_str!("../migrations/v012_queue_storage_compat.sql");
const V13_UP: &str = include_str!("../migrations/v013_storage_auto_finalize.sql");
const V14_UP: &str = include_str!("../migrations/v014_storage_transition_role.sql");
const V15_UP: &str = include_str!("../migrations/v015_cron_missed_fire_policy.sql");
const V16_UP: &str = include_str!("../migrations/v016_drop_queue_lanes_available_count.sql");
const V17_UP: &str = include_str!("../migrations/v017_shard_queue_enqueue_heads.sql");
const V18_UP: &str = include_str!("../migrations/v018_insert_job_compat_ordering_key.sql");
const V19_UP: &str = include_str!("../migrations/v019_queue_storage_jobs_compat_shard_joins.sql");
const V20_UP: &str = include_str!("../migrations/v020_active_queue_storage_schema_fallback.sql");
const V21_UP: &str = include_str!("../migrations/v021_shard_aware_lane_indexes.sql");
const V22_UP: &str = include_str!("../migrations/v022_delete_compat_terminal_counter.sql");
const V23_UP: &str = include_str!("../migrations/v023_install_queue_storage_substrate.sql");
const V24_UP: &str = include_str!("../migrations/v024_receipt_plane_fillfactor.sql");
const V25_UP: &str = include_str!("../migrations/v025_drop_leases_state_hb_index.sql");
const V26_UP: &str = include_str!("../migrations/v026_cron_jobs_pause.sql");
const V27_UP: &str = include_str!("../migrations/v027_sequence_lane_cursors.sql");
const V28_UP: &str = include_str!("../migrations/v028_ready_tombstones.sql");
const V29_UP: &str = include_str!("../migrations/v029_batch_operations.sql");
const V30_UP: &str = include_str!("../migrations/v030_terminal_count_deltas.sql");
const V31_UP: &str = include_str!("../migrations/v031_queue_storage_failed_done_indexes.sql");
const V32_UP: &str = include_str!("../migrations/v032_failed_terminal_retention.sql");
const V33_UP: &str = include_str!("../migrations/v033_receipt_rescue_cursors.sql");
const V34_UP: &str = include_str!("../migrations/v034_receipt_terminal_delete_closures.sql");
const V35_UP: &str = include_str!("../migrations/v035_receipt_deadline_rescue_cursors.sql");
const V36_UP: &str = include_str!("../migrations/v036_compact_receipt_completions.sql");
const V37_UP: &str = include_str!("../migrations/v037_ready_segments.sql");
const V38_UP: &str = include_str!("../migrations/v038_compact_claim_batches.sql");
const V39_UP: &str = include_str!("../migrations/v039_claim_head_cold_routing.sql");
const V40_UP: &str = include_str!("../migrations/v040_queue_runtime_overrides.sql");
const V41_UP: &str = include_str!("../migrations/v041_compact_deadline_claims.sql");
const V42_UP: &str = include_str!("../migrations/v042_ring_rotation_ledger.sql");

/// Old version numbers from pre-0.4 releases that used V3/V4/V5 numbering.
/// Also tolerates the unreleased inline-V6 branch numbering used during review.
/// Maps old max version → equivalent new version.
fn normalize_legacy_version(old_version: i32) -> i32 {
    match old_version {
        v if v >= 6 => 4, // legacy/unreleased V6 admin metadata = V4 (new)
        5 => 3,           // V5 (0.3.x) = V3 (new)
        4 => 2,           // V4 = V2 (new)
        3 => 1,           // V3 = V1 (new)
        _ => 0,           // Pre-canonical or fresh
    }
}

/// Run all pending migrations against the database.
///
/// Applies only migrations newer than the current schema version.
/// V1 bootstraps the canonical schema from scratch; V2+ are incremental
/// and use `IF NOT EXISTS` guards so they are safe to re-run. Legacy
/// `schema_version` rows from pre-0.4 releases are normalized to the new
/// numbering in [`current_version`] before the pending set is computed.
///
/// Safe to call concurrently from any number of processes against the same
/// database: a transaction-scoped advisory lock serializes runners, and every
/// step is idempotent, so concurrent runs converge on a single application.
///
/// Takes `&PgPool` for ergonomic use from Rust.
pub async fn run(pool: &PgPool) -> Result<(), AwaError> {
    let lock_key: i64 = 0x4157_415f_4d49_4752; // "AWA_MIGR"

    // Run the version check, ADR-037 finalize gate, and every migration step
    // inside a single transaction guarded by a *transaction-scoped* advisory
    // lock. `pg_advisory_xact_lock` gives us two properties a session-scoped
    // `pg_advisory_lock` did not:
    //
    //   1. Serialization: concurrent runners on the same database block on the
    //      lock, then re-read `schema_version` inside it and no-op. Advisory
    //      locks are per-database, so runners against different databases never
    //      contend — the correct semantics on both counts.
    //
    //   2. Cancellation safety: the lock is released automatically when the
    //      transaction ends. If this future is dropped mid-migration (e.g. a
    //      Rust caller wraps `run` in `tokio::time::timeout`), the transaction
    //      rolls back and the lock is freed immediately. A session-scoped lock
    //      would instead ride the pooled connection back into the pool still
    //      held — sqlx does not `DISCARD ALL` on release — and every later
    //      migrate in any process would block until that connection happened
    //      to close.
    //
    // Wrapping the steps in one transaction also makes a partial upgrade
    // atomic: a failed or cancelled run leaves no half-applied step. This is
    // only sound because every migration is transaction-safe (no
    // `CREATE INDEX CONCURRENTLY`, `VACUUM`, or explicit transaction control).
    let mut tx = pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_key)
        .execute(&mut *tx)
        .await?;
    apply_migrations(&mut tx).await?;
    tx.commit().await?;

    // Best-effort admin-metadata cache warmup, deliberately outside the
    // migration transaction: it must not extend the lock hold, and a slow or
    // timed-out refresh must not roll back the committed schema.
    warm_admin_metadata_cache(pool).await;

    Ok(())
}

async fn apply_migrations(conn: &mut PgConnection) -> Result<(), AwaError> {
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(&mut *conn)
            .await?;

    let current = if has_schema {
        current_version_conn(conn).await?
    } else {
        0
    };

    if has_schema && current < CURRENT_VERSION {
        check_storage_finalized_gate(conn).await?;
    }

    if !(has_schema && current == CURRENT_VERSION) {
        for &(version, description, steps) in MIGRATIONS {
            if version <= current {
                continue;
            }
            info!(version, description, "Applying migration");
            for step in steps {
                sqlx::raw_sql(step).execute(&mut *conn).await?;
            }
            info!(version, "Migration applied");
        }
    } else {
        info!(version = current, "Schema is up to date");
    }

    Ok(())
}

/// Warm the admin-metadata cache after migrations commit.
///
/// Since v006 removed the synchronous triggers on `jobs_hot`, the cache is
/// only updated by the maintenance leader. Refreshing here guarantees
/// `queue_stats()` and `state_counts()` return accurate data immediately after
/// `migrate()`. Best-effort: any failure is swallowed, since the schema is
/// already committed and the leader will refresh the cache anyway.
///
/// Runs on a *detached* connection, outside the migration transaction, so it
/// neither holds the migration advisory lock nor risks rolling back committed
/// schema on a slow refresh.
///
/// ## Why a detached connection, not `pool.begin()`
///
/// `migrations::run` may be driven on a short-lived runtime that is dropped the
/// instant it returns — the Python bridge builds a throwaway `current_thread`
/// runtime and `block_on`s the whole migration on it (see awa-python's
/// `run_migrations_offthread`). A *pooled* connection returns to the pool via
/// an async step on its owning runtime; if that runtime is torn down before the
/// return completes, the return is guillotined and the connection's pool
/// semaphore permit is leaked — one leaked permit eventually deadlocks every
/// later `acquire`, poisoning the client's whole pool. (This was #408's
/// regression: the warmup's `pool.begin()` was the last pool op before the
/// throwaway runtime dropped.)
///
/// `acquire().detach()` hands us a `PgConnection` the pool no longer tracks:
/// there is no return-to-pool step to race and no permit to strand. We close it
/// explicitly with `close().await`, which completes inline before the runtime
/// goes away. The pool is one connection smaller afterwards; it re-opens a
/// replacement lazily on the next `acquire`. This runs once per `migrate()`, so
/// the extra connect is negligible.
async fn warm_admin_metadata_cache(pool: &PgPool) {
    // Detach immediately so nothing borrowed from the pool outlives this
    // function on a runtime that may be about to be dropped.
    let Ok(conn) = pool.acquire().await else {
        return;
    };
    let mut conn = conn.detach();

    let has_refresh: Result<bool, _> = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'refresh_admin_metadata' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'awa'))",
    )
    .fetch_one(&mut conn)
    .await;

    if matches!(has_refresh, Ok(true)) {
        // Managed transaction (not raw `BEGIN; … COMMIT;`): the SET LOCAL scopes
        // a short statement timeout so a maintenance leader still holding the
        // cache advisory lock during a slow shutdown can't block us — but if
        // that timeout fires the batch aborts and a raw trailing COMMIT would
        // never run, leaving the connection "idle in transaction (aborted)". A
        // managed transaction's guard rolls back on drop, keeping it clean.
        if let Ok(mut tx) = conn.begin().await {
            let refreshed = sqlx::raw_sql(
                "SET LOCAL statement_timeout = '5s'; SELECT awa.refresh_admin_metadata();",
            )
            .execute(&mut *tx)
            .await;
            match refreshed {
                Ok(_) => {
                    let _ = tx.commit().await;
                }
                Err(_) => {
                    let _ = tx.rollback().await;
                }
            }
        }
    }

    // Detached: nothing returns this to the pool, so close it explicitly (inline,
    // before any short-lived runtime is dropped) rather than leaking the socket.
    let _ = conn.close().await;
}

/// The 0.7 migrate gate (#370 / ADR-037).
///
/// The canonical engine is deprecated in 0.7 and its code paths are removed
/// in 0.8, so pending migrations are only applied when the cluster can never
/// again route work to it: the storage transition must be finalized
/// (`state = 'active'`), or the install must be fresh. "Fresh" mirrors what
/// `awa.storage_auto_finalize_if_fresh` accepts at worker startup — an
/// unprepared canonical state with no jobs and no recently-live runtimes —
/// so the two doors admit exactly the same clusters. Anything else is a
/// cluster that must complete the staged 0.6 transition before upgrading.
///
/// Runs against any schema age: pre-v010 schemas (no transition machinery)
/// are treated as unprepared canonical, and each probe guards for tables
/// that don't exist yet at old versions.
async fn check_storage_finalized_gate(conn: &mut PgConnection) -> Result<(), AwaError> {
    let transition: Option<(String, Option<String>)> =
        if relation_exists(conn, "storage_transition_state").await? {
            sqlx::query_as(
                "SELECT state, prepared_engine FROM awa.storage_transition_state WHERE singleton",
            )
            .fetch_optional(&mut *conn)
            .await?
        } else {
            None
        };

    let (state, prepared_engine) = match transition {
        Some((state, prepared_engine)) => (state, prepared_engine),
        None => ("canonical".to_string(), None),
    };

    if state == "active" {
        return Ok(());
    }

    let effectively_fresh = state == "canonical"
        && prepared_engine.is_none()
        && !canonical_jobs_exist(conn).await?
        && !recently_live_runtimes_exist(conn).await?;

    if effectively_fresh {
        return Ok(());
    }

    Err(AwaError::StorageNotFinalized { state })
}

/// Relation existence probe covering both tables and views — `awa.jobs` is
/// a view over `jobs_hot` on current schemas.
async fn relation_exists(conn: &mut PgConnection, relation: &str) -> Result<bool, AwaError> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("awa.{relation}"))
        .fetch_one(&mut *conn)
        .await?;
    Ok(exists)
}

async fn canonical_jobs_exist(conn: &mut PgConnection) -> Result<bool, AwaError> {
    if !relation_exists(conn, "jobs").await? {
        return Ok(false);
    }
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM awa.jobs)")
        .fetch_one(&mut *conn)
        .await?;
    Ok(exists)
}

async fn recently_live_runtimes_exist(conn: &mut PgConnection) -> Result<bool, AwaError> {
    if !relation_exists(conn, "runtime_instances").await? {
        return Ok(false);
    }
    // The 90-second window matches the heartbeat-staleness gate used by
    // awa.storage_auto_finalize_if_fresh and storage_enter_mixed_transition.
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM awa.runtime_instances WHERE last_seen_at + make_interval(secs => 90) >= now())",
    )
    .fetch_one(&mut *conn)
    .await?;
    Ok(exists)
}

/// Read-only schema version probe: the raw `MAX(version)` with no legacy
/// normalization and **no writes**.
///
/// Health probes and other read paths must use this instead of
/// [`current_version`]: that helper rewrites `schema_version` rows when its
/// legacy-numbering heuristic fires, and the heuristic also matches a schema
/// *newer* than this binary (any version outside the known range with a
/// row at 6 or above) — exactly the supported rolling-deploy skew, which a
/// probe must observe without mutating.
pub async fn current_version_readonly(pool: &PgPool) -> Result<i32, AwaError> {
    let mut conn = pool.acquire().await?;
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

    let raw_version = version.unwrap_or(0);

    // If max version is within the current MIGRATIONS range and the expected
    // tables exist, this is a current install — skip legacy detection.
    if (1..=CURRENT_VERSION).contains(&raw_version) {
        // Quick check: does the schema match what we expect at this version?
        // If queue_state_counts exists, we're past v4 in the current numbering.
        let has_admin_tables: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'queue_state_counts')",
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(false);

        // Current v4+ has queue_state_counts. If we're at v4+ and have
        // the table, this is definitely a current install.
        if raw_version >= 4 && has_admin_tables {
            return Ok(raw_version);
        }
        // Current v1-v3 don't have queue_state_counts.
        if raw_version <= 3 {
            let has_runtime: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap_or(false);
            // v2+ has runtime_instances. If present, current install.
            if (raw_version >= 2 && has_runtime) || raw_version == 1 {
                return Ok(raw_version);
            }
        }
    }

    // Detect legacy version numbering from pre-0.4 releases.
    // Legacy installs used a different numbering scheme where v3-v6 mapped
    // to what is now v1-v4.
    let has_legacy_high: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM awa.schema_version WHERE version >= 6)")
            .fetch_one(&mut *conn)
            .await
            .unwrap_or(false);

    let has_admin_metadata: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'queue_state_counts')",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap_or(false);

    let is_legacy_v5_only = raw_version == 5 && !has_legacy_high && !has_admin_metadata;
    let is_legacy_v4_only = raw_version == 4 && !has_legacy_high && !has_admin_metadata;

    // Also detect a single legacy V3 row (0.3.0 with only canonical schema)
    // by checking if runtime_instances exists — if not, this is legacy V3.
    let is_legacy_v3_only = raw_version == 3
        && !has_legacy_high
        && {
            let has_runtime: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = 'awa' AND table_name = 'runtime_instances')",
        )
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(false);
            !has_runtime
        };

    if has_legacy_high || is_legacy_v5_only || is_legacy_v4_only || is_legacy_v3_only {
        let normalized = normalize_legacy_version(raw_version);
        info!(
            old_version = raw_version,
            new_version = normalized,
            "Normalizing legacy version numbering"
        );
        // Replace legacy rows so future calls return the new numbering.
        sqlx::query("DELETE FROM awa.schema_version WHERE version >= 3")
            .execute(&mut *conn)
            .await?;
        for &(v, desc, _) in MIGRATIONS {
            if v <= normalized {
                sqlx::query(
                    "INSERT INTO awa.schema_version (version, description) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING",
                )
                .bind(v)
                .bind(desc)
                .execute(&mut *conn)
                .await?;
            }
        }
        return Ok(normalized);
    }

    Ok(raw_version)
}

/// Get the raw SQL for all migrations (for extraction / external tooling).
pub fn migration_sql() -> Vec<(i32, &'static str, String)> {
    MIGRATIONS
        .iter()
        .map(|&(v, d, steps)| (v, d, steps.join("\n")))
        .collect()
}

/// Get migration SQL for a version range `(from, to]` — `from` is exclusive,
/// `to` is inclusive. Returns only migrations where `from < version <= to`.
pub fn migration_sql_range(from: i32, to: i32) -> Vec<(i32, &'static str, String)> {
    MIGRATIONS
        .iter()
        .filter(|&&(v, _, _)| v > from && v <= to)
        .map(|&(v, d, steps)| (v, d, steps.join("\n")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_sql_range_all() {
        let all = migration_sql_range(0, CURRENT_VERSION);
        assert_eq!(all.len(), MIGRATIONS.len());
        assert_eq!(all.first().unwrap().0, 1);
        assert_eq!(all.last().unwrap().0, CURRENT_VERSION);
    }

    #[test]
    fn migration_sql_range_subset() {
        let subset = migration_sql_range(2, CURRENT_VERSION);
        assert!(subset.iter().all(|(v, _, _)| *v > 2));
        let expected = MIGRATIONS.iter().filter(|&&(v, _, _)| v > 2).count();
        assert_eq!(subset.len(), expected);
    }

    #[test]
    fn migration_sql_range_single() {
        let single = migration_sql_range(2, 3);
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].0, 3);
        assert!(!single[0].2.is_empty());
    }

    #[test]
    fn migration_sql_range_empty_when_equal() {
        let empty = migration_sql_range(CURRENT_VERSION, CURRENT_VERSION);
        assert!(empty.is_empty());
    }

    #[test]
    fn migration_sql_range_empty_when_inverted() {
        let empty = migration_sql_range(3, 1);
        assert!(empty.is_empty());
    }

    #[test]
    fn migration_sql_range_matches_full() {
        let full = migration_sql();
        let ranged = migration_sql_range(0, CURRENT_VERSION);
        assert_eq!(full.len(), ranged.len());
        for (f, r) in full.iter().zip(ranged.iter()) {
            assert_eq!(f.0, r.0);
            assert_eq!(f.1, r.1);
            assert_eq!(f.2, r.2);
        }
    }
}
