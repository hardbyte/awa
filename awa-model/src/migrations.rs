use crate::error::AwaError;
use sqlx::postgres::PgConnection;
use sqlx::PgPool;
use tracing::info;

/// Current schema version.
pub const CURRENT_VERSION: i32 = 13;

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
/// and use `IF NOT EXISTS` guards so they are safe to re-run.
///
/// by replacing the legacy `schema_version` rows with the new numbering.
///
/// Takes `&PgPool` for ergonomic use from Rust.
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

    // Ensure the admin metadata cache is warm. Since v006 removed the
    // synchronous triggers on jobs_hot, the cache is only updated by the
    // maintenance leader. Refreshing here guarantees queue_stats() and
    // state_counts() return accurate data immediately after migrate().
    let has_refresh: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'refresh_admin_metadata' AND pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'awa'))",
    )
    .fetch_one(&mut *conn)
    .await?;
    if has_refresh {
        // Best-effort cache warmup. Uses a short statement timeout to avoid
        // blocking if a previous runtime's maintenance leader is still
        // holding the cache advisory lock during a slow shutdown.
        // Wrapped in an explicit transaction because SET LOCAL is only
        // effective inside a transaction block (not in autocommit mode).
        let _ = sqlx::raw_sql(
            "BEGIN; SET LOCAL statement_timeout = '5s'; SELECT awa.refresh_admin_metadata(); COMMIT;",
        )
        .execute(&mut *conn)
        .await;
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
