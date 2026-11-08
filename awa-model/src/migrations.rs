use crate::error::AwaError;
use sqlx::postgres::PgConnection;
use sqlx::PgPool;
use tracing::info;

/// Current schema version.
pub const CURRENT_VERSION: i32 = 40;

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
/// Representation changes require a release-specific compatibility and
/// authority-transition procedure.
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
    (
        40,
        "Allow queue-storage finalization with live canonical drain-only runtimes",
        &[V40_UP],
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
const V23_UP: &str = concat!(
    include_str!("../migrations/v023_install_queue_storage_substrate.sql"),
    "\n",
    include_str!("../migrations/v023_install_default_queue_storage_substrate.sql"),
);
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
const V40_UP: &str = include_str!("../migrations/v040_finalize_with_drain_runtimes.sql");

/// An idempotent schema repair shipped in a 0.6 patch release.
///
/// The 0.6 series cannot add migration versions — awa 0.7 owns v041 and
/// above — so a fix that must reach canonical clusters travels as a patch:
/// `awa migrate` applies it when the schema is exactly at [`CURRENT_VERSION`]
/// and `applied_probe` reports it missing, and the 0.7 migration carrying the
/// same SQL re-applies it cleanly over a patched cluster. A patch never writes
/// `awa.schema_version` and is never applied to a schema newer than this
/// binary, where a later migration may have superseded it.
#[derive(Debug, Clone, Copy)]
pub struct SchemaPatch {
    pub name: &'static str,
    pub description: &'static str,
    pub sql: &'static str,
    /// Boolean SQL: TRUE when the patch is already present.
    pub applied_probe: &'static str,
}

/// Rows naming what still lacks the `prepared_lane_sequences` patch: the
/// helper refresh function, the installer, and each installed queue-storage
/// substrate whose lane helpers predate it. A substrate is any schema the
/// patch would refresh: it has `claim_ready_runtime` and the lane tables and
/// functions the helpers call. Catalog lookups only, so a schema the probing
/// role cannot use is still reported rather than skipped.
macro_rules! prepared_lane_sequences_pending_sql {
    () => {
        "SELECT 'awa.refresh_lane_sequence_helpers'::text AS target \
         WHERE to_regprocedure('awa.refresh_lane_sequence_helpers(text)') IS NULL \
         UNION ALL \
         SELECT 'awa.install_queue_storage_substrate' \
         WHERE EXISTS ( \
             SELECT 1 FROM pg_proc AS p \
             WHERE p.pronamespace = 'awa'::regnamespace \
               AND p.proname = 'install_queue_storage_substrate' \
               AND p.prosrc NOT LIKE '%refresh_lane_sequence_helpers%') \
         UNION ALL \
         SELECT format('lane helpers in schema %s', n.nspname) \
         FROM pg_namespace AS n \
         WHERE EXISTS ( \
                 SELECT 1 FROM pg_proc AS p \
                 WHERE p.pronamespace = n.oid \
                   AND p.proname = 'claim_ready_runtime' \
                   AND oidvectortypes(p.proargtypes) \
                       = 'text, bigint, double precision, double precision') \
           AND EXISTS (SELECT 1 FROM pg_class AS c \
                       WHERE c.relnamespace = n.oid AND c.relname = 'queue_enqueue_heads') \
           AND EXISTS (SELECT 1 FROM pg_class AS c \
                       WHERE c.relnamespace = n.oid AND c.relname = 'queue_claim_heads') \
           AND EXISTS (SELECT 1 FROM pg_proc AS p \
                       WHERE p.pronamespace = n.oid \
                         AND p.proname = 'queue_lane_sequence_name' \
                         AND oidvectortypes(p.proargtypes) = 'text, text, smallint, smallint') \
           AND EXISTS (SELECT 1 FROM pg_proc AS p \
                       WHERE p.pronamespace = n.oid \
                         AND p.proname = 'set_sequence_next' \
                         AND oidvectortypes(p.proargtypes) = 'text, bigint') \
           AND (SELECT count(DISTINCT p.proname) FROM pg_proc AS p \
                WHERE p.pronamespace = n.oid \
                  AND p.proname IN ('ensure_lane_sequences', \
                                    'queue_enqueue_head_sequence_sync', \
                                    'queue_claim_head_sequence_sync') \
                  AND p.prosrc LIKE '%has not been provisioned%') < 3"
    };
}

/// Rows naming what still lacks the `definer_lane_provisioning` patch: the
/// lane-sequence check and repair functions, and each installed queue-storage
/// substrate (found as for `prepared_lane_sequences`) without
/// the SECURITY DEFINER provisioning functions or whose lane helpers do not
/// create sequences through them. Lane sequences the patch could not reassign
/// do not keep it pending: `awa storage lane-sequences` reports those.
macro_rules! definer_lane_provisioning_pending_sql {
    () => {
        "SELECT 'awa.lane_sequence_problems'::text AS target \
         WHERE to_regprocedure('awa.lane_sequence_problems(text,text)') IS NULL \
         UNION ALL \
         SELECT 'awa.repair_lane_sequence' \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM pg_proc AS p \
             WHERE p.oid = to_regprocedure('awa.repair_lane_sequence(text,text)') \
               AND p.prosrc LIKE '%set_config(''role'', v_original_role, true)%') \
         UNION ALL \
         SELECT 'awa.refresh_lane_sequence_helpers' \
         WHERE EXISTS ( \
             SELECT 1 FROM pg_proc AS p \
             WHERE p.pronamespace = 'awa'::regnamespace \
               AND p.proname = 'refresh_lane_sequence_helpers' \
               AND p.prosrc NOT LIKE '%provision_lane_sequences%') \
         UNION ALL \
         SELECT format('lane provisioning in schema %s', n.nspname) \
         FROM pg_namespace AS n \
         WHERE EXISTS ( \
                 SELECT 1 FROM pg_proc AS p \
                 WHERE p.pronamespace = n.oid \
                   AND p.proname = 'claim_ready_runtime' \
                   AND oidvectortypes(p.proargtypes) \
                       = 'text, bigint, double precision, double precision') \
           AND EXISTS (SELECT 1 FROM pg_class AS c \
                       WHERE c.relnamespace = n.oid AND c.relname = 'queue_enqueue_heads') \
           AND EXISTS (SELECT 1 FROM pg_class AS c \
                       WHERE c.relnamespace = n.oid AND c.relname = 'queue_claim_heads') \
           AND EXISTS (SELECT 1 FROM pg_proc AS p \
                       WHERE p.pronamespace = n.oid \
                         AND p.proname = 'queue_lane_sequence_name' \
                         AND oidvectortypes(p.proargtypes) = 'text, text, smallint, smallint') \
           AND EXISTS (SELECT 1 FROM pg_proc AS p \
                       WHERE p.pronamespace = n.oid \
                         AND p.proname = 'set_sequence_next' \
                         AND oidvectortypes(p.proargtypes) = 'text, bigint') \
           AND ((SELECT count(DISTINCT p.proname) FROM pg_proc AS p \
                 WHERE p.pronamespace = n.oid \
                   AND p.proname IN ('provision_lane', 'provision_lane_sequences') \
                   AND p.prosecdef \
                   AND oidvectortypes(p.proargtypes) = 'text, smallint, smallint') < 2 \
                OR (SELECT count(DISTINCT p.proname) FROM pg_proc AS p \
                    WHERE p.pronamespace = n.oid \
                      AND p.proname IN ('ensure_lane_sequences', \
                                        'queue_enqueue_head_sequence_sync', \
                                        'queue_claim_head_sequence_sync') \
                      AND p.prosrc LIKE '%provision_lane_sequences%') < 3)"
    };
}

/// SQL returning one `text` row per object or schema that still needs the
/// named patch, for patches that touch more than one place.
fn patch_targets_sql(name: &str) -> Option<&'static str> {
    match name {
        "prepared_lane_sequences" => Some(concat!(
            prepared_lane_sequences_pending_sql!(),
            " ORDER BY 1"
        )),
        "definer_lane_provisioning" => Some(concat!(
            definer_lane_provisioning_pending_sql!(),
            " ORDER BY 1"
        )),
        _ => None,
    }
}

pub const SCHEMA_PATCHES: &[SchemaPatch] = &[
    SchemaPatch {
    name: "wait_free_dirty_marks",
    description: "Wait-free admin dirty-key marks (awa 0.7 migration v046)",
    sql: include_str!("../migrations/patches/wait_free_dirty_marks.sql"),
    // Tables alone are not enough: a runner that applies the script statement by
    // statement and stops early leaves the blocking trigger bodies in place.
    applied_probe: "SELECT to_regclass('awa.admin_dirty_queue_marks') IS NOT NULL \
                    AND to_regclass('awa.admin_dirty_kind_marks') IS NOT NULL \
                    AND (SELECT count(*) FROM pg_proc \
                         WHERE pronamespace = 'awa'::regnamespace \
                           AND proname IN ('mark_dirty_keys_insert', 'mark_dirty_keys_delete', \
                                           'mark_dirty_keys_update', 'recompute_dirty_admin_metadata') \
                           AND prosrc LIKE '%admin_dirty_queue_marks%') = 4 \
                    AND EXISTS (SELECT 1 FROM pg_proc \
                                WHERE pronamespace = 'awa'::regnamespace \
                                  AND proname = 'refresh_admin_metadata' \
                                  AND prosrc LIKE '%admin_dirty_queue_marks%' \
                                  AND prosrc NOT LIKE '%TRUNCATE%')",
    },
    SchemaPatch {
        name: "prepared_lane_sequences",
        description: "Use provisioned lane sequences without runtime DDL (awa 0.7 migration v049)",
        // The helper and installer definitions first, so new schemas pick up
        // the updated helpers; the patch then replaces just those helpers on
        // every installed queue-storage schema.
        sql: concat!(
            include_str!("../migrations/v023_install_queue_storage_substrate.sql"),
            "\n",
            include_str!("../migrations/patches/prepared_lane_sequences.sql"),
        ),
        // Applied only when the installer and every installed substrate carry
        // the new definitions, so a custom schema restored or prepared with
        // the old helpers keeps the patch pending after `awa` passes.
        applied_probe: concat!(
            "SELECT NOT EXISTS (",
            prepared_lane_sequences_pending_sql!(),
            ")"
        ),
    },
    SchemaPatch {
        name: "definer_lane_provisioning",
        description: "Create queue lanes as the schema owner (awa 0.7 migration v053)",
        // The v023 definitions first, so the installer and the helper refresh
        // carry the provisioning functions; the patch then refreshes each
        // installed schema and repairs its existing lane sequences.
        sql: concat!(
            include_str!("../migrations/v023_install_queue_storage_substrate.sql"),
            "\n",
            include_str!("../migrations/patches/definer_lane_provisioning.sql"),
        ),
        applied_probe: concat!(
            "SELECT NOT EXISTS (",
            definer_lane_provisioning_pending_sql!(),
            ")"
        ),
    },
    SchemaPatch {
        name: "insert_jobs_compat_batch",
        description:
            "Batch producer insert with a short enqueue lane lock window (awa 0.7 migration v055)",
        sql: include_str!("../migrations/patches/insert_jobs_compat_batch.sql"),
        applied_probe: crate::insert::BATCH_FUNCTION_INSTALLED_SQL,
    },
];

/// Schema patches a database still needs to reach this binary's schema shape.
///
/// Below [`CURRENT_VERSION`] every patch is pending, because the migrations
/// that export alongside it leave the pre-patch function bodies in place; at
/// [`CURRENT_VERSION`] each patch is probed; on a newer schema nothing is
/// pending, since a later migration may have superseded the patch.
pub async fn pending_schema_patches(pool: &PgPool) -> Result<Vec<SchemaPatch>, AwaError> {
    let mut conn = pool.acquire().await?;
    let current = current_version_conn(&mut conn).await?;
    if current < CURRENT_VERSION {
        return Ok(SCHEMA_PATCHES.to_vec());
    }
    if current > CURRENT_VERSION {
        return Ok(Vec::new());
    }
    pending_schema_patches_conn(&mut conn).await
}

async fn pending_schema_patches_conn(
    conn: &mut PgConnection,
) -> Result<Vec<SchemaPatch>, AwaError> {
    let mut pending = Vec::new();
    for patch in SCHEMA_PATCHES {
        let applied: bool = sqlx::query_scalar(patch.applied_probe)
            .fetch_one(&mut *conn)
            .await?;
        if !applied {
            pending.push(*patch);
        }
    }
    Ok(pending)
}

/// Whether one schema patch is present, and if not, where it is missing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaPatchStatus {
    pub name: &'static str,
    pub description: &'static str,
    pub applied: bool,
    /// What still lacks the patch, such as a queue-storage schema whose lane
    /// helpers predate it. Empty when the patch is applied.
    pub needs_repair: Vec<String>,
}

/// Probe every schema patch against the database, naming what each missing
/// patch still has to repair. Unlike [`pending_schema_patches`] this probes
/// regardless of the schema version, so it describes a database that has not
/// been migrated yet as well as one that has.
pub async fn schema_patch_status(pool: &PgPool) -> Result<Vec<SchemaPatchStatus>, AwaError> {
    let mut conn = pool.acquire().await?;
    schema_patch_status_conn(&mut conn).await
}

async fn schema_patch_status_conn(
    conn: &mut PgConnection,
) -> Result<Vec<SchemaPatchStatus>, AwaError> {
    let mut statuses = Vec::with_capacity(SCHEMA_PATCHES.len());
    for patch in SCHEMA_PATCHES {
        let applied: bool = sqlx::query_scalar(patch.applied_probe)
            .fetch_one(&mut *conn)
            .await?;
        let needs_repair = match (applied, patch_targets_sql(patch.name)) {
            (true, _) => Vec::new(),
            (false, Some(targets)) => sqlx::query_scalar(targets).fetch_all(&mut *conn).await?,
            (false, None) => vec![patch.name.to_string()],
        };
        statuses.push(SchemaPatchStatus {
            name: patch.name,
            description: patch.description,
            applied,
            needs_repair,
        });
    }
    Ok(statuses)
}

/// Storage routing as recorded in `awa.storage_transition_state`.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct StorageRouting {
    pub state: String,
    pub current_engine: String,
    pub prepared_engine: Option<String>,
    pub queue_storage_schema: Option<String>,
    pub transition_epoch: i64,
}

impl std::fmt::Display for StorageRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "state {}, engine {}", self.state, self.current_engine)?;
        if let Some(schema) = &self.queue_storage_schema {
            write!(f, ", schema {schema}")?;
        }
        if let Some(prepared) = &self.prepared_engine {
            write!(f, ", prepared {prepared}")?;
        }
        write!(f, ", epoch {}", self.transition_epoch)
    }
}

async fn storage_routing_conn(conn: &mut PgConnection) -> Result<Option<StorageRouting>, AwaError> {
    let exists: bool =
        sqlx::query_scalar("SELECT to_regclass('awa.storage_transition_state') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await?;
    if !exists {
        return Ok(None);
    }
    let routing = sqlx::query_as::<_, StorageRouting>(
        "SELECT state, current_engine, prepared_engine, \
                details ->> 'schema' AS queue_storage_schema, transition_epoch \
         FROM awa.storage_transition_state WHERE singleton",
    )
    .fetch_optional(&mut *conn)
    .await?;
    Ok(routing)
}

/// What one schema patch did during a migration run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatchOutcome {
    /// Applied by this run.
    Applied,
    /// Already present before this run.
    AlreadyPresent,
    /// Not considered: the schema is newer than this binary, whose later
    /// migrations own these repairs.
    NotApplicable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchReport {
    pub name: &'static str,
    pub description: &'static str,
    pub outcome: PatchOutcome,
    /// Places this run repaired, such as queue-storage schemas whose lane
    /// helpers were replaced.
    pub repaired: Vec<String>,
    /// Places still lacking the patch after this run, usually a schema the
    /// migrating role cannot use or does not own. Empty on success.
    pub still_needs_repair: Vec<String>,
}

/// The result of [`run_with_report`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    pub version_before: i32,
    pub version_after: i32,
    pub migrations_applied: Vec<i32>,
    pub patches: Vec<PatchReport>,
    /// Storage routing before and after the run. Migrations and patches never
    /// change routing; both are `None` before the storage tables exist.
    pub storage_routing_before: Option<StorageRouting>,
    pub storage_routing_after: Option<StorageRouting>,
    /// What the run did to each lane sequence that needed repair, when it
    /// applied `definer_lane_provisioning`. Each sequence is repaired in its
    /// own short transaction after the patch commits; a sequence left behind
    /// does not make the run incomplete.
    pub lane_sequences: Vec<crate::storage::LaneSequenceRepair>,
}

impl MigrationReport {
    /// Whether every patch this binary owns is present on every substrate.
    pub fn complete(&self) -> bool {
        self.patches
            .iter()
            .all(|patch| patch.still_needs_repair.is_empty())
    }
}

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
    run_with_report(pool).await.map(|_| ())
}

/// Advisory lock key that serializes migration runners, including SQL
/// exported by `awa migrate --sql`. A session lock taken by [`run`] and a
/// transaction lock taken by exported SQL on the same key exclude each other.
pub const MIGRATION_LOCK_KEY: i64 = 0x4157_415f_4d49_4752; // "AWA_MIGR"

/// [`run`], returning what the run changed: the schema version, the
/// migrations and patches it applied, where each patch is still missing, and
/// storage routing before and after.
pub async fn run_with_report(pool: &PgPool) -> Result<MigrationReport, AwaError> {
    let mut conn = pool.acquire().await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *conn)
        .await?;

    let result = run_inner(&mut conn).await;

    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_LOCK_KEY)
        .execute(&mut *conn)
        .await;

    result
}

async fn run_inner(conn: &mut PgConnection) -> Result<MigrationReport, AwaError> {
    let has_schema: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = 'awa')")
            .fetch_one(&mut *conn)
            .await?;

    let current = if has_schema {
        current_version_conn(conn).await?
    } else {
        0
    };
    let storage_routing_before = storage_routing_conn(conn).await?;
    let mut migrations_applied = Vec::new();

    if current > CURRENT_VERSION {
        info!(
            schema_version = current,
            supported_schema_version = CURRENT_VERSION,
            "Schema is newer but forward-compatible; no migrations to apply"
        );
    } else if !(has_schema && current == CURRENT_VERSION) {
        for &(version, description, steps) in MIGRATIONS {
            if version <= current {
                continue;
            }
            info!(version, description, "Applying migration");
            for step in steps {
                sqlx::raw_sql(step).execute(&mut *conn).await?;
            }
            info!(version, "Migration applied");
            migrations_applied.push(version);
        }
    } else {
        info!(version = current, "Schema is up to date");
    }

    // Patches ride on the schema this binary owns. A newer schema belongs
    // to a newer binary whose migrations may have superseded them.
    let mut patches = Vec::with_capacity(SCHEMA_PATCHES.len());
    if current <= CURRENT_VERSION {
        let before = schema_patch_status_conn(conn).await?;
        for status in &before {
            if status.applied {
                continue;
            }
            let patch = SCHEMA_PATCHES
                .iter()
                .find(|patch| patch.name == status.name)
                .expect("status comes from SCHEMA_PATCHES");
            info!(
                patch = patch.name,
                description = patch.description,
                "Applying schema patch"
            );
            // One simple-query round trip runs as one implicit transaction:
            // an error or a cancelled statement leaves nothing applied.
            sqlx::raw_sql(patch.sql).execute(&mut *conn).await?;
            info!(patch = patch.name, "Schema patch applied");
        }
        let after = schema_patch_status_conn(conn).await?;
        for (before, after) in before.into_iter().zip(after) {
            if !after.needs_repair.is_empty() {
                tracing::warn!(
                    patch = after.name,
                    needs_repair = ?after.needs_repair,
                    "Schema patch is still missing; run awa migrate as a role that owns these schemas"
                );
            }
            let repaired = before
                .needs_repair
                .iter()
                .filter(|target| !after.needs_repair.contains(target))
                .cloned()
                .collect();
            patches.push(PatchReport {
                name: after.name,
                description: after.description,
                outcome: if before.applied {
                    PatchOutcome::AlreadyPresent
                } else {
                    PatchOutcome::Applied
                },
                repaired,
                still_needs_repair: after.needs_repair,
            });
        }
    } else {
        patches.extend(SCHEMA_PATCHES.iter().map(|patch| PatchReport {
            name: patch.name,
            description: patch.description,
            outcome: PatchOutcome::NotApplicable,
            repaired: Vec::new(),
            still_needs_repair: Vec::new(),
        }));
    }

    // ALTER SEQUENCE ... OWNER takes the sequence's ACCESS EXCLUSIVE lock
    // until its transaction ends, so the patch only installs the definitions
    // and each lane sequence is repaired here, in its own transaction with a
    // bounded lock wait, once the patch has committed.
    let mut lane_sequences = Vec::new();
    if patches.iter().any(|patch| {
        patch.name == "definer_lane_provisioning" && patch.outcome == PatchOutcome::Applied
    }) {
        match crate::storage::repair_lane_sequences_in_every_schema(conn).await {
            Ok(repairs) => {
                for next_step in crate::storage::lane_sequence_repair_next_steps(&repairs) {
                    tracing::warn!("{next_step}");
                }
                lane_sequences = repairs;
            }
            Err(error) => tracing::warn!(
                %error,
                "Could not repair lane sequences; run `awa storage lane-sequences --repair`"
            ),
        }
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

    let version_after = if current > CURRENT_VERSION {
        current
    } else {
        current_version_conn(conn).await?
    };
    Ok(MigrationReport {
        version_before: current,
        version_after,
        migrations_applied,
        patches,
        storage_routing_before,
        storage_routing_after: storage_routing_conn(conn).await?,
        lane_sequences,
    })
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

    if raw_version > CURRENT_VERSION {
        if forward_compatible_v043_columns(conn, raw_version).await? {
            return Ok(raw_version);
        }
        tracing::error!(
            schema_version = raw_version,
            supported_schema_version = CURRENT_VERSION,
            "This Awa binary is too old for the database schema; upgrade Awa before retrying"
        );
        return Err(AwaError::SchemaNotMigrated {
            expected: CURRENT_VERSION,
            found: raw_version,
        });
    }

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

/// The 0.6.2 rolling-upgrade stepping-stone recognizes exactly the additive
/// v043 shape, and only before any queue-storage schema has flipped to ledger
/// authority. This lets an already-rolled 0.6.2 fleet survive a migration-first
/// 0.7 rollout. Unknown future schemas and the post-flip boundary fail closed.
async fn forward_compatible_v043_columns(
    conn: &mut PgConnection,
    raw_version: i32,
) -> Result<bool, AwaError> {
    if raw_version != 43 {
        return Ok(false);
    }

    // quote_ident is computed by PostgreSQL before the identifier is used in
    // the dynamic query below. Queue-storage schema names are validated when
    // installed, but retaining quoting here keeps this probe safe for catalogs
    // created by external migration tooling too.
    let schemas: Vec<String> = sqlx::query_scalar(
        "SELECT quote_ident(n.nspname) \
         FROM pg_namespace AS n \
         JOIN pg_class AS c ON c.relnamespace = n.oid \
         WHERE c.relname = 'ring_cursor_authority' \
           AND c.relkind IN ('r', 'p') \
         ORDER BY n.nspname",
    )
    .fetch_all(&mut *conn)
    .await?;

    if schemas.is_empty() {
        return Ok(false);
    }

    for schema in schemas {
        let authority: Option<String> = sqlx::query_scalar(&format!(
            "SELECT authority FROM {schema}.ring_cursor_authority WHERE singleton"
        ))
        .fetch_optional(&mut *conn)
        .await?;
        if authority.as_deref() != Some("columns") {
            return Ok(false);
        }
    }

    Ok(true)
}

/// Get the raw SQL for all migrations (for extraction / external tooling).
///
/// Versioned migrations only. The idempotent [`SCHEMA_PATCHES`] that `run`
/// applies after the last migration are exported separately by
/// [`schema_patch_sql`], because they have no version of their own.
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

/// Schema patches as `(name, description, sql)`, for external runners. Each
/// patch is idempotent and versionless: apply it after the migrations that
/// bring the schema to [`CURRENT_VERSION`], as a repeatable migration
/// (`R__<name>.sql` in Flyway terms), never as another `V40` file.
pub fn schema_patch_sql(patches: &[SchemaPatch]) -> Vec<(&'static str, &'static str, String)> {
    patches
        .iter()
        .map(|patch| (patch.name, patch.description, patch.sql.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_patches_are_idempotent_and_versionless() {
        for patch in SCHEMA_PATCHES {
            // The installer body carries guarded DDL that only runs when the
            // function is called; lint the statements the patch runs itself.
            let top_level = match patch.sql.split("$install$").collect::<Vec<_>>().as_slice() {
                [before, _body, after] => format!("{before}{after}"),
                _ => patch.sql.to_string(),
            };
            let upper = top_level.to_uppercase();
            assert!(
                !upper.contains("INSERT INTO AWA.SCHEMA_VERSION"),
                "patch {} must not record a schema version",
                patch.name
            );
            assert!(
                !upper.contains("CREATE TABLE AWA.")
                    && !upper.contains("CREATE FUNCTION")
                    && !upper.contains("DROP TABLE")
                    && !upper.contains("ALTER TABLE"),
                "patch {} must use IF NOT EXISTS / OR REPLACE forms only",
                patch.name
            );
            assert!(!patch.applied_probe.is_empty());
        }
    }

    #[test]
    fn prepared_lane_patch_does_not_rerun_the_installer() {
        // The installer locks live queue-storage tables until commit; on an
        // existing database the patch replaces only the lane helpers.
        let patch = include_str!("../migrations/patches/prepared_lane_sequences.sql");
        let code: String = patch
            .lines()
            .filter(|line| !line.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(!code.contains("install_queue_storage_substrate"));
        assert!(code.contains("PERFORM awa.refresh_lane_sequence_helpers(v_schema)"));
    }

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
    fn schema_patch_sql_is_versionless_and_complete() {
        let exported = schema_patch_sql(SCHEMA_PATCHES);
        assert_eq!(exported.len(), SCHEMA_PATCHES.len());
        for ((name, _, sql), patch) in exported.iter().zip(SCHEMA_PATCHES) {
            assert_eq!(*name, patch.name);
            assert_eq!(sql, patch.sql);
        }
        assert!(migration_sql().iter().all(|(_, description, _)| {
            !SCHEMA_PATCHES.iter().any(|p| p.description == *description)
        }));
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
