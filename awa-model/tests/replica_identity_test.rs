//! Every awa table must stay writable while it is in a logical-replication
//! publication: PostgreSQL rejects UPDATE and DELETE on a published table
//! without a primary key, replica-identity index or FULL identity.

use awa_model::{migrations, PruneOutcome, QueueStorage, QueueStorageConfig};
use sqlx::PgPool;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const PATCH: &str = "replica_identity";

fn patch() -> &'static migrations::SchemaPatch {
    migrations::SCHEMA_PATCHES
        .iter()
        .find(|patch| patch.name == PATCH)
        .expect("the patch is registered")
}

async fn patch_pending(pool: &PgPool) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(migrations::schema_patch_status(pool)
        .await?
        .into_iter()
        .find(|status| status.name == PATCH)
        .expect("the patch is registered")
        .needs_repair)
}

fn custom_store(schema: &str, receipts: bool) -> Result<QueueStorage, Box<dyn std::error::Error>> {
    Ok(QueueStorage::new(QueueStorageConfig {
        schema: schema.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        lease_claim_receipts: receipts,
        ..Default::default()
    })?)
}

fn is_delta_table(table: &str) -> bool {
    table
        .strip_prefix("queue_terminal_count_deltas")
        .is_some_and(|suffix| {
            suffix.is_empty()
                || suffix.strip_prefix('_').is_some_and(|slot| {
                    !slot.is_empty() && slot.bytes().all(|b| b.is_ascii_digit())
                })
        })
}

/// Tables without a usable replica identity, and tables that use FULL without
/// being expected to.
async fn identity_problems(pool: &PgPool, schemas: &[&str]) -> Result<Vec<String>, sqlx::Error> {
    let tables: Vec<(String, String, String, bool, bool)> = sqlx::query_as(
        "SELECT n.nspname::text, c.relname::text, c.relreplident::text,
                EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary),
                EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident)
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = ANY($1) AND c.relkind IN ('r', 'p')
         ORDER BY 1, 2",
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;
    assert!(
        tables.len() > 100,
        "expected the full schema, found {}",
        tables.len()
    );
    let mut problems = Vec::new();
    for (schema, table, replident, primary_key, identity_index) in tables {
        let usable = match replident.as_str() {
            "d" => primary_key,
            "i" => identity_index,
            "f" => true,
            _ => false,
        };
        if !usable {
            problems.push(format!(
                "{schema}.{table}: no usable replica identity ({replident})"
            ));
        }
        let full_expected = is_delta_table(&table)
            || (schema == "awa"
                && matches!(
                    table.as_str(),
                    "admin_dirty_queue_marks" | "admin_dirty_kind_marks"
                ));
        if replident == "f" && !full_expected {
            problems.push(format!(
                "{schema}.{table}: unexpected REPLICA IDENTITY FULL"
            ));
        }
        if schema == "awa" && table == "job_unique_claims" && replident != "i" {
            problems.push(format!("{schema}.{table}: expected its uniqueness index"));
        }
    }
    Ok(problems)
}

/// UPDATE and DELETE on every table and partition. PostgreSQL checks the
/// replica identity at executor start, so `WHERE false` is enough.
async fn assert_update_and_delete_accepted(pool: &PgPool, schemas: &[&str]) -> TestResult {
    let tables: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT n.nspname::text, c.relname::text, a.attname::text
         FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = 1
         WHERE n.nspname = ANY($1) AND c.relkind = 'r'
         ORDER BY 1, 2",
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;
    assert!(
        tables.len() > 100,
        "expected the full schema, found {}",
        tables.len()
    );
    for (schema, table, column) in tables {
        let mut tx = pool.begin().await?;
        sqlx::raw_sql(&format!(
            "UPDATE {schema}.{table} SET {column} = {column} WHERE false;
             DELETE FROM {schema}.{table} WHERE false;"
        ))
        .execute(&mut *tx)
        .await
        .unwrap_or_else(|err| panic!("{schema}.{table} rejected UPDATE or DELETE: {err}"));
        tx.rollback().await?;
    }
    Ok(())
}

/// Puts the 0.6.17 state back: no identity on any patch target.
async fn clear_identities(pool: &PgPool, schemas: &[&str]) -> TestResult {
    sqlx::raw_sql(
        "ALTER TABLE awa.admin_dirty_queue_marks REPLICA IDENTITY DEFAULT;
         ALTER TABLE awa.admin_dirty_kind_marks REPLICA IDENTITY DEFAULT;
         ALTER TABLE awa.job_unique_claims REPLICA IDENTITY DEFAULT;",
    )
    .execute(pool)
    .await?;
    let deltas: Vec<String> = sqlx::query_scalar(
        "SELECT format('%I.%I', n.nspname, c.relname)
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = ANY($1) AND c.relname ~ '^queue_terminal_count_deltas(_[0-9]+)?$'",
    )
    .bind(schemas)
    .fetch_all(pool)
    .await?;
    assert!(!deltas.is_empty());
    for delta in deltas {
        sqlx::raw_sql(&format!("ALTER TABLE {delta} REPLICA IDENTITY DEFAULT"))
            .execute(pool)
            .await?;
    }
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn every_table_has_a_usable_replica_identity(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    custom_store("ri_receipts", true)?
        .prepare_schema(&pool)
        .await?;
    custom_store("ri_legacy", false)?
        .prepare_schema(&pool)
        .await?;
    let schemas = ["awa", "ri_receipts", "ri_legacy"];

    let problems = identity_problems(&pool, &schemas).await?;
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    assert!(patch_pending(&pool).await?.is_empty());
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn replica_identity_patch_repairs_existing_tables_and_replays_without_locks(
    pool: PgPool,
) -> TestResult {
    migrations::run(&pool).await?;
    custom_store("ri_custom", true)?
        .prepare_schema(&pool)
        .await?;
    let schemas = ["awa", "ri_custom"];
    clear_identities(&pool, &schemas).await?;

    let pending = patch_pending(&pool).await?;
    for expected in [
        "replica identity of awa.admin_dirty_kind_marks",
        "replica identity of awa.admin_dirty_queue_marks",
        "replica identity of awa.job_unique_claims",
        "replica identity of awa.queue_terminal_count_deltas",
        "replica identity of awa.queue_terminal_count_deltas_15",
        "replica identity of ri_custom.queue_terminal_count_deltas",
        "replica identity of ri_custom.queue_terminal_count_deltas_3",
    ] {
        assert!(
            pending.iter().any(|target| target == expected),
            "{expected} missing from {pending:?}"
        );
    }
    assert_eq!(pending.len(), 3 + 17 + 5, "{pending:?}");

    // The patch locks exactly the tables it alters, until its transaction ends.
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(patch().sql).execute(&mut *tx).await?;
    let locks: Vec<String> = sqlx::query_scalar(
        "SELECT format('%s.%s %s', n.nspname, c.relname, l.mode)
         FROM pg_locks l
         JOIN pg_class c ON c.oid = l.relation
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation'
           AND n.nspname = ANY($1)
           AND l.mode NOT IN ('AccessShareLock', 'RowShareLock', 'RowExclusiveLock')
         ORDER BY 1",
    )
    .bind(&schemas[..])
    .fetch_all(&mut *tx)
    .await?;
    let mut expected: Vec<String> = pending
        .iter()
        .map(|target| {
            format!(
                "{} AccessExclusiveLock",
                target.trim_start_matches("replica identity of ")
            )
        })
        .collect();
    expected.push("awa.idx_awa_jobs_unique ShareLock".to_string());
    expected.sort();
    assert_eq!(locks, expected);
    tx.commit().await?;

    assert!(patch_pending(&pool).await?.is_empty());
    let problems = identity_problems(&pool, &schemas).await?;
    assert!(problems.is_empty(), "{}", problems.join("\n"));

    let mut tx = pool.begin().await?;
    sqlx::raw_sql(patch().sql).execute(&mut *tx).await?;
    let replay_locks: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks l
         JOIN pg_class c ON c.oid = l.relation
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation'
           AND n.nspname = ANY($1)",
    )
    .bind(&schemas[..])
    .fetch_one(&mut *tx)
    .await?;
    assert_eq!(replay_locks, 0, "a replay must lock no table");
    tx.rollback().await?;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn replica_identity_patch_gives_up_on_a_lock_wait_and_changes_nothing(
    pool: PgPool,
) -> TestResult {
    migrations::run(&pool).await?;
    clear_identities(&pool, &["awa"]).await?;

    let mut holder = pool.begin().await?;
    sqlx::query("INSERT INTO awa.admin_dirty_kind_marks (kind) VALUES ('held')")
        .execute(&mut *holder)
        .await?;

    let started = std::time::Instant::now();
    let error = sqlx::raw_sql(patch().sql)
        .execute(&pool)
        .await
        .expect_err("the patch cannot lock a table another transaction writes");
    assert!(started.elapsed() < Duration::from_secs(5));
    let db_error = error.as_database_error().expect("a database error");
    assert_eq!(db_error.code().as_deref(), Some("55P03"), "{error}");
    assert!(
        db_error.message().contains("admin_dirty_kind_marks"),
        "{error}"
    );
    holder.rollback().await?;

    let identity: String = sqlx::query_scalar(
        "SELECT relreplident::text FROM pg_class WHERE oid = 'awa.admin_dirty_queue_marks'::regclass",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(identity, "d", "a failed patch leaves every table unchanged");

    sqlx::raw_sql(patch().sql).execute(&pool).await?;
    assert!(patch_pending(&pool).await?.is_empty());
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn all_tables_publication_accepts_update_and_delete(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    let receipts = custom_store("ri_receipts", true)?;
    receipts.prepare_schema(&pool).await?;
    sqlx::raw_sql("SET client_min_messages = error; CREATE PUBLICATION awa_ri_all FOR ALL TABLES")
        .execute(&pool)
        .await?;

    // Partitions created after the patch and the publication.
    let legacy = custom_store("ri_legacy", false)?;
    legacy.prepare_schema(&pool).await?;
    let schemas = ["awa", "ri_receipts", "ri_legacy"];
    assert_update_and_delete_accepted(&pool, &schemas).await?;

    // Unique-claim release and the dirty-mark drain delete rows.
    sqlx::raw_sql(
        "INSERT INTO awa.job_unique_claims (unique_key, job_id) VALUES ('\\x01', 1);
         DELETE FROM awa.job_unique_claims WHERE job_id = 1;
         INSERT INTO awa.jobs (kind, queue, args) VALUES ('ri', 'ri_canonical', '{}');",
    )
    .execute(&pool)
    .await?;
    let marks = "SELECT (SELECT count(*) FROM awa.admin_dirty_queue_marks)
                      + (SELECT count(*) FROM awa.admin_dirty_kind_marks)";
    let before: i64 = sqlx::query_scalar(marks).fetch_one(&pool).await?;
    assert!(before > 0, "the canonical insert marked its queue and kind");
    sqlx::raw_sql(
        "SELECT awa.recompute_dirty_admin_metadata(100);
         SELECT awa.refresh_admin_metadata();",
    )
    .execute(&pool)
    .await?;
    let after: i64 = sqlx::query_scalar(marks).fetch_one(&pool).await?;
    assert_eq!(after, 0, "the drain deleted the marks");

    // Queue-storage traffic and maintenance, on a schema prepared before and
    // one prepared after the publication.
    for store in [&receipts, &legacy] {
        store.enqueue_batch(&pool, "ri", 2, 20).await?;
        let claimed = store
            .claim_runtime_batch(&pool, "ri", 20, Duration::from_secs(60))
            .await?;
        assert_eq!(claimed.len(), 20);
        store.complete_runtime_batch(&pool, &claimed).await?;
        store.rotate(&pool).await?;
        store.rotate_leases(&pool).await?;
        store.rotate_claims(&pool).await?;
        store.rollup_terminal_count_deltas(&pool, 4).await?;
        let pruned = store.prune_oldest(&pool, Duration::ZERO).await?;
        assert!(matches!(pruned, PruneOutcome::Pruned { .. }), "{pruned:?}");
        store.prune_oldest_leases(&pool).await?;
        store.prune_oldest_claims(&pool).await?;
        store.rebuild_terminal_counters(&pool).await?;
    }
    Ok(())
}
