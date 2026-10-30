//! Partial GIN indexes over job tags on queue storage (ADR-049).
//!
//! Each table where a job's row outlives its claim gets, on every partition,
//! an index over `payload -> 'tags'` for rows that have tags. Postgres keeps
//! it current for every binary, pruning's truncation empties it, and no
//! insert path has to remember it.
//!
//! Building one blocks writes to its table for the length of the build, and
//! only the table's owner may build it. So indexes are created two ways:
//! - [`index_empty_partitions`] indexes only partitions that are empty and
//!   free to lock without waiting, which is instant. Schema preparation and
//!   migration run it, so new and freshly installed schemas come out
//!   indexed.
//! - [`build_tag_indexes`] is the migrator's online build for populated
//!   schemas: it waits for each ring partition to be emptied by pruning, or
//!   with [`BuildMode::Now`] builds populated ones `CONCURRENTLY`.

use crate::audited_sql;
use crate::error::{map_sqlx_error, AwaError};
use serde::{Deserialize, Serialize};
use sqlx::{Connection, PgConnection, PgPool};

/// The index definition, shared by every partition and table.
fn index_ddl(schema: &str, table: &str, concurrently: bool) -> String {
    format!(
        "CREATE INDEX {}IF NOT EXISTS {} ON {}.{} \
         USING gin ((payload -> 'tags') jsonb_path_ops) \
         WITH (fastupdate = off) \
         WHERE (payload -> 'tags') <> '[]'::jsonb",
        if concurrently { "CONCURRENTLY " } else { "" },
        quote_ident(&index_name(table)),
        quote_ident(schema),
        quote_ident(table),
    )
}

fn index_name(table: &str) -> String {
    format!("{table}_tags_idx")
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// One table that needs a tag index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagIndexTarget {
    pub schema: String,
    pub table: String,
    /// A ring partition, emptied by pruning; otherwise a plain table.
    pub ring: bool,
}

/// Where a target's index stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TagIndexState {
    Valid,
    /// Left behind by an interrupted concurrent build.
    Invalid,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagIndexStatus {
    pub target: TagIndexTarget,
    pub state: TagIndexState,
}

/// Every table in every queue-storage schema that should carry a tag index,
/// with the state of its index.
pub async fn tag_index_status(pool: &PgPool) -> Result<Vec<TagIndexStatus>, AwaError> {
    let mut conn = pool.acquire().await.map_err(map_sqlx_error)?;
    tag_index_status_on(&mut conn).await
}

async fn tag_index_status_on(conn: &mut PgConnection) -> Result<Vec<TagIndexStatus>, AwaError> {
    // A database not yet migrated to 0.7 has no schema discovery function,
    // and no tag indexes either.
    let discoverable: bool =
        sqlx::query_scalar("SELECT to_regprocedure('awa.queue_storage_schemas()') IS NOT NULL")
            .fetch_one(&mut *conn)
            .await
            .map_err(map_sqlx_error)?;
    if !discoverable {
        return Ok(Vec::new());
    }
    let rows: Vec<(String, String, bool, Option<bool>)> = sqlx::query_as(
        r#"
        WITH schemas AS (
            SELECT schema_name FROM awa.queue_storage_schemas() AS s(schema_name)
        ),
        targets AS (
            SELECT n.nspname AS schema_name, c.relname AS table_name,
                   parent.relname IS NOT NULL AS ring, c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN schemas ON schemas.schema_name = n.nspname
            LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
            LEFT JOIN pg_class parent ON parent.oid = i.inhparent
            WHERE c.relkind = 'r'
              AND (
                  parent.relname IN ('ready_entries', 'done_entries')
                  OR (parent.relname IS NULL AND c.relname IN ('deferred_jobs', 'dlq_entries'))
              )
        )
        SELECT t.schema_name, t.table_name, t.ring, idx.indisvalid
        FROM targets t
        LEFT JOIN pg_class ic
          ON ic.relname = t.table_name || '_tags_idx'
         AND ic.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = t.schema_name)
        LEFT JOIN pg_index idx ON idx.indexrelid = ic.oid
        ORDER BY t.schema_name, t.ring, t.table_name
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;
    Ok(rows
        .into_iter()
        .map(|(schema, table, ring, valid)| TagIndexStatus {
            target: TagIndexTarget {
                schema,
                table,
                ring,
            },
            state: match valid {
                Some(true) => TagIndexState::Valid,
                Some(false) => TagIndexState::Invalid,
                None => TagIndexState::Missing,
            },
        })
        .collect())
}

/// Largest heap, in bytes, the plain build will lock and scan. An empty
/// table can still hold a burst's dead tuples until vacuum, and both the
/// emptiness check and the build would read them under a lock that blocks
/// writers.
const EMPTY_HEAP_BYTES: i64 = 64 * 1024;

/// Index `target` if the caller owns it, it is empty and small, and it can
/// be locked without waiting: the build is then instant and blocks nothing.
/// Returns whether it built the index.
async fn index_if_empty(
    conn: &mut PgConnection,
    target: &TagIndexTarget,
) -> Result<bool, AwaError> {
    let relation = format!(
        "{}.{}",
        quote_ident(&target.schema),
        quote_ident(&target.table)
    );
    let (owned, heap_bytes): (bool, i64) = sqlx::query_as(
        "SELECT pg_has_role(current_user, c.relowner, 'USAGE'), pg_relation_size(c.oid) \
         FROM pg_class c WHERE c.oid = $1::regclass",
    )
    .bind(&relation)
    .fetch_one(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;
    if !owned || heap_bytes > EMPTY_HEAP_BYTES {
        return Ok(false);
    }
    let mut tx = conn.begin().await.map_err(map_sqlx_error)?;
    let built = build_if_empty(&mut tx, target, &relation).await;
    // Roll back explicitly on every path that does not commit: a dropped
    // transaction is only rolled back the next time its connection is used,
    // and until then it would sit in the pool holding the lock and a
    // snapshot that stalls concurrent index builds.
    match built {
        Ok(true) => {
            tx.commit().await.map_err(map_sqlx_error)?;
            Ok(true)
        }
        Ok(false) => {
            tx.rollback().await.map_err(map_sqlx_error)?;
            Ok(false)
        }
        Err(err) => {
            tx.rollback().await.map_err(map_sqlx_error)?;
            if is_retryable(&err) {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }
}

async fn build_if_empty(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: &TagIndexTarget,
    relation: &str,
) -> Result<bool, AwaError> {
    // SHARE blocks writers; NOWAIT means a busy partition is left for a
    // later pass rather than queueing a lock that would stall its lane.
    sqlx::query(audited_sql(format!(
        "LOCK TABLE {relation} IN SHARE MODE NOWAIT"
    )))
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    let empty: bool = sqlx::query_scalar(audited_sql(format!(
        "SELECT NOT EXISTS (SELECT 1 FROM {relation})"
    )))
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    if !empty {
        return Ok(false);
    }
    sqlx::query(audited_sql(index_ddl(&target.schema, &target.table, false)))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
    Ok(true)
}

/// A lock that wasn't available in time, or a lost deadlock: the target is
/// left for a later pass.
fn is_retryable(err: &AwaError) -> bool {
    match err {
        AwaError::Database(sqlx::Error::Database(db)) => {
            matches!(db.code().as_deref(), Some("55P03" | "40P01"))
        }
        _ => false,
    }
}

/// Index every missing target that is empty, owned and free to lock: the
/// cheap pass schema preparation and migration run. Returns how many it
/// built.
pub async fn index_empty_partitions(conn: &mut PgConnection) -> Result<usize, AwaError> {
    let mut built = 0;
    for status in tag_index_status_on(conn).await? {
        if status.state == TagIndexState::Missing && index_if_empty(conn, &status.target).await? {
            built += 1;
        }
    }
    Ok(built)
}

/// How [`build_tag_indexes`] treats populated tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildMode {
    /// Index ring partitions only once pruning has emptied them; build the
    /// unpartitioned tables concurrently.
    WhenEmpty,
    /// Build every populated table now with `CREATE INDEX CONCURRENTLY`. It
    /// holds back vacuum while it runs and blocks pruning of the partition
    /// it is building.
    Now,
}

/// What one pass of [`build_tag_indexes`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagIndexBuild {
    pub built: usize,
    pub repaired: usize,
    pub already_valid: usize,
    /// Left for a later pass: populated ring partitions, and tables that
    /// were busy past the lock timeout or lost a deadlock.
    pub waiting: Vec<TagIndexTarget>,
}

impl TagIndexBuild {
    pub fn complete(&self) -> bool {
        self.waiting.is_empty()
    }
}

/// Run `ddl` outside a transaction with a short lock timeout:
/// `CONCURRENTLY` refuses to run in a transaction.
async fn run_concurrently(conn: &mut PgConnection, ddl: String) -> Result<(), AwaError> {
    sqlx::query("SET lock_timeout = '5s'")
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx_error)?;
    let result = sqlx::query(audited_sql(ddl))
        .execute(&mut *conn)
        .await
        .map_err(map_sqlx_error);
    let _ = sqlx::query("RESET lock_timeout").execute(&mut *conn).await;
    result.map(|_| ())
}

/// Whether `target`'s index exists and is valid.
async fn index_valid(conn: &mut PgConnection, target: &TagIndexTarget) -> Result<bool, AwaError> {
    let valid: Option<bool> = sqlx::query_scalar(
        "SELECT idx.indisvalid FROM pg_index idx \
         JOIN pg_class ic ON ic.oid = idx.indexrelid \
         JOIN pg_namespace n ON n.oid = ic.relnamespace \
         WHERE n.nspname = $1 AND ic.relname = $2",
    )
    .bind(&target.schema)
    .bind(index_name(&target.table))
    .fetch_optional(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;
    Ok(valid == Some(true))
}

/// Drop the `_ccnew` copies a failed `REINDEX CONCURRENTLY` leaves behind:
/// they are invalid, so no query uses them, but every write maintains them.
async fn drop_abandoned_copies(conn: &mut PgConnection) -> Result<(), AwaError> {
    let abandoned: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT n.nspname, ic.relname
        FROM pg_index idx
        JOIN pg_class ic ON ic.oid = idx.indexrelid
        JOIN pg_namespace n ON n.oid = ic.relnamespace
        WHERE NOT idx.indisvalid
          AND ic.relname LIKE '%\_tags\_idx\_ccnew%'
          AND n.nspname IN (SELECT s FROM awa.queue_storage_schemas() AS q(s))
        "#,
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(map_sqlx_error)?;
    for (schema, index) in abandoned {
        let dropped = run_concurrently(
            conn,
            format!(
                "DROP INDEX CONCURRENTLY IF EXISTS {}.{}",
                quote_ident(&schema),
                quote_ident(&index)
            ),
        )
        .await;
        match dropped {
            Err(err) if !is_retryable(&err) => return Err(err),
            _ => {}
        }
    }
    Ok(())
}

/// One pass of the migrator's online build. Builds one index at a time,
/// repairs indexes an interrupted build left invalid, and reports what is
/// left for a later pass: ring partitions waiting for pruning to empty them,
/// and tables that stayed busy past the lock timeout. Safe to repeat.
pub async fn build_tag_indexes(pool: &PgPool, mode: BuildMode) -> Result<TagIndexBuild, AwaError> {
    let mut outcome = TagIndexBuild::default();
    // One connection throughout, so a pool of one is enough.
    let mut conn = pool.acquire().await.map_err(map_sqlx_error)?;
    let statuses = tag_index_status_on(&mut conn).await?;
    if !statuses.is_empty() {
        drop_abandoned_copies(&mut conn).await?;
    }
    for status in statuses {
        let target = status.target;
        let ddl = match status.state {
            TagIndexState::Valid => {
                outcome.already_valid += 1;
                continue;
            }
            TagIndexState::Invalid => format!(
                "REINDEX INDEX CONCURRENTLY {}.{}",
                quote_ident(&target.schema),
                quote_ident(&index_name(&target.table))
            ),
            TagIndexState::Missing => {
                if index_if_empty(&mut conn, &target).await? {
                    outcome.built += 1;
                    continue;
                }
                if target.ring && mode == BuildMode::WhenEmpty {
                    outcome.waiting.push(target);
                    continue;
                }
                index_ddl(&target.schema, &target.table, true)
            }
        };
        match run_concurrently(&mut conn, ddl).await {
            Err(err) if is_retryable(&err) => {
                outcome.waiting.push(target);
                continue;
            }
            result => result?,
        }
        // `IF NOT EXISTS` also succeeds on an index another session is still
        // building, and a cancelled build leaves an invalid one: only a valid
        // index counts as done.
        if !index_valid(&mut conn, &target).await? {
            outcome.waiting.push(target);
        } else if status.state == TagIndexState::Invalid {
            outcome.repaired += 1;
        } else {
            outcome.built += 1;
        }
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_repeats_the_query_predicate() {
        let ddl = index_ddl("awa", "ready_entries_3", false);
        assert!(ddl.contains("\"awa\".\"ready_entries_3\""), "{ddl}");
        assert!(ddl.contains("\"ready_entries_3_tags_idx\""), "{ddl}");
        assert!(
            ddl.contains("WHERE (payload -> 'tags') <> '[]'::jsonb"),
            "{ddl}"
        );
        assert!(ddl.contains("fastupdate = off"), "{ddl}");
        assert!(index_ddl("awa", "dlq_entries", true).contains("CONCURRENTLY"));
    }
}
