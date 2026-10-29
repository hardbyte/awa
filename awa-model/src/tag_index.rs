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

/// Index `target` if the caller owns it, it is empty, and it can be locked
/// without waiting: the build is then instant and blocks nothing. Returns
/// whether it built the index.
async fn index_if_empty(
    conn: &mut PgConnection,
    target: &TagIndexTarget,
) -> Result<bool, AwaError> {
    let mut tx = conn.begin().await.map_err(map_sqlx_error)?;
    let relation = format!(
        "{}.{}",
        quote_ident(&target.schema),
        quote_ident(&target.table)
    );
    let owned: bool = sqlx::query_scalar(
        "SELECT pg_has_role(current_user, c.relowner, 'USAGE') \
         FROM pg_class c WHERE c.oid = $1::regclass",
    )
    .bind(&relation)
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    if !owned {
        tx.rollback().await.map_err(map_sqlx_error)?;
        return Ok(false);
    }
    // SHARE blocks writers; NOWAIT means a busy partition is left for a
    // later pass rather than queueing a lock that would stall its lane.
    let locked = sqlx::query(audited_sql(format!(
        "LOCK TABLE {relation} IN SHARE MODE NOWAIT"
    )))
    .execute(tx.as_mut())
    .await;
    if let Err(err) = locked {
        let err = map_sqlx_error(err);
        // Roll back now: a dropped transaction is only rolled back the next
        // time its connection is used, and until then it would sit in the
        // pool holding a snapshot that stalls concurrent index builds.
        tx.rollback().await.map_err(map_sqlx_error)?;
        if is_lock_not_available(&err) {
            return Ok(false);
        }
        return Err(err);
    }
    let empty: bool = sqlx::query_scalar(audited_sql(format!(
        "SELECT NOT EXISTS (SELECT 1 FROM {relation})"
    )))
    .fetch_one(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    if !empty {
        tx.rollback().await.map_err(map_sqlx_error)?;
        return Ok(false);
    }
    sqlx::query(audited_sql(index_ddl(&target.schema, &target.table, false)))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(true)
}

fn is_lock_not_available(err: &AwaError) -> bool {
    match err {
        AwaError::Database(sqlx::Error::Database(db)) => db.code().as_deref() == Some("55P03"),
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
    /// Populated ring partitions left for a later pass.
    pub waiting: Vec<TagIndexTarget>,
}

impl TagIndexBuild {
    pub fn complete(&self) -> bool {
        self.waiting.is_empty()
    }
}

/// Run `ddl` outside a transaction with a short lock timeout, on a
/// connection of its own: `CONCURRENTLY` refuses to run in a transaction.
async fn run_concurrently(pool: &PgPool, ddl: String) -> Result<(), AwaError> {
    let mut conn = pool.acquire().await.map_err(map_sqlx_error)?;
    sqlx::query("SET lock_timeout = '5s'")
        .execute(conn.as_mut())
        .await
        .map_err(map_sqlx_error)?;
    let result = sqlx::query(audited_sql(ddl))
        .execute(conn.as_mut())
        .await
        .map_err(map_sqlx_error);
    let _ = sqlx::query("RESET lock_timeout")
        .execute(conn.as_mut())
        .await;
    result.map(|_| ())
}

/// One pass of the migrator's online build. Builds one index at a time,
/// repairs indexes an interrupted build left invalid, and reports the ring
/// partitions still waiting for pruning to empty them. Safe to repeat.
pub async fn build_tag_indexes(pool: &PgPool, mode: BuildMode) -> Result<TagIndexBuild, AwaError> {
    let mut outcome = TagIndexBuild::default();
    let mut conn = pool.acquire().await.map_err(map_sqlx_error)?;
    for status in tag_index_status_on(&mut conn).await? {
        let target = status.target;
        match status.state {
            TagIndexState::Valid => outcome.already_valid += 1,
            TagIndexState::Invalid => {
                run_concurrently(
                    pool,
                    format!(
                        "REINDEX INDEX CONCURRENTLY {}.{}",
                        quote_ident(&target.schema),
                        quote_ident(&index_name(&target.table))
                    ),
                )
                .await?;
                outcome.repaired += 1;
            }
            TagIndexState::Missing => {
                if index_if_empty(&mut conn, &target).await? {
                    outcome.built += 1;
                } else if !target.ring || mode == BuildMode::Now {
                    run_concurrently(pool, index_ddl(&target.schema, &target.table, true)).await?;
                    outcome.built += 1;
                } else {
                    outcome.waiting.push(target);
                }
            }
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
