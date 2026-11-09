//! Protect owned read connections while SQLx is still acknowledging BEGIN,
//! and detect ring partitions truncated while a read's snapshot is in use.
//!
//! SQLx records transaction depth after BEGIN completes. Dropping that future
//! earlier can leave a transaction that its normal rollback guard cannot see.

use std::future::Future;

use sqlx::pool::PoolConnection;
use sqlx::{PgConnection, PgPool, Postgres};

pub(crate) struct BeginGuard {
    // Callers borrow this field directly so the separate flag can be cleared
    // while the resulting transaction retains its connection borrow.
    pub(crate) connection: PoolConnection<Postgres>,
    pub(crate) begin_pending: bool,
}

impl BeginGuard {
    pub(crate) async fn acquire(pool: &PgPool) -> Result<Self, sqlx::Error> {
        Ok(Self {
            connection: pool.acquire().await?,
            begin_pending: true,
        })
    }
}

impl Drop for BeginGuard {
    fn drop(&mut self) {
        if self.begin_pending {
            self.connection.close_on_drop();
        }
    }
}

/// The partitioned queue-storage tables whose partitions prune and rollup
/// reclaim with `TRUNCATE`.
const RING_PARENT_TABLES: [&str; 13] = [
    "ready_entries",
    "ready_tombstones",
    "ready_claim_attempt_batches",
    "ready_segments",
    "receipt_completion_batches",
    "receipt_completion_tombstones",
    "done_entries",
    "queue_terminal_count_deltas",
    "leases",
    "lease_claims",
    "lease_claim_batches",
    "lease_claim_closures",
    "lease_claim_closure_batches",
];

/// The relfilenode of every ring partition in a queue-storage schema.
///
/// `TRUNCATE` is not MVCC-safe. A snapshot taken before a truncating
/// transaction commits sees the partition empty once it reads it after that
/// commit, and cannot see rows the same transaction moved to another
/// partition, as prune does with retained failed rows. Every `TRUNCATE` gives
/// the partition a new relfilenode. Read on the snapshot's connection once
/// before its transaction begins and once after it ends, each statement with
/// its own snapshot, equal fingerprints mean no partition was truncated while
/// the read's snapshot was in use.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PartitionFingerprint(Vec<i64>);

impl PartitionFingerprint {
    pub(crate) async fn read(
        connection: &mut PgConnection,
        schema: &str,
    ) -> Result<Self, sqlx::Error> {
        let parents: Vec<String> = RING_PARENT_TABLES
            .iter()
            .map(|table| format!("{schema}.{table}"))
            .collect();
        let relfilenodes: Vec<i64> = sqlx::query_scalar(
            r#"
            SELECT COALESCE(array_agg(child.relfilenode::bigint ORDER BY child.oid), ARRAY[]::bigint[])
            FROM pg_catalog.pg_inherits AS inheritance
            JOIN pg_catalog.pg_class AS child ON child.oid = inheritance.inhrelid
            WHERE inheritance.inhparent = ANY (
                ARRAY(SELECT to_regclass(parent) FROM unnest($1::text[]) AS parent)::oid[]
            )
            "#,
        )
        .bind(parents)
        .fetch_one(connection)
        .await?;
        Ok(Self(relfilenodes))
    }

    /// Whether no ring partition was truncated since `self` was read.
    pub(crate) async fn unchanged(
        &self,
        connection: &mut PgConnection,
        schema: &str,
    ) -> Result<bool, sqlx::Error> {
        Ok(Self::read(connection, schema).await? == *self)
    }
}

/// Times a snapshot read runs before it returns a value whose snapshot
/// overlapped a truncation.
const SNAPSHOT_READ_ATTEMPTS: usize = 3;

/// Runs `read` until a run's snapshot overlaps no ring partition truncation,
/// at most [`SNAPSHOT_READ_ATTEMPTS`] times. `read` returns its value and
/// whether its [`PartitionFingerprint`] was unchanged. Only maintenance
/// passes truncate, so the last run's value is returned even if it overlapped
/// one.
pub(crate) async fn read_until_untruncated<T, E, F, Fut>(mut read: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(T, bool), E>>,
{
    for _ in 1..SNAPSHOT_READ_ATTEMPTS {
        let (value, untruncated) = read().await?;
        if untruncated {
            return Ok(value);
        }
        tracing::debug!("a ring partition was truncated under a snapshot read; reading again");
    }
    read().await.map(|(value, _)| value)
}

#[cfg(test)]
mod tests {
    use super::{PartitionFingerprint, RING_PARENT_TABLES};
    use crate::queue_storage::{QueueStorage, QueueStorageConfig};

    #[sqlx::test(migrations = false)]
    async fn fingerprint_covers_every_ring_partition_and_sees_truncation(pool: sqlx::PgPool) {
        crate::migrations::run(&pool).await.unwrap();
        let store = QueueStorage::new(QueueStorageConfig {
            schema: "awa_qs_partition_fingerprint".into(),
            ..Default::default()
        })
        .unwrap();
        store.install(&pool).await.unwrap();
        let schema = store.schema();

        let partitioned: Vec<String> = sqlx::query_scalar(
            "SELECT parent.relname::text
             FROM pg_catalog.pg_partitioned_table AS partitioned
             JOIN pg_catalog.pg_class AS parent ON parent.oid = partitioned.partrelid
             WHERE parent.relnamespace = $1::regnamespace
             ORDER BY 1",
        )
        .bind(schema)
        .fetch_all(&pool)
        .await
        .unwrap();
        let mut listed: Vec<String> = RING_PARENT_TABLES.map(str::to_string).to_vec();
        listed.sort();
        assert_eq!(listed, partitioned);

        let partitions: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_catalog.pg_class
             WHERE relnamespace = $1::regnamespace AND relkind = 'r' AND relispartition",
        )
        .bind(schema)
        .fetch_one(&pool)
        .await
        .unwrap();
        let mut connection = pool.acquire().await.unwrap();
        let fingerprint = PartitionFingerprint::read(&mut connection, schema)
            .await
            .unwrap();
        assert_eq!(fingerprint.0.len() as i64, partitions);
        assert!(fingerprint
            .unchanged(&mut connection, schema)
            .await
            .unwrap());

        sqlx::query(&format!("TRUNCATE TABLE {schema}.done_entries_0"))
            .execute(&pool)
            .await
            .unwrap();
        assert!(!fingerprint
            .unchanged(&mut connection, schema)
            .await
            .unwrap());
    }
}
