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
struct PartitionFingerprint(Vec<i64>);

impl PartitionFingerprint {
    async fn read(connection: &mut PgConnection, schema: &str) -> Result<Self, sqlx::Error> {
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
    async fn unchanged(
        &self,
        connection: &mut PgConnection,
        schema: &str,
    ) -> Result<bool, sqlx::Error> {
        Ok(Self::read(connection, schema).await? == *self)
    }
}

/// How one run of a snapshot read keeps ring truncations out of its result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TruncationGuard {
    /// Compare [`PartitionFingerprint`]s around the snapshot. Takes no
    /// partition locks beyond those of each query.
    Detect,
    /// Lock every ring partition in `ACCESS SHARE` mode before the snapshot
    /// is taken and hold the locks until commit, so no truncation can commit
    /// while the snapshot is in use. Prune waits for, or gives up on, the read.
    Lock,
}

/// One run of a repeatable-read snapshot read under a [`TruncationGuard`].
///
/// Call [`Self::before_begin`] on the read's connection before `BEGIN`,
/// [`Self::after_begin`] after `BEGIN` and before any statement that takes the
/// snapshot, and [`Self::untruncated`] after `COMMIT`.
pub(crate) struct GuardedSnapshot {
    fingerprint: Option<PartitionFingerprint>,
}

impl GuardedSnapshot {
    pub(crate) async fn before_begin(
        connection: &mut PgConnection,
        schema: &str,
        guard: TruncationGuard,
    ) -> Result<Self, sqlx::Error> {
        let fingerprint = match guard {
            TruncationGuard::Detect => Some(PartitionFingerprint::read(connection, schema).await?),
            TruncationGuard::Lock => None,
        };
        Ok(Self { fingerprint })
    }

    pub(crate) async fn after_begin(
        &self,
        connection: &mut PgConnection,
        schema: &str,
    ) -> Result<(), sqlx::Error> {
        if self.fingerprint.is_none() {
            let parents = RING_PARENT_TABLES
                .iter()
                .map(|table| format!("{schema}.{table}"))
                .collect::<Vec<_>>()
                .join(", ");
            sqlx::query(&format!("LOCK TABLE {parents} IN ACCESS SHARE MODE"))
                .execute(connection)
                .await?;
        }
        Ok(())
    }

    /// Whether no ring partition was truncated while the snapshot was in use.
    pub(crate) async fn untruncated(
        self,
        connection: &mut PgConnection,
        schema: &str,
    ) -> Result<bool, sqlx::Error> {
        match self.fingerprint {
            Some(fingerprint) => fingerprint.unchanged(connection, schema).await,
            None => Ok(true),
        }
    }
}

/// Runs of a snapshot read that detect truncation before one locks it out.
const DETECTING_ATTEMPTS: usize = 3;

/// Runs `read` until a run's snapshot overlaps no ring partition truncation.
/// The first [`DETECTING_ATTEMPTS`] runs use [`TruncationGuard::Detect`]; if
/// every one overlapped a truncation, a last run uses
/// [`TruncationGuard::Lock`], whose result cannot overlap one. `read` returns
/// its value and [`GuardedSnapshot::untruncated`].
pub(crate) async fn read_until_untruncated<T, E, F, Fut>(mut read: F) -> Result<T, E>
where
    F: FnMut(TruncationGuard) -> Fut,
    Fut: Future<Output = Result<(T, bool), E>>,
{
    for attempt in 1..=DETECTING_ATTEMPTS {
        let (value, untruncated) = read(TruncationGuard::Detect).await?;
        if untruncated {
            return Ok(value);
        }
        tracing::debug!(
            attempt,
            "a ring partition was truncated under a snapshot read; reading again"
        );
    }
    tracing::debug!(
        attempts = DETECTING_ATTEMPTS,
        "ring partitions were truncated under every snapshot read; reading with them locked"
    );
    read(TruncationGuard::Lock).await.map(|(value, _)| value)
}

#[cfg(test)]
mod tests {
    use super::{
        read_until_untruncated, GuardedSnapshot, PartitionFingerprint, TruncationGuard,
        DETECTING_ATTEMPTS, RING_PARENT_TABLES,
    };
    use crate::queue_storage::{QueueStorage, QueueStorageConfig};

    #[tokio::test]
    async fn reads_with_partitions_locked_after_every_detecting_run_overlaps_a_truncation() {
        let mut guards = Vec::new();
        let value = read_until_untruncated(|guard| {
            guards.push(guard);
            let untruncated = guard == TruncationGuard::Lock;
            async move { Ok::<_, ()>((value_for(guard), untruncated)) }
        })
        .await
        .unwrap();
        let mut expected = vec![TruncationGuard::Detect; DETECTING_ATTEMPTS];
        expected.push(TruncationGuard::Lock);
        assert_eq!(guards, expected);
        assert_eq!(value, "locked");
    }

    fn value_for(guard: TruncationGuard) -> &'static str {
        match guard {
            TruncationGuard::Detect => "detected",
            TruncationGuard::Lock => "locked",
        }
    }

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

    #[sqlx::test(migrations = false)]
    async fn locked_snapshot_keeps_truncation_out_until_commit(pool: sqlx::PgPool) {
        crate::migrations::run(&pool).await.unwrap();
        let store = QueueStorage::new(QueueStorageConfig {
            schema: "awa_qs_locked_snapshot".into(),
            ..Default::default()
        })
        .unwrap();
        store.install(&pool).await.unwrap();
        let schema = store.schema();

        let mut reader = pool.acquire().await.unwrap();
        let snapshot = GuardedSnapshot::before_begin(&mut reader, schema, TruncationGuard::Lock)
            .await
            .unwrap();
        let mut tx = sqlx::Connection::begin_with(
            &mut *reader,
            "BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY",
        )
        .await
        .unwrap();
        snapshot.after_begin(tx.as_mut(), schema).await.unwrap();

        let mut pruner = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(pruner.as_mut())
            .await
            .unwrap();
        let error = sqlx::query(&format!("TRUNCATE TABLE {schema}.done_entries_0"))
            .execute(pruner.as_mut())
            .await
            .expect_err("truncation must wait for the locked snapshot");
        assert_eq!(
            error
                .as_database_error()
                .and_then(|db| db.code())
                .as_deref(),
            Some("55P03")
        );
        pruner.rollback().await.unwrap();

        tx.commit().await.unwrap();
        assert!(snapshot.untruncated(&mut reader, schema).await.unwrap());
    }
}
