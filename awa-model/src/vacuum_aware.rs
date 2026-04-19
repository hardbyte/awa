use crate::error::AwaError;
use sqlx::PgPool;
use std::collections::BTreeMap;

const DEFAULT_SCHEMA: &str = "awa_exp";
const DEFAULT_QUEUE_SLOT_COUNT: usize = 16;
const DEFAULT_LEASE_SLOT_COUNT: usize = 8;

#[derive(Debug, Clone)]
pub struct VacuumAwareConfig {
    pub schema: String,
    pub queue_slot_count: usize,
    pub lease_slot_count: usize,
}

impl Default for VacuumAwareConfig {
    fn default() -> Self {
        Self {
            schema: DEFAULT_SCHEMA.to_string(),
            queue_slot_count: DEFAULT_QUEUE_SLOT_COUNT,
            lease_slot_count: DEFAULT_LEASE_SLOT_COUNT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub struct ClaimedEntry {
    pub queue: String,
    pub priority: i16,
    pub lane_seq: i64,
    pub ready_slot: i32,
    pub ready_generation: i64,
    pub lease_slot: i32,
    pub lease_generation: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueCounts {
    pub available: i64,
    pub running: i64,
    pub completed: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotateOutcome {
    Rotated { slot: i32, generation: i64 },
    SkippedBusy { slot: i32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneOutcome {
    Noop,
    Pruned { slot: i32 },
    Blocked { slot: i32 },
    SkippedActive { slot: i32 },
}

fn map_sqlx_error(err: sqlx::Error) -> AwaError {
    if let sqlx::Error::Database(ref db_err) = err {
        if db_err.code().as_deref() == Some("23505") {
            return AwaError::UniqueConflict {
                constraint: db_err.constraint().map(|c| c.to_string()),
            };
        }
    }
    AwaError::Database(err)
}

fn validate_ident(ident: &str) -> Result<(), AwaError> {
    let mut chars = ident.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => {
            return Err(AwaError::Validation(format!(
                "invalid SQL identifier: {ident}"
            )));
        }
    }

    if chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Ok(())
    } else {
        Err(AwaError::Validation(format!(
            "invalid SQL identifier: {ident}"
        )))
    }
}

fn ready_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.ready_entries_{slot}")
}

fn done_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.done_entries_{slot}")
}

fn lease_child_name(schema: &str, slot: usize) -> String {
    format!("{schema}.leases_{slot}")
}

/// Experimental vacuum-aware storage prototype.
///
/// Design goals:
/// - append-only queue segments in a rotated ring
/// - append-only completion segments keyed back to the queue segment
/// - a separate, faster rotating lease ring so delete churn is bounded by the
///   lease cycle rather than by queue retention
/// - hot mutable state restricted to queue cursors and counters
///
/// This is intentionally experimental and currently powers benchmark/tests
/// rather than the main Awa runtime.
pub struct VacuumAwareStore {
    config: VacuumAwareConfig,
}

impl VacuumAwareStore {
    pub fn new(config: VacuumAwareConfig) -> Result<Self, AwaError> {
        if config.queue_slot_count < 4 {
            return Err(AwaError::Validation(
                "vacuum-aware queue storage requires at least 4 queue slots".into(),
            ));
        }
        if config.lease_slot_count < 2 {
            return Err(AwaError::Validation(
                "vacuum-aware lease storage requires at least 2 lease slots".into(),
            ));
        }
        validate_ident(&config.schema)?;
        Ok(Self { config })
    }

    pub fn schema(&self) -> &str {
        &self.config.schema
    }

    pub fn slot_count(&self) -> usize {
        self.queue_slot_count()
    }

    pub fn queue_slot_count(&self) -> usize {
        self.config.queue_slot_count
    }

    pub fn lease_slot_count(&self) -> usize {
        self.config.lease_slot_count
    }

    pub fn ready_child_relname(&self, slot: usize) -> String {
        format!("ready_entries_{slot}")
    }

    pub fn done_child_relname(&self, slot: usize) -> String {
        format!("done_entries_{slot}")
    }

    pub fn leases_relname(&self) -> &'static str {
        "leases"
    }

    pub fn leases_child_relname(&self, slot: usize) -> String {
        format!("leases_{slot}")
    }

    pub async fn install(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();

        sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_ring_state (
                singleton      BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                current_slot   INT NOT NULL,
                generation     BIGINT NOT NULL,
                slot_count     INT NOT NULL
            )
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_ring_state (singleton, current_slot, generation, slot_count)
            VALUES (TRUE, 0, 0, $1)
            ON CONFLICT (singleton) DO NOTHING
            "#
        ))
        .bind(self.queue_slot_count() as i32)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_ring_slots (
                slot        INT PRIMARY KEY,
                generation  BIGINT NOT NULL
            )
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.lease_ring_state (
                singleton      BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                current_slot   INT NOT NULL,
                generation     BIGINT NOT NULL,
                slot_count     INT NOT NULL
            )
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.lease_ring_state (singleton, current_slot, generation, slot_count)
            VALUES (TRUE, 0, 0, $1)
            ON CONFLICT (singleton) DO NOTHING
            "#
        ))
        .bind(self.lease_slot_count() as i32)
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.lease_ring_slots (
                slot        INT PRIMARY KEY,
                generation  BIGINT NOT NULL
            )
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.queue_lanes (
                queue           TEXT NOT NULL,
                priority        SMALLINT NOT NULL,
                next_seq        BIGINT NOT NULL DEFAULT 1,
                claim_seq       BIGINT NOT NULL DEFAULT 1,
                available_count BIGINT NOT NULL DEFAULT 0,
                running_count   BIGINT NOT NULL DEFAULT 0,
                completed_count BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (queue, priority)
            )
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.leases (
                lease_slot        INT NOT NULL,
                lease_generation  BIGINT NOT NULL,
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                queue             TEXT NOT NULL,
                priority          SMALLINT NOT NULL,
                lane_seq          BIGINT NOT NULL,
                claimed_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (lease_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (lease_slot)
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.ready_entries (
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                queue             TEXT NOT NULL,
                priority          SMALLINT NOT NULL,
                lane_seq          BIGINT NOT NULL,
                created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                payload           JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                PRIMARY KEY (ready_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (ready_slot)
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.done_entries (
                ready_slot        INT NOT NULL,
                ready_generation  BIGINT NOT NULL,
                queue             TEXT NOT NULL,
                priority          SMALLINT NOT NULL,
                lane_seq          BIGINT NOT NULL,
                completed_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
                PRIMARY KEY (ready_slot, queue, priority, lane_seq)
            ) PARTITION BY LIST (ready_slot)
            "#
        ))
        .execute(pool)
        .await
        .map_err(map_sqlx_error)?;

        for slot in 0..self.queue_slot_count() {
            sqlx::query(&format!(
                r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.ready_entries
                FOR VALUES IN ({slot})
                "#,
                ready_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_ready_{slot}_lane
                    ON {} (queue, priority, lane_seq)
                "#,
                ready_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.done_entries
                FOR VALUES IN ({slot})
                "#,
                done_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_done_{slot}_lane
                    ON {} (queue, priority, lane_seq)
                "#,
                done_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.lease_slot_count() {
            sqlx::query(&format!(
                r#"
                CREATE TABLE IF NOT EXISTS {} PARTITION OF {schema}.leases
                FOR VALUES IN ({slot})
                "#,
                lease_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_lane
                    ON {} (queue, priority, lane_seq)
                "#,
                lease_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;

            sqlx::query(&format!(
                r#"
                CREATE INDEX IF NOT EXISTS idx_{schema}_leases_{slot}_ready_ref
                    ON {} (ready_slot, ready_generation)
                "#,
                lease_child_name(schema, slot)
            ))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;
        }

        self.reset(pool).await
    }

    pub async fn reset(&self, pool: &PgPool) -> Result<(), AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            TRUNCATE
                {schema}.ready_entries,
                {schema}.done_entries,
                {schema}.leases,
                {schema}.queue_lanes,
                {schema}.queue_ring_slots,
                {schema}.lease_ring_slots
            "#
        ))
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_state
            SET current_slot = 0,
                generation = 0,
                slot_count = $1
            WHERE singleton = TRUE
            "#
        ))
        .bind(self.queue_slot_count() as i32)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.lease_ring_state
            SET current_slot = 0,
                generation = 0,
                slot_count = $1
            WHERE singleton = TRUE
            "#
        ))
        .bind(self.lease_slot_count() as i32)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        for slot in 0..self.queue_slot_count() {
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.queue_ring_slots (slot, generation)
                VALUES ($1, $2)
                "#
            ))
            .bind(slot as i32)
            .bind(if slot == 0 { 0_i64 } else { -1_i64 })
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        for slot in 0..self.lease_slot_count() {
            sqlx::query(&format!(
                r#"
                INSERT INTO {schema}.lease_ring_slots (slot, generation)
                VALUES ($1, $2)
                "#
            ))
            .bind(slot as i32)
            .bind(if slot == 0 { 0_i64 } else { -1_i64 })
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)
    }

    async fn ensure_lane<'a>(
        &self,
        tx: &mut sqlx::Transaction<'a, sqlx::Postgres>,
        queue: &str,
        priority: i16,
    ) -> Result<(), AwaError> {
        let schema = self.schema();
        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.queue_lanes (queue, priority)
            VALUES ($1, $2)
            ON CONFLICT (queue, priority) DO NOTHING
            "#
        ))
        .bind(queue)
        .bind(priority)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        Ok(())
    }

    pub async fn enqueue_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        priority: i16,
        count: i64,
    ) -> Result<i64, AwaError> {
        if count <= 0 {
            return Ok(0);
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        self.ensure_lane(&mut tx, queue, priority).await?;

        let ring: (i32, i64) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let start_seq: i64 = sqlx::query_scalar(&format!(
            r#"
            UPDATE {schema}.queue_lanes
            SET next_seq = next_seq + $3,
                available_count = available_count + $3
            WHERE queue = $1 AND priority = $2
            RETURNING next_seq - $3
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(count)
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.ready_entries (
                ready_slot,
                ready_generation,
                queue,
                priority,
                lane_seq,
                payload
            )
            SELECT
                $1,
                $2,
                $3,
                $4,
                $5 + g - 1,
                jsonb_build_object('lane_seq', $5 + g - 1)
            FROM generate_series(1, $6::bigint) AS g
            "#
        ))
        .bind(ring.0)
        .bind(ring.1)
        .bind(queue)
        .bind(priority)
        .bind(start_seq)
        .bind(count)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(count)
    }

    pub async fn claim_batch(
        &self,
        pool: &PgPool,
        queue: &str,
        max_batch: i64,
    ) -> Result<Vec<ClaimedEntry>, AwaError> {
        if max_batch <= 0 {
            return Ok(Vec::new());
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let lane: Option<(i16, i64, i64, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT priority, claim_seq, next_seq, available_count
            FROM {schema}.queue_lanes
            WHERE queue = $1
              AND claim_seq < next_seq
              AND available_count > 0
            ORDER BY priority ASC
            LIMIT 1
            FOR UPDATE
            "#
        ))
        .bind(queue)
        .fetch_optional(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let Some((priority, claim_seq, next_seq, available_count)) = lane else {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        };

        let reserve = (next_seq - claim_seq).min(available_count).min(max_batch);
        if reserve <= 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(Vec::new());
        }

        let start_seq = claim_seq;
        let end_seq = claim_seq + reserve;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_lanes
            SET claim_seq = $3,
                available_count = available_count - $4,
                running_count = running_count + $4
            WHERE queue = $1 AND priority = $2
            "#
        ))
        .bind(queue)
        .bind(priority)
        .bind(end_seq)
        .bind(reserve)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let lease_ring: (i32, i64) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let claimed: Vec<ClaimedEntry> = sqlx::query_as(&format!(
            r#"
            INSERT INTO {schema}.leases (
                lease_slot,
                lease_generation,
                ready_slot,
                ready_generation,
                queue,
                priority,
                lane_seq,
                claimed_at
            )
            SELECT
                $1,
                $2,
                ready.ready_slot,
                ready.ready_generation,
                ready.queue,
                ready.priority,
                ready.lane_seq,
                clock_timestamp()
            FROM {schema}.ready_entries AS ready
            WHERE ready.queue = $3
              AND ready.priority = $4
              AND ready.lane_seq >= $5
              AND ready.lane_seq < $6
            ORDER BY ready.lane_seq ASC
            RETURNING
                queue,
                priority,
                lane_seq,
                ready_slot,
                ready_generation,
                lease_slot,
                lease_generation
            "#
        ))
        .bind(lease_ring.0)
        .bind(lease_ring.1)
        .bind(queue)
        .bind(priority)
        .bind(start_seq)
        .bind(end_seq)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if claimed.len() as i64 != reserve {
            return Err(AwaError::Validation(format!(
                "vacuum-aware claim reservation mismatch: reserved {reserve}, claimed {}",
                claimed.len()
            )));
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(claimed)
    }

    pub async fn complete_batch(
        &self,
        pool: &PgPool,
        claimed: &[ClaimedEntry],
    ) -> Result<usize, AwaError> {
        if claimed.is_empty() {
            return Ok(0);
        }

        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let lease_slots: Vec<i32> = claimed.iter().map(|entry| entry.lease_slot).collect();
        let queues: Vec<String> = claimed.iter().map(|entry| entry.queue.clone()).collect();
        let priorities: Vec<i16> = claimed.iter().map(|entry| entry.priority).collect();
        let lane_seqs: Vec<i64> = claimed.iter().map(|entry| entry.lane_seq).collect();

        let moved: Vec<ClaimedEntry> = sqlx::query_as(&format!(
            r#"
            WITH completed(lease_slot, queue, priority, lane_seq) AS (
                SELECT * FROM unnest($1::int[], $2::text[], $3::smallint[], $4::bigint[])
            )
            DELETE FROM {schema}.leases AS leases
            USING completed
            WHERE leases.lease_slot = completed.lease_slot
              AND leases.queue = completed.queue
              AND leases.priority = completed.priority
              AND leases.lane_seq = completed.lane_seq
            RETURNING
                leases.queue,
                leases.priority,
                leases.lane_seq,
                leases.ready_slot,
                leases.ready_generation,
                leases.lease_slot,
                leases.lease_generation
            "#
        ))
        .bind(&lease_slots)
        .bind(&queues)
        .bind(&priorities)
        .bind(&lane_seqs)
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if moved.is_empty() {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(0);
        }

        let moved_ready_slots: Vec<i32> = moved.iter().map(|entry| entry.ready_slot).collect();
        let moved_ready_generations: Vec<i64> =
            moved.iter().map(|entry| entry.ready_generation).collect();
        let moved_queues: Vec<String> = moved.iter().map(|entry| entry.queue.clone()).collect();
        let moved_priorities: Vec<i16> = moved.iter().map(|entry| entry.priority).collect();
        let moved_lane_seqs: Vec<i64> = moved.iter().map(|entry| entry.lane_seq).collect();

        sqlx::query(&format!(
            r#"
            INSERT INTO {schema}.done_entries (
                ready_slot,
                ready_generation,
                queue,
                priority,
                lane_seq,
                completed_at
            )
            SELECT ready_slot, ready_generation, queue, priority, lane_seq, clock_timestamp()
            FROM unnest(
                $1::int[],
                $2::bigint[],
                $3::text[],
                $4::smallint[],
                $5::bigint[]
            ) AS moved(ready_slot, ready_generation, queue, priority, lane_seq)
            "#
        ))
        .bind(&moved_ready_slots)
        .bind(&moved_ready_generations)
        .bind(&moved_queues)
        .bind(&moved_priorities)
        .bind(&moved_lane_seqs)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let mut per_lane: BTreeMap<(String, i16), i64> = BTreeMap::new();
        for entry in &moved {
            *per_lane
                .entry((entry.queue.clone(), entry.priority))
                .or_insert(0) += 1;
        }

        for ((queue, priority), count) in per_lane {
            sqlx::query(&format!(
                r#"
                UPDATE {schema}.queue_lanes
                SET running_count = GREATEST(0, running_count - $3),
                    completed_count = completed_count + $3
                WHERE queue = $1 AND priority = $2
                "#
            ))
            .bind(&queue)
            .bind(priority)
            .bind(count)
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;
        }

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(moved.len())
    }

    pub async fn queue_counts(&self, pool: &PgPool, queue: &str) -> Result<QueueCounts, AwaError> {
        let schema = self.schema();
        let row: Option<(i64, i64, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT
                COALESCE(sum(available_count), 0)::bigint AS available,
                COALESCE(sum(running_count), 0)::bigint AS running,
                COALESCE(sum(completed_count), 0)::bigint AS completed
            FROM {schema}.queue_lanes
            WHERE queue = $1
            "#
        ))
        .bind(queue)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let (available, running, completed) = row.unwrap_or((0, 0, 0));
        Ok(QueueCounts {
            available,
            running,
            completed,
        })
    }

    pub async fn rotate(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let next_slot = (state.0 + 1).rem_euclid(state.2);
        let ready_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            ready_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
        let done_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            done_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if ready_count > 0 || done_count > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
        }

        let next_generation = state.1 + 1;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_state
            SET current_slot = $1,
                generation = $2
            WHERE singleton = TRUE
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.queue_ring_slots
            SET generation = $2
            WHERE slot = $1
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    pub async fn rotate_leases(&self, pool: &PgPool) -> Result<RotateOutcome, AwaError> {
        let schema = self.schema();
        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;

        let state: (i32, i64, i32) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot, generation, slot_count
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            FOR UPDATE
            "#
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        let next_slot = (state.0 + 1).rem_euclid(state.2);
        let lease_count: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            lease_child_name(schema, next_slot as usize)
        ))
        .fetch_one(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        if lease_count > 0 {
            tx.commit().await.map_err(map_sqlx_error)?;
            return Ok(RotateOutcome::SkippedBusy { slot: next_slot });
        }

        let next_generation = state.1 + 1;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.lease_ring_state
            SET current_slot = $1,
                generation = $2
            WHERE singleton = TRUE
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        sqlx::query(&format!(
            r#"
            UPDATE {schema}.lease_ring_slots
            SET generation = $2
            WHERE slot = $1
            "#
        ))
        .bind(next_slot)
        .bind(next_generation)
        .execute(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;

        tx.commit().await.map_err(map_sqlx_error)?;
        Ok(RotateOutcome::Rotated {
            slot: next_slot,
            generation: next_generation,
        })
    }

    pub async fn prune_oldest(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();

        let state: (i32,) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.queue_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        let target: Option<(i32, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT slot, generation
            FROM {schema}.queue_ring_slots
            WHERE generation >= 0
              AND slot <> $1
            ORDER BY generation ASC, slot ASC
            LIMIT 1
            "#
        ))
        .bind(state.0)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let Some((slot, generation)) = target else {
            return Ok(PruneOutcome::Noop);
        };

        let active_leases: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {schema}.leases
            WHERE ready_slot = $1
              AND ready_generation = $2
            "#
        ))
        .bind(slot)
        .bind(generation)
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        if active_leases > 0 {
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let pending: i64 = sqlx::query_scalar(&format!(
            r#"
            SELECT count(*)::bigint
            FROM {} AS ready
            LEFT JOIN {} AS done
              ON done.ready_generation = ready.ready_generation
             AND done.queue = ready.queue
             AND done.priority = ready.priority
             AND done.lane_seq = ready.lane_seq
            WHERE done.lane_seq IS NULL
            "#,
            ready_child_name(schema, slot as usize),
            done_child_name(schema, slot as usize),
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        if pending > 0 {
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let truncate = sqlx::query(&format!(
            "TRUNCATE TABLE {}, {}",
            ready_child_name(schema, slot as usize),
            done_child_name(schema, slot as usize),
        ))
        .execute(tx.as_mut())
        .await;

        match truncate {
            Ok(_) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned { slot })
            }
            Err(_) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
            }
        }
    }

    pub async fn prune_oldest_leases(&self, pool: &PgPool) -> Result<PruneOutcome, AwaError> {
        let schema = self.schema();

        let state: (i32,) = sqlx::query_as(&format!(
            r#"
            SELECT current_slot
            FROM {schema}.lease_ring_state
            WHERE singleton = TRUE
            "#
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        let target: Option<(i32, i64)> = sqlx::query_as(&format!(
            r#"
            SELECT slot, generation
            FROM {schema}.lease_ring_slots
            WHERE generation >= 0
              AND slot <> $1
            ORDER BY generation ASC, slot ASC
            LIMIT 1
            "#
        ))
        .bind(state.0)
        .fetch_optional(pool)
        .await
        .map_err(map_sqlx_error)?;

        let Some((slot, _generation)) = target else {
            return Ok(PruneOutcome::Noop);
        };

        let active_leases: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*)::bigint FROM {}",
            lease_child_name(schema, slot as usize),
        ))
        .fetch_one(pool)
        .await
        .map_err(map_sqlx_error)?;

        if active_leases > 0 {
            return Ok(PruneOutcome::SkippedActive { slot });
        }

        let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
        sqlx::query("SET LOCAL lock_timeout = '50ms'")
            .execute(tx.as_mut())
            .await
            .map_err(map_sqlx_error)?;

        let truncate = sqlx::query(&format!(
            "TRUNCATE TABLE {}",
            lease_child_name(schema, slot as usize),
        ))
        .execute(tx.as_mut())
        .await;

        match truncate {
            Ok(_) => {
                tx.commit().await.map_err(map_sqlx_error)?;
                Ok(PruneOutcome::Pruned { slot })
            }
            Err(_) => {
                let _ = tx.rollback().await;
                Ok(PruneOutcome::Blocked { slot })
            }
        }
    }

    pub async fn vacuum_leases(&self, pool: &PgPool) -> Result<(), AwaError> {
        sqlx::query(&format!("VACUUM {}", self.leases_table()))
            .execute(pool)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }

    fn leases_table(&self) -> String {
        format!("{}.{}", self.schema(), self.leases_relname())
    }
}
