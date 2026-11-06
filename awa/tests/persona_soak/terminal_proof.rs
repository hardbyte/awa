//! Durable evidence for returned eventful jobs and the NO_EVENTS sample.
//! Handler returns are not commits; lifecycle hooks are best-effort notifications.

use super::config::Persona;
use super::ledger::{expect, flag, JobRecord, TERMINAL_PROOF_STRIDE};
use super::shared::Shared;
use super::stop::Stop;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;

const MAX_ROWS: usize = 100_000;

pub async fn run(shared: Arc<Shared>, pool: PgPool, stop: Stop) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = stop.wait() => return,
            _ = tick.tick() => observe(&shared, &pool).await,
        }
    }
}

/// One statement gives one snapshot. Explicit eventful attempts have bounded
/// input/result counts and a two-second statement timeout. NO_EVENTS keeps its
/// immutable sample; neither a returned handler nor a missed query proves a commit.
pub async fn observe(shared: &Shared, pool: &PgPool) {
    let schema = shared.info.queue_storage_schema.as_deref().unwrap_or("awa");
    let mut pending = Vec::new();
    shared.ledger.for_each(|id, record| {
        if record.has(flag::RETURNED_COMPLETED)
            && !record.has(flag::NO_EVENTS)
            && !record.has_durable_terminal()
            && !record.has(flag::OPERATOR_REMOVED)
            && !record.has(flag::DLQ_PURGED)
            && pending.len() <= MAX_ROWS
        {
            pending.push((id, record.latest_run_lease));
        }
    });
    if pending.len() > MAX_ROWS {
        shared.incomplete(
            "durable terminal observer exceeded its pending job cap",
            vec![],
        );
        return;
    }
    let (pending_ids, pending_leases): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
    let result = async {
        let mut tx = pool.begin().await?;
        sqlx::query("SET LOCAL statement_timeout = '2s'")
            .execute(tx.as_mut())
            .await?;
        let rows: Vec<(i64, i64, String)> = sqlx::query_as(&format!(
            "WITH pending AS (
                 SELECT * FROM unnest($3::bigint[], $4::bigint[]) AS p(job_id, run_lease)
             )
             SELECT done.job_id, done.run_lease, done.state::text
             FROM {schema}.done_entries AS done
             JOIN pending USING (job_id, run_lease)
             UNION ALL
             SELECT dlq.job_id, dlq.run_lease, 'failed'
             FROM {schema}.dlq_entries AS dlq
             JOIN pending USING (job_id, run_lease)
             UNION ALL
             SELECT item.job_id, item.run_lease, 'completed'
             FROM {schema}.receipt_completion_batches AS batch
             CROSS JOIN LATERAL unnest(batch.job_ids, batch.run_leases) AS item(job_id, run_lease)
             JOIN pending USING (job_id, run_lease)
             WHERE batch.job_ids && $3
             UNION ALL
             SELECT job_id, run_lease, state::text FROM {schema}.done_entries
             WHERE job_id % $1 = 0 AND finalized_at >= now() - interval '30 seconds'
             UNION ALL
             SELECT job_id, run_lease, 'failed' FROM {schema}.dlq_entries
             WHERE job_id % $1 = 0 AND dlq_at >= now() - interval '30 seconds'
             UNION ALL
             SELECT item.job_id, item.run_lease, 'completed'
             FROM {schema}.receipt_completion_batches AS batch
             CROSS JOIN LATERAL unnest(batch.job_ids, batch.run_leases) AS item(job_id, run_lease)
             WHERE item.job_id % $1 = 0 AND batch.finalized_at >= now() - interval '30 seconds'
             LIMIT $2"
        ))
        .bind(TERMINAL_PROOF_STRIDE)
        .bind(MAX_ROWS as i64 + 1)
        .bind(pending_ids)
        .bind(pending_leases)
        .fetch_all(tx.as_mut())
        .await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(rows)
    }
    .await;
    let rows = match result {
        Ok(rows) if rows.len() <= MAX_ROWS => rows,
        Ok(_) => {
            shared.incomplete("durable terminal observer exceeded its row cap", vec![]);
            return;
        }
        Err(error) => {
            shared.incomplete(
                format!("durable terminal observer query failed: {error}"),
                vec![],
            );
            return;
        }
    };
    shared
        .run
        .add("terminal_proof_observer_rows", rows.len() as i64);
    let now = shared.now_ms() as u32;
    for (id, run_lease, state) in rows {
        shared.ledger.with(
            id,
            || JobRecord::new(Persona::Chaos, u32::MAX, now, expect::ANY_TERMINAL, 0),
            |record| {
                record.note_observed_terminal(run_lease, &state);
            },
        );
    }
}
