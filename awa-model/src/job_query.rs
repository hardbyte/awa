//! Compiles a [`JobFilter`] to SQL, once per storage engine (ADR-049).
//!
//! The SQL narrows candidates: it writes a predicate only for a field the
//! filter sets, so a prepared statement's generic plan keeps nothing it
//! cannot use, and it reads only the storage a filter's states can reach.
//! It does not decide a job's state. On queue storage the available branch
//! compares lane sequences with a cursor outside MVCC (ADR-044), so a job
//! whose claim is in flight can sit in no branch or in two; the batched
//! loader settles each candidate's current row and the filter is re-checked
//! against it.

use crate::audited_sql;
use crate::error::{map_sqlx_error, AwaError};
use crate::filter::{JobCount, JobFilter};
use crate::job::{JobRow, JobState};
use crate::queue_storage::QueueStorage;
use chrono::{DateTime, Utc};
use sqlx::PgPool;

/// Every state, in the order a job usually moves through them.
pub(crate) const ALL_STATES: [JobState; 8] = [
    JobState::Scheduled,
    JobState::Available,
    JobState::Running,
    JobState::WaitingExternal,
    JobState::Retryable,
    JobState::Completed,
    JobState::Failed,
    JobState::Cancelled,
];

/// Which way, and how far, a candidate query reads.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Window {
    /// Ids below `before_id` (all ids when `None`), largest first.
    NewestFirst { before_id: Option<i64>, limit: i64 },
    /// Ids in `(after_id, max_id]`, smallest first: a batch scan.
    OldestFirst {
        after_id: i64,
        max_id: i64,
        limit: i64,
    },
    /// Every candidate, for counting; with a cap, at most one past it.
    Unbounded { cap: Option<i64> },
}

#[derive(Clone)]
pub(crate) enum Bind {
    Text(String),
    Int(i64),
    Ints(Vec<i64>),
    Time(DateTime<Utc>),
    States(Vec<String>),
    Json(serde_json::Value),
    TextArray(Vec<String>),
}

struct Statement {
    sql: String,
    binds: Vec<Bind>,
}

impl Statement {
    fn param(&mut self, bind: Bind) -> String {
        self.binds.push(bind);
        format!("${}", self.binds.len())
    }

    fn query_scalar<'q>(
        self,
    ) -> sqlx::query::QueryScalar<'q, sqlx::Postgres, i64, sqlx::postgres::PgArguments> {
        let mut query = sqlx::query_scalar(audited_sql(self.sql));
        for bind in self.binds {
            query = match bind {
                Bind::Text(v) => query.bind(v),
                Bind::Int(v) => query.bind(v),
                Bind::Ints(v) => query.bind(v),
                Bind::Time(v) => query.bind(v),
                Bind::States(v) => query.bind(v),
                Bind::Json(v) => query.bind(v),
                Bind::TextArray(v) => query.bind(v),
            };
        }
        query
    }
}

/// A `WHERE` fragment over `dlq_entries` and its parameters, numbered from
/// `$1`, for the DLQ's list, purge and bulk-retry statements.
pub(crate) struct Predicate {
    pub(crate) conditions: Vec<String>,
    binds: Vec<Bind>,
}

impl Predicate {
    pub(crate) fn param(&mut self, bind: Bind) -> String {
        self.binds.push(bind);
        format!("${}", self.binds.len())
    }

    /// `WHERE ...`, or nothing when no condition applies.
    pub(crate) fn where_clause(&self) -> String {
        if self.conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", self.conditions.join(" AND "))
        }
    }

    pub(crate) fn bind_query_as<'q, O>(
        &self,
        mut query: sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryAs<'q, sqlx::Postgres, O, sqlx::postgres::PgArguments> {
        for bind in self.binds.iter().cloned() {
            query = match bind {
                Bind::Text(v) => query.bind(v),
                Bind::Int(v) => query.bind(v),
                Bind::Ints(v) => query.bind(v),
                Bind::Time(v) => query.bind(v),
                Bind::States(v) => query.bind(v),
                Bind::Json(v) => query.bind(v),
                Bind::TextArray(v) => query.bind(v),
            };
        }
        query
    }

    pub(crate) fn bind_query<'q>(
        &self,
        mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
        for bind in self.binds.iter().cloned() {
            query = match bind {
                Bind::Text(v) => query.bind(v),
                Bind::Int(v) => query.bind(v),
                Bind::Ints(v) => query.bind(v),
                Bind::Time(v) => query.bind(v),
                Bind::States(v) => query.bind(v),
                Bind::Json(v) => query.bind(v),
                Bind::TextArray(v) => query.bind(v),
            };
        }
        query
    }
}

/// The DLQ rows `filter` selects, or `None` when it can select none: every
/// DLQ row is a dead-lettered, failed job.
pub(crate) fn dlq_predicate(filter: &JobFilter) -> Option<Predicate> {
    if filter.dead_lettered == Some(false)
        || (!filter.states.is_empty() && !filter.states.contains(&JobState::Failed))
    {
        return None;
    }
    let mut predicate = Predicate {
        conditions: Vec::new(),
        binds: Vec::new(),
    };
    if let Some(kind) = &filter.kind {
        let p = predicate.param(Bind::Text(kind.clone()));
        predicate.conditions.push(format!("kind = {p}"));
    }
    if let Some(queue) = &filter.queue {
        let p = predicate.param(Bind::Text(queue.clone()));
        predicate.conditions.push(format!("queue = {p}"));
    }
    if !filter.tags.is_empty() {
        let p = predicate.param(Bind::Json(serde_json::json!(filter.tags)));
        predicate.conditions.push(format!(
            "(payload -> 'tags') <> '[]'::jsonb AND payload -> 'tags' @> {p}::jsonb"
        ));
    }
    if let Some(ids) = &filter.ids {
        let p = predicate.param(Bind::Ints(ids.clone()));
        predicate
            .conditions
            .push(format!("job_id = ANY({p}::bigint[])"));
    }
    if let Some(gte) = filter.created_at_gte {
        let p = predicate.param(Bind::Time(gte));
        predicate.conditions.push(format!("created_at >= {p}"));
    }
    if let Some(lt) = filter.created_at_lt {
        let p = predicate.param(Bind::Time(lt));
        predicate.conditions.push(format!("created_at < {p}"));
    }
    Some(predicate)
}

fn state_names(states: &[JobState]) -> Vec<String> {
    states.iter().map(|state| state.to_string()).collect()
}

/// One place a job's row can live, described by the expressions a filter
/// predicate needs.
struct Branch {
    /// The states a row in this branch can report.
    states: &'static [JobState],
    dead_lettered: bool,
    from: String,
    conditions: Vec<String>,
    id: &'static str,
    kind: &'static str,
    queue: &'static str,
    /// `None` when every row reports the branch's single state.
    state: Option<&'static str>,
    created_at: &'static str,
    /// The payload column holding this row's tags, when it has one an index
    /// can serve.
    payload: Option<&'static str>,
}

impl Branch {
    fn compile(
        &self,
        statement: &mut Statement,
        filter: &JobFilter,
        states: &[JobState],
        window: Window,
    ) -> String {
        let mut conditions = self.conditions.clone();
        if let Some(kind) = &filter.kind {
            let p = statement.param(Bind::Text(kind.clone()));
            conditions.push(format!("{} = {p}", self.kind));
        }
        if let Some(queue) = &filter.queue {
            let p = statement.param(Bind::Text(queue.clone()));
            conditions.push(format!("{} = {p}", self.queue));
        }
        let reachable: Vec<JobState> = self
            .states
            .iter()
            .copied()
            .filter(|state| states.contains(state))
            .collect();
        if let Some(column) = self.state {
            if reachable.len() < self.states.len() {
                let p = statement.param(Bind::States(state_names(&reachable)));
                conditions.push(format!("{column} = ANY({p}::awa.job_state[])"));
            }
        }
        if !filter.tags.is_empty() {
            let payload = self
                .payload
                .expect("branches without a tag payload are skipped for tag filters");
            let p = statement.param(Bind::Json(serde_json::json!(filter.tags)));
            // Repeats the partial index's predicate so the planner can use it.
            conditions.push(format!(
                "({payload} -> 'tags') <> '[]'::jsonb AND {payload} -> 'tags' @> {p}::jsonb"
            ));
        }
        if let Some(ids) = &filter.ids {
            let p = statement.param(Bind::Ints(ids.clone()));
            conditions.push(format!("{} = ANY({p}::bigint[])", self.id));
        }
        if let Some(gte) = filter.created_at_gte {
            let p = statement.param(Bind::Time(gte));
            conditions.push(format!("{} >= {p}", self.created_at));
        }
        if let Some(lt) = filter.created_at_lt {
            let p = statement.param(Bind::Time(lt));
            conditions.push(format!("{} < {p}", self.created_at));
        }
        let tail = match window {
            Window::NewestFirst { before_id, limit } => {
                if let Some(before_id) = before_id {
                    let p = statement.param(Bind::Int(before_id));
                    conditions.push(format!("{} < {p}", self.id));
                }
                let p = statement.param(Bind::Int(limit));
                format!(" ORDER BY {} DESC LIMIT {p}", self.id)
            }
            Window::OldestFirst {
                after_id,
                max_id,
                limit,
            } => {
                let after = statement.param(Bind::Int(after_id));
                let max = statement.param(Bind::Int(max_id));
                conditions.push(format!("{} > {after} AND {} <= {max}", self.id, self.id));
                let p = statement.param(Bind::Int(limit));
                format!(" ORDER BY {} ASC LIMIT {p}", self.id)
            }
            Window::Unbounded { .. } => String::new(),
        };
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        format!(
            "(SELECT {} AS job_id FROM {}{where_clause}{tail})",
            self.id, self.from
        )
    }
}

/// Ready rows joined to the claim, lease or completion that names them.
fn ready_join(schema: &str, alias: &str, id: &str, run_lease: &str, lane_seq: &str) -> String {
    format!(
        "JOIN {schema}.ready_entries AS r \
           ON r.ready_slot = {alias}.ready_slot \
          AND r.ready_generation = {alias}.ready_generation \
          AND r.queue = {alias}.queue \
          AND r.run_lease + 1 = {run_lease} \
          AND r.enqueue_shard = {alias}.enqueue_shard \
          AND r.lane_seq = {lane_seq} \
          AND r.job_id = {id} \
          AND NOT EXISTS ( \
              SELECT 1 FROM {schema}.ready_tombstones AS tomb \
              WHERE tomb.ready_slot = r.ready_slot \
                AND tomb.ready_generation = r.ready_generation \
                AND tomb.queue = r.queue \
                AND tomb.priority = r.priority \
                AND tomb.enqueue_shard = r.enqueue_shard \
                AND tomb.lane_seq = r.lane_seq)"
    )
}

/// A receipt claim is still open while nothing has closed or superseded it.
fn open_receipt_conditions(schema: &str, id: &str, run_lease: &str) -> Vec<String> {
    ["leases", "done_entries", "deferred_jobs", "dlq_entries"]
        .iter()
        .map(|table| {
            format!(
                "NOT EXISTS (SELECT 1 FROM {schema}.{table} AS x \
                 WHERE x.job_id = {id} AND x.run_lease = {run_lease})"
            )
        })
        .collect()
}

fn done_ready_keys() -> &'static str {
    "r.ready_slot = d.ready_slot \
     AND r.ready_generation = d.ready_generation \
     AND r.queue = d.queue \
     AND r.priority = d.priority \
     AND r.enqueue_shard = d.enqueue_shard \
     AND r.lane_seq = d.lane_seq"
}

fn queue_storage_branches(schema: &str, filter: &JobFilter) -> Vec<Branch> {
    use JobState::*;
    let tagged = !filter.tags.is_empty();
    let mut branches = vec![Branch {
        states: &[Available],
        dead_lettered: false,
        from: format!(
            "{schema}.ready_entries AS r JOIN claim_cursors AS cc \
               ON cc.queue = r.queue AND cc.priority = r.priority \
              AND cc.enqueue_shard = r.enqueue_shard"
        ),
        conditions: vec![
            "r.lane_seq >= cc.claim_seq".to_string(),
            format!(
                "NOT EXISTS (SELECT 1 FROM {schema}.ready_tombstones AS tomb \
                 WHERE tomb.ready_slot = r.ready_slot \
                   AND tomb.ready_generation = r.ready_generation \
                   AND tomb.queue = r.queue AND tomb.priority = r.priority \
                   AND tomb.enqueue_shard = r.enqueue_shard \
                   AND tomb.lane_seq = r.lane_seq)"
            ),
        ],
        id: "r.job_id",
        kind: "r.kind",
        queue: "r.queue",
        state: None,
        created_at: "r.created_at",
        payload: Some("r.payload"),
    }];
    branches.push(Branch {
        states: &[Scheduled, Retryable],
        dead_lettered: false,
        from: format!("{schema}.deferred_jobs AS d"),
        conditions: vec![],
        id: "d.job_id",
        kind: "d.kind",
        queue: "d.queue",
        state: Some("d.state"),
        created_at: "d.created_at",
        payload: Some("d.payload"),
    });
    branches.push(Branch {
        states: &[Running, WaitingExternal],
        dead_lettered: false,
        from: format!(
            "{schema}.leases AS l {}",
            ready_join(schema, "l", "l.job_id", "l.run_lease", "l.lane_seq")
        ),
        conditions: vec![],
        id: "l.job_id",
        kind: "r.kind",
        queue: "l.queue",
        state: Some("l.state"),
        created_at: "r.created_at",
        payload: Some("r.payload"),
    });
    let mut claim_conditions = vec![
        "c.closed_at IS NULL".to_string(),
        format!(
            "NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closures AS cx \
             WHERE cx.claim_slot = c.claim_slot AND cx.job_id = c.job_id \
               AND cx.run_lease = c.run_lease)"
        ),
        format!(
            "NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb \
             WHERE cb.receipt_ranges @> c.receipt_id)"
        ),
    ];
    claim_conditions.extend(open_receipt_conditions(schema, "c.job_id", "c.run_lease"));
    branches.push(Branch {
        states: &[Running],
        dead_lettered: false,
        from: format!(
            "{schema}.lease_claims AS c {}",
            ready_join(schema, "c", "c.job_id", "c.run_lease", "c.lane_seq")
        ),
        conditions: claim_conditions,
        id: "c.job_id",
        kind: "r.kind",
        queue: "c.queue",
        state: None,
        created_at: "r.created_at",
        payload: Some("r.payload"),
    });
    let mut batch_conditions = vec![
        format!(
            "NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closures AS cx \
             WHERE cx.claim_slot = b.claim_slot AND cx.job_id = i.job_id \
               AND cx.run_lease = i.run_lease)"
        ),
        format!(
            "NOT EXISTS (SELECT 1 FROM {schema}.lease_claim_closure_batches AS cb \
             WHERE cb.claim_slot = b.claim_slot AND cb.receipt_ranges @> i.receipt_id)"
        ),
    ];
    batch_conditions.extend(open_receipt_conditions(schema, "i.job_id", "i.run_lease"));
    branches.push(Branch {
        states: &[Running],
        dead_lettered: false,
        from: format!(
            "{schema}.lease_claim_batches AS b \
             CROSS JOIN LATERAL unnest(b.job_ids, b.run_leases, b.receipt_ids, b.lane_seqs) \
               AS i(job_id, run_lease, receipt_id, lane_seq) {}",
            ready_join(schema, "b", "i.job_id", "i.run_lease", "i.lane_seq")
        ),
        conditions: batch_conditions,
        id: "i.job_id",
        kind: "r.kind",
        queue: "b.queue",
        state: None,
        created_at: "r.created_at",
        payload: Some("r.payload"),
    });
    let done_created = "COALESCE(d.created_at, r.created_at, d.finalized_at)";
    if tagged {
        // A done row identical to its ready row stores a NULL payload; its
        // tags live on the ready row in the same ring slot. Each half reads
        // the payload an index covers, and the NULL test keeps them disjoint.
        branches.push(Branch {
            states: &[Completed, Failed, Cancelled],
            dead_lettered: false,
            from: format!(
                "{schema}.done_entries AS d LEFT JOIN {schema}.ready_entries AS r ON {}",
                done_ready_keys()
            ),
            conditions: vec!["d.payload IS NOT NULL".to_string()],
            id: "d.job_id",
            kind: "d.kind",
            queue: "d.queue",
            state: Some("d.state"),
            created_at: done_created,
            payload: Some("d.payload"),
        });
        branches.push(Branch {
            states: &[Completed, Failed, Cancelled],
            dead_lettered: false,
            from: format!(
                "{schema}.ready_entries AS r JOIN {schema}.done_entries AS d ON {}",
                done_ready_keys()
            ),
            conditions: vec!["d.payload IS NULL".to_string()],
            id: "d.job_id",
            kind: "d.kind",
            queue: "d.queue",
            state: Some("d.state"),
            created_at: done_created,
            payload: Some("r.payload"),
        });
    } else {
        branches.push(Branch {
            states: &[Completed, Failed, Cancelled],
            dead_lettered: false,
            from: format!(
                "{schema}.done_entries AS d LEFT JOIN {schema}.ready_entries AS r ON {}",
                done_ready_keys()
            ),
            conditions: vec![],
            id: "d.job_id",
            kind: "d.kind",
            queue: "d.queue",
            state: Some("d.state"),
            created_at: done_created,
            payload: None,
        });
        // Compact completions. A tagged job never takes this path
        // (`receipt_fast_complete_candidate` requires an empty tag list), so
        // tag filters skip it.
        branches.push(Branch {
            states: &[Completed],
            dead_lettered: false,
            from: format!(
                "{schema}.receipt_completion_batches AS b \
                 CROSS JOIN LATERAL unnest(b.job_ids, b.run_leases, b.lane_seqs) \
                   AS i(job_id, run_lease, lane_seq) \
                 JOIN {schema}.ready_entries AS r \
                   ON r.ready_slot = b.ready_slot \
                  AND r.ready_generation = b.ready_generation \
                  AND r.queue = b.queue AND r.priority = b.priority \
                  AND r.enqueue_shard = b.enqueue_shard \
                  AND r.lane_seq = i.lane_seq AND r.job_id = i.job_id"
            ),
            conditions: vec![format!(
                "NOT EXISTS (SELECT 1 FROM {schema}.receipt_completion_tombstones AS tomb \
                 WHERE tomb.ready_slot = b.ready_slot \
                   AND tomb.ready_generation = b.ready_generation \
                   AND tomb.job_id = i.job_id AND tomb.run_lease = i.run_lease)"
            )],
            id: "i.job_id",
            kind: "r.kind",
            queue: "b.queue",
            state: None,
            created_at: "r.created_at",
            payload: None,
        });
    }
    branches.push(Branch {
        states: &[Failed],
        dead_lettered: true,
        from: format!("{schema}.dlq_entries AS q"),
        conditions: vec![],
        id: "q.job_id",
        kind: "q.kind",
        queue: "q.queue",
        state: None,
        created_at: "q.created_at",
        payload: Some("q.payload"),
    });
    branches
}

/// The candidate statement for queue storage, or `None` when no branch can
/// hold a match.
fn queue_storage_statement(
    schema: &str,
    filter: &JobFilter,
    eligible: &[JobState],
    window: Window,
) -> Option<Statement> {
    let states = filter.effective_states(eligible);
    let branches: Vec<Branch> = queue_storage_branches(schema, filter)
        .into_iter()
        .filter(|branch| branch.states.iter().any(|state| states.contains(state)))
        .filter(|branch| {
            filter
                .dead_lettered
                .is_none_or(|wanted| wanted == branch.dead_lettered)
        })
        .filter(|branch| filter.tags.is_empty() || branch.payload.is_some())
        .collect();
    if branches.is_empty() {
        return None;
    }
    let mut statement = Statement {
        sql: String::new(),
        binds: Vec::new(),
    };
    let needs_cursors = branches
        .iter()
        .any(|branch| branch.from.contains("claim_cursors"));
    let parts: Vec<String> = branches
        .iter()
        .map(|branch| branch.compile(&mut statement, filter, &states, window))
        .collect();
    let cursors = if needs_cursors {
        // One cursor read per lane: the function is VOLATILE and would
        // otherwise run once per ready row (#422).
        format!(
            "WITH claim_cursors AS MATERIALIZED ( \
               SELECT claims.queue, claims.priority, claims.enqueue_shard, \
                      {schema}.sequence_next_value(claims.seq_name) AS claim_seq \
               FROM {schema}.queue_claim_heads AS claims) "
        )
    } else {
        String::new()
    };
    statement.sql = finish(cursors, parts.join(" UNION ALL "), window, &mut statement);
    Some(statement)
}

fn canonical_statement(
    filter: &JobFilter,
    eligible: &[JobState],
    window: Window,
) -> Option<Statement> {
    // The canonical engine has no DLQ.
    if filter.dead_lettered == Some(true) {
        return None;
    }
    let states = filter.effective_states(eligible);
    if states.is_empty() {
        return None;
    }
    let mut statement = Statement {
        sql: String::new(),
        binds: Vec::new(),
    };
    // `awa.jobs` is a view over a set-returning function, which no
    // predicate can push into, so read the two tables directly.
    let parts: Vec<String> = ["awa.jobs_hot", "awa.scheduled_jobs"]
        .iter()
        .map(|table| {
            let mut conditions = Vec::new();
            if states.len() < ALL_STATES.len() {
                let p = statement.param(Bind::States(state_names(&states)));
                conditions.push(format!("j.state = ANY({p}::awa.job_state[])"));
            }
            if let Some(kind) = &filter.kind {
                let p = statement.param(Bind::Text(kind.clone()));
                conditions.push(format!("j.kind = {p}"));
            }
            if let Some(queue) = &filter.queue {
                let p = statement.param(Bind::Text(queue.clone()));
                conditions.push(format!("j.queue = {p}"));
            }
            if !filter.tags.is_empty() {
                let p = statement.param(Bind::TextArray(filter.tags.clone()));
                conditions.push(format!("j.tags @> {p}::text[]"));
            }
            if let Some(ids) = &filter.ids {
                let p = statement.param(Bind::Ints(ids.clone()));
                conditions.push(format!("j.id = ANY({p}::bigint[])"));
            }
            if let Some(gte) = filter.created_at_gte {
                let p = statement.param(Bind::Time(gte));
                conditions.push(format!("j.created_at >= {p}"));
            }
            if let Some(lt) = filter.created_at_lt {
                let p = statement.param(Bind::Time(lt));
                conditions.push(format!("j.created_at < {p}"));
            }
            let tail = match window {
                Window::NewestFirst { before_id, limit } => {
                    if let Some(before_id) = before_id {
                        let p = statement.param(Bind::Int(before_id));
                        conditions.push(format!("j.id < {p}"));
                    }
                    let p = statement.param(Bind::Int(limit));
                    format!(" ORDER BY j.id DESC LIMIT {p}")
                }
                Window::OldestFirst {
                    after_id,
                    max_id,
                    limit,
                } => {
                    let after = statement.param(Bind::Int(after_id));
                    let max = statement.param(Bind::Int(max_id));
                    conditions.push(format!("j.id > {after} AND j.id <= {max}"));
                    let p = statement.param(Bind::Int(limit));
                    format!(" ORDER BY j.id ASC LIMIT {p}")
                }
                Window::Unbounded { .. } => String::new(),
            };
            let where_clause = if conditions.is_empty() {
                String::new()
            } else {
                format!(" WHERE {}", conditions.join(" AND "))
            };
            format!("(SELECT j.id AS job_id FROM {table} AS j{where_clause}{tail})")
        })
        .collect();
    statement.sql = finish(
        String::new(),
        parts.join(" UNION ALL "),
        window,
        &mut statement,
    );
    Some(statement)
}

/// Wraps the branches: candidate ids de-duplicated in window order, or the
/// capped count of distinct ids.
fn finish(prefix: String, union: String, window: Window, statement: &mut Statement) -> String {
    match window {
        Window::NewestFirst { limit, .. } => {
            let p = statement.param(Bind::Int(limit));
            format!(
                "{prefix}SELECT job_id FROM ({union}) AS candidates \
                 GROUP BY job_id ORDER BY job_id DESC LIMIT {p}"
            )
        }
        Window::OldestFirst { limit, .. } => {
            let p = statement.param(Bind::Int(limit));
            format!(
                "{prefix}SELECT job_id FROM ({union}) AS candidates \
                 GROUP BY job_id ORDER BY job_id ASC LIMIT {p}"
            )
        }
        Window::Unbounded { cap: Some(cap) } => {
            let p = statement.param(Bind::Int(cap + 1));
            format!(
                "{prefix}SELECT count(*)::bigint FROM ( \
                   SELECT DISTINCT job_id FROM ({union}) AS candidates LIMIT {p}) AS capped"
            )
        }
        Window::Unbounded { cap: None } => {
            format!("{prefix}SELECT count(DISTINCT job_id)::bigint FROM ({union}) AS candidates")
        }
    }
}

/// The statement for whichever engine is active, or `None` when no job can
/// match.
async fn statement(
    pool: &PgPool,
    filter: &JobFilter,
    eligible: &[JobState],
    window: Window,
) -> Result<(Option<QueueStorage>, Option<Statement>), AwaError> {
    match QueueStorage::active_schema(pool).await? {
        Some(schema) => {
            let statement = queue_storage_statement(&schema, filter, eligible, window);
            Ok((Some(QueueStorage::from_existing_schema(schema)?), statement))
        }
        None => Ok((None, canonical_statement(filter, eligible, window))),
    }
}

/// Run a candidate or count statement under a statement timeout, so an
/// expensive filter cannot hold a connection indefinitely.
async fn fetch_scalars(pool: &PgPool, statement: Statement) -> Result<Vec<i64>, AwaError> {
    let mut tx = pool.begin().await.map_err(map_sqlx_error)?;
    sqlx::query(audited_sql(format!(
        "SET LOCAL statement_timeout = '{}ms'",
        READ_TIMEOUT.as_millis()
    )))
    .execute(tx.as_mut())
    .await
    .map_err(map_sqlx_error)?;
    let rows = statement
        .query_scalar()
        .fetch_all(tx.as_mut())
        .await
        .map_err(map_sqlx_error)?;
    tx.commit().await.map_err(map_sqlx_error)?;
    Ok(rows)
}

/// How long one filtered read may run.
pub(crate) const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Load the current rows of `ids`, in that order, each with whether it is
/// dead-lettered.
async fn load(
    pool: &PgPool,
    store: Option<&QueueStorage>,
    ids: &[i64],
) -> Result<Vec<(JobRow, bool)>, AwaError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    match store {
        Some(store) => store.load_jobs_without_output(pool, ids).await,
        None => {
            let rows: Vec<JobRow> = sqlx::query_as(
                "SELECT * FROM awa.jobs_hot WHERE id = ANY($1) \
                 UNION ALL SELECT * FROM awa.scheduled_jobs WHERE id = ANY($1)",
            )
            .bind(ids)
            .fetch_all(pool)
            .await
            .map_err(map_sqlx_error)?;
            let mut by_id: std::collections::HashMap<i64, JobRow> =
                rows.into_iter().map(|job| (job.id, job)).collect();
            Ok(ids
                .iter()
                .filter_map(|id| by_id.remove(id).map(|job| (job, false)))
                .collect())
        }
    }
}

/// Jobs in `filter`'s population that `eligible` allows, in `window` order:
/// candidates from SQL, loaded in batches and re-checked against their
/// current rows, reading further while the page is short.
pub(crate) async fn find(
    pool: &PgPool,
    filter: &JobFilter,
    eligible: &[JobState],
    window: Window,
) -> Result<Vec<JobRow>, AwaError> {
    let limit = match window {
        Window::NewestFirst { limit, .. } | Window::OldestFirst { limit, .. } => limit,
        Window::Unbounded { .. } => {
            return Err(AwaError::Validation(
                "an unbounded window can only be counted".to_string(),
            ))
        }
    };
    let effective = filter.effective_states(eligible);
    // Over-fetch so a page usually fills in one round even when some
    // candidates turn out to have moved on.
    let fetch = (limit * 2).clamp(limit, limit + 200);
    let mut cursor = window;
    let mut jobs = Vec::new();
    // Bounded so a pathological stream of stale candidates cannot loop.
    for _ in 0..32 {
        let cursor_window = match cursor {
            Window::NewestFirst { before_id, .. } => Window::NewestFirst {
                before_id,
                limit: fetch,
            },
            Window::OldestFirst {
                after_id, max_id, ..
            } => Window::OldestFirst {
                after_id,
                max_id,
                limit: fetch,
            },
            Window::Unbounded { .. } => unreachable!("rejected above"),
        };
        let (store, statement) = statement(pool, filter, eligible, cursor_window).await?;
        let Some(statement) = statement else {
            return Ok(Vec::new());
        };
        let ids = fetch_scalars(pool, statement).await?;
        let exhausted = (ids.len() as i64) < fetch;
        for (job, dead_lettered) in load(pool, store.as_ref(), &ids).await? {
            if effective.contains(&job.state) && filter.matches(&job, dead_lettered) {
                jobs.push(job);
                if jobs.len() as i64 >= limit {
                    return Ok(jobs);
                }
            }
        }
        let Some(&last) = ids.last() else {
            return Ok(jobs);
        };
        if exhausted {
            return Ok(jobs);
        }
        cursor = match cursor {
            Window::NewestFirst { limit, .. } => Window::NewestFirst {
                before_id: Some(last),
                limit,
            },
            Window::OldestFirst { max_id, limit, .. } => Window::OldestFirst {
                after_id: last,
                max_id,
                limit,
            },
            Window::Unbounded { .. } => unreachable!("rejected above"),
        };
    }
    Ok(jobs)
}

/// How many jobs `filter` selects among `eligible` states, counted in SQL
/// over candidates. With a `cap`, counting stops past it. Advisory for jobs
/// whose claim is in flight.
pub(crate) async fn count(
    pool: &PgPool,
    filter: &JobFilter,
    eligible: &[JobState],
    cap: Option<i64>,
) -> Result<JobCount, AwaError> {
    let (_, statement) = statement(pool, filter, eligible, Window::Unbounded { cap }).await?;
    let Some(statement) = statement else {
        return Ok(JobCount::Exact(0));
    };
    let n = fetch_scalars(pool, statement)
        .await?
        .first()
        .copied()
        .unwrap_or(0);
    Ok(match cap {
        Some(cap) if n > cap => JobCount::AtLeast(cap),
        _ => JobCount::Exact(n),
    })
}

/// The largest candidate id `filter` selects among `eligible`, the upper
/// bound a batch scan freezes.
pub(crate) async fn max_id(
    pool: &PgPool,
    filter: &JobFilter,
    eligible: &[JobState],
) -> Result<Option<i64>, AwaError> {
    let window = Window::NewestFirst {
        before_id: None,
        limit: 1,
    };
    let (_, statement) = statement(pool, filter, eligible, window).await?;
    let Some(statement) = statement else {
        return Ok(None);
    };
    Ok(fetch_scalars(pool, statement).await?.first().copied())
}

/// Candidate ids for a batch scan's next chunk, oldest first. The runner
/// re-checks each job when it applies the operation, so stale candidates
/// are counted as skipped there.
pub(crate) async fn scan_ids(
    pool: &PgPool,
    filter: &JobFilter,
    eligible: &[JobState],
    after_id: i64,
    max_id: i64,
    limit: i64,
) -> Result<Vec<i64>, AwaError> {
    let window = Window::OldestFirst {
        after_id,
        max_id,
        limit,
    };
    let (_, statement) = statement(pool, filter, eligible, window).await?;
    let Some(statement) = statement else {
        return Ok(Vec::new());
    };
    fetch_scalars(pool, statement).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(filter: &JobFilter, eligible: &[JobState]) -> Option<String> {
        queue_storage_statement(
            "awa",
            filter,
            eligible,
            Window::NewestFirst {
                before_id: None,
                limit: 10,
            },
        )
        .map(|statement| statement.sql)
    }

    #[test]
    fn only_present_predicates_are_written() {
        let sql = sql(&JobFilter::new(), &ALL_STATES).unwrap();
        assert!(!sql.contains("IS NULL OR"), "{sql}");
        assert!(!sql.contains("kind ="), "{sql}");
        let sql = self::sql(&JobFilter::new().kind("k"), &ALL_STATES).unwrap();
        assert!(sql.contains("r.kind = $"), "{sql}");
    }

    #[test]
    fn states_prune_branches() {
        let failed = sql(&JobFilter::new().state(JobState::Failed), &ALL_STATES).unwrap();
        assert!(!failed.contains("deferred_jobs AS d"), "{failed}");
        assert!(!failed.contains("claim_cursors"), "{failed}");
        assert!(failed.contains("dlq_entries AS q"), "{failed}");
        let batch = sql(
            &JobFilter::new(),
            &[JobState::Available, JobState::Scheduled],
        )
        .unwrap();
        assert!(!batch.contains("done_entries AS d LEFT"), "{batch}");
        assert!(!batch.contains("dlq_entries AS q"), "{batch}");
        assert!(sql(
            &JobFilter::new().state(JobState::Running),
            &[JobState::Available]
        )
        .is_none());
    }

    #[test]
    fn tag_filters_read_indexed_payloads_and_skip_compact_completions() {
        let sql = sql(&JobFilter::new().tag("order:1"), &ALL_STATES).unwrap();
        assert!(!sql.contains("receipt_completion_batches"), "{sql}");
        assert!(sql.contains("d.payload IS NULL"), "{sql}");
        assert!(sql.contains("d.payload IS NOT NULL"), "{sql}");
        assert!(
            sql.contains("(r.payload -> 'tags') <> '[]'::jsonb"),
            "{sql}"
        );
    }

    #[test]
    fn dead_lettered_selects_only_the_dlq() {
        let sql = sql(&JobFilter::new().dead_lettered(true), &ALL_STATES).unwrap();
        assert!(sql.contains("dlq_entries AS q"), "{sql}");
        assert!(!sql.contains("done_entries"), "{sql}");
        let none = self::sql(&JobFilter::new().dead_lettered(false), &ALL_STATES).unwrap();
        assert!(!none.contains("dlq_entries AS q"), "{none}");
    }
}
