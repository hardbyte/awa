//! Checks specific to live `enqueue_shards` changes (ADR-025): every shard drains,
//! claim cursors keep moving while their shard has backlog, jobs run exactly once,
//! `ordering_key` jobs stay FIFO within a settled window, and no role ever hits a
//! permission or missing-lane error.

use super::config::Persona;
use super::ledger::flag;
use super::shared::Shared;
use super::stop::Stop;
use sqlx::PgPool;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MAX_ROWS: usize = 25;
/// Same-key starts further behind the newest start than this are an order inversion.
const INVERSION_WINDOW: u64 = 8;
/// Seconds a shard above the configured count may keep backlog once every runtime has
/// observed the new count.
const ABOVE_COUNT_DRAIN_SECONDS: i64 = 45;

#[derive(Debug, Clone, Default)]
pub struct QueuePlan {
    pub configured: i16,
    pub changed_at_ms: i64,
    /// When every runtime and producer store had observed `configured`; None while the
    /// fleet disagrees.
    pub settled_at_ms: Option<i64>,
}

/// What the shard-ops persona and the oracle share.
#[derive(Default)]
pub struct ShardOpsState {
    pub plans: Mutex<HashMap<String, QueuePlan>>,
    /// Id of the current settled window; 0 while a change is propagating.
    window: AtomicU64,
    window_counter: AtomicU64,
    producers_held: AtomicBool,
    pub producers_inserting: AtomicI64,
    seq: AtomicU64,
    start_tick: AtomicU64,
    /// Executions each enqueued job should have: 1, or 2 for a job that retries once.
    expected: Mutex<HashMap<u64, u8>>,
    /// Executions per job sequence number, and the job id that ran.
    executions: Mutex<HashMap<u64, (u16, i64)>>,
    /// Executions per job sequence number whose handler returned outside a chaos window.
    returned: Mutex<HashMap<u64, u16>>,
    keyed_window: Mutex<HashMap<(u64, u64), u64>>,
    keyed_starts: Mutex<Vec<(u64, u64, u64)>>,
    reported: Mutex<HashMap<String, u64>>,
}

impl ShardOpsState {
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn window(&self) -> u64 {
        self.window.load(Ordering::SeqCst)
    }

    pub fn open_window(&self) -> u64 {
        let id = self.window_counter.fetch_add(1, Ordering::SeqCst) + 1;
        self.window.store(id, Ordering::SeqCst);
        id
    }

    pub fn close_window(&self) {
        self.window.store(0, Ordering::SeqCst);
    }

    pub fn hold_producers(&self, held: bool) {
        self.producers_held.store(held, Ordering::SeqCst);
    }

    pub fn producers_held(&self) -> bool {
        self.producers_held.load(Ordering::SeqCst)
    }

    pub fn note_enqueued(&self, seq: u64, executions: u8) {
        self.expected
            .lock()
            .expect("expected")
            .insert(seq, executions);
    }

    pub fn note_keyed(&self, key: u64, key_seq: u64, window: u64) {
        self.keyed_window
            .lock()
            .expect("keyed window")
            .insert((key, key_seq), window);
    }

    /// Called by the worker at the start of every execution.
    pub fn note_execution(&self, seq: u64, job_id: i64, key: Option<(u64, u64)>) {
        let mut executions = self.executions.lock().expect("executions");
        let entry = executions.entry(seq).or_insert((0, job_id));
        entry.0 += 1;
        drop(executions);
        if let Some((key, key_seq)) = key {
            let tick = self.start_tick.fetch_add(1, Ordering::SeqCst);
            self.keyed_starts
                .lock()
                .expect("keyed starts")
                .push((key, key_seq, tick));
        }
    }

    /// Called by the worker when the handler returns its result.
    pub fn note_returned(&self, seq: u64, during_chaos: bool) {
        if !during_chaos {
            *self
                .returned
                .lock()
                .expect("returned")
                .entry(seq)
                .or_insert(0) += 1;
        }
    }

    pub fn configured(&self, queue: &str) -> i16 {
        self.plans
            .lock()
            .expect("plans")
            .get(queue)
            .map_or(1, |plan| plan.configured.max(1))
    }

    /// Reports `summary` as a violation the first time `key` is seen and only counts
    /// repeats, so a persistent failure does not bury the report.
    pub fn violation_once(
        &self,
        shared: &Shared,
        kind: &str,
        key: &str,
        summary: String,
        rows: Vec<String>,
    ) {
        let first = {
            let mut reported = self.reported.lock().expect("reported");
            let count = reported.entry(format!("{kind}:{key}")).or_insert(0);
            *count += 1;
            *count == 1
        };
        if first {
            shared.violation(kind, summary, rows);
        }
        shared
            .stats(Persona::ShardOps)
            .add(&format!("{kind}_total"), 1);
    }
}

/// One lane's cursors and backlog, as read from the store.
#[derive(Debug, Clone)]
struct ShardRow {
    queue: String,
    priority: i16,
    shard: i16,
    claim_seq: i64,
    next_seq: i64,
    available: i64,
}

impl ShardRow {
    fn json(&self, extra: &str) -> String {
        format!(
            "{{\"queue\":\"{}\",\"priority\":{},\"enqueue_shard\":{},\"claim_seq\":{},\"next_seq\":{},\"lag\":{},\"available\":{}{extra}}}",
            self.queue,
            self.priority,
            self.shard,
            self.claim_seq,
            self.next_seq,
            self.next_seq - self.claim_seq,
            self.available
        )
    }
}

fn schema(shared: &Shared) -> String {
    shared
        .info
        .queue_storage_schema
        .clone()
        .unwrap_or_else(|| "awa".to_string())
}

/// Per-shard claim cursor, enqueue cursor and the rows the cursor has not yet claimed
/// (the lag query from ADR-025, with the unclaimed rows counted exactly because aborted
/// transactions leave gaps in the sequences).
async fn shard_rows(
    pool: &PgPool,
    schema: &str,
    queues: &[String],
) -> Result<Vec<ShardRow>, sqlx::Error> {
    let rows: Vec<(String, i16, i16, i64, i64, i64)> = sqlx::query_as(&format!(
        "WITH claims AS MATERIALIZED (
             SELECT queue, priority, enqueue_shard, {schema}.sequence_next_value(seq_name) AS claim_seq
             FROM {schema}.queue_claim_heads WHERE queue = ANY($1)
         ),
         enqueues AS MATERIALIZED (
             SELECT queue, priority, enqueue_shard, {schema}.sequence_next_value(seq_name) AS next_seq
             FROM {schema}.queue_enqueue_heads WHERE queue = ANY($1)
         ),
         available AS (
             SELECT r.queue, r.priority, r.enqueue_shard, count(*)::bigint AS n
             FROM {schema}.ready_entries r
             JOIN claims c ON c.queue = r.queue AND c.priority = r.priority AND c.enqueue_shard = r.enqueue_shard
             WHERE r.queue = ANY($1) AND r.lane_seq >= c.claim_seq
               AND NOT EXISTS (
                 SELECT 1 FROM {schema}.ready_tombstones t
                 WHERE t.ready_slot = r.ready_slot AND t.ready_generation = r.ready_generation
                   AND t.queue = r.queue AND t.priority = r.priority
                   AND t.enqueue_shard = r.enqueue_shard AND t.lane_seq = r.lane_seq
               )
             GROUP BY r.queue, r.priority, r.enqueue_shard
         )
         SELECT c.queue, c.priority, c.enqueue_shard, c.claim_seq::bigint, e.next_seq::bigint,
                COALESCE(a.n, 0)::bigint
         FROM claims c
         JOIN enqueues e USING (queue, priority, enqueue_shard)
         LEFT JOIN available a USING (queue, priority, enqueue_shard)
         ORDER BY c.queue, c.priority, c.enqueue_shard"
    ))
    .bind(queues)
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(
            |(queue, priority, shard, claim_seq, next_seq, available)| ShardRow {
                queue,
                priority,
                shard,
                claim_seq,
                next_seq,
                available,
            },
        )
        .collect())
}

/// Periodic per-shard checks until `stop`: claim cursors advance while their shard has
/// backlog and idle workers, and shards above a lowered count drain.
pub async fn watch(shared: Arc<Shared>, pool: PgPool, queues: Vec<String>, stop: Stop) {
    let schema = schema(&shared);
    // (queue, priority, shard) -> (claim cursor, when it last moved, already reported)
    let mut cursors: HashMap<(String, i16, i16), (i64, i64, bool)> = HashMap::new();
    // (queue, priority, shard) -> when backlog above the configured count was first seen
    let mut above_since: HashMap<(String, i16, i16), (i64, bool)> = HashMap::new();
    while !stop.sleep(Duration::from_secs(5)).await {
        let Ok(rows) = shard_rows(&pool, &schema, &queues).await else {
            continue;
        };
        let now = shared.now_ms();
        let mut window = shared.config.stall_window;
        if shared.chaos_active() {
            window += shared.config.chaos_slack;
        }
        let mut shards_seen: BTreeMap<String, i64> = BTreeMap::new();
        for row in rows {
            let key = (row.queue.clone(), row.priority, row.shard);
            let seen = shards_seen.entry(row.queue.clone()).or_insert(0);
            *seen = (*seen).max(i64::from(row.shard) + 1);

            let entry = cursors
                .entry(key.clone())
                .or_insert((row.claim_seq, now, false));
            if entry.0 != row.claim_seq {
                *entry = (row.claim_seq, now, false);
            } else if let Some(queue) = shared.queues.get(&row.queue) {
                let capacity = queue.capacity.load(Ordering::Relaxed);
                let in_flight = queue.in_flight.load(Ordering::Relaxed);
                let idle_workers = capacity > 0 && in_flight < capacity;
                if row.available <= 0 || !idle_workers || queue.is_held() {
                    entry.1 = now;
                } else if !entry.2 && (now - entry.1) as u64 >= window.as_millis() as u64 {
                    entry.2 = true;
                    shared.violation(
                        "shard_cursor_stalled",
                        format!(
                            "queue {} priority {} shard {}: {} unclaimed job(s), claim cursor frozen at {} for {}s with {in_flight}/{capacity} workers busy",
                            row.queue,
                            row.priority,
                            row.shard,
                            row.available,
                            row.claim_seq,
                            (now - entry.1) / 1000
                        ),
                        vec![row.json(&format!(
                            ",\"configured_shards\":{}",
                            shared.shardops.configured(&row.queue)
                        ))],
                    );
                }
            }

            let plan = shared
                .shardops
                .plans
                .lock()
                .expect("plans")
                .get(&row.queue)
                .cloned();
            let above = plan.as_ref().is_some_and(|plan| {
                i32::from(row.shard) >= i32::from(plan.configured)
                    && plan
                        .settled_at_ms
                        .is_some_and(|at| now - at >= ABOVE_COUNT_DRAIN_SECONDS * 1000)
            });
            if above && row.available > 0 {
                let since = above_since.entry(key).or_insert((now, false));
                if !since.1 && now - since.0 >= ABOVE_COUNT_DRAIN_SECONDS * 1000 {
                    since.1 = true;
                    shared.violation(
                        "shard_above_count_not_draining",
                        format!(
                            "queue {} shard {} is above the configured count {} and still holds {} unclaimed job(s) {}s after every runtime observed the change",
                            row.queue,
                            row.shard,
                            plan.as_ref().map_or(0, |plan| plan.configured),
                            row.available,
                            (now - since.0) / 1000
                        ),
                        vec![row.json("")],
                    );
                }
            } else {
                above_since.remove(&key);
            }
        }
        for (queue, shards) in shards_seen {
            shared
                .stats(Persona::ShardOps)
                .set_max(&format!("highest_shard_count_{queue}"), shards);
        }
    }
}

/// After the system quiesced: every shard of every shard-ops queue is empty, every
/// enqueued job ran exactly the expected number of times, and keyed jobs kept their
/// order within each settled window.
pub async fn final_check(shared: &Arc<Shared>, pool: &PgPool, queues: &[String]) {
    let quiesced = shared.quiesced.load(Ordering::Relaxed);
    let schema = schema(shared);
    match shard_rows(pool, &schema, queues).await {
        Ok(rows) => {
            let stuck: Vec<String> = rows
                .iter()
                .filter(|row| row.available > 0)
                .map(|row| {
                    row.json(&format!(
                        ",\"configured_shards\":{}",
                        shared.shardops.configured(&row.queue)
                    ))
                })
                .take(MAX_ROWS)
                .collect();
            shared.run.add("shardops_lanes_final", rows.len() as i64);
            if !stuck.is_empty() {
                let message = format!(
                    "{} shard(s) still hold unclaimed jobs after the load stopped",
                    stuck.len()
                );
                if quiesced {
                    shared.violation("shard_not_drained", message, stuck);
                } else {
                    shared.finding("shard_backlog_at_end", message, stuck);
                }
            }
        }
        Err(error) => shared.note("shards", format!("final shard query failed: {error}")),
    }

    let state = &shared.shardops;
    let expected = state.expected.lock().expect("expected").clone();
    let executions = state.executions.lock().expect("executions").clone();
    let returned = state.returned.lock().expect("returned").clone();
    let mut missing = Vec::new();
    let mut repeated = Vec::new();
    let mut rescued = Vec::new();
    let mut rescued_after_return = Vec::new();
    let mut short = Vec::new();
    for (seq, want) in &expected {
        let (ran, job_id) = executions.get(seq).copied().unwrap_or((0, 0));
        let returned = returned.get(seq).copied().unwrap_or(0);
        let record = shared.ledger.get(job_id);
        let row = format!(
            "{{\"seq\":{seq},\"job_id\":{job_id},\"expected_executions\":{want},\"executions\":{ran},\"returned\":{returned},\"max_attempt\":{},\"ledger_flags\":{}}}",
            record.as_ref().map_or(0, |record| record.max_attempt),
            record.as_ref().map_or(0, |record| record.flags)
        );
        if ran == 0 {
            missing.push(row);
        } else if ran > u16::from(*want) {
            // A rescue re-runs a job whose attempt lost its lease, which is at-least-once
            // delivery working as designed when a handler never returned. This persona's
            // runtimes only stop gracefully, so when every execution returned outside a
            // chaos window the rescue re-ran a finished attempt: a duplicate.
            let was_rescued = record
                .as_ref()
                .is_some_and(|record| record.has(flag::RESCUED));
            if was_rescued && returned >= ran {
                rescued_after_return.push(row);
            } else if was_rescued {
                rescued.push(row);
            } else {
                repeated.push(row);
            }
        } else if ran < u16::from(*want) {
            short.push(row);
        }
    }
    shared
        .stats(Persona::ShardOps)
        .add("tracked_jobs", expected.len() as i64);
    let report = |kind: &str, what: &str, rows: Vec<String>, fatal: bool| {
        if rows.is_empty() {
            return;
        }
        let summary = format!("{} job(s) {what}", rows.len());
        let rows = rows.into_iter().take(MAX_ROWS).collect();
        if fatal {
            shared.violation(kind, summary, rows);
        } else {
            shared.finding(kind, summary, rows);
        }
    };
    report(
        "lost_job",
        "were enqueued across a shard change and never ran",
        missing,
        quiesced,
    );
    report(
        "rescued_reexecution",
        "ran again after a stale-heartbeat rescue",
        rescued,
        false,
    );
    report(
        "rescued_after_return",
        "ran again after a stale-heartbeat rescue although every execution's handler had returned",
        rescued_after_return,
        true,
    );
    report(
        "duplicate_execution",
        "ran more often than their handler asked for",
        repeated,
        true,
    );
    report(
        "retry_not_executed",
        "ran fewer times than their handler asked for",
        short,
        quiesced,
    );

    let windows = state.keyed_window.lock().expect("keyed window").clone();
    let mut starts = state.keyed_starts.lock().expect("keyed starts").clone();
    starts.sort_by_key(|(_, _, tick)| *tick);
    let mut newest: HashMap<(u64, u64), u64> = HashMap::new();
    let mut inversions = Vec::new();
    let mut checked = 0u64;
    let mut worst = 0u64;
    let mut seen_per_key: HashSet<(u64, u64)> = HashSet::new();
    for (key, key_seq, _) in starts {
        let window = windows.get(&(key, key_seq)).copied().unwrap_or(0);
        if window == 0 {
            continue;
        }
        if !seen_per_key.insert((key, key_seq)) {
            continue;
        }
        checked += 1;
        let top = newest.entry((key, window)).or_insert(0);
        if key_seq + INVERSION_WINDOW < *top {
            worst = worst.max(*top - key_seq);
            if inversions.len() < MAX_ROWS {
                inversions.push(format!(
                    "{{\"ordering_key\":{key},\"window\":{window},\"started_seq\":{key_seq},\"newest_started_seq\":{top}}}"
                ));
            }
        } else {
            *top = (*top).max(key_seq);
        }
    }
    shared
        .stats(Persona::ShardOps)
        .add("keyed_starts_checked", checked as i64);
    shared
        .stats(Persona::ShardOps)
        .set_max("keyed_worst_inversion", worst as i64);
    if !inversions.is_empty() {
        shared.violation(
            "ordering_key_inversion",
            format!(
                "keyed jobs started out of order within a settled shard-count window (worst distance {worst})"
            ),
            inversions,
        );
    }
}

struct LoggedError {
    role: String,
    application: String,
    message: String,
    statement: String,
}

fn is_lane_error(message: &str) -> bool {
    message.contains("permission denied")
        || message.contains("must be owner")
        || message.contains("has not been provisioned")
        || message.contains("provision")
        || is_lane_race(message)
}

/// Two sessions creating the same lane sequence at once: `CREATE SEQUENCE IF NOT EXISTS`
/// loses the race with a unique violation on the catalog.
fn is_lane_race(message: &str) -> bool {
    message.contains("pg_class_relname_nsp_index")
}

/// Permission-denied and missing-lane errors in the server log, one violation per role
/// and error message with the lane, the sequence's owner and the statement.
pub async fn scan_server_log(shared: &Arc<Shared>, pool: &PgPool, log: &str) {
    let mut errors: Vec<LoggedError> = Vec::new();
    let lines: Vec<&str> = log.lines().collect();
    let is_entry = |line: &str| {
        let bytes = line.as_bytes();
        bytes.len() > 20 && bytes[4] == b'-' && bytes[7] == b'-' && bytes[10] == b' '
    };
    let mut index = 0;
    while index < lines.len() {
        let line = lines[index];
        index += 1;
        let Some((head, message)) = line.split_once(" ERROR:  ") else {
            continue;
        };
        if !is_lane_error(message) {
            continue;
        }
        let identity = head
            .rsplit_once("] ")
            .map_or("?", |(_, identity)| identity.trim());
        let (role, application) = identity.split_once('@').unwrap_or((identity, ""));
        let (role, application) = (role.to_string(), application.to_string());
        let mut statement = String::new();
        while index < lines.len() && !is_entry(lines[index]) {
            if let Some(text) = lines[index].trim_start().strip_prefix("STATEMENT:") {
                statement = text.trim().chars().take(400).collect();
            } else if !statement.is_empty() {
                statement.push(' ');
                statement.push_str(lines[index].trim());
            }
            index += 1;
        }
        errors.push(LoggedError {
            role,
            application,
            message: message.trim().to_string(),
            statement,
        });
    }
    if errors.is_empty() {
        return;
    }
    let schema = schema(shared);
    let mut groups: BTreeMap<(String, String), (u64, String, BTreeSet<String>)> = BTreeMap::new();
    for error in &errors {
        let entry = groups
            .entry((error.role.clone(), error.message.clone()))
            .or_insert((0, error.statement.clone(), BTreeSet::new()));
        entry.0 += 1;
        entry.2.insert(error.application.clone());
    }
    shared
        .run
        .add("shard_lane_errors_logged", errors.len() as i64);
    for ((role, message), (count, statement, applications)) in groups.into_iter().take(MAX_ROWS) {
        let (lane, owner) = describe_sequence(pool, &schema, &message).await;
        let applications = applications.into_iter().collect::<Vec<_>>().join(",");
        let kind = if is_lane_race(&message) {
            "lane_creation_race"
        } else {
            "permission_denied"
        };
        shared.violation(
            kind,
            format!("role {role}: {message} ({count}x; lane {lane}; sequence owner {owner})"),
            vec![format!(
                "{{\"role\":\"{role}\",\"applications\":{},\"error\":{},\"lane\":{},\"sequence_owner\":{},\"occurrences\":{count},\"statement\":{}}}",
                json_string(&applications),
                json_string(&message),
                json_string(&lane),
                json_string(&owner),
                json_string(&statement)
            )],
        );
    }
}

fn json_string(text: &str) -> String {
    serde_json::Value::String(text.to_string()).to_string()
}

/// The lane and owning role of the sequence an error message names, when it names one.
async fn describe_sequence(pool: &PgPool, schema: &str, message: &str) -> (String, String) {
    let Some(name) = message
        .split("sequence ")
        .nth(1)
        .map(|rest| {
            rest.split_whitespace()
                .next()
                .unwrap_or("")
                .trim_matches('"')
        })
        .map(|name| {
            name.rsplit('.')
                .next()
                .unwrap_or(name)
                .trim_matches('"')
                .to_string()
        })
        .filter(|name| !name.is_empty())
    else {
        return ("unknown".into(), "unknown".into());
    };
    let owner: Option<String> = sqlx::query_scalar(
        "SELECT pg_get_userbyid(c.relowner) FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(schema)
    .bind(&name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();
    let lane: Option<String> = sqlx::query_scalar(&format!(
        "SELECT queue || ' priority ' || priority || ' shard ' || enqueue_shard
         FROM {schema}.queue_enqueue_heads WHERE seq_name = $1
         UNION ALL
         SELECT queue || ' priority ' || priority || ' shard ' || enqueue_shard
         FROM {schema}.queue_claim_heads WHERE seq_name = $1
         LIMIT 1"
    ))
    .bind(&name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();
    (
        lane.unwrap_or_else(|| format!("sequence {name}")),
        owner.unwrap_or_else(|| "unknown".into()),
    )
}
