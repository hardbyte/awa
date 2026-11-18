//! Postgres-side resources, sampled through the run and the idle phase after it:
//!
//! - the private memory (`RssAnon`) of every client backend, read from
//!   `/proc/<pid>/status` inside the server's container (or on this host for a local
//!   server), labelled by `application_name`;
//! - backends per runtime fleet against the connections the harness budgeted for it;
//! - named prepared statements on the runtimes' own pooled connections;
//! - transactions and WAL written per second while the workload is idle.
//!
//! Sampling and judging are separate: the judgements are pure functions over the samples,
//! run once the run is over.

use super::intent::{Intent, IntentSpan};
use super::shared::Shared;
use super::stop::Stop;
use serde::Serialize;
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// `application_name` of the maintenance leader's dedicated idle-check connection, where
/// a runtime has one.
pub const IDLE_CHECK_APPLICATION: &str = "awa-maintenance-idle-check";

#[derive(Debug, Clone, Serialize)]
pub struct BackendSample {
    pub at_ms: i64,
    pub pid: i32,
    pub application: String,
    pub backend_start: String,
    pub state: String,
    pub rss_anon_kib: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FleetConnections {
    pub fleet: String,
    pub backends: i64,
    pub allowed: i64,
    pub runtimes: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionSample {
    pub at_ms: i64,
    pub total: i64,
    pub queues: usize,
    pub idle_check: i64,
    pub fleets: Vec<FleetConnections>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PreparedSample {
    pub at_ms: i64,
    pub runtime: String,
    pub statements: i64,
    pub generic_plans: i64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct IdleSample {
    pub at_ms: i64,
    /// `xact_commit + xact_rollback` of the soak database.
    pub xacts: i64,
    /// Transactions the sampler itself ran since its previous sample.
    pub own_xacts: i64,
    pub wal_bytes: i64,
    pub autovacuum_workers: i64,
}

#[derive(Default)]
pub struct ResourceLog {
    backends: Mutex<Vec<BackendSample>>,
    /// How backend memory is read, or why it is not.
    memory_source: Mutex<Option<Result<String, String>>>,
    connections: Mutex<Vec<ConnectionSample>>,
    prepared: Mutex<Vec<PreparedSample>>,
    prepared_source: Mutex<Option<String>>,
    idle: Mutex<Vec<IdleSample>>,
    idle_window: Mutex<Option<(i64, i64)>>,
    verdict: Mutex<Option<ResourceVerdict>>,
}

impl ResourceLog {
    pub fn set_idle_window(&self, from_ms: i64, to_ms: i64) {
        *self.idle_window.lock().expect("idle window") = Some((from_ms, to_ms));
    }

    pub fn backends(&self) -> Vec<BackendSample> {
        self.backends.lock().expect("backend samples").clone()
    }

    pub fn verdict(&self) -> Option<ResourceVerdict> {
        self.verdict.lock().expect("resource verdict").clone()
    }
}

/// Reads `RssAnon` for a set of server pids.
enum MemoryReader {
    Docker(String),
    LocalProc,
}

impl MemoryReader {
    fn describe(&self) -> String {
        match self {
            MemoryReader::Docker(container) => {
                format!("/proc/<pid>/status inside container {container}")
            }
            MemoryReader::LocalProc => "/proc/<pid>/status on this host".into(),
        }
    }

    fn read(&self, pids: &[i32]) -> Result<HashMap<i32, i64>, String> {
        if pids.is_empty() {
            return Ok(HashMap::new());
        }
        match self {
            MemoryReader::Docker(container) => {
                let list = pids
                    .iter()
                    .map(i32::to_string)
                    .collect::<Vec<_>>()
                    .join(" ");
                let script = format!(
                    "for p in {list}; do v=$(grep '^RssAnon:' /proc/$p/status 2>/dev/null); [ -n \"$v\" ] && echo \"$p $v\"; done"
                );
                let output = Command::new("docker")
                    .args(["exec", container, "sh", "-c", &script])
                    .output()
                    .map_err(|error| format!("docker exec: {error}"))?;
                if !output.status.success() {
                    return Err(format!(
                        "docker exec {container}: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    ));
                }
                Ok(parse_rss_lines(&String::from_utf8_lossy(&output.stdout)))
            }
            MemoryReader::LocalProc => Ok(pids
                .iter()
                .filter_map(|pid| {
                    let comm = std::fs::read_to_string(format!("/proc/{pid}/comm")).ok()?;
                    if !comm.trim().starts_with("postgres") {
                        return None;
                    }
                    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
                    Some((*pid, rss_anon_kib(&status)?))
                })
                .collect()),
        }
    }
}

fn rss_anon_kib(status: &str) -> Option<i64> {
    status
        .lines()
        .find(|line| line.starts_with("RssAnon:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

/// Lines of `<pid> RssAnon: <kib> kB`.
fn parse_rss_lines(text: &str) -> HashMap<i32, i64> {
    text.lines()
        .filter_map(|line| {
            let mut words = line.split_whitespace();
            let pid = words.next()?.parse().ok()?;
            let _label = words.next()?;
            Some((pid, words.next()?.parse().ok()?))
        })
        .collect()
}

fn host_of(url: &str) -> Option<String> {
    let rest = url.split_once("://")?.1;
    let authority = rest.split('/').next()?;
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    Some(
        host_port
            .rsplit_once(':')
            .map_or(host_port, |(host, _)| host)
            .to_string(),
    )
}

fn memory_reader(shared: &Shared) -> Result<MemoryReader, String> {
    if let Some(container) = &shared.config.pg_container {
        return Ok(MemoryReader::Docker(container.clone()));
    }
    let local = host_of(&shared.config.database_url)
        .is_some_and(|host| matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1" | "[::1]"));
    if local && std::path::Path::new("/proc/self/status").exists() {
        return Ok(MemoryReader::LocalProc);
    }
    Err("the server runs neither in a container named by AWA_SOAK_PG_CONTAINER nor on this host, so backend memory cannot be read from /proc; pg_backend_memory_contexts only reports the calling session and pg_log_backend_memory_contexts only writes to the server log".into())
}

/// Whether `application` is a pooled connection of one of the harness's runtime hosts:
/// `soak_<fleet>_<replica>`.
pub fn runtime_host_of<'a>(application: &str, fleets: &'a [String]) -> Option<&'a str> {
    fleets
        .iter()
        .filter(|fleet| {
            application
                .strip_prefix("soak_")
                .and_then(|rest| rest.strip_prefix(fleet.as_str()))
                .and_then(|rest| rest.strip_prefix('_'))
                .is_some_and(|replica| {
                    !replica.is_empty() && replica.chars().all(|ch| ch.is_ascii_digit())
                })
        })
        .max_by_key(|fleet| fleet.len())
        .map(String::as_str)
}

/// Samples until `stop`, every `AWA_SOAK_RESOURCE_SAMPLE_SECONDS`.
pub async fn sample(shared: Arc<Shared>, pool: PgPool, stop: Stop) {
    let reader = memory_reader(&shared);
    *shared
        .resources
        .memory_source
        .lock()
        .expect("memory source") = Some(
        reader
            .as_ref()
            .map(MemoryReader::describe)
            .map_err(Clone::clone),
    );
    let every = shared.config.checks.resource_sample_every;
    let mut own_xacts = 0i64;
    loop {
        own_xacts += sample_once(&shared, &pool, reader.as_ref().ok()).await;
        own_xacts = sample_idle(&shared, &pool, own_xacts).await;
        if stop.sleep(every).await {
            return;
        }
    }
}

type ActivityRow = (i32, String, String, String);

/// One sample of backends, their memory, connections per fleet and prepared statements.
/// Returns the transactions it ran on the soak database.
async fn sample_once(shared: &Shared, pool: &PgPool, reader: Option<&MemoryReader>) -> i64 {
    let at_ms = shared.now_ms();
    let mut own = 0;
    let activity: Result<Vec<ActivityRow>, _> = sqlx::query_as(
        "SELECT pid, COALESCE(application_name, ''), COALESCE(backend_start::text, ''), COALESCE(state, '')
         FROM pg_stat_activity
         WHERE datname = current_database() AND backend_type = 'client backend'",
    )
    .fetch_all(pool)
    .await;
    own += 1;
    let activity = match activity {
        Ok(rows) => {
            shared.samplers.ok("resources.activity");
            rows
        }
        Err(error) => {
            shared.sample_skipped("resources.activity", &error);
            return own;
        }
    };
    let memory = match reader {
        Some(reader) => {
            let pids: Vec<i32> = activity.iter().map(|row| row.0).collect();
            let read = tokio::task::spawn_blocking({
                let pids = pids.clone();
                let reader = match reader {
                    MemoryReader::Docker(container) => MemoryReader::Docker(container.clone()),
                    MemoryReader::LocalProc => MemoryReader::LocalProc,
                };
                move || reader.read(&pids)
            })
            .await
            .map_err(|error| error.to_string())
            .and_then(|result| result);
            match read {
                Ok(memory) => {
                    shared.samplers.ok("resources.backend_memory");
                    memory
                }
                Err(error) => {
                    let count =
                        shared
                            .samplers
                            .skipped("resources.backend_memory", "read_failed", &error);
                    if count <= 3 {
                        shared.note("resources", format!("backend memory not read: {error}"));
                    }
                    HashMap::new()
                }
            }
        }
        None => HashMap::new(),
    };
    {
        let mut backends = shared.resources.backends.lock().expect("backend samples");
        for (pid, application, backend_start, state) in &activity {
            backends.push(BackendSample {
                at_ms,
                pid: *pid,
                application: application.clone(),
                backend_start: backend_start.clone(),
                state: state.clone(),
                rss_anon_kib: memory.get(pid).copied(),
            });
        }
    }

    let fleets: Vec<Arc<super::fleet::Fleet>> = shared
        .fleets
        .lock()
        .expect("fleets")
        .values()
        .cloned()
        .collect();
    let names: Vec<String> = fleets.iter().map(|fleet| fleet.name.clone()).collect();
    let mut per_fleet: BTreeMap<String, i64> = BTreeMap::new();
    let mut idle_check = 0;
    for (_, application, _, _) in &activity {
        if application == IDLE_CHECK_APPLICATION {
            idle_check += 1;
        } else if let Some(fleet) = runtime_host_of(application, &names) {
            *per_fleet.entry(fleet.to_string()).or_default() += 1;
        }
    }
    let extra = i64::from(shared.config.checks.connections_extra_per_runtime);
    let mut fleet_rows = Vec::new();
    for fleet in &fleets {
        let runtimes = fleet.live_replicas().await.len() as i64;
        fleet_rows.push(FleetConnections {
            fleet: fleet.name.clone(),
            backends: per_fleet.get(&fleet.name).copied().unwrap_or(0),
            allowed: runtimes * (i64::from(fleet.pool_size()) + extra),
            runtimes,
        });
    }
    shared
        .resources
        .connections
        .lock()
        .expect("connection samples")
        .push(ConnectionSample {
            at_ms,
            total: activity.len() as i64,
            queues: shared.queues.all().len(),
            idle_check,
            fleets: fleet_rows,
        });

    for fleet in &fleets {
        for (runtime, handle, runtime_pool) in fleet.host_pools().await {
            let query = handle.spawn(async move {
                let mut connection =
                    tokio::time::timeout(Duration::from_secs(1), runtime_pool.acquire())
                        .await
                        .map_err(|_| sqlx::Error::PoolTimedOut)??;
                sqlx::query_as::<_, (i64, i64)>(
                    "SELECT count(*)::bigint, count(*) FILTER (WHERE generic_plans > 0)::bigint
                     FROM pg_prepared_statements",
                )
                .persistent(false)
                .fetch_one(&mut *connection)
                .await
            });
            own += 1;
            match query.await {
                Ok(Ok((statements, generic_plans))) => {
                    shared.samplers.ok("resources.prepared_statements");
                    shared
                        .resources
                        .prepared
                        .lock()
                        .expect("prepared samples")
                        .push(PreparedSample {
                            at_ms,
                            runtime,
                            statements,
                            generic_plans,
                        });
                }
                Ok(Err(error)) => shared.sample_skipped("resources.prepared_statements", &error),
                Err(error) => {
                    shared.samplers.skipped(
                        "resources.prepared_statements",
                        "runtime_gone",
                        &error.to_string(),
                    );
                }
            }
        }
    }
    *shared
        .resources
        .prepared_source
        .lock()
        .expect("prepared source") = Some(
        "one idle connection of each runtime's own pool per sample, read on that connection".into(),
    );
    own
}

/// Records the counters behind the idle rates; returns the sampler's own transactions not
/// yet attributed to a sample.
async fn sample_idle(shared: &Shared, pool: &PgPool, own_xacts: i64) -> i64 {
    let row: Result<(i64, i64, i64), _> = sqlx::query_as(
        "SELECT (SELECT COALESCE(xact_commit + xact_rollback, 0)::bigint FROM pg_stat_database
                 WHERE datname = current_database()),
                (pg_current_wal_lsn() - '0/0'::pg_lsn)::bigint,
                (SELECT count(*)::bigint FROM pg_stat_activity WHERE backend_type = 'autovacuum worker')",
    )
    .fetch_one(pool)
    .await;
    match row {
        Ok((xacts, wal_bytes, autovacuum_workers)) => {
            shared.samplers.ok("resources.idle_counters");
            shared
                .resources
                .idle
                .lock()
                .expect("idle samples")
                .push(IdleSample {
                    at_ms: shared.now_ms(),
                    xacts,
                    own_xacts: own_xacts + 1,
                    wal_bytes,
                    autovacuum_workers,
                });
            0
        }
        Err(error) => {
            shared.sample_skipped("resources.idle_counters", &error);
            own_xacts + 1
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BackendGrowth {
    pub pid: i32,
    pub application: String,
    pub idle_check: bool,
    pub baseline_kib: i64,
    pub end_kib: i64,
    pub growth_kib: i64,
    pub span_seconds: f64,
    /// Run time of the first judged sample.
    pub baseline_at_ms: i64,
    /// `(at_s, kib)` of every judged sample, for the reproducer.
    pub series: Vec<(f64, i64)>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct MemoryJudgement {
    pub not_evaluated: Option<String>,
    pub judged: Vec<BackendGrowth>,
    pub total_growth_kib: i64,
    pub over_backend_limit: Vec<BackendGrowth>,
    pub over_total_limit: bool,
}

fn median(values: &mut [i64]) -> i64 {
    values.sort_unstable();
    values[values.len() / 2]
}

type BackendSeries = (String, Vec<(i64, i64)>);

/// Judges each pooled backend over at least `min_window_ms` once `warmup_ms` has passed
/// both since the load began and since the backend was first seen, so a backend opened by
/// a restarted runtime gets its own warm-up. Its growth is the median of its last three
/// judged samples less the median of its first three.
pub fn judge_backend_memory(
    samples: &[BackendSample],
    is_pooled: impl Fn(&str) -> bool,
    load_started_ms: i64,
    warmup_ms: i64,
    min_window_ms: i64,
    backend_limit_kib: i64,
    total_limit_kib: i64,
) -> MemoryJudgement {
    // (pid, backend start) -> (application, (at_ms, kib))
    let mut series: BTreeMap<(i32, String), BackendSeries> = BTreeMap::new();
    for sample in samples {
        let Some(kib) = sample.rss_anon_kib else {
            continue;
        };
        if !is_pooled(&sample.application) && sample.application != IDLE_CHECK_APPLICATION {
            continue;
        }
        series
            .entry((sample.pid, sample.backend_start.clone()))
            .or_insert_with(|| (sample.application.clone(), Vec::new()))
            .1
            .push((sample.at_ms, kib));
    }
    let mut longest = 0;
    let mut judgement = MemoryJudgement::default();
    for ((pid, _), (application, mut points)) in series {
        let first_seen = points.first().map_or(0, |first| first.0);
        let warm_from = (load_started_ms + warmup_ms).max(first_seen + warmup_ms);
        points.retain(|point| point.0 >= warm_from);
        let span =
            points.last().map_or(0, |last| last.0) - points.first().map_or(0, |first| first.0);
        longest = longest.max(span);
        if span < min_window_ms || points.len() < 6 {
            continue;
        }
        let mut head: Vec<i64> = points.iter().take(3).map(|point| point.1).collect();
        let mut tail: Vec<i64> = points.iter().rev().take(3).map(|point| point.1).collect();
        let baseline_kib = median(&mut head);
        let end_kib = median(&mut tail);
        judgement.judged.push(BackendGrowth {
            pid,
            idle_check: application == IDLE_CHECK_APPLICATION,
            application,
            baseline_kib,
            end_kib,
            growth_kib: end_kib - baseline_kib,
            span_seconds: span as f64 / 1000.0,
            baseline_at_ms: points.first().map_or(0, |first| first.0),
            series: points
                .iter()
                .map(|(at, kib)| (*at as f64 / 1000.0, *kib))
                .collect(),
        });
    }
    if judgement.judged.is_empty() {
        judgement.not_evaluated = Some(format!(
            "no pooled backend was observed for {} s after its warm-up (longest {} s)",
            min_window_ms / 1000,
            longest / 1000
        ));
        return judgement;
    }
    judgement
        .judged
        .sort_by_key(|backend| std::cmp::Reverse(backend.growth_kib));
    judgement.total_growth_kib = judgement
        .judged
        .iter()
        .filter(|backend| !backend.idle_check)
        .map(|backend| backend.growth_kib.max(0))
        .sum();
    judgement.over_backend_limit = judgement
        .judged
        .iter()
        .filter(|backend| backend.growth_kib > backend_limit_kib)
        .cloned()
        .collect();
    judgement.over_total_limit = judgement.total_growth_kib > total_limit_kib;
    judgement
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionOverrun {
    pub fleet: String,
    pub first_at_ms: i64,
    pub samples: Vec<(f64, i64, i64)>,
}

/// A fleet holding more backends than its budget in two consecutive samples, neither
/// inside a disruption.
pub fn judge_connections(
    samples: &[ConnectionSample],
    spans: &[IntentSpan],
    grace_ms: i64,
) -> Vec<ConnectionOverrun> {
    let disrupted = |at_ms: i64| {
        super::intent::covering(spans, None, at_ms, grace_ms)
            .any(|span| span.intent == Intent::Disruption)
    };
    let mut overruns: BTreeMap<String, ConnectionOverrun> = BTreeMap::new();
    for pair in samples.windows(2) {
        if disrupted(pair[0].at_ms) || disrupted(pair[1].at_ms) {
            continue;
        }
        for fleet in &pair[1].fleets {
            let Some(before) = pair[0].fleets.iter().find(|row| row.fleet == fleet.fleet) else {
                continue;
            };
            if fleet.backends > fleet.allowed && before.backends > before.allowed {
                overruns
                    .entry(fleet.fleet.clone())
                    .or_insert_with(|| ConnectionOverrun {
                        fleet: fleet.fleet.clone(),
                        first_at_ms: pair[0].at_ms,
                        samples: Vec::new(),
                    })
                    .samples
                    .extend([
                        (
                            pair[0].at_ms as f64 / 1000.0,
                            before.backends,
                            before.allowed,
                        ),
                        (pair[1].at_ms as f64 / 1000.0, fleet.backends, fleet.allowed),
                    ]);
            }
        }
    }
    overruns.into_values().collect()
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct IdleJudgement {
    pub not_evaluated: Option<String>,
    pub measured_seconds: f64,
    pub excluded_seconds: f64,
    pub xacts_per_second: f64,
    pub wal_bytes_per_second: f64,
}

/// Rates over `[from_ms, to_ms]`, leaving out intervals with an autovacuum worker running,
/// and the sampler's own transactions.
pub fn judge_idle(samples: &[IdleSample], from_ms: i64, to_ms: i64, min_ms: i64) -> IdleJudgement {
    let window: Vec<&IdleSample> = samples
        .iter()
        .filter(|sample| sample.at_ms >= from_ms && sample.at_ms <= to_ms)
        .collect();
    let mut judgement = IdleJudgement::default();
    let (mut measured, mut excluded, mut xacts, mut wal) = (0i64, 0i64, 0i64, 0i64);
    for pair in window.windows(2) {
        let elapsed = pair[1].at_ms - pair[0].at_ms;
        if pair[0].autovacuum_workers > 0 || pair[1].autovacuum_workers > 0 {
            excluded += elapsed;
            continue;
        }
        measured += elapsed;
        xacts += (pair[1].xacts - pair[0].xacts - pair[1].own_xacts).max(0);
        wal += (pair[1].wal_bytes - pair[0].wal_bytes).max(0);
    }
    judgement.measured_seconds = measured as f64 / 1000.0;
    judgement.excluded_seconds = excluded as f64 / 1000.0;
    if measured < min_ms {
        judgement.not_evaluated = Some(format!(
            "{:.0} s of idle measured ({:.0} s left out while autovacuum ran), {} s needed",
            judgement.measured_seconds,
            judgement.excluded_seconds,
            min_ms / 1000
        ));
        return judgement;
    }
    judgement.xacts_per_second = xacts as f64 * 1000.0 / measured as f64;
    judgement.wal_bytes_per_second = wal as f64 * 1000.0 / measured as f64;
    judgement
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct ResourceVerdict {
    pub memory_source: Option<String>,
    pub memory: MemoryJudgement,
    pub connection_peak_total: i64,
    pub connection_peak_queues: usize,
    pub fleet_peaks: Vec<FleetConnections>,
    pub idle_check_peak: i64,
    pub connection_overruns: Vec<ConnectionOverrun>,
    pub prepared_source: Option<String>,
    pub prepared_samples: usize,
    pub prepared_max: Option<PreparedSample>,
    pub idle: IdleJudgement,
}

const IDLE_MIN_MS: i64 = 30_000;

/// Judges the samples and raises the resource violations.
pub fn evaluate(shared: &Shared) {
    let checks = &shared.config.checks;
    let spans = shared.intents.spans();
    let grace_ms = checks.pin_release.as_millis() as i64;
    let fleets: Vec<String> = shared
        .fleets
        .lock()
        .expect("fleets")
        .keys()
        .cloned()
        .collect();
    let mut verdict = ResourceVerdict::default();

    let source = shared
        .resources
        .memory_source
        .lock()
        .expect("memory source")
        .clone();
    verdict.memory = match &source {
        Some(Ok(_)) => judge_backend_memory(
            &shared.resources.backends(),
            |application| runtime_host_of(application, &fleets).is_some(),
            shared.load_started_ms(),
            checks.pg_memory_warmup.as_millis() as i64,
            checks.pg_memory_min_window.as_millis() as i64,
            (checks.pg_memory_backend_growth_mib * 1024.0) as i64,
            (checks.pg_memory_total_growth_mib * 1024.0) as i64,
        ),
        Some(Err(reason)) => MemoryJudgement {
            not_evaluated: Some(format!("not measured: {reason}")),
            ..Default::default()
        },
        None => MemoryJudgement {
            not_evaluated: Some("not sampled".into()),
            ..Default::default()
        },
    };
    verdict.memory_source = source.and_then(Result::ok);
    for backend in &verdict.memory.over_backend_limit {
        shared.violation_at(
            "pg_backend_memory_growth",
            backend.baseline_at_ms,
            format!(
                "backend {} ({}) private memory grew {:.1} MiB to {:.1} MiB over {:.0} s after the warm-up (limit {:.0} MiB); see pg-backends.csv",
                backend.pid,
                backend.application,
                backend.growth_kib as f64 / 1024.0,
                backend.end_kib as f64 / 1024.0,
                backend.span_seconds,
                checks.pg_memory_backend_growth_mib
            ),
            vec![series_row(backend)],
        );
    }
    if verdict.memory.over_total_limit {
        shared.violation_at(
            "pg_backend_memory_growth_total",
            verdict
                .memory
                .judged
                .iter()
                .map(|backend| backend.baseline_at_ms)
                .min()
                .unwrap_or(0),
            format!(
                "the {} judged pooled backends grew {:.1} MiB together after the warm-up (limit {:.0} MiB); see pg-backends.csv",
                verdict.memory.judged.iter().filter(|backend| !backend.idle_check).count(),
                verdict.memory.total_growth_kib as f64 / 1024.0,
                checks.pg_memory_total_growth_mib
            ),
            verdict.memory.judged.iter().take(10).map(series_row).collect(),
        );
    }

    let connections = shared
        .resources
        .connections
        .lock()
        .expect("connections")
        .clone();
    if let Some(peak) = connections.iter().max_by_key(|sample| sample.total) {
        verdict.connection_peak_total = peak.total;
        verdict.connection_peak_queues = peak.queues;
    }
    verdict.idle_check_peak = connections
        .iter()
        .map(|sample| sample.idle_check)
        .max()
        .unwrap_or(0);
    let mut peaks: BTreeMap<String, FleetConnections> = BTreeMap::new();
    for sample in &connections {
        for fleet in &sample.fleets {
            let entry = peaks
                .entry(fleet.fleet.clone())
                .or_insert_with(|| fleet.clone());
            if fleet.backends > entry.backends {
                *entry = fleet.clone();
            }
        }
    }
    verdict.fleet_peaks = peaks.into_values().collect();
    verdict.connection_overruns = judge_connections(&connections, &spans, grace_ms);
    for overrun in &verdict.connection_overruns {
        shared.violation_at(
            "connection_budget_exceeded",
            overrun.first_at_ms,
            format!(
                "fleet {} held more backends than its pools and listeners allow in consecutive samples",
                overrun.fleet
            ),
            overrun
                .samples
                .iter()
                .take(10)
                .map(|(at, backends, allowed)| {
                    format!("{{\"fleet\":\"{}\",\"at_s\":{at:.1},\"backends\":{backends},\"allowed\":{allowed}}}", overrun.fleet)
                })
                .collect(),
        );
    }

    let prepared = shared.resources.prepared.lock().expect("prepared").clone();
    verdict.prepared_samples = prepared.len();
    verdict.prepared_max = prepared
        .iter()
        .max_by_key(|sample| sample.statements)
        .cloned();
    verdict.prepared_source = shared
        .resources
        .prepared_source
        .lock()
        .expect("prepared source")
        .clone();
    let over: Vec<&PreparedSample> = prepared
        .iter()
        .filter(|sample| sample.statements > checks.prepared_statements_max)
        .collect();
    if let Some(first) = over.first() {
        shared.violation_at(
            "prepared_statements_unbounded",
            first.at_ms,
            format!(
                "{} sample(s) found more than {} named prepared statements on a runtime connection, at most {}",
                over.len(),
                checks.prepared_statements_max,
                over.iter().map(|sample| sample.statements).max().unwrap_or(0)
            ),
            over.iter()
                .take(10)
                .map(|sample| serde_json::to_string(sample).unwrap_or_default())
                .collect(),
        );
    }

    let idle_window = *shared.resources.idle_window.lock().expect("idle window");
    verdict.idle = match idle_window {
        Some((from, to)) => judge_idle(
            &shared.resources.idle.lock().expect("idle samples"),
            from + checks.idle_warmup.as_millis() as i64,
            to,
            IDLE_MIN_MS,
        ),
        None => IdleJudgement {
            not_evaluated: Some(
                "no idle phase (AWA_SOAK_IDLE_SECONDS=0, or the run did not quiesce)".into(),
            ),
            ..Default::default()
        },
    };
    if verdict.idle.not_evaluated.is_none() {
        let at_ms = idle_window.map_or(0, |(from, _)| from);
        if verdict.idle.xacts_per_second > checks.idle_xacts_per_second_max {
            shared.violation_at(
                "idle_transactions",
                at_ms,
                format!(
                    "{:.1} transactions/s while the workload was idle (ceiling {:.1}) over {:.0} s",
                    verdict.idle.xacts_per_second,
                    checks.idle_xacts_per_second_max,
                    verdict.idle.measured_seconds
                ),
                vec![],
            );
        }
        if verdict.idle.wal_bytes_per_second > checks.idle_wal_bytes_per_second_max {
            shared.violation_at(
                "idle_wal",
                at_ms,
                format!(
                    "{:.0} WAL bytes/s while the workload was idle (ceiling {:.0}) over {:.0} s",
                    verdict.idle.wal_bytes_per_second,
                    checks.idle_wal_bytes_per_second_max,
                    verdict.idle.measured_seconds
                ),
                vec![],
            );
        }
    }
    *shared.resources.verdict.lock().expect("resource verdict") = Some(verdict);
}

fn series_row(backend: &BackendGrowth) -> String {
    serde_json::json!({
        "pid": backend.pid,
        "application": backend.application,
        "baseline_mib": backend.baseline_kib as f64 / 1024.0,
        "end_mib": backend.end_kib as f64 / 1024.0,
        "series_at_s_kib": backend.series.iter().step_by((backend.series.len() / 12).max(1)).collect::<Vec<_>>(),
    })
    .to_string()
}

pub fn backends_csv(samples: &[BackendSample]) -> String {
    let mut out = String::from("at_s,pid,application,backend_start,state,rss_anon_kib\n");
    for sample in samples {
        let _ = writeln!(
            out,
            "{:.1},{},{:?},{:?},{},{}",
            sample.at_ms as f64 / 1000.0,
            sample.pid,
            sample.application,
            sample.backend_start,
            sample.state,
            sample
                .rss_anon_kib
                .map_or(String::new(), |kib| kib.to_string())
        );
    }
    out
}

pub fn render(out: &mut String, verdict: Option<&ResourceVerdict>, shared: &Shared) {
    let checks = &shared.config.checks;
    let _ = writeln!(out, "## Postgres resources");
    let Some(verdict) = verdict else {
        let _ = writeln!(out, "- not sampled\n");
        return;
    };
    match &verdict.memory.not_evaluated {
        Some(reason) => {
            let _ = writeln!(
                out,
                "- private memory per pooled backend: not evaluated: {reason}"
            );
        }
        None => {
            let pooled: Vec<&BackendGrowth> = verdict
                .memory
                .judged
                .iter()
                .filter(|backend| !backend.idle_check)
                .collect();
            let _ = writeln!(
                out,
                "- private memory per pooled backend (RssAnon, {}): {} backends judged after a {} s warm-up over at least {} s; largest growth {:.1} MiB (limit {:.0}), together {:.1} MiB (limit {:.0}) ([pg-backends.csv](pg-backends.csv))",
                verdict.memory_source.as_deref().unwrap_or("?"),
                pooled.len(),
                checks.pg_memory_warmup.as_secs(),
                checks.pg_memory_min_window.as_secs(),
                pooled.first().map_or(0.0, |backend| backend.growth_kib as f64 / 1024.0),
                checks.pg_memory_backend_growth_mib,
                verdict.memory.total_growth_kib as f64 / 1024.0,
                checks.pg_memory_total_growth_mib,
            );
            for backend in pooled.iter().take(5) {
                let _ = writeln!(
                    out,
                    "    - {} pid {}: {:.1} to {:.1} MiB over {:.0} s",
                    backend.application,
                    backend.pid,
                    backend.baseline_kib as f64 / 1024.0,
                    backend.end_kib as f64 / 1024.0,
                    backend.span_seconds
                );
            }
            match verdict
                .memory
                .judged
                .iter()
                .find(|backend| backend.idle_check)
            {
                Some(backend) => {
                    let _ = writeln!(
                        out,
                        "- maintenance idle-check connection ({IDLE_CHECK_APPLICATION}): {:.1} to {:.1} MiB over {:.0} s",
                        backend.baseline_kib as f64 / 1024.0,
                        backend.end_kib as f64 / 1024.0,
                        backend.span_seconds
                    );
                }
                None => {
                    let _ = writeln!(
                        out,
                        "- maintenance idle-check connection ({IDLE_CHECK_APPLICATION}): none seen for the whole window (peak {} at once)",
                        verdict.idle_check_peak
                    );
                }
            }
        }
    }
    let _ = writeln!(
        out,
        "- backends: peak {} on the soak database with {} queues registered; budget {} usable by personas, {} extra per runtime allowed",
        verdict.connection_peak_total,
        verdict.connection_peak_queues,
        shared.budget.usable(),
        checks.connections_extra_per_runtime
    );
    for fleet in &verdict.fleet_peaks {
        let _ = writeln!(
            out,
            "    - {}: peak {} backends with {} runtime(s), allowed {}",
            fleet.fleet, fleet.backends, fleet.runtimes, fleet.allowed
        );
    }
    let _ = match &verdict.prepared_max {
        Some(max) => writeln!(
            out,
            "- named prepared statements per runtime connection: at most {} ({} with generic plans) on {}, over {} samples (limit {}); {}",
            max.statements,
            max.generic_plans,
            max.runtime,
            verdict.prepared_samples,
            checks.prepared_statements_max,
            verdict.prepared_source.as_deref().unwrap_or("")
        ),
        None => writeln!(out, "- named prepared statements per runtime connection: not measured (no runtime connection could be sampled)"),
    };
    let _ = match &verdict.idle.not_evaluated {
        Some(reason) => writeln!(out, "- idle rates: not evaluated: {reason}"),
        None => writeln!(
            out,
            "- idle rates over {:.0} s ({:.0} s left out while autovacuum ran): {:.1} transactions/s (ceiling {:.0}), {:.0} WAL bytes/s (ceiling {:.0})",
            verdict.idle.measured_seconds,
            verdict.idle.excluded_seconds,
            verdict.idle.xacts_per_second,
            checks.idle_xacts_per_second_max,
            verdict.idle.wal_bytes_per_second,
            checks.idle_wal_bytes_per_second_max
        ),
    };
    let _ = writeln!(out);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(at_s: i64, pid: i32, application: &str, mib: f64) -> BackendSample {
        BackendSample {
            at_ms: at_s * 1000,
            pid,
            application: application.into(),
            backend_start: format!("start-{pid}"),
            state: "idle".into(),
            rss_anon_kib: Some((mib * 1024.0) as i64),
        }
    }

    fn pooled(application: &str) -> bool {
        runtime_host_of(
            application,
            &[
                "tom".to_string(),
                "sam-stripe".to_string(),
                "sam".to_string(),
            ],
        )
        .is_some()
    }

    /// The #787 shape: pooled backends climbing from 17 MB towards 55 MB over 25 idle minutes.
    fn climbing(
        pid: i32,
        application: &str,
        from_mib: f64,
        per_minute_mib: f64,
    ) -> Vec<BackendSample> {
        (0..=25)
            .map(|minute| {
                backend(
                    minute * 60,
                    pid,
                    application,
                    from_mib + per_minute_mib * minute as f64,
                )
            })
            .collect()
    }

    #[test]
    fn runtime_hosts_are_recognised_by_fleet_and_replica() {
        let fleets = ["sam".to_string(), "sam-stripe".to_string()];
        assert_eq!(runtime_host_of("soak_sam_0", &fleets), Some("sam"));
        assert_eq!(
            runtime_host_of("soak_sam-stripe_1", &fleets),
            Some("sam-stripe")
        );
        assert_eq!(runtime_host_of("soak_sam_producer", &fleets), None);
        assert_eq!(runtime_host_of("soak_oracle", &fleets), None);
    }

    #[test]
    fn backend_memory_growth_after_warm_up_fails() {
        let mut samples = climbing(101, "soak_tom_0", 17.0, 1.5);
        samples.extend(climbing(102, "soak_tom_1", 17.0, 0.1));
        samples.extend(climbing(103, "soak_oracle", 17.0, 5.0));
        samples.extend(climbing(104, IDLE_CHECK_APPLICATION, 12.0, 0.0));
        let judgement =
            judge_backend_memory(&samples, pooled, 0, 300_000, 600_000, 24 * 1024, 64 * 1024);
        assert!(judgement.not_evaluated.is_none());
        assert_eq!(
            judgement.judged.len(),
            3,
            "the oracle's own backend is not judged"
        );
        assert_eq!(judgement.over_backend_limit.len(), 1);
        assert_eq!(judgement.over_backend_limit[0].pid, 101);
        assert!(judgement.judged.iter().any(|backend| backend.idle_check));
        assert!(!judgement.over_total_limit);
    }

    #[test]
    fn backend_memory_that_levels_off_passes() {
        let mut samples: Vec<BackendSample> = (0..=25)
            .map(|minute| {
                let mib = if minute < 10 {
                    17.0 + 1.8 * minute as f64
                } else {
                    35.0 + (minute % 2) as f64 * 0.4
                };
                backend(minute * 60, 201, "soak_tom_0", mib)
            })
            .collect();
        samples.extend(climbing(202, "soak_sam-stripe_0", 20.0, 0.05));
        let judgement =
            judge_backend_memory(&samples, pooled, 0, 900_000, 600_000, 24 * 1024, 64 * 1024);
        assert!(judgement.over_backend_limit.is_empty(), "{judgement:?}");
        assert!(!judgement.over_total_limit);
        assert_eq!(judgement.judged.len(), 2);
    }

    #[test]
    fn many_small_climbs_fail_the_total() {
        let samples: Vec<BackendSample> = (0..12)
            .flat_map(|index| climbing(300 + index, &format!("soak_tom_{index}"), 17.0, 0.6))
            .collect();
        let judgement =
            judge_backend_memory(&samples, pooled, 0, 300_000, 600_000, 24 * 1024, 64 * 1024);
        assert!(judgement.over_backend_limit.is_empty());
        assert!(judgement.over_total_limit, "{}", judgement.total_growth_kib);
    }

    #[test]
    fn a_backend_of_a_restarted_runtime_gets_its_own_warm_up() {
        // Opened at 10 minutes, filling its caches for five minutes, then flat.
        let samples: Vec<BackendSample> = (10..=30)
            .map(|minute| {
                let mib = 4.0 + 18.0 * (minute - 10).min(5) as f64;
                backend(minute * 60, 401, "soak_tom_0", mib)
            })
            .collect();
        let judgement =
            judge_backend_memory(&samples, pooled, 0, 300_000, 600_000, 24 * 1024, 64 * 1024);
        assert!(judgement.over_backend_limit.is_empty(), "{judgement:?}");
        assert_eq!(judgement.judged.len(), 1);
        assert_eq!(judgement.judged[0].baseline_at_ms, 15 * 60_000);
    }

    #[test]
    fn short_runs_are_not_evaluated() {
        let samples = climbing(101, "soak_tom_0", 17.0, 5.0);
        let judgement =
            judge_backend_memory(&samples[..12], pooled, 0, 300_000, 600_000, 1024, 1024);
        assert!(judgement.not_evaluated.is_some());
        assert!(judgement.over_backend_limit.is_empty());
    }

    fn connections(at_s: i64, backends: i64) -> ConnectionSample {
        ConnectionSample {
            at_ms: at_s * 1000,
            total: backends,
            queues: 10,
            idle_check: 0,
            fleets: vec![FleetConnections {
                fleet: "tom".into(),
                backends,
                allowed: 24,
                runtimes: 2,
            }],
        }
    }

    #[test]
    fn connection_overruns_need_two_samples_outside_disruptions() {
        let log = super::super::intent::IntentLog::default();
        log.window(None, Intent::Disruption, "kill", 40_000, 45_000);
        let spans = log.spans();
        let blip = [connections(0, 20), connections(15, 30), connections(30, 20)];
        assert!(judge_connections(&blip, &spans, 0).is_empty());
        let restart = [connections(30, 30), connections(45, 30)];
        assert!(judge_connections(&restart, &spans, 0).is_empty());
        let held = [
            connections(60, 20),
            connections(75, 30),
            connections(90, 31),
        ];
        let overruns = judge_connections(&held, &spans, 0);
        assert_eq!(overruns.len(), 1);
        assert_eq!(overruns[0].first_at_ms, 75_000);
    }

    fn idle(at_s: i64, xacts: i64, wal: i64, autovacuum: i64) -> IdleSample {
        IdleSample {
            at_ms: at_s * 1000,
            xacts,
            own_xacts: 3,
            wal_bytes: wal,
            autovacuum_workers: autovacuum,
        }
    }

    #[test]
    fn idle_rates_leave_out_autovacuum_and_the_sampler() {
        let samples = [
            idle(0, 0, 0, 0),
            idle(15, 303, 15_000, 0),
            idle(30, 606, 30_000, 1),
            idle(45, 909, 9_000_000, 1),
            idle(60, 1212, 9_015_000, 0),
            idle(75, 1515, 9_030_000, 0),
        ];
        let judgement = judge_idle(&samples, 0, 75_000, 30_000);
        assert!(judgement.not_evaluated.is_none());
        assert_eq!(judgement.measured_seconds, 30.0);
        assert_eq!(judgement.excluded_seconds, 45.0);
        assert!((judgement.xacts_per_second - 20.0).abs() < 1e-9);
        assert!((judgement.wal_bytes_per_second - 1000.0).abs() < 1e-9);
        let short = judge_idle(&samples, 0, 20_000, 30_000);
        assert!(short.not_evaluated.is_some());
    }

    #[test]
    fn rss_lines_parse() {
        let parsed = parse_rss_lines("12 RssAnon:\t  4096 kB\n13 RssAnon: 10 kB\ngarbage\n");
        assert_eq!(parsed.get(&12), Some(&4096));
        assert_eq!(parsed.get(&13), Some(&10));
        assert_eq!(
            host_of("postgres://u:p@localhost:5432/db").as_deref(),
            Some("localhost")
        );
    }
}
