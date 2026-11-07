//! Admin UI users, driving the real `awa serve` HTTP server with the requests the web
//! UI makes (see `awa-ui/frontend/src/lib/api.ts`) at the UI's own refresh intervals.
//!
//! - Watchers: several dashboard tabs polling overview, queues, counts and timeseries.
//! - Support: searching and paging through jobs, opening details, DLQ, runtime and cron.
//! - UiOps: the UI's mutations (retry, cancel, pause, DLQ retry and purge, bulk actions).
//! - Impatient: requests abandoned mid-flight and bursts of refreshes.

use super::super::adminserver::APPLICATION_NAME;
use super::super::config::Persona;
use super::super::db::connect_pool;
use super::super::ledger::flag;
use super::super::rng::{percentile, Rng};
use super::super::shared::Shared;
use super::{alex, mia};
use reqwest::Method;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Latency samples per endpoint over the run, and the number of requests in flight.
#[derive(Default)]
pub struct UiMetrics {
    samples: Mutex<HashMap<String, Vec<(i64, i64)>>>,
    pub inflight: AtomicI64,
    base_url: Mutex<Option<String>>,
    reported_errors: Mutex<HashMap<String, u32>>,
}

impl UiMetrics {
    pub fn set_base_url(&self, url: String) {
        *self.base_url.lock().expect("ui base url") = Some(url);
    }

    fn base_url(&self) -> String {
        self.base_url
            .lock()
            .expect("ui base url")
            .clone()
            .unwrap_or_default()
    }

    fn record(&self, endpoint: &str, at_ms: i64, latency_ms: i64) {
        let mut samples = self.samples.lock().expect("ui samples");
        let list = samples.entry(endpoint.to_string()).or_default();
        if list.len() < 200_000 {
            list.push((at_ms, latency_ms));
        }
    }

    /// True the first few times an error class is seen for an endpoint.
    fn first_errors(&self, key: &str) -> bool {
        let mut reported = self.reported_errors.lock().expect("ui errors");
        let count = reported.entry(key.to_string()).or_insert(0);
        *count += 1;
        *count <= 3
    }
}

pub enum Outcome {
    Json(Value),
    Status(u16),
    Timeout,
    Failed,
}

impl Outcome {
    fn json(self) -> Option<Value> {
        match self {
            Outcome::Json(value) => Some(value),
            _ => None,
        }
    }
}

pub struct Http {
    client: reqwest::Client,
    shared: Arc<Shared>,
    persona: Persona,
}

impl Http {
    pub fn new(shared: &Arc<Shared>, persona: Persona) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(shared.config.ui_client_timeout)
                .pool_max_idle_per_host(4)
                .build()
                .expect("http client"),
            shared: shared.clone(),
            persona,
        }
    }

    pub async fn get(&self, endpoint: &str, path: &str) -> Outcome {
        self.request(Method::GET, endpoint, path, None).await
    }

    pub async fn post(&self, endpoint: &str, path: &str, body: Value) -> Outcome {
        self.request(Method::POST, endpoint, path, Some(body)).await
    }

    pub async fn request(
        &self,
        method: Method,
        endpoint: &str,
        path: &str,
        body: Option<Value>,
    ) -> Outcome {
        let name = format!("{method} {endpoint}");
        let started = Instant::now();
        let at_ms = self.shared.now_ms();
        self.shared.ui.inflight.fetch_add(1, Ordering::Relaxed);
        let mut request = self
            .client
            .request(method, format!("{}{path}", self.shared.ui.base_url()));
        if let Some(body) = body {
            request = request.json(&body);
        }
        let outcome = match request.send().await {
            Ok(response) => {
                let status = response.status();
                match response.text().await {
                    Ok(text) if status.is_success() => {
                        Outcome::Json(serde_json::from_str(&text).unwrap_or(Value::Null))
                    }
                    Ok(text) => {
                        if status.is_server_error() && self.shared.ui.first_errors(&name) {
                            self.shared.finding(
                                "ui_server_error",
                                format!("{name} answered {status}"),
                                vec![text.chars().take(300).collect()],
                            );
                        }
                        Outcome::Status(status.as_u16())
                    }
                    Err(error) if error.is_timeout() => Outcome::Timeout,
                    Err(_) => Outcome::Failed,
                }
            }
            Err(error) if error.is_timeout() => Outcome::Timeout,
            Err(_) => Outcome::Failed,
        };
        self.shared.ui.inflight.fetch_sub(1, Ordering::Relaxed);
        let latency = started.elapsed().as_millis() as i64;
        self.shared.ui.record(&name, at_ms, latency);
        let stats = self.shared.stats(self.persona);
        stats.observe(&name, latency);
        match &outcome {
            Outcome::Timeout => {
                stats.add("client_timeouts", 1);
                if self.shared.ui.first_errors(&format!("timeout {name}")) {
                    self.shared.finding(
                        "ui_request_hang",
                        format!(
                            "{name} did not answer within the {}s client timeout",
                            self.shared.config.ui_client_timeout.as_secs()
                        ),
                        vec![format!("{{\"path\":{path:?}}}")],
                    );
                }
            }
            Outcome::Failed => stats.add("connection_errors", 1),
            Outcome::Status(code) => stats.add(&format!("http_{code}"), 1),
            Outcome::Json(_) => {}
        }
        outcome
    }

    /// Sends a GET and abandons it after `after`, closing the connection mid-request.
    pub async fn get_abandoned(&self, path: &str, after: Duration) -> bool {
        let url = format!("{}{path}", self.shared.ui.base_url());
        let send = self.client.get(url).send();
        tokio::select! {
            _ = send => false,
            _ = tokio::time::sleep(after) => true,
        }
    }
}

fn ids_of(value: &Value) -> Vec<i64> {
    value
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter_map(|row| row.get("id").and_then(Value::as_i64))
                .collect()
        })
        .unwrap_or_default()
}

async fn poll_interval(http: &Http) -> Duration {
    let ms = http
        .get("/api/capabilities", "/api/capabilities")
        .await
        .json()
        .and_then(|value| value.get("poll_interval_ms").and_then(Value::as_u64))
        .unwrap_or(5_000);
    Duration::from_millis(ms)
}

/// Dashboard tabs: each polls what the overview, the shell and (for some tabs) the
/// queues, kinds and cron pages poll, every server-suggested interval.
pub async fn run_watchers(shared: Arc<Shared>) -> Result<(), String> {
    let tabs = shared.config.scaled(4) as usize;
    shared.note(
        "watchers",
        format!("{tabs} dashboard tabs polling the admin UI"),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for tab in 0..tabs {
        let shared = shared.clone();
        tasks.spawn(async move {
            let http = Http::new(&shared, Persona::Watchers);
            let mut rng = Rng::for_task(shared.config.seed, &format!("watcher-{tab}"));
            let interval = poll_interval(&http).await;
            shared
                .load_stop
                .sleep(Duration::from_millis(
                    rng.below(interval.as_millis() as u64),
                ))
                .await;
            while !shared.load_stop.is_stopped() {
                // The overview page and the shell, in parallel as the browser does.
                tokio::join!(
                    http.get("/api/stats", "/api/stats"),
                    http.get("/api/stats/timeseries", "/api/stats/timeseries?minutes=60"),
                    http.get("/api/queues", "/api/queues"),
                    http.get("/api/jobs", "/api/jobs?state=failed&limit=10"),
                    http.get("/api/runtime", "/api/runtime"),
                    http.get("/api/storage", "/api/storage?history=true"),
                );
                match tab % 3 {
                    1 => {
                        tokio::join!(
                            http.get("/api/queues/runtime", "/api/queues/runtime"),
                            http.get("/api/cron", "/api/cron")
                        );
                    }
                    2 => {
                        http.get("/api/kinds", "/api/kinds").await;
                    }
                    _ => {}
                }
                let jitter = rng.range(90, 110);
                shared.load_stop.sleep(interval * jitter as u32 / 100).await;
            }
        });
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}

pub async fn run_support(shared: Arc<Shared>) -> Result<(), String> {
    let http = Http::new(&shared, Persona::Support);
    let mut rng = Rng::for_task(shared.config.seed, "support");
    let kinds = [
        "mia_job",
        "tom_job",
        "priya_job",
        "sam_job",
        "alex_job",
        "py_job",
        "py_shared_job",
    ];
    let states = [
        "available",
        "running",
        "scheduled",
        "retryable",
        "completed",
        "failed",
        "cancelled",
    ];
    let mut known: Vec<i64> = Vec::new();
    shared.note(
        "support",
        "searching, paging and opening jobs, DLQ, runtime and cron pages",
    );
    while !shared
        .load_stop
        .sleep(Duration::from_millis(rng.range(600, 1800)))
        .await
    {
        match rng.below(10) {
            0 | 1 => {
                let kind = rng.pick(&kinds).expect("kinds");
                known.extend(ids_of(
                    &http
                        .get("/api/jobs", &format!("/api/jobs?kind={kind}&limit=50"))
                        .await
                        .json()
                        .unwrap_or(Value::Null),
                ));
            }
            2 => {
                let queues = shared.queues.all();
                if let Some(queue) = rng.pick(&queues) {
                    known.extend(ids_of(
                        &http
                            .get(
                                "/api/jobs",
                                &format!("/api/jobs?queue={}&limit=50", queue.name),
                            )
                            .await
                            .json()
                            .unwrap_or(Value::Null),
                    ));
                }
            }
            3 => {
                let state = rng.pick(&states).expect("states");
                known.extend(ids_of(
                    &http
                        .get("/api/jobs", &format!("/api/jobs?state={state}&limit=100"))
                        .await
                        .json()
                        .unwrap_or(Value::Null),
                ));
            }
            4 if rng.chance(30) => {
                http.get("/api/jobs", "/api/jobs?tag=soak&limit=20").await;
            }
            4 => {}
            5 => {
                // Follow one job the way the detail page does: a fast poll for a while.
                if let Some(&id) = rng.pick(&known) {
                    for _ in 0..5 {
                        http.get("/api/jobs/{id}", &format!("/api/jobs/{id}")).await;
                        if shared.load_stop.sleep(Duration::from_secs(2)).await {
                            break;
                        }
                    }
                }
                http.get(
                    "/api/jobs/{id}",
                    &format!("/api/jobs/{}", 9_000_000_000_i64 + rng.below(1000) as i64),
                )
                .await;
            }
            6 => {
                // Page through a large result set with the before_id cursor.
                let state = *rng.pick(&["completed", "available"]).expect("states");
                let mut before: Option<i64> = None;
                for page in 0..10 {
                    let path = match before {
                        Some(id) => format!("/api/jobs?state={state}&limit=100&before_id={id}"),
                        None => format!("/api/jobs?state={state}&limit=100"),
                    };
                    let endpoint = if page == 0 {
                        "/api/jobs (first page)"
                    } else {
                        "/api/jobs (later page)"
                    };
                    let ids = ids_of(
                        &http
                            .get(endpoint, &path)
                            .await
                            .json()
                            .unwrap_or(Value::Null),
                    );
                    match ids.iter().min() {
                        Some(&lowest) if ids.len() >= 100 => before = Some(lowest),
                        _ => break,
                    }
                }
            }
            7 => {
                let dlq = http
                    .get("/api/dlq", "/api/dlq?limit=50")
                    .await
                    .json()
                    .unwrap_or(Value::Null);
                let ids = ids_of(&dlq);
                if let Some(last) = dlq.as_array().and_then(|rows| rows.last()) {
                    if let (Some(id), Some(at)) = (
                        last.get("id").and_then(Value::as_i64),
                        last.get("dlq_at").and_then(Value::as_str),
                    ) {
                        let at = at.replace('+', "%2B");
                        http.get(
                            "/api/dlq (later page)",
                            &format!("/api/dlq?limit=50&before_id={id}&before_dlq_at={at}"),
                        )
                        .await;
                    }
                }
                if let Some(&id) = rng.pick(&ids) {
                    http.get("/api/dlq/{id}", &format!("/api/dlq/{id}")).await;
                }
                http.get("/api/dlq/depth", "/api/dlq/depth").await;
            }
            8 => {
                tokio::join!(
                    http.get("/api/runtime", "/api/runtime"),
                    http.get("/api/queues/runtime", "/api/queues/runtime"),
                    http.get("/api/cron", "/api/cron"),
                );
            }
            _ => {
                tokio::join!(
                    http.get("/api/kinds", "/api/kinds"),
                    http.get("/api/stats/kinds", "/api/stats/kinds"),
                    http.get("/api/stats/queues", "/api/stats/queues"),
                );
            }
        }
        if known.len() > 500 {
            known.drain(..known.len() - 500);
        }
    }
    Ok(())
}

/// POSTs a resume until it succeeds; a queue or schedule left paused looks like a stall.
async fn resume_until_ok(http: &Http, shared: &Shared, path: &str, endpoint: &str) {
    for attempt in 0..8 {
        if http.post(endpoint, path, json!({})).await.json().is_some() {
            return;
        }
        tokio::time::sleep(Duration::from_secs(2 + attempt)).await;
    }
    shared.violation(
        "resume_failed",
        format!("{path} did not succeed after eight attempts"),
        vec![],
    );
}

pub async fn run_ui_operator(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::UiOps;
    let http = Http::new(&shared, persona);
    let mut rng = Rng::for_task(shared.config.seed, "uiops");
    shared.note("uiops", "operating through the UI's mutation endpoints");
    while !shared
        .load_stop
        .sleep(Duration::from_millis(rng.range(2_000, 5_000)))
        .await
    {
        match rng.below(9) {
            0 => {
                let mut candidates = shared.queues.of(Persona::Sam);
                candidates.extend(shared.queues.of(Persona::Tom));
                if let Some(queue) = rng.pick(&candidates).cloned() {
                    queue.hold();
                    let paused = http
                        .post(
                            "/api/queues/{queue}/pause",
                            &format!("/api/queues/{}/pause", queue.name),
                            json!({"paused_by": "soak-ui"}),
                        )
                        .await
                        .json()
                        .is_some();
                    if paused {
                        shared.stats(persona).add("pauses", 1);
                        shared
                            .stop_sleep_load(Duration::from_secs(rng.range(3, 6)))
                            .await;
                    }
                    // A pause whose answer was lost may still have applied, so always resume.
                    resume_until_ok(
                        &http,
                        &shared,
                        &format!("/api/queues/{}/resume", queue.name),
                        "/api/queues/{queue}/resume",
                    )
                    .await;
                    shared.stop_sleep_load(Duration::from_secs(3)).await;
                    queue.release();
                }
            }
            1 | 2 => {
                // Cancel live jobs from a list view, singly or in bulk.
                let mut candidates = shared.queues.of(Persona::Tom);
                candidates.extend(shared.queues.of(Persona::Sam));
                let Some(queue) = rng.pick(&candidates).cloned() else {
                    continue;
                };
                let state = *rng
                    .pick(&["available", "scheduled", "running"])
                    .expect("states");
                let list = http
                    .get(
                        "/api/jobs",
                        &format!("/api/jobs?queue={}&state={state}&limit=20", queue.name),
                    )
                    .await
                    .json()
                    .unwrap_or(Value::Null);
                let ids: Vec<i64> = ids_of(&list).into_iter().take(6).collect();
                if ids.is_empty() {
                    continue;
                }
                let now = shared.now_ms() as u32;
                if rng.chance(50) {
                    let completed_before = shared
                        .ledger
                        .get(ids[0])
                        .is_some_and(|record| record.completed_events > 0);
                    if http
                        .post(
                            "/api/jobs/{id}/cancel",
                            &format!("/api/jobs/{}/cancel", ids[0]),
                            json!({}),
                        )
                        .await
                        .json()
                        .is_some()
                    {
                        shared.record_operator_cancel(persona, ids[0], completed_before, now);
                    }
                } else if let Some(cancelled) = http
                    .post(
                        "/api/jobs/bulk-cancel",
                        "/api/jobs/bulk-cancel",
                        json!({"ids": ids}),
                    )
                    .await
                    .json()
                {
                    for id in ids_of(&cancelled) {
                        shared.record_operator_cancel(persona, id, false, now);
                    }
                }
            }
            3 | 4 => {
                let queue = if rng.chance(50) {
                    mia::INFER_QUEUE
                } else {
                    mia::DEADLINE_QUEUE
                };
                let dlq = http
                    .get("/api/dlq", &format!("/api/dlq?queue={queue}&limit=10"))
                    .await
                    .json()
                    .unwrap_or(Value::Null);
                let ids = ids_of(&dlq);
                let Some(&id) = rng.pick(&ids) else { continue };
                if rng.chance(60) {
                    shared.ledger.mark(id, flag::DLQ_RETRIED);
                    if http
                        .post(
                            "/api/dlq/{id}/retry",
                            &format!("/api/dlq/{id}/retry"),
                            json!({}),
                        )
                        .await
                        .json()
                        .is_some()
                    {
                        shared.stats(persona).add("dlq_retried", 1);
                    }
                } else {
                    shared.ledger.mark(id, flag::DLQ_PURGED);
                    let outcome = http
                        .request(
                            Method::DELETE,
                            "/api/dlq/{id}",
                            &format!("/api/dlq/{id}"),
                            None,
                        )
                        .await;
                    if outcome.json().is_some() {
                        shared.stats(persona).add("dlq_purged", 1);
                    }
                }
            }
            5 => {
                // Bulk actions on the DLQ by filter, as the DLQ page's toolbar does.
                if rng.chance(50) {
                    shared.ledger.mark_where(
                        |record| record.persona == Persona::Mia && record.has(flag::EXHAUSTED),
                        flag::DLQ_RETRIED,
                    );
                    http.post(
                        "/api/dlq/bulk-retry",
                        "/api/dlq/bulk-retry",
                        json!({"queue": mia::INFER_QUEUE}),
                    )
                    .await;
                } else {
                    shared.ledger.mark_where(
                        |record| record.persona == Persona::Mia && record.has(flag::EXHAUSTED),
                        flag::DLQ_PURGED,
                    );
                    http.post(
                        "/api/dlq/bulk-purge",
                        "/api/dlq/bulk-purge",
                        json!({"queue": mia::TRAIN_QUEUE}),
                    )
                    .await;
                }
            }
            6 => {
                let name = format!("soak_tom_cron_{}", rng.below(4));
                let paused = http
                    .post(
                        "/api/cron/{name}/pause",
                        &format!("/api/cron/{name}/pause"),
                        json!({"paused_by": "soak-ui"}),
                    )
                    .await
                    .json()
                    .is_some();
                if paused {
                    shared.stop_sleep_load(Duration::from_secs(8)).await;
                }
                resume_until_ok(
                    &http,
                    &shared,
                    &format!("/api/cron/{name}/resume"),
                    "/api/cron/{name}/resume",
                )
                .await;
            }
            7 => {
                let name = format!("soak_tom_cron_{}", rng.below(4));
                http.post(
                    "/api/cron/{name}/trigger",
                    &format!("/api/cron/{name}/trigger"),
                    json!({}),
                )
                .await;
            }
            _ => {
                // Preview then submit a batch operation, as the batch operations page does.
                let payload = json!({
                    "op_kind": "set_priority",
                    "spec": { "priority": 2 },
                    "filter": { "queue": alex::QUEUE_B, "state": "available" },
                    "submitted_by": "soak-ui",
                });
                http.post(
                    "/api/batch-ops/preview",
                    "/api/batch-ops/preview",
                    payload.clone(),
                )
                .await;
                http.post("/api/batch-ops", "/api/batch-ops", payload).await;
                http.get("/api/batch-ops", "/api/batch-ops?limit=20").await;
            }
        }
    }
    Ok(())
}

/// Requests abandoned mid-flight and bursts of refreshes, while sampling the server's
/// database sessions: an abandoned request must not keep holding a connection.
pub async fn run_impatient(shared: Arc<Shared>) -> Result<(), String> {
    let persona = Persona::Impatient;
    let http = Arc::new(Http::new(&shared, persona));
    let sampler_pool = connect_pool(
        &shared.config.database_url,
        "awa_ui_sampler",
        shared.budget.reserve("ui-sampler", 1, 1),
    )
    .await
    .map_err(|error| error.to_string())?;
    let statement_timeout = shared.config.ui_statement_timeout;
    let sole_ui_persona = [Persona::Watchers, Persona::Support, Persona::UiOps]
        .iter()
        .all(|other| !shared.config.has(*other));
    let heavy = [
        "/api/kinds",
        "/api/queues",
        "/api/stats",
        "/api/stats/timeseries?minutes=1440",
        "/api/jobs?limit=100",
        "/api/jobs?state=completed&limit=100",
        "/api/dlq?limit=100",
        "/api/runtime",
        "/api/storage?history=true",
    ];

    let sampler = {
        let shared = shared.clone();
        let pool = sampler_pool.clone();
        tokio::spawn(async move {
            let mut reported = false;
            loop {
                if shared.drain_stop.is_stopped() {
                    return;
                }
                let row: Result<(i64, i64, f64), _> = sqlx::query_as(&format!(
                    "SELECT count(*)::bigint, count(*) FILTER (WHERE state = 'active')::bigint,
                            COALESCE(max(extract(epoch FROM now() - query_start)) FILTER (WHERE state = 'active'), 0)::float8
                     FROM pg_stat_activity WHERE application_name = '{APPLICATION_NAME}'"
                ))
                .fetch_one(&pool)
                .await;
                if let Ok((sessions, active, oldest)) = row {
                    shared.run.set_max("ui_server_sessions_peak", sessions);
                    shared.run.set_max("ui_server_active_sessions_peak", active);
                    shared
                        .run
                        .set_max("ui_server_oldest_active_query_s", oldest as i64);
                    shared.run.observe("ui_server_sessions", sessions);
                    if oldest > statement_timeout.as_secs_f64() + 20.0 && !reported {
                        reported = true;
                        shared.violation(
                            "ui_query_outlived_statement_timeout",
                            format!("an admin server query has been active for {oldest:.0}s with a {}s statement timeout", statement_timeout.as_secs()),
                            vec![format!("{{\"sessions\":{sessions},\"active\":{active},\"oldest_active_s\":{oldest:.0}}}")],
                        );
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        })
    };

    let mut rng = Rng::for_task(shared.config.seed, "impatient");
    let mut next_burst = Instant::now() + Duration::from_secs(20);
    shared.note(
        "impatient",
        "abandoning requests mid-flight and refreshing in bursts",
    );
    while !shared
        .load_stop
        .sleep(Duration::from_millis(rng.range(200, 900)))
        .await
    {
        let path = *rng.pick(&heavy).expect("paths");
        let after = Duration::from_millis(rng.range(2, 150));
        if http.get_abandoned(path, after).await {
            shared.stats(persona).add("abandoned_requests", 1);
        } else {
            shared.stats(persona).add("completed_before_abandon", 1);
        }
        if Instant::now() >= next_burst {
            next_burst = Instant::now() + Duration::from_secs(rng.range(30, 70));
            let mut burst = tokio::task::JoinSet::new();
            let size = shared.config.scaled(12);
            for _ in 0..size {
                let http = http.clone();
                let path = *rng.pick(&heavy).expect("paths");
                let abandon = rng.chance(70);
                let after = Duration::from_millis(rng.range(5, 120));
                burst.spawn(async move {
                    if abandon {
                        http.get_abandoned(path, after).await
                    } else {
                        http.get(path, path).await;
                        false
                    }
                });
            }
            let mut abandoned = 0;
            while let Some(result) = burst.join_next().await {
                if result.unwrap_or(false) {
                    abandoned += 1;
                }
            }
            shared.stats(persona).add("bursts", 1);
            shared.stats(persona).add("burst_abandoned", abandoned);
            // With no other UI user running, every query still active after the burst
            // belongs to an abandoned request; time how long the server keeps running them.
            if sole_ui_persona {
                let released_started = Instant::now();
                loop {
                    let active: i64 = sqlx::query_scalar(&format!(
                        "SELECT count(*)::bigint FROM pg_stat_activity WHERE application_name = '{APPLICATION_NAME}' AND state = 'active'"
                    ))
                    .fetch_one(&sampler_pool)
                    .await
                    .unwrap_or(0);
                    if active == 0
                        || released_started.elapsed() > Duration::from_secs(90)
                        || shared.load_stop.is_stopped()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                let seconds = released_started.elapsed().as_secs() as i64;
                shared
                    .stats(persona)
                    .observe("release_after_burst_s", seconds);
                if seconds >= 5 {
                    shared.finding(
                        "ui_abandoned_request_runs_on",
                        format!("after a burst of abandoned requests the server kept {seconds}s of queries running (statement timeout {}s)", statement_timeout.as_secs()),
                        vec![],
                    );
                }
            }
        }
    }

    // With every UI client stopped, the server must release its connections once the
    // statements it was running reach their timeout.
    let quiet_started = Instant::now();
    while shared.ui.inflight.load(Ordering::Relaxed) > 0
        && quiet_started.elapsed() < Duration::from_secs(60)
    {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let deadline = Instant::now() + statement_timeout + Duration::from_secs(15);
    let mut active = i64::MAX;
    while Instant::now() < deadline {
        let row: Result<(i64, i64), _> = sqlx::query_as(&format!(
            "SELECT count(*)::bigint, count(*) FILTER (WHERE state = 'active')::bigint
             FROM pg_stat_activity WHERE application_name = '{APPLICATION_NAME}'"
        ))
        .fetch_one(&sampler_pool)
        .await;
        if let Ok((sessions, now_active)) = row {
            active = now_active;
            shared.run.add("ui_server_sessions_after_quiet", 0);
            shared
                .run
                .set_max("ui_server_sessions_after_quiet", sessions);
            if active == 0 {
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if active != 0 {
        shared.violation(
            "ui_sessions_not_released",
            format!("{active} admin server database session(s) still running a query {}s after every UI client stopped", (statement_timeout + Duration::from_secs(15)).as_secs()),
            vec![],
        );
    }
    shared.drain_stop.wait().await;
    let _ = sampler.await;
    sampler_pool.close().await;
    Ok(())
}

impl Shared {
    /// Sleeps unless the load phase ends first.
    pub async fn stop_sleep_load(&self, duration: Duration) -> bool {
        self.load_stop.sleep(duration).await
    }
}

/// After the run: latency growth and slow-endpoint findings.
pub fn analyse(shared: &Shared) {
    let samples = shared.ui.samples.lock().expect("ui samples").clone();
    let run_ms = shared.config.load.as_millis() as i64;
    for (endpoint, mut points) in samples {
        if points.len() < 20 {
            continue;
        }
        points.sort_by_key(|point| point.0);
        let quarter = points.len() / 4;
        let median = |slice: &[(i64, i64)]| {
            let mut values: Vec<i64> = slice.iter().map(|point| point.1).collect();
            values.sort_unstable();
            percentile(&values, 0.5)
        };
        let early = median(&points[..quarter]);
        let late = median(&points[points.len() - quarter..]);
        let mut all: Vec<i64> = points.iter().map(|point| point.1).collect();
        all.sort_unstable();
        let p99 = percentile(&all, 0.99);
        let max = all.last().copied().unwrap_or(0);
        if endpoint.starts_with("GET") && late >= 500 && late >= early.max(1) * 3 {
            shared.finding(
                "ui_latency_grows",
                format!("{endpoint}: median latency {early} ms in the first quarter of its samples, {late} ms in the last (run {}s)", run_ms / 1000),
                vec![format!("{{\"endpoint\":\"{endpoint}\",\"samples\":{},\"p99_ms\":{p99},\"max_ms\":{max}}}", points.len())],
            );
        }
        if p99 >= 10_000 {
            shared.finding(
                "ui_endpoint_slow",
                format!(
                    "{endpoint}: p99 {p99} ms, max {max} ms over {} requests",
                    points.len()
                ),
                vec![],
            );
        }
    }
}
