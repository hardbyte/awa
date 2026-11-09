//! Durable controls and runtime observations for `maintenance.prune`.
use crate::AwaError;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

pub const PRUNE_SCOPE: &str = "maintenance.prune";

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RuntimeControl {
    pub scope: String,
    pub revision: i64,
    pub halt_id: Option<Uuid>,
    pub mode: String,
    pub reason: String,
    pub actor: String,
    pub updated_at: DateTime<Utc>,
}

pub async fn read(conn: &mut PgConnection) -> Result<RuntimeControl, sqlx::Error> {
    sqlx::query_as("SELECT scope, revision, halt_id, mode, reason, actor, updated_at FROM awa.runtime_controls WHERE scope = 'maintenance.prune'")
        .fetch_one(conn).await
}

pub async fn halt(
    pool: &PgPool,
    scope: &str,
    reason: &str,
    mode: &str,
) -> Result<RuntimeControl, AwaError> {
    Ok(
        sqlx::query_as("SELECT * FROM awa.halt_runtime_scope($1, $2, $3)")
            .bind(scope)
            .bind(reason)
            .bind(mode)
            .fetch_one(pool)
            .await?,
    )
}

pub async fn resume(pool: &PgPool, scope: &str, halt_id: Uuid) -> Result<RuntimeControl, AwaError> {
    Ok(
        sqlx::query_as("SELECT * FROM awa.resume_runtime_scope($1, $2)")
            .bind(scope)
            .bind(halt_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Version of the runtime observation protocol understood by fleet status.
pub const OBSERVATION_PROTOCOL_VERSION: u64 = 1;

/// Aggregate status of the current request across the live runtime snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeControlState {
    /// No halt is requested and live observations are compatible and fresh.
    /// Runtime observations may still lag the resume; an empty fleet is running.
    Running,
    /// No halt is requested, but a live runtime cannot confirm control observation.
    Degraded,
    /// No live runtimes, or at least one live runtime cannot confirm observation.
    Unconfirmed,
    /// A live runtime has not yet observed the current revision and halt.
    Pending,
    /// A matching observation does not establish callback settlement.
    Unknown,
    /// Every live runtime observes the halt, but some admitted work remains.
    Draining,
    /// Every live runtime confirms closed admission and settled callbacks.
    Halted,
}

/// Whether a runtime's latest report can participate in halt confirmation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeObservationStatus {
    #[default]
    Missing,
    Unsupported,
    IncompatibleProtocol,
    StaleReport,
    StaleControl,
    Observing,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeControlRuntime {
    pub instance_id: Uuid,
    pub hostname: Option<String>,
    pub pid: i32,
    pub version: String,
    pub leader: bool,
    pub shutting_down: bool,
    pub last_seen_at: DateTime<Utc>,
    pub snapshot_interval_ms: i64,
    pub live: bool,
    pub observation: Option<serde_json::Value>,
    /// Maximum age of a report, based on this runtime's snapshot cadence.
    #[serde(skip_deserializing)]
    pub report_freshness_ms: i64,
    #[serde(skip_deserializing)]
    pub report_recent: bool,
    #[serde(skip_deserializing)]
    pub observation_status: RuntimeObservationStatus,
    #[serde(skip_deserializing)]
    pub observes_current_request: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeControlStatus {
    pub requested: RuntimeControl,
    pub state: RuntimeControlState,
    /// Database time used to classify runtime snapshots and report recency.
    pub observed_at: DateTime<Utc>,
    /// Includes stale snapshots; `live` is a best-effort heartbeat assessment.
    pub runtimes: Vec<RuntimeControlRuntime>,
    /// Live runtimes without a supported, fresh observation of this scope.
    pub unobserving_runtimes: Vec<RuntimeControlRuntime>,
}

#[derive(sqlx::FromRow)]
struct StatusSnapshot {
    requested: sqlx::types::Json<RuntimeControl>,
    observed_at: DateTime<Utc>,
    runtimes: sqlx::types::Json<Vec<RuntimeControlRuntime>>,
}

/// Read the request and all runtime observations from one database snapshot.
///
/// A report is recent for max(2 * max(interval, 1s), 5s) after its database
/// receipt time. The report's `fresh` field describes the control permission at
/// report creation; it does not extend that permission's lifetime. Runtime
/// liveness follows the admin snapshot cutoff: max(3 * max(interval, 1s), 30s).
/// Heartbeats cannot prove that a process has stopped, so this is an operational
/// status report, not a fencing mechanism.
pub async fn status(pool: &PgPool) -> Result<RuntimeControlStatus, AwaError> {
    let mut begin_guard = crate::read_transaction::BeginGuard::acquire(pool).await?;
    let mut tx = sqlx::Connection::begin_with(
        &mut *begin_guard.connection,
        "BEGIN ISOLATION LEVEL READ COMMITTED, READ ONLY",
    )
    .await?;
    begin_guard.begin_pending = false;
    let snapshot: StatusSnapshot = sqlx::query_as(
        r#"
        WITH status_clock AS MATERIALIZED (SELECT statement_timestamp() AS at)
        SELECT to_jsonb(requested) AS requested, status_clock.at AS observed_at,
               COALESCE((
                   SELECT jsonb_agg(
                       jsonb_build_object(
                           'instance_id', runtime.instance_id,
                           'hostname', runtime.hostname,
                           'pid', runtime.pid,
                           'version', runtime.version,
                           'leader', runtime.leader,
                           'shutting_down', runtime.shutting_down,
                           'last_seen_at', runtime.last_seen_at,
                           'snapshot_interval_ms', runtime.snapshot_interval_ms,
                           'live', EXTRACT(EPOCH FROM (status_clock.at - runtime.last_seen_at)) * 1000
                               <= GREATEST(GREATEST(runtime.snapshot_interval_ms::numeric, 1000) * 3, 30000),
                           'observation', report.observation
                       ) ORDER BY runtime.last_seen_at DESC, runtime.instance_id
                   )
                   FROM awa.runtime_instances runtime
                   LEFT JOIN awa.runtime_control_observations report USING (instance_id)
               ), '[]'::jsonb) AS runtimes
        FROM awa.runtime_controls requested CROSS JOIN status_clock
        WHERE requested.scope = 'maintenance.prune'
        "#,
    )
    .fetch_one(tx.as_mut())
    .await?;
    tx.commit().await?;

    let requested = snapshot.requested.0;
    let mut runtimes = snapshot.runtimes.0;
    for runtime in &mut runtimes {
        runtime.report_freshness_ms = runtime
            .snapshot_interval_ms
            .max(1_000)
            .saturating_mul(2)
            .max(5_000);
        runtime.report_recent = runtime
            .observation
            .as_ref()
            .and_then(|observation| observation["received_at"].as_str())
            .and_then(|received_at| DateTime::parse_from_rfc3339(received_at).ok())
            .is_some_and(|received_at| {
                let age = snapshot.observed_at.signed_duration_since(received_at);
                age >= chrono::Duration::zero()
                    && age < chrono::Duration::milliseconds(runtime.report_freshness_ms)
            });
        runtime.observation_status = observation_status(runtime);
        runtime.observes_current_request = runtime.observation_status
            == RuntimeObservationStatus::Observing
            && runtime.observation.as_ref().is_some_and(|observation| {
                observation["observed_revision"].as_i64() == Some(requested.revision)
                    && observation["halt_id"] == serde_json::json!(requested.halt_id)
            });
    }
    let unobserving_runtimes: Vec<_> = runtimes
        .iter()
        .filter(|runtime| {
            runtime.live && runtime.observation_status != RuntimeObservationStatus::Observing
        })
        .cloned()
        .collect();
    let state = aggregate_state(&requested, &runtimes, &unobserving_runtimes);
    Ok(RuntimeControlStatus {
        requested,
        state,
        observed_at: snapshot.observed_at,
        runtimes,
        unobserving_runtimes,
    })
}

fn observation_status(runtime: &RuntimeControlRuntime) -> RuntimeObservationStatus {
    let Some(observation) = &runtime.observation else {
        return RuntimeObservationStatus::Missing;
    };
    if observation["supported"].as_bool() != Some(true) {
        return RuntimeObservationStatus::Unsupported;
    }
    if observation["protocol_version"].as_u64() != Some(OBSERVATION_PROTOCOL_VERSION)
        || observation["scope"].as_str() != Some(PRUNE_SCOPE)
    {
        return RuntimeObservationStatus::IncompatibleProtocol;
    }
    if !runtime.report_recent {
        return RuntimeObservationStatus::StaleReport;
    }
    if observation["fresh"].as_bool() != Some(true) {
        return RuntimeObservationStatus::StaleControl;
    }
    RuntimeObservationStatus::Observing
}

fn aggregate_state(
    requested: &RuntimeControl,
    runtimes: &[RuntimeControlRuntime],
    unobserving_runtimes: &[RuntimeControlRuntime],
) -> RuntimeControlState {
    if requested.halt_id.is_none() {
        return if unobserving_runtimes.is_empty() {
            RuntimeControlState::Running
        } else {
            RuntimeControlState::Degraded
        };
    }
    let live: Vec<_> = runtimes.iter().filter(|runtime| runtime.live).collect();
    if live.is_empty() || !unobserving_runtimes.is_empty() {
        return RuntimeControlState::Unconfirmed;
    }
    if live.iter().any(|runtime| !runtime.observes_current_request) {
        return RuntimeControlState::Pending;
    }
    let mut draining = false;
    for runtime in live {
        let observation = runtime.observation.as_ref().expect("observing runtime");
        if observation["settlement_unknown"].as_bool() != Some(false)
            || observation["admission_allowed"].as_bool() != Some(false)
        {
            return RuntimeControlState::Unknown;
        }
        match (
            observation["state"].as_str(),
            observation["outstanding_callbacks"].as_u64(),
        ) {
            (Some("halted"), Some(0)) => {}
            (Some("draining"), Some(outstanding)) if outstanding > 0 => draining = true,
            _ => return RuntimeControlState::Unknown,
        }
    }
    if draining {
        RuntimeControlState::Draining
    } else {
        RuntimeControlState::Halted
    }
}
