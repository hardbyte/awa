//! `awa health` — cluster-level readiness probe over the database (#368).
//!
//! For environments without HTTP probing (cron drivers, systemd
//! `ExecStartPre`, CI smoke checks): answers "can a worker built from this
//! binary do useful work against this database, and is the fleet
//! heartbeating?" from the schema alone. The per-process equivalents are the
//! worker's `/healthz` / `/readyz` endpoints (`AWA_HEALTH_ADDR`).
//!
//! Exit codes: 0 = ready, 1 = not ready (unreachable database, missing or
//! stale schema). Fleet numbers are informational — an idle cluster with no
//! workers is still "ready" for producers and migrations.

use serde::Serialize;
use sqlx::PgPool;

/// The `--json` output. Field names are a documented CLI surface.
#[derive(Debug, Serialize)]
pub struct HealthReport {
    pub ready: bool,
    pub postgres_connected: bool,
    pub schema_version: Option<i32>,
    pub expected_schema_version: i32,
    pub schema_compatible: bool,
    pub storage_state: Option<String>,
    pub active_engine: Option<String>,
    /// Runtimes whose heartbeat was seen inside the 90s liveness window.
    pub live_runtimes: i64,
    /// Live runtimes currently reporting `healthy = false`.
    pub unhealthy_runtimes: i64,
}

/// The report shape for a database that could not be reached at all.
pub fn unreachable_report() -> HealthReport {
    HealthReport {
        ready: false,
        postgres_connected: false,
        schema_version: None,
        expected_schema_version: awa_model::migrations::CURRENT_VERSION,
        schema_compatible: false,
        storage_state: None,
        active_engine: None,
        live_runtimes: 0,
        unhealthy_runtimes: 0,
    }
}

pub async fn probe(pool: &PgPool) -> HealthReport {
    let postgres_connected = sqlx::query("SELECT 1").execute(pool).await.is_ok();

    let schema_version = if postgres_connected {
        awa_model::migrations::current_version(pool).await.ok()
    } else {
        None
    };
    let expected_schema_version = awa_model::migrations::CURRENT_VERSION;
    // A schema newer than the binary is the supported rolling-deploy skew;
    // an older schema means `awa migrate` has not run.
    let schema_compatible =
        schema_version.is_some_and(|version| version >= expected_schema_version);

    let (storage_state, active_engine) = if schema_compatible {
        match awa_model::storage::status(pool).await {
            Ok(status) => (Some(status.state), Some(status.active_engine)),
            Err(_) => (None, None),
        }
    } else {
        (None, None)
    };

    let (live_runtimes, unhealthy_runtimes) = if schema_compatible {
        sqlx::query_as::<_, (i64, i64)>(
            r#"
            SELECT
                count(*)::bigint,
                count(*) FILTER (WHERE NOT healthy)::bigint
            FROM awa.runtime_instances
            WHERE last_seen_at + make_interval(secs => 90) >= now()
            "#,
        )
        .fetch_one(pool)
        .await
        .unwrap_or((0, 0))
    } else {
        (0, 0)
    };

    HealthReport {
        ready: postgres_connected && schema_compatible,
        postgres_connected,
        schema_version,
        expected_schema_version,
        schema_compatible,
        storage_state,
        active_engine,
        live_runtimes,
        unhealthy_runtimes,
    }
}

pub fn render_human(report: &HealthReport) -> String {
    let mut lines = Vec::new();
    lines.push(format!(
        "ready:              {}",
        if report.ready { "yes" } else { "NO" }
    ));
    lines.push(format!(
        "postgres:           {}",
        if report.postgres_connected {
            "connected"
        } else {
            "UNREACHABLE"
        }
    ));
    lines.push(format!(
        "schema version:     {} (binary expects >= {}){}",
        report
            .schema_version
            .map_or_else(|| "none".to_string(), |v| v.to_string()),
        report.expected_schema_version,
        if report.schema_compatible {
            ""
        } else {
            " — run `awa migrate`"
        }
    ));
    if let (Some(state), Some(engine)) = (&report.storage_state, &report.active_engine) {
        lines.push(format!("storage:            {state} ({engine})"));
    }
    lines.push(format!(
        "live runtimes:      {} ({} unhealthy)",
        report.live_runtimes, report.unhealthy_runtimes
    ));
    lines.join("\n")
}
