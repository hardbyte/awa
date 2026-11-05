//! Orchestration: set up the database, start the personas and the oracle, run the
//! load, let the system quiesce, reconcile, and report.

use super::adminserver::AdminServer;
use super::config::{Persona, SoakConfig};
use super::db::{self, Budget};
use super::oracle::Oracle;
use super::personas;
use super::pgserver::PgWatch;
use super::report::{self, Outcome};
use super::shared::Shared;
use super::stop::Stop;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Connections kept free of persona pools: oracle, chaos sessions, admin probes.
const HEADROOM: u32 = 14;

pub async fn run_soak(config: SoakConfig) -> Result<Arc<Shared>, String> {
    eprintln!(
        "{}: preparing database {}",
        report::HEADER,
        config.database_name()
    );
    db::recreate_database(&config).await?;
    let oracle_pool = db::connect_pool(&config.database_url, "soak_oracle", 4)
        .await
        .map_err(|error| format!("oracle connect: {error}"))?;
    let info = db::server_info(&oracle_pool)
        .await
        .map_err(|error| format!("server info: {error}"))?;
    let watch = Arc::new(
        PgWatch::begin(&config, &oracle_pool)
            .await
            .map_err(|error| format!("pg watch: {error}"))?,
    );
    let budget = Budget::new(&info, HEADROOM);
    eprintln!(
        "{}: engine={} server={} max_connections={} connections_now={} usable={} seed={} personas={:?} load={}s scale={}",
        report::HEADER,
        info.engine,
        info.version,
        info.max_connections,
        info.connections_at_start,
        budget.usable(),
        config.seed,
        config.personas.iter().map(|persona| persona.name()).collect::<Vec<_>>(),
        config.load.as_secs(),
        config.scale
    );
    if info.engine != "queue_storage" {
        return Err(format!(
            "expected the queue_storage engine, found {}",
            info.engine
        ));
    }
    let shared = Shared::new(config, info, budget);

    // The UI personas talk to the real admin server, run as a child process.
    let ui_personas = [
        Persona::Watchers,
        Persona::Support,
        Persona::UiOps,
        Persona::Impatient,
    ];
    let admin_server = if ui_personas
        .iter()
        .any(|persona| shared.config.has(*persona))
    {
        std::fs::create_dir_all(&shared.config.report_dir).map_err(|error| error.to_string())?;
        let server = AdminServer::start(&shared.config).await?;
        shared.ui.set_base_url(server.base_url.clone());
        shared.note(
            "run",
            format!(
                "admin server at {} (statement timeout {}s, client timeout {}s)",
                server.base_url,
                server.statement_timeout.as_secs(),
                shared.config.ui_client_timeout.as_secs()
            ),
        );
        Some(server)
    } else {
        None
    };

    // Personas start their runtimes and then produce until the load stops. Chaos, the
    // operator and the UI users reach other personas' fleets and queues, so they start
    // once the rest are up.
    let mut tasks: Vec<(Persona, tokio::task::JoinHandle<Result<(), String>>)> = Vec::new();
    for persona in [
        Persona::Mia,
        Persona::Tom,
        Persona::Priya,
        Persona::Sam,
        Persona::Python,
    ] {
        if shared.config.has(persona) {
            tasks.push((persona, spawn_persona(persona, shared.clone())));
        }
    }
    tokio::time::sleep(Duration::from_secs(8)).await;
    for persona in [
        Persona::Alex,
        Persona::Chaos,
        Persona::Watchers,
        Persona::Support,
        Persona::UiOps,
        Persona::Impatient,
    ] {
        if shared.config.has(persona) {
            tasks.push((persona, spawn_persona(persona, shared.clone())));
        }
    }

    let oracle_stop = Stop::new();
    let oracle_task = {
        let oracle = Oracle::new(shared.clone(), oracle_pool.clone(), watch.clone());
        let stop = oracle_stop.clone();
        tokio::spawn(async move { oracle.run(stop).await })
    };
    shared.note(
        "run",
        format!("load phase started: {}s", shared.config.load.as_secs()),
    );
    shared.started_load(Instant::now());

    let load_deadline = Instant::now() + shared.config.load;
    let mut next_progress = Instant::now() + Duration::from_secs(30);
    while Instant::now() < load_deadline && !shared.load_stop.is_stopped() {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some((persona, _)) = tasks.iter().find(|(_, task)| task.is_finished()) {
            // A persona only returns early on a setup failure or a panic.
            let persona = *persona;
            let index = tasks
                .iter()
                .position(|(p, _)| *p == persona)
                .expect("finished task");
            let (_, task) = tasks.remove(index);
            match task.await {
                Ok(Ok(())) => shared.note("run", format!("{} finished early", persona.name())),
                Ok(Err(error)) => shared.violation(
                    "persona_failed",
                    format!("{} stopped: {error}", persona.name()),
                    vec![],
                ),
                Err(error) => shared.violation(
                    "persona_panicked",
                    format!("{} panicked: {error}", persona.name()),
                    vec![],
                ),
            }
        }
        if Instant::now() >= next_progress {
            next_progress = Instant::now() + Duration::from_secs(30);
            progress(&shared);
        }
    }

    shared.note("run", "load phase over; letting the system quiesce");
    shared.load_stop.trigger();
    oracle_stop.trigger();
    let mut oracle = oracle_task
        .await
        .map_err(|error| format!("oracle task: {error}"))?;
    let quiesced = oracle.wait_quiescent(shared.config.drain_grace).await;
    shared
        .quiesced
        .store(quiesced, std::sync::atomic::Ordering::Relaxed);
    shared.note("run", format!("quiesced: {quiesced}"));
    oracle.final_check(quiesced).await;
    let excerpts = oracle.crash_scan();
    for excerpt in &excerpts {
        shared.violation(
            "server_crash_log",
            "the Postgres log contains crash lines",
            vec![excerpt.clone()],
        );
    }
    let log_tail = watch.log_since_start(&shared.config);

    shared.drain_stop.trigger();
    for (persona, task) in tasks {
        match tokio::time::timeout(Duration::from_secs(180), task).await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => shared.violation(
                "persona_failed",
                format!("{} stopped: {error}", persona.name()),
                vec![],
            ),
            Ok(Err(error)) => shared.violation(
                "persona_panicked",
                format!("{} panicked: {error}", persona.name()),
                vec![],
            ),
            Err(_) => shared.violation(
                "shutdown_hang",
                format!("{} did not shut down within 180s", persona.name()),
                vec![],
            ),
        }
    }

    personas::ui::analyse(&shared);
    if let Some(mut server) = admin_server {
        if !server.is_running() {
            shared.violation(
                "admin_server_died",
                "the awa serve process exited during the run; see awa-serve.log",
                vec![],
            );
        }
        server.stop();
    }
    let outcome = Outcome {
        quiesced,
        elapsed_seconds: shared.started.elapsed().as_secs_f64(),
        crash_excerpts: excerpts,
        log_tail,
    };
    if let Err(error) = report::write(&shared, &outcome) {
        eprintln!("could not write the report: {error}");
    }
    oracle_pool.close().await;
    Ok(shared)
}

fn spawn_persona(
    persona: Persona,
    shared: Arc<Shared>,
) -> tokio::task::JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        match persona {
            Persona::Mia => personas::mia::run(shared).await,
            Persona::Tom => personas::tom::run(shared).await,
            Persona::Priya => personas::priya::run(shared).await,
            Persona::Sam => personas::sam::run(shared).await,
            Persona::Alex => personas::alex::run(shared).await,
            Persona::Chaos => personas::chaos::run(shared).await,
            Persona::Watchers => personas::ui::run_watchers(shared).await,
            Persona::Support => personas::ui::run_support(shared).await,
            Persona::UiOps => personas::ui::run_ui_operator(shared).await,
            Persona::Impatient => personas::ui::run_impatient(shared).await,
            Persona::Python => personas::python::run(shared).await,
        }
    })
}

fn progress(shared: &Arc<Shared>) {
    let mut line = format!("t={:.0}s", shared.now_ms() as f64 / 1000.0);
    for persona in Persona::ALL {
        if !shared.config.has(persona) {
            continue;
        }
        let counters = shared.stats(persona).counters();
        let enqueued: i64 = counters
            .iter()
            .filter(|(name, _)| name.starts_with("enqueued"))
            .map(|(_, value)| *value)
            .sum();
        line.push_str(&format!(
            " {}[enq={} exec={}]",
            persona.name(),
            enqueued,
            counters.get("executions").copied().unwrap_or(0)
        ));
    }
    let violations = shared.violations.lock().expect("violations").len();
    line.push_str(&format!(" violations={violations}"));
    shared.note("run", line);
}
