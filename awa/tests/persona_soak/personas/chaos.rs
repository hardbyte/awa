//! Chaos: kills a runtime without a shutdown handshake, terminates the maintenance
//! leader's connection, terminates random runtime connections, exhausts a runtime's
//! pool and holds a long idle-in-transaction session.

use super::super::config::Persona;
use super::super::db::connect_pool;
use super::super::rng::Rng;
use super::super::shared::Shared;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The advisory lock the maintenance leader holds for as long as it leads.
const MAINTENANCE_LEADER_LOCK_KEY: i64 = 0x4157_415f_4d41_494e;
const RUNTIME_FLEETS: [&str; 5] = ["mia", "tom", "priya", "sam", "sam-stripe"];

async fn kill_runtime(shared: &Arc<Shared>, rng: &mut Rng) {
    let Some(&name) = rng.pick(&RUNTIME_FLEETS) else {
        return;
    };
    let Some(fleet) = shared.fleet(name) else {
        return;
    };
    let live = fleet.live_replicas().await;
    // Never take the last runtime of a fleet down; the point is a survivor that rescues.
    if live.len() < 2 {
        return;
    }
    let Some(&index) = rng.pick(&live) else {
        return;
    };
    shared.note(
        "chaos",
        format!("killing {name} replica {index} without shutdown"),
    );
    shared.chaos_window(Duration::from_secs(45));
    match fleet.kill(index, Some(Duration::from_secs(4))).await {
        Ok(_) => shared.stats(Persona::Chaos).add("runtime_kills", 1),
        Err(error) => shared.violation(
            "restart_failed",
            format!("restarting killed {name} replica {index}: {error}"),
            vec![],
        ),
    }
}

async fn kill_python(shared: &Arc<Shared>) {
    let handle = shared.python.lock().expect("python handle").clone();
    let Some(handle) = handle else { return };
    shared.note("chaos", "killing the Python process without shutdown");
    shared.chaos_window(Duration::from_secs(45));
    if let Err(error) = handle
        .kill_and_restart(shared, Duration::from_secs(4))
        .await
    {
        shared.violation(
            "restart_failed",
            format!("restarting the Python process: {error}"),
            vec![],
        );
    }
}

async fn terminate_leader(shared: &Arc<Shared>, pool: &PgPool) {
    // Maintenance leadership is an advisory lock held by one runtime connection.
    let holders: Result<Vec<(i32, String)>, _> = sqlx::query_as(
        "SELECT l.pid, COALESCE(a.application_name, '')
         FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
         WHERE l.locktype = 'advisory' AND l.granted AND a.application_name LIKE 'soak\\_%'
           AND ((l.classid::bigint << 32) | l.objid::bigint) = $1
         ORDER BY l.pid",
    )
    .bind(MAINTENANCE_LEADER_LOCK_KEY)
    .fetch_all(pool)
    .await;
    let Ok(holders) = holders else { return };
    let Some((pid, application)) = holders.first().cloned() else {
        shared.note("chaos", "no advisory-lock holder found to terminate");
        return;
    };
    shared.note(
        "chaos",
        format!("terminating maintenance leader backend {pid} ({application})"),
    );
    shared.chaos_window(Duration::from_secs(40));
    let _ = sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(pid)
        .fetch_one(pool)
        .await;
    shared.stats(Persona::Chaos).add("leader_terminations", 1);
}

async fn terminate_runtime_connections(shared: &Arc<Shared>, pool: &PgPool, rng: &mut Rng) {
    let count = rng.range(3, 12) as i64;
    let terminated: Result<Vec<(i32,)>, _> = sqlx::query_as(
        "SELECT pid FROM (
             SELECT pid FROM pg_stat_activity
             WHERE application_name LIKE 'soak\\_%' AND application_name NOT LIKE '%producer%'
               AND application_name NOT LIKE '%operator%' AND application_name NOT LIKE '%oracle%'
               AND application_name NOT LIKE '%chaos%' AND backend_type = 'client backend'
               AND pid <> pg_backend_pid()
             ORDER BY random() LIMIT $1
         ) victims WHERE pg_terminate_backend(pid)",
    )
    .bind(count)
    .fetch_all(pool)
    .await;
    let killed = terminated.map(|rows| rows.len()).unwrap_or(0);
    shared.note(
        "chaos",
        format!("pg_terminate_backend on {killed} runtime connection(s)"),
    );
    shared.chaos_window(Duration::from_secs(30));
    shared
        .stats(Persona::Chaos)
        .add("connections_terminated", killed as i64);
}

/// Takes every connection of one runtime's pool for `hold`, starving its heartbeats
/// and claims the way a connection-pool exhaustion bug would.
async fn exhaust_pool(shared: &Arc<Shared>, rng: &mut Rng, hold: Duration) {
    let Some(&name) = rng.pick(&RUNTIME_FLEETS) else {
        return;
    };
    let Some(fleet) = shared.fleet(name) else {
        return;
    };
    let live = fleet.live_replicas().await;
    let Some(&index) = rng.pick(&live) else {
        return;
    };
    let Some(pool) = fleet.pool_of(index).await else {
        return;
    };
    let size = pool.options().get_max_connections() as usize;
    shared.note(
        "chaos",
        format!(
            "exhausting the {size}-connection pool of {name} replica {index} for {}s",
            hold.as_secs()
        ),
    );
    shared.chaos_window(hold + Duration::from_secs(40));
    let mut held = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while held.len() < size && Instant::now() < deadline {
        if let Ok(Ok(connection)) =
            tokio::time::timeout(Duration::from_secs(2), pool.acquire()).await
        {
            held.push(connection);
        }
    }
    shared.stats(Persona::Chaos).add("pool_exhaustions", 1);
    shared
        .stats(Persona::Chaos)
        .add("pool_connections_held", held.len() as i64);
    shared.load_stop.sleep(hold).await;
    drop(held);
}

/// One session that begins a transaction, takes a snapshot and sits idle, pinning the
/// xmin horizon so vacuum and prune cannot reclaim anything newer.
async fn idle_in_transaction(shared: Arc<Shared>, hold: Duration) {
    let Ok(pool) = connect_pool(&shared.config.database_url, "soak_chaos_idle", 1).await else {
        return;
    };
    let Ok(mut transaction) = pool.begin().await else {
        return;
    };
    if sqlx::query("SELECT txid_current()")
        .execute(&mut *transaction)
        .await
        .is_err()
    {
        return;
    }
    shared.note(
        "chaos",
        format!(
            "holding an idle-in-transaction session for {}s",
            hold.as_secs()
        ),
    );
    shared
        .stats(Persona::Chaos)
        .add("idle_in_transaction_holds", 1);
    shared.load_stop.sleep(hold).await;
    let _ = transaction.rollback().await;
    pool.close().await;
}

pub async fn run(shared: Arc<Shared>) -> Result<(), String> {
    let pool = connect_pool(
        &shared.config.database_url,
        "soak_chaos_control",
        shared.budget.reserve("chaos", 3, 2),
    )
    .await
    .map_err(|error| error.to_string())?;
    let mut rng = Rng::for_task(shared.config.seed, "chaos");
    let load = shared.config.load.as_secs_f64();
    let pace = (load / 900.0).clamp(0.15, 1.0);
    let second = |seconds: f64| Duration::from_secs_f64(seconds * pace);
    let mut schedule: Vec<(&str, Duration, Instant)> = vec![
        ("kill", second(120.0), Instant::now() + second(60.0)),
        ("python", second(200.0), Instant::now() + second(110.0)),
        ("leader", second(150.0), Instant::now() + second(90.0)),
        ("terminate", second(75.0), Instant::now() + second(45.0)),
        ("pool", second(240.0), Instant::now() + second(150.0)),
        ("idle", second(300.0), Instant::now() + second(30.0)),
    ];
    shared.note("chaos", "armed: runtime kill, leader termination, connection termination, pool exhaustion, idle transaction");
    while !shared.load_stop.is_stopped() {
        let now = Instant::now();
        for (name, interval, due) in schedule.iter_mut() {
            if *due > now || shared.load_stop.is_stopped() {
                continue;
            }
            *due = Instant::now() + *interval;
            match *name {
                "kill" => kill_runtime(&shared, &mut rng).await,
                "python" => kill_python(&shared).await,
                "leader" => terminate_leader(&shared, &pool).await,
                "terminate" => terminate_runtime_connections(&shared, &pool, &mut rng).await,
                "pool" => {
                    exhaust_pool(&shared, &mut rng, Duration::from_secs(20).max(second(20.0))).await
                }
                "idle" => {
                    tokio::spawn(idle_in_transaction(
                        shared.clone(),
                        second(90.0).max(Duration::from_secs(30)),
                    ));
                }
                _ => {}
            }
        }
        shared.load_stop.sleep(Duration::from_millis(500)).await;
    }
    pool.close().await;
    Ok(())
}
