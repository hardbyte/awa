//! Database lifecycle, connection budgeting and the per-client runtime host.

use super::config::{replace_database_name, Persona, SoakConfig};
use super::roles;
use awa::Client;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Opens a pool whose connections carry `application_name`, so the oracle and the
/// chaos persona can tell the runtimes apart in `pg_stat_activity`.
pub async fn connect_pool(
    url: &str,
    application_name: &str,
    max_connections: u32,
) -> Result<PgPool, sqlx::Error> {
    connect_pool_with_timeout(url, application_name, max_connections, None).await
}

/// As [`connect_pool`], with a server-side `statement_timeout` on every connection.
pub async fn connect_pool_with_timeout(
    url: &str,
    application_name: &str,
    max_connections: u32,
    statement_timeout: Option<Duration>,
) -> Result<PgPool, sqlx::Error> {
    let mut options = PgConnectOptions::from_str(url)?.application_name(application_name);
    if let Some(timeout) = statement_timeout {
        options = options.options([("statement_timeout", timeout.as_millis().to_string())]);
    }
    PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(60))
        .connect_with(options)
        .await
}

/// Drops and recreates the soak database, applies migrations and activates queue
/// storage, the production engine.
pub async fn recreate_database(config: &SoakConfig) -> Result<(), String> {
    let name = config.database_name();
    assert!(name.starts_with("awa_soak"), "database name guard");
    let admin = connect_pool(&config.admin_url(), "awa_soak_admin", 2)
        .await
        .map_err(|error| format!("connect to maintenance database: {error}"))?;
    sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
        .execute(&admin)
        .await
        .map_err(|error| format!("drop database: {error}"))?;
    let restricted_roles = config.has(Persona::ShardOps);
    if restricted_roles {
        roles::create_roles(&admin).await?;
        sqlx::query(&format!("CREATE DATABASE {name} OWNER {}", roles::OWNER))
            .execute(&admin)
            .await
            .map_err(|error| format!("create database: {error}"))?;
    } else {
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&admin)
            .await
            .map_err(|error| format!("create database: {error}"))?;
    }
    admin.close().await;

    // With the shard-ops persona the schema belongs to a non-superuser owner role, as in
    // a production install, and the application roles get the documented grants.
    let setup_url = if restricted_roles {
        roles::url_for_role(config, roles::OWNER)
    } else {
        config.database_url.clone()
    };
    let pool = connect_pool(&setup_url, "awa_soak_setup", 2)
        .await
        .map_err(|error| format!("connect to soak database: {error}"))?;
    awa::model::migrations::run(&pool)
        .await
        .map_err(|error| format!("migrate: {error}"))?;
    awa_testing::setup::activate_queue_storage(&pool).await;
    pool.close().await;
    if restricted_roles {
        let superuser = connect_pool(&config.database_url, "awa_soak_setup_grants", 1)
            .await
            .map_err(|error| format!("connect to soak database: {error}"))?;
        let mixed_personas = config
            .personas
            .iter()
            .any(|persona| *persona != Persona::ShardOps);
        roles::grant_runtime(
            &superuser,
            &name,
            config.shardops_schema_create,
            mixed_personas,
        )
        .await?;
        superuser.close().await;
    }
    Ok(())
}

#[derive(Debug, Clone, Default)]
pub struct ServerInfo {
    pub version: String,
    pub max_connections: u32,
    pub superuser_reserved: u32,
    pub connections_at_start: i64,
    pub postmaster_start: String,
    pub engine: String,
    pub queue_storage_schema: Option<String>,
    pub awa_version: String,
}

pub async fn server_info(pool: &PgPool) -> Result<ServerInfo, sqlx::Error> {
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(pool)
        .await?;
    let max_connections: String = sqlx::query_scalar("SHOW max_connections")
        .fetch_one(pool)
        .await?;
    let reserved: String = sqlx::query_scalar("SHOW superuser_reserved_connections")
        .fetch_one(pool)
        .await?;
    let connections: i64 = sqlx::query_scalar(
        "SELECT count(*)::bigint FROM pg_stat_activity WHERE datname IS NOT NULL",
    )
    .fetch_one(pool)
    .await?;
    let postmaster_start: String = sqlx::query_scalar("SELECT pg_postmaster_start_time()::text")
        .fetch_one(pool)
        .await?;
    let schema: Option<String> = sqlx::query_scalar("SELECT awa.active_queue_storage_schema()")
        .fetch_one(pool)
        .await?;
    Ok(ServerInfo {
        version,
        max_connections: max_connections.parse().unwrap_or(100),
        superuser_reserved: reserved.parse().unwrap_or(3),
        connections_at_start: connections,
        postmaster_start,
        engine: if schema.is_some() {
            "queue_storage"
        } else {
            "canonical"
        }
        .to_string(),
        queue_storage_schema: schema,
        awa_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Hands out connections to the personas so that all of them together stay inside
/// the server's `max_connections`.
pub struct Budget {
    usable: u32,
    allocations: Mutex<BTreeMap<String, u32>>,
}

impl Budget {
    /// `headroom` connections are kept free for the oracle, operator probes and chaos
    /// sessions on top of the server's superuser reservation.
    pub fn new(info: &ServerInfo, headroom: u32) -> Self {
        let usable = info
            .max_connections
            .saturating_sub(info.superuser_reserved)
            .saturating_sub(headroom)
            .saturating_sub(info.connections_at_start.max(0) as u32);
        Self {
            usable,
            allocations: Mutex::new(BTreeMap::new()),
        }
    }

    /// Reserves up to `want` connections under `label`, never fewer than `min`.
    /// Returns the amount granted; panics when even `min` does not fit, because the
    /// run would exhaust the server before it tested anything.
    pub fn reserve(&self, label: &str, want: u32, min: u32) -> u32 {
        let mut allocations = self.allocations.lock().expect("budget lock");
        let used: u32 = allocations.values().sum();
        let available = self.usable.saturating_sub(used);
        assert!(
            available >= min,
            "connection budget exhausted: {label} needs at least {min}, {available} of {} left; \
             raise max_connections or lower AWA_SOAK_SCALE / persona count. Allocations: {allocations:?}",
            self.usable
        );
        let granted = want.min(available).max(min);
        allocations.insert(label.to_string(), granted);
        granted
    }

    pub fn release(&self, label: &str) {
        self.allocations.lock().expect("budget lock").remove(label);
    }

    pub fn usable(&self) -> u32 {
        self.usable
    }

    pub fn table(&self) -> Vec<(String, u32)> {
        self.allocations
            .lock()
            .expect("budget lock")
            .iter()
            .map(|(label, count)| (label.clone(), *count))
            .collect()
    }
}

/// A function that builds one runtime's client over its own pool.
pub type ClientFactory = Arc<dyn Fn(PgPool) -> Client + Send + Sync>;

/// One awa runtime living on its own tokio runtime, so it can be stopped gracefully
/// or killed the way a crashed process dies: tasks vanish mid-job without any
/// shutdown handshake and the connections drop.
pub struct ClientHost {
    pub label: String,
    runtime: tokio::runtime::Runtime,
    client: Arc<Client>,
    pool: PgPool,
}

impl ClientHost {
    pub async fn spawn(
        url: String,
        label: String,
        pool_size: u32,
        worker_threads: usize,
        factory: ClientFactory,
    ) -> Result<Self, String> {
        let thread_label = label.clone();
        tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(worker_threads)
                .thread_name(format!("soak-{thread_label}"))
                .enable_all()
                .build()
                .map_err(|error| format!("build runtime: {error}"))?;
            let (pool, client) = runtime.block_on(async {
                let pool = connect_pool(&url, &format!("soak_{thread_label}"), pool_size)
                    .await
                    .map_err(|error| format!("connect pool: {error}"))?;
                let client = factory(pool.clone());
                client
                    .start()
                    .await
                    .map_err(|error| format!("start client: {error}"))?;
                Ok::<_, String>((pool, client))
            })?;
            Ok(Self {
                label: thread_label,
                runtime,
                client: Arc::new(client),
                pool,
            })
        })
        .await
        .map_err(|error| format!("spawn host task: {error}"))?
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Graceful stop: stop claiming, drain in-flight jobs, then close the pool.
    pub async fn shutdown(self, timeout: Duration) {
        let client = self.client.clone();
        let pool = self.pool.clone();
        let _ = self
            .runtime
            .handle()
            .spawn(async move {
                client.shutdown(timeout).await;
                pool.close().await;
            })
            .await;
        self.runtime.shutdown_background();
    }

    /// Abrupt stop: no shutdown handshake, as when a process is killed.
    pub fn kill(self) {
        self.runtime.shutdown_background();
    }
}

pub fn url_for_database(config: &SoakConfig, name: &str) -> String {
    replace_database_name(&config.database_url, name)
}
