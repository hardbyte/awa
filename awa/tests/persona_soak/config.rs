//! Run configuration, read from the environment.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

/// The harness's fast heartbeat cadence, so that rescues happen many times in a short run.
pub const DEFAULT_HEARTBEAT_SECONDS: u64 = 2;
pub const DEFAULT_STALENESS_SECONDS: u64 = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Persona {
    Mia,
    Tom,
    Priya,
    Sam,
    Alex,
    Chaos,
    /// Admin UI users, through the real HTTP server.
    Watchers,
    Support,
    UiOps,
    Impatient,
    /// A separate Python process: producers, async workers and the migrate path.
    Python,
    /// Handlers that batch-insert unique child jobs into their own queue.
    Fanout,
    /// An operator changing `enqueue_shards` on hot queues under live load, with the
    /// actors running as different non-superuser roles.
    ShardOps,
    /// A separate process that claims as much as it can and is SIGKILLed again and again.
    ClaimAbandon,
}

impl Persona {
    pub const ALL: [Persona; 14] = [
        Persona::Mia,
        Persona::Tom,
        Persona::Priya,
        Persona::Sam,
        Persona::Alex,
        Persona::Chaos,
        Persona::Watchers,
        Persona::Support,
        Persona::UiOps,
        Persona::Impatient,
        Persona::Python,
        Persona::Fanout,
        Persona::ShardOps,
        Persona::ClaimAbandon,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Persona::Mia => "mia",
            Persona::Tom => "tom",
            Persona::Priya => "priya",
            Persona::Sam => "sam",
            Persona::Alex => "alex",
            Persona::Chaos => "chaos",
            Persona::Watchers => "watchers",
            Persona::Support => "support",
            Persona::UiOps => "uiops",
            Persona::Impatient => "impatient",
            Persona::Python => "python",
            Persona::Fanout => "fanout",
            Persona::ShardOps => "shardops",
            Persona::ClaimAbandon => "abandon",
        }
    }

    pub fn index(self) -> u8 {
        self as u8
    }

    pub fn from_index(index: u8) -> Persona {
        Persona::ALL[(index as usize).min(Persona::ALL.len() - 1)]
    }
}

impl FromStr for Persona {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Persona::ALL
            .into_iter()
            .find(|persona| persona.name() == value.trim().to_ascii_lowercase())
            .ok_or_else(|| {
                format!("unknown persona {value:?}; expected one of mia,tom,priya,sam,alex,chaos,watchers,support,uiops,impatient,python,fanout,shardops,abandon")
            })
    }
}

#[derive(Debug, Clone)]
pub struct SoakConfig {
    /// URL of the disposable soak database. Its database name must start with `awa_soak`.
    pub database_url: String,
    pub personas: Vec<Persona>,
    /// Length of the load phase.
    pub load: Duration,
    /// How long the oracle waits for the system to quiesce after load stops.
    pub drain_grace: Duration,
    pub seed: u64,
    /// Multiplies each persona's rates and queue counts.
    pub scale: f64,
    pub report_dir: PathBuf,
    /// Postgres server log file readable from this host.
    pub pg_log_file: Option<PathBuf>,
    /// Docker container running the server, used to read its log.
    pub pg_container: Option<String>,
    /// Log file path inside `pg_container`; when absent `docker logs` is used.
    pub pg_container_log: Option<String>,
    /// Seconds a scheduled job may be overdue before the promotion check fails.
    pub promotion_overdue: Duration,
    /// Seconds a queue may hold available jobs with idle workers before the stall check fails.
    pub stall_window: Duration,
    /// The `awa` binary that serves the admin UI; located next to the test binary when unset.
    pub awa_bin: Option<PathBuf>,
    /// Python interpreter of a virtualenv with the `awa` extension installed.
    pub python_bin: Option<PathBuf>,
    /// Server-side statement timeout of the admin server, and the matching client timeout.
    pub ui_statement_timeout: Duration,
    pub ui_client_timeout: Duration,
    /// Connections of the admin server's pool, and seconds between the operator's admin reads.
    pub ui_pool_max: u32,
    pub admin_read_every: Duration,
    /// The load phase stops early when the report directory's filesystem has less free space.
    pub min_free_disk_gb: i64,
    /// Extra tolerance added to the stall and promotion thresholds while chaos is active.
    pub chaos_slack: Duration,
    /// Multiplies the shard-ops persona's rates independently of `scale`, which is
    /// usually lowered to keep the other personas light.
    pub shardops_scale: f64,
    /// Whether the application roles may create objects in the schema. Without it the
    /// operator provisions every lane before the shard count is raised, as the
    /// restricted-runtime procedure in `docs/security.md` requires.
    pub shardops_schema_create: bool,
    /// Claim-and-abandon persona: claims held by each killed process, seconds between
    /// kills, how many kills, and the bound within which the rescue backlog has to clear
    /// and the claim ring has to resume after a kill. Independent of `scale`.
    pub abandon_claims: u64,
    pub abandon_kill_every: Duration,
    pub abandon_kills: u32,
    pub abandon_bound: Duration,
    /// Pause between a kill and the replacement process.
    pub abandon_restart_delay: Duration,
    /// Overrides the heartbeat and deadline rescue intervals of every runtime, to run the
    /// soak with production's cadence instead of the harness's fast one.
    pub rescue_interval: Option<Duration>,
    /// Heartbeat interval and the heartbeat staleness after which a claim may be rescued,
    /// for every runtime. The library's own defaults are 30 s and 90 s.
    pub heartbeat_interval: Duration,
    pub heartbeat_staleness: Duration,
}

fn env_string(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn env_parse<T: FromStr>(name: &str) -> Option<T> {
    env_string(name).and_then(|value| value.trim().parse().ok())
}

impl SoakConfig {
    pub fn from_env() -> Result<Self, String> {
        let database_url = env_string("AWA_SOAK_DATABASE_URL").ok_or(
            "AWA_SOAK_DATABASE_URL is required (postgres://user:pass@host:port/awa_soak_<name>)",
        )?;
        let database_name = database_name(&database_url);
        if !database_name.starts_with("awa_soak")
            || !database_name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            return Err(format!(
                "refusing to run: the soak drops and recreates its database, so the name must \
                 match awa_soak[A-Za-z0-9_]*, got {database_name:?}"
            ));
        }

        let personas = match env_string("AWA_SOAK_PERSONAS") {
            Some(list) => list
                .split(',')
                .map(str::parse)
                .collect::<Result<Vec<Persona>, _>>()?,
            None => Persona::ALL.to_vec(),
        };
        let seed = env_parse("AWA_SOAK_SEED").unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos() as u64)
                .unwrap_or(1)
        });
        let report_dir = env_string("AWA_SOAK_REPORT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join(format!("awa-soak-{seed}")));

        Ok(Self {
            database_url,
            personas,
            load: Duration::from_secs(env_parse("AWA_SOAK_SECONDS").unwrap_or(900)),
            drain_grace: Duration::from_secs(
                env_parse("AWA_SOAK_DRAIN_GRACE_SECONDS").unwrap_or(120),
            ),
            seed,
            scale: env_parse("AWA_SOAK_SCALE").unwrap_or(1.0),
            report_dir,
            pg_log_file: env_string("AWA_SOAK_PG_LOG").map(PathBuf::from),
            pg_container: env_string("AWA_SOAK_PG_CONTAINER"),
            pg_container_log: env_string("AWA_SOAK_PG_CONTAINER_LOG"),
            awa_bin: env_string("AWA_SOAK_AWA_BIN").map(PathBuf::from),
            python_bin: env_string("AWA_SOAK_PYTHON").map(PathBuf::from),
            ui_statement_timeout: Duration::from_secs(
                env_parse("AWA_SOAK_UI_STATEMENT_TIMEOUT_SECONDS").unwrap_or(30),
            ),
            ui_client_timeout: Duration::from_secs(
                env_parse("AWA_SOAK_UI_CLIENT_TIMEOUT_SECONDS").unwrap_or(45),
            ),
            ui_pool_max: env_parse("AWA_SOAK_UI_POOL_MAX").unwrap_or(10),
            admin_read_every: Duration::from_secs(
                env_parse("AWA_SOAK_ADMIN_READ_SECONDS").unwrap_or(2),
            ),
            min_free_disk_gb: env_parse("AWA_SOAK_MIN_FREE_DISK_GB").unwrap_or(8),
            promotion_overdue: Duration::from_secs(
                env_parse("AWA_SOAK_PROMOTION_OVERDUE_SECONDS").unwrap_or(10),
            ),
            stall_window: Duration::from_secs(env_parse("AWA_SOAK_STALL_SECONDS").unwrap_or(30)),
            chaos_slack: Duration::from_secs(
                env_parse("AWA_SOAK_CHAOS_SLACK_SECONDS").unwrap_or(45),
            ),
            shardops_scale: env_parse("AWA_SOAK_SHARDOPS_SCALE").unwrap_or(1.0),
            shardops_schema_create: env_parse::<u8>("AWA_SOAK_SHARDOPS_SCHEMA_CREATE")
                .map(|value| value != 0)
                .unwrap_or(true),
            abandon_claims: env_parse("AWA_SOAK_ABANDON_CLAIMS").unwrap_or(4_000),
            abandon_kill_every: Duration::from_secs(
                env_parse("AWA_SOAK_ABANDON_KILL_SECONDS").unwrap_or(180),
            ),
            abandon_kills: env_parse("AWA_SOAK_ABANDON_KILLS").unwrap_or(4),
            abandon_bound: Duration::from_secs(
                env_parse("AWA_SOAK_ABANDON_BOUND_SECONDS").unwrap_or(120),
            ),
            abandon_restart_delay: Duration::from_secs(
                env_parse("AWA_SOAK_ABANDON_RESTART_SECONDS").unwrap_or(3),
            ),
            rescue_interval: env_parse::<u64>("AWA_SOAK_RESCUE_INTERVAL_SECONDS")
                .map(Duration::from_secs),
            heartbeat_interval: Duration::from_secs(
                env_parse("AWA_SOAK_HEARTBEAT_SECONDS").unwrap_or(DEFAULT_HEARTBEAT_SECONDS),
            ),
            heartbeat_staleness: Duration::from_secs(
                env_parse("AWA_SOAK_STALENESS_SECONDS").unwrap_or(DEFAULT_STALENESS_SECONDS),
            ),
        })
    }

    pub fn has(&self, persona: Persona) -> bool {
        self.personas.contains(&persona)
    }

    /// Scales a count, never below one.
    pub fn scaled(&self, base: u64) -> u64 {
        ((base as f64 * self.scale).round() as u64).max(1)
    }

    pub fn database_name(&self) -> String {
        database_name(&self.database_url)
    }

    /// The same server, connected to the maintenance database.
    pub fn admin_url(&self) -> String {
        replace_database_name(&self.database_url, "postgres")
    }
}

pub fn database_name(url: &str) -> String {
    let without_query = url.split_once('?').map_or(url, |(prefix, _)| prefix);
    without_query
        .rsplit_once('/')
        .map(|(_, name)| name.to_string())
        .unwrap_or_default()
}

pub fn replace_database_name(url: &str, database_name: &str) -> String {
    let (without_query, query) = match url.split_once('?') {
        Some((prefix, query)) => (prefix, Some(query)),
        None => (url, None),
    };
    let base = without_query
        .rsplit_once('/')
        .map_or(without_query, |(base, _)| base);
    match query {
        Some(query) => format!("{base}/{database_name}?{query}"),
        None => format!("{base}/{database_name}"),
    }
}

/// `url` with its user and password replaced.
pub fn replace_credentials(url: &str, user: &str, password: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_string();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let host = rest[..authority_end]
        .rsplit_once('@')
        .map_or(&rest[..authority_end], |(_, host)| host);
    format!(
        "{scheme}://{user}:{password}@{host}{}",
        &rest[authority_end..]
    )
}
