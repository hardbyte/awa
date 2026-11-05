//! Run configuration, read from the environment.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Persona {
    Mia,
    Tom,
    Priya,
    Sam,
    Alex,
    Chaos,
}

impl Persona {
    pub const ALL: [Persona; 6] = [
        Persona::Mia,
        Persona::Tom,
        Persona::Priya,
        Persona::Sam,
        Persona::Alex,
        Persona::Chaos,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Persona::Mia => "mia",
            Persona::Tom => "tom",
            Persona::Priya => "priya",
            Persona::Sam => "sam",
            Persona::Alex => "alex",
            Persona::Chaos => "chaos",
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
                format!("unknown persona {value:?}; expected one of mia,tom,priya,sam,alex,chaos")
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
    /// Extra tolerance added to the stall and promotion thresholds while chaos is active.
    pub chaos_slack: Duration,
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
            promotion_overdue: Duration::from_secs(
                env_parse("AWA_SOAK_PROMOTION_OVERDUE_SECONDS").unwrap_or(10),
            ),
            stall_window: Duration::from_secs(env_parse("AWA_SOAK_STALL_SECONDS").unwrap_or(30)),
            chaos_slack: Duration::from_secs(
                env_parse("AWA_SOAK_CHAOS_SLACK_SECONDS").unwrap_or(45),
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
