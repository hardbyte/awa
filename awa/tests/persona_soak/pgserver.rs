//! Server crash detection: postmaster start time, and a scan of the server log for
//! the lines a crashed backend leaves behind.

use super::config::SoakConfig;
use sqlx::PgPool;
use std::process::Command;

const CRASH_PATTERNS: [&str; 6] = [
    "terminated by signal",
    "TRAP:",
    "server process (PID",
    "PANIC:",
    "all server processes terminated",
    "database system was not properly shut down",
];

pub struct PgWatch {
    pub started_at: String,
    log_start_offset: u64,
}

impl PgWatch {
    pub async fn begin(config: &SoakConfig, pool: &PgPool) -> Result<Self, sqlx::Error> {
        let started_at: String = sqlx::query_scalar("SELECT pg_postmaster_start_time()::text")
            .fetch_one(pool)
            .await?;
        Ok(Self {
            started_at,
            log_start_offset: current_log_size(config),
        })
    }

    pub async fn postmaster_start(pool: &PgPool) -> Result<String, sqlx::Error> {
        sqlx::query_scalar("SELECT pg_postmaster_start_time()::text")
            .fetch_one(pool)
            .await
    }

    /// Log text written since the run began, or None when no log source is configured.
    pub fn log_since_start(&self, config: &SoakConfig) -> Option<String> {
        if let Some(path) = &config.pg_log_file {
            let bytes = std::fs::read(path).ok()?;
            let from = (self.log_start_offset as usize).min(bytes.len());
            return Some(String::from_utf8_lossy(&bytes[from..]).into_owned());
        }
        let container = config.pg_container.as_ref()?;
        let output = match &config.pg_container_log {
            Some(path) => Command::new("docker")
                .args([
                    "exec",
                    container,
                    "sh",
                    "-c",
                    &format!("tail -c +{} {path}", self.log_start_offset + 1),
                ])
                .output()
                .ok()?,
            None => Command::new("docker")
                .args(["logs", container])
                .output()
                .ok()?,
        };
        let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
        text.push_str(&String::from_utf8_lossy(&output.stderr));
        Some(text)
    }
}

fn current_log_size(config: &SoakConfig) -> u64 {
    if let Some(path) = &config.pg_log_file {
        return std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    }
    let (Some(container), Some(path)) = (&config.pg_container, &config.pg_container_log) else {
        return 0;
    };
    Command::new("docker")
        .args(["exec", container, "sh", "-c", &format!("wc -c < {path}")])
        .output()
        .ok()
        .and_then(|output| String::from_utf8_lossy(&output.stdout).trim().parse().ok())
        .unwrap_or(0)
}

/// Lines of `log` that indicate a crashed server process, each with a few lines of
/// context on either side.
pub fn crash_excerpts(log: &str) -> Vec<String> {
    let lines: Vec<&str> = log.lines().collect();
    let mut excerpts = Vec::new();
    let mut next_allowed = 0;
    for (index, line) in lines.iter().enumerate() {
        if index >= next_allowed && CRASH_PATTERNS.iter().any(|pattern| line.contains(pattern)) {
            let from = index.saturating_sub(5);
            let to = (index + 8).min(lines.len());
            excerpts.push(lines[from..to].join("\n"));
            next_allowed = to;
        }
    }
    excerpts
}
