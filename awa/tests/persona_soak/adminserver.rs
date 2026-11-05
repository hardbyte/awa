//! The real admin HTTP server (`awa serve`), run as a child process against the soak
//! database so the UI personas exercise the same code the web UI talks to.

use super::config::SoakConfig;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub const APPLICATION_NAME: &str = "soak_ui_server";

pub struct AdminServer {
    child: Child,
    pub base_url: String,
    pub statement_timeout: Duration,
    pub pool_max: u32,
}

/// Looks for the `awa` binary next to the test executable, where
/// `cargo build --release -p awa-cli` puts it, unless `AWA_SOAK_AWA_BIN` names one.
pub fn locate_binary(config: &SoakConfig) -> Result<PathBuf, String> {
    if let Some(path) = &config.awa_bin {
        return path
            .exists()
            .then(|| path.clone())
            .ok_or_else(|| format!("AWA_SOAK_AWA_BIN {} does not exist", path.display()));
    }
    let exe = std::env::current_exe().map_err(|error| error.to_string())?;
    let candidate = exe
        .parent()
        .and_then(|deps| deps.parent())
        .map(|profile| profile.join("awa"));
    match candidate {
        Some(path) if path.exists() => Ok(path),
        _ => Err(
            "the admin UI personas need the awa binary: run `cargo build --release -p awa-cli` \
                  or set AWA_SOAK_AWA_BIN"
                .to_string(),
        ),
    }
}

impl AdminServer {
    pub async fn start(config: &SoakConfig) -> Result<Self, String> {
        let binary = locate_binary(config)?;
        let port = TcpListener::bind("127.0.0.1:0")
            .and_then(|listener| listener.local_addr())
            .map_err(|error| format!("pick a port: {error}"))?
            .port();
        let separator = if config.database_url.contains('?') {
            '&'
        } else {
            '?'
        };
        let url = format!(
            "{}{separator}application_name={APPLICATION_NAME}",
            config.database_url
        );
        let statement_timeout = config.ui_statement_timeout;
        let log = std::fs::File::create(config.report_dir.join("awa-serve.log"))
            .or_else(|_| {
                std::fs::create_dir_all(&config.report_dir)?;
                std::fs::File::create(config.report_dir.join("awa-serve.log"))
            })
            .map_err(|error| format!("create awa-serve.log: {error}"))?;
        let child = Command::new(&binary)
            .args([
                "--database-url",
                &url,
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ])
            .args([
                "--statement-timeout",
                &statement_timeout.as_secs().to_string(),
            ])
            .args([
                "--pool-max",
                &config.ui_pool_max.to_string(),
                "--pool-min",
                "2",
            ])
            .stdout(Stdio::from(
                log.try_clone().map_err(|error| error.to_string())?,
            ))
            .stderr(Stdio::from(log))
            .spawn()
            .map_err(|error| format!("spawn {}: {error}", binary.display()))?;
        let mut server = Self {
            child,
            base_url: format!("http://127.0.0.1:{port}"),
            statement_timeout,
            pool_max: config.ui_pool_max,
        };
        let client = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(Some(status)) = server.child.try_wait() {
                return Err(format!("awa serve exited during startup: {status}"));
            }
            if let Ok(response) = client
                .get(format!("{}/api/capabilities", server.base_url))
                .send()
                .await
            {
                if response.status().is_success() {
                    return Ok(server);
                }
            }
            if Instant::now() >= deadline {
                return Err("awa serve did not start listening within 60s".to_string());
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }

    pub fn stop(self) {}
}

impl Drop for AdminServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
