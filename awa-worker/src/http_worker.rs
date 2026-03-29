//! HTTP Worker — dispatches jobs to serverless functions via HTTP.
//!
//! See ADR-018 for design rationale.

use crate::context::JobContext;
use crate::executor::{JobError, JobResult, Worker};
use std::collections::HashMap;
use std::time::Duration;

/// How the HTTP worker interacts with the function endpoint.
#[derive(Debug, Clone)]
pub enum HttpWorkerMode {
    /// POST the job and park in `waiting_external`. The function calls back
    /// via the callback URL when done. Best for long-running functions.
    Async,
    /// POST the job and await the HTTP response. Map status code to
    /// `JobResult`. Best for short-lived functions (< 30s).
    Sync {
        /// Timeout for the HTTP round-trip.
        response_timeout: Duration,
    },
}

/// Configuration for an HTTP worker.
#[derive(Debug, Clone)]
pub struct HttpWorkerConfig {
    /// Function endpoint URL. The job payload is POSTed here.
    pub url: String,
    /// Async (callback) or sync (await response) mode.
    pub mode: HttpWorkerMode,
    /// How long the callback has before timeout rescue fires (async mode only).
    /// Defaults to 1 hour.
    pub callback_timeout: Duration,
    /// Static headers to include in every request (e.g., auth tokens).
    pub headers: HashMap<String, String>,
    /// HMAC signing key for callback authentication. When set, the worker
    /// signs the callback ID with blake3 keyed hashing and includes the
    /// signature as `X-Awa-Signature`.
    pub hmac_secret: Option<[u8; 32]>,
    /// Base URL for the callback endpoint. The full callback URL is
    /// `{callback_base_url}/api/callbacks/{callback_id}/complete`.
    /// Required for async mode.
    pub callback_base_url: Option<String>,
    /// HTTP request timeout for the initial POST to the function.
    /// Defaults to 30 seconds.
    pub request_timeout: Duration,
}

impl Default for HttpWorkerConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            mode: HttpWorkerMode::Async,
            callback_timeout: Duration::from_secs(3600),
            headers: HashMap::new(),
            hmac_secret: None,
            callback_base_url: None,
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// A `Worker` implementation that dispatches jobs to HTTP endpoints.
///
/// In async mode, the worker registers a callback, POSTs the job to the
/// function URL, and returns `WaitForCallback`. The function is expected
/// to call back when done.
///
/// In sync mode, the worker POSTs the job and maps the HTTP response
/// status to a `JobResult`.
///
/// # Example
///
/// ```ignore
/// use awa::{Client, HttpWorker, HttpWorkerConfig, HttpWorkerMode};
/// use std::time::Duration;
///
/// let client = Client::builder(pool)
///     .http_worker("generate_pdf", HttpWorkerConfig {
///         url: "https://pdf-service.run.app/generate".into(),
///         mode: HttpWorkerMode::Async,
///         callback_base_url: Some("https://awa.example.com".into()),
///         ..Default::default()
///     })
///     .queue("default", 4)
///     .build()?;
/// ```
pub struct HttpWorker {
    kind: &'static str,
    client: reqwest::Client,
    config: HttpWorkerConfig,
}

impl HttpWorker {
    /// Create a new HTTP worker for the given job kind.
    pub fn new(kind: String, config: HttpWorkerConfig) -> Self {
        let mut builder = reqwest::Client::builder()
            .timeout(config.request_timeout)
            .user_agent("awa-http-worker/0.5");

        // For sync mode, use the response timeout instead
        if let HttpWorkerMode::Sync { response_timeout } = &config.mode {
            builder = builder.timeout(*response_timeout);
        }

        let client = builder.build().expect("failed to build HTTP client");

        Self {
            // Leak the kind string to satisfy Worker::kind() -> &'static str.
            // Workers are registered once at startup and live for the process
            // lifetime, so this is a deliberate ~16-byte-per-kind trade-off.
            kind: Box::leak(kind.into_boxed_str()),
            client,
            config,
        }
    }

    /// Compute the blake3 keyed-hash signature for a callback ID.
    fn sign_callback_id(&self, callback_id: &str) -> Option<String> {
        self.config.hmac_secret.map(|key| {
            let hash = blake3::keyed_hash(&key, callback_id.as_bytes());
            hash.to_hex().to_string()
        })
    }
}

/// JSON body sent to the function endpoint.
#[derive(serde::Serialize)]
struct HttpWorkerRequest<'a> {
    job_id: i64,
    kind: &'a str,
    args: &'a serde_json::Value,
    attempt: i16,
    max_attempts: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    callback_url: Option<String>,
}

#[async_trait::async_trait]
impl Worker for HttpWorker {
    fn kind(&self) -> &'static str {
        self.kind
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        match &self.config.mode {
            HttpWorkerMode::Async => self.perform_async(ctx).await,
            HttpWorkerMode::Sync { .. } => self.perform_sync(ctx).await,
        }
    }
}

impl HttpWorker {
    async fn perform_async(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        // Register callback before POSTing to avoid race where function
        // calls back before the DB knows about the callback.
        let guard = ctx
            .register_callback(self.config.callback_timeout)
            .await
            .map_err(JobError::retryable)?;

        let callback_id_str = guard.id().to_string();

        // Build callback URL
        let callback_url = self.config.callback_base_url.as_ref().map(|base| {
            format!(
                "{}/api/callbacks/{}/complete",
                base.trim_end_matches('/'),
                callback_id_str
            )
        });

        let body = HttpWorkerRequest {
            job_id: ctx.job.id,
            kind: self.kind,
            args: &ctx.job.args,
            attempt: ctx.job.attempt,
            max_attempts: ctx.job.max_attempts,
            callback_id: Some(callback_id_str.clone()),
            callback_url,
        };

        let mut request = self.client.post(&self.config.url).json(&body);

        // Add static headers
        for (key, value) in &self.config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        // Add HMAC signature
        if let Some(signature) = self.sign_callback_id(&callback_id_str) {
            request = request.header("X-Awa-Signature", &signature);
        }

        // POST to function — we expect 2xx to mean "accepted"
        let response = request.send().await.map_err(JobError::retryable)?;

        let status = response.status();
        if status.is_success() {
            // Function accepted the job — park and wait for callback
            Ok(JobResult::WaitForCallback(guard))
        } else if status.is_server_error() {
            // 5xx — transient, retry
            let body_text = response.text().await.unwrap_or_default();
            Err(JobError::retryable_msg(format!(
                "function returned {status}: {body_text}"
            )))
        } else {
            // 4xx — likely a permanent error (bad request, auth failure)
            let body_text = response.text().await.unwrap_or_default();
            Err(JobError::Terminal(format!(
                "function returned {status}: {body_text}"
            )))
        }
    }

    async fn perform_sync(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let body = HttpWorkerRequest {
            job_id: ctx.job.id,
            kind: self.kind,
            args: &ctx.job.args,
            attempt: ctx.job.attempt,
            max_attempts: ctx.job.max_attempts,
            callback_id: None,
            callback_url: None,
        };

        let mut request = self.client.post(&self.config.url).json(&body);

        for (key, value) in &self.config.headers {
            request = request.header(key.as_str(), value.as_str());
        }

        let response = request.send().await.map_err(JobError::retryable)?;

        let status = response.status();
        if status.is_success() {
            Ok(JobResult::Completed)
        } else if status.is_server_error() {
            let body_text = response.text().await.unwrap_or_default();
            Err(JobError::retryable_msg(format!(
                "function returned {status}: {body_text}"
            )))
        } else {
            let body_text = response.text().await.unwrap_or_default();
            Err(JobError::Terminal(format!(
                "function returned {status}: {body_text}"
            )))
        }
    }
}

/// Verify a blake3 keyed-hash signature for a callback ID.
///
/// Used by callback receiver endpoints to authenticate incoming requests.
pub fn verify_callback_signature(
    hmac_secret: &[u8; 32],
    callback_id: &str,
    provided_signature: &str,
) -> bool {
    let expected = blake3::keyed_hash(hmac_secret, callback_id.as_bytes());
    // Parse the provided hex into a Hash so the comparison uses blake3's
    // constant-time PartialEq implementation.
    blake3::Hash::from_hex(provided_signature).is_ok_and(|h| expected == h)
}
