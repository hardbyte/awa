pub mod client;
mod completion;
pub mod context;
pub mod dispatcher;
pub mod events;
pub mod executor;
pub mod heartbeat;
pub mod maintenance;
pub mod metrics;
mod runtime;

// Re-exports
pub use awa_model::{CallbackConfig, PeriodicJob, PeriodicJobBuilder};
pub use client::{BuildError, Client, ClientBuilder, HealthCheck, QueueCapacity, QueueHealth};
pub use context::{CallbackGuard, CallbackToken, JobContext};
pub use dispatcher::{QueueConfig, RateLimit};
pub use events::{JobEvent, UntypedJobEvent};
pub use executor::{JobError, JobResult, Worker};
pub use maintenance::RetentionPolicy;
pub use metrics::AwaMetrics;
