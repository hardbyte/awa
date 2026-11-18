//! Persona-driven soak harness with an invariant oracle. See the test binary
//! `persona_soak_test.rs` for how to run it.

pub mod abandon_oracle;
pub mod admin_latency;
pub mod adminserver;
pub mod config;
pub mod db;
pub mod drain_check;
pub mod fleet;
pub mod gate_metrics;
pub mod gate_proof;
pub mod intent;
pub mod ledger;
pub mod maintenance_checks;
pub mod maintenance_metrics;
pub mod memory;
pub mod oracle;
pub mod personas;
pub mod pgserver;
pub mod python_probe;
pub mod registry;
pub mod report;
pub mod resources;
pub mod ring_locks;
pub mod rng;
pub mod roles;
pub mod run;
pub mod sampling;
pub mod shard_oracle;
pub mod shared;
pub mod stop;
pub mod terminal_proof;
pub mod worker_errors;
