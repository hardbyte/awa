//! Persona-driven soak harness with an invariant oracle. See the test binary
//! `persona_soak_test.rs` for how to run it.

pub mod abandon_oracle;
pub mod adminserver;
pub mod config;
pub mod db;
pub mod fleet;
pub mod gate_metrics;
pub mod gate_proof;
pub mod ledger;
pub mod maintenance_metrics;
pub mod oracle;
pub mod personas;
pub mod pgserver;
pub mod python_probe;
pub mod registry;
pub mod report;
pub mod ring_locks;
pub mod rng;
pub mod roles;
pub mod run;
pub mod shard_oracle;
pub mod shared;
pub mod stop;
pub mod terminal_proof;
pub mod worker_errors;
