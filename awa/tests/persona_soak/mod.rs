//! Persona-driven soak harness with an invariant oracle. See the test binary
//! `persona_soak_test.rs` for how to run it.

pub mod config;
pub mod db;
pub mod fleet;
pub mod ledger;
pub mod oracle;
pub mod personas;
pub mod pgserver;
pub mod registry;
pub mod report;
pub mod rng;
pub mod run;
pub mod shared;
pub mod stop;
