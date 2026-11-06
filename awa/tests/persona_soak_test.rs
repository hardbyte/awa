//! Persona soak: several application-shaped workloads (Mia, Tom, Priya, Sam, Alex and
//! a chaos agent) run concurrently against one disposable database on the queue-storage
//! engine while an invariant oracle checks the system. Ignored by default.
//!
//! ```text
//! AWA_SOAK_DATABASE_URL=postgres://postgres:pw@localhost:5432/awa_soak_run \
//! AWA_SOAK_SECONDS=900 \
//! cargo test --release -p awa --test persona_soak_test -- --ignored --nocapture
//! ```
//!
//! See `docs/test-plan.md` for the environment variables and `scripts/persona-soak.sh`
//! for the engine matrix.

#![allow(dead_code)]
mod persona_soak;

use persona_soak::config::SoakConfig;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "drops and recreates an awa_soak* database; see the module docs"]
async fn persona_soak() {
    let config = SoakConfig::from_env().unwrap_or_else(|error| panic!("{error}"));
    let shared = persona_soak::run::run_soak(config)
        .await
        .unwrap_or_else(|error| panic!("soak setup failed: {error}"));
    let violations = shared.violations.lock().expect("violations").len();
    assert_eq!(
        violations,
        0,
        "{violations} invariant violation(s); see {}",
        shared.config.report_dir.display()
    );
}

/// The claiming process of the claim-and-abandon persona. The harness runs this test binary
/// again with only this test selected and kills the process with SIGKILL; started any
/// other way it returns at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run by the claim-and-abandon persona as a separate process"]
async fn claim_abandon_child() {
    persona_soak::personas::abandon::run_child().await;
}
