use awa_model::{migrations, runtime_control};
use runtime_control::{
    RuntimeControl, RuntimeControlState, RuntimeControlStatus, RuntimeObservationStatus,
};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

async fn requested_halt(pool: &PgPool) -> RuntimeControl {
    migrations::run(pool).await.unwrap();
    runtime_control::halt(pool, runtime_control::PRUNE_SCOPE, "status test", "drain")
        .await
        .unwrap()
}

async fn runtime(pool: &PgPool, version: &str, leader: bool) -> Uuid {
    let instance_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO awa.runtime_instances (
            instance_id, hostname, pid, version, started_at, last_seen_at,
            snapshot_interval_ms, healthy, postgres_connected, poll_loop_alive,
            heartbeat_alive, maintenance_alive, shutting_down, leader
        ) VALUES ($1, 'status-test-host', 42, $2, clock_timestamp(), clock_timestamp(),
                  1000, true, true, true, true, true, false, $3)",
    )
    .bind(instance_id)
    .bind(version)
    .bind(leader)
    .execute(pool)
    .await
    .unwrap();
    instance_id
}

fn halted_observation(halt: &RuntimeControl) -> Value {
    json!({
        "protocol_version": runtime_control::OBSERVATION_PROTOCOL_VERSION,
        "scope": runtime_control::PRUNE_SCOPE,
        "supported": true,
        "fresh": true,
        "observed_revision": halt.revision,
        "halt_id": halt.halt_id,
        "admission_allowed": false,
        "outstanding_callbacks": 0,
        "settlement_unknown": false,
        "report_sequence": 1,
        "state": "halted"
    })
}

async fn report(pool: &PgPool, instance_id: Uuid, observation: Value) {
    sqlx::query(
        "INSERT INTO awa.runtime_control_observations(instance_id, observation)
         VALUES ($1, $2 || jsonb_build_object('received_at', clock_timestamp()))
         ON CONFLICT (instance_id) DO UPDATE SET observation = EXCLUDED.observation",
    )
    .bind(instance_id)
    .bind(observation)
    .execute(pool)
    .await
    .unwrap();
}

fn assert_unobserving(status: &RuntimeControlStatus, instance_id: Uuid, leader: bool) {
    assert_eq!(status.state, RuntimeControlState::Unconfirmed);
    assert_eq!(status.unobserving_runtimes.len(), 1);
    let old = &status.unobserving_runtimes[0];
    assert_eq!(old.instance_id, instance_id);
    assert_eq!(old.version, "0.6.15");
    assert_eq!(old.hostname.as_deref(), Some("status-test-host"));
    assert_eq!(old.pid, 42);
    assert_eq!(old.leader, leader);
    assert!(old.live);
    assert_eq!(old.observation_status, RuntimeObservationStatus::Missing);
    assert!(old.observation.is_none());
}

#[sqlx::test(migrations = false)]
async fn old_live_leader_is_named_and_blocks_halted_despite_compatible_standby(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let old_leader = runtime(&pool, "0.6.15", true).await;
    let standby = runtime(&pool, "0.6.16", false).await;
    report(&pool, standby, halted_observation(&halt)).await;

    let status = runtime_control::status(&pool).await.unwrap();
    assert_eq!(status.requested.halt_id, halt.halt_id);
    assert_eq!(status.requested.revision, halt.revision);
    assert_eq!(status.runtimes.len(), 2);
    assert_unobserving(&status, old_leader, true);
}

#[sqlx::test(migrations = false)]
async fn old_live_nonleader_also_blocks_halted_and_stale_snapshot_remains_visible(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let old_standby = runtime(&pool, "0.6.15", false).await;
    let leader = runtime(&pool, "0.6.16", true).await;
    report(&pool, leader, halted_observation(&halt)).await;
    assert_unobserving(
        &runtime_control::status(&pool).await.unwrap(),
        old_standby,
        false,
    );

    sqlx::query(
        "UPDATE awa.runtime_instances SET last_seen_at = clock_timestamp() - interval '1 minute'
         WHERE instance_id = $1",
    )
    .bind(old_standby)
    .execute(&pool)
    .await
    .unwrap();
    let status = runtime_control::status(&pool).await.unwrap();
    assert_eq!(status.state, RuntimeControlState::Halted);
    assert!(status.unobserving_runtimes.is_empty());
    let old = status
        .runtimes
        .iter()
        .find(|runtime| runtime.instance_id == old_standby)
        .unwrap();
    assert!(!old.live);
    assert!(old.observation.is_none());
}

#[sqlx::test(migrations = false)]
async fn runtime_liveness_uses_three_snapshot_intervals_with_thirty_second_floor(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let leader = runtime(&pool, "0.6.16", true).await;
    report(&pool, leader, halted_observation(&halt)).await;
    let old = runtime(&pool, "0.6.15", false).await;
    sqlx::query(
        "UPDATE awa.runtime_instances
         SET snapshot_interval_ms = 30999, last_seen_at = clock_timestamp() - interval '60 seconds'
         WHERE instance_id = $1",
    )
    .bind(old)
    .execute(&pool)
    .await
    .unwrap();
    assert_unobserving(&runtime_control::status(&pool).await.unwrap(), old, false);

    sqlx::query(
        "UPDATE awa.runtime_instances SET last_seen_at = clock_timestamp() - interval '95 seconds'
         WHERE instance_id = $1",
    )
    .bind(old)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        runtime_control::status(&pool).await.unwrap().state,
        RuntimeControlState::Halted
    );
}

#[sqlx::test(migrations = false)]
async fn all_compatible_live_runtimes_must_match_current_halt_and_settle_callbacks(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let leader = runtime(&pool, "0.6.16", true).await;
    let standby = runtime(&pool, "custom-build", false).await;
    for instance in [leader, standby] {
        report(&pool, instance, halted_observation(&halt)).await;
    }
    let status = runtime_control::status(&pool).await.unwrap();
    assert_eq!(status.state, RuntimeControlState::Halted);
    assert!(status.unobserving_runtimes.is_empty());
    assert!(status.runtimes.iter().all(|runtime| {
        runtime.live
            && runtime.report_recent
            && runtime.observes_current_request
            && runtime.observation_status == RuntimeObservationStatus::Observing
    }));

    for (changes, expected) in [
        (
            json!({"halt_id": Uuid::new_v4()}),
            RuntimeControlState::Pending,
        ),
        (
            json!({"observed_revision": halt.revision - 1}),
            RuntimeControlState::Pending,
        ),
        (
            json!({"settlement_unknown": true}),
            RuntimeControlState::Unknown,
        ),
        (
            json!({"admission_allowed": true}),
            RuntimeControlState::Unknown,
        ),
        (
            json!({"state": "draining", "outstanding_callbacks": 2}),
            RuntimeControlState::Draining,
        ),
        (
            json!({"state": "halted", "outstanding_callbacks": 1}),
            RuntimeControlState::Unknown,
        ),
        (
            json!({"settlement_unknown": null}),
            RuntimeControlState::Unknown,
        ),
    ] {
        let mut observation = halted_observation(&halt);
        observation
            .as_object_mut()
            .unwrap()
            .extend(changes.as_object().unwrap().clone());
        report(&pool, standby, observation).await;
        let status = runtime_control::status(&pool).await.unwrap();
        assert_eq!(status.state, expected, "changes: {changes}");
        assert!(status.unobserving_runtimes.is_empty());
    }
}

#[sqlx::test(migrations = false)]
async fn incompatible_or_stale_reports_are_explicitly_unobserving(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let leader = runtime(&pool, "0.6.16", true).await;
    for (changes, expected) in [
        (
            json!({"supported": false}),
            RuntimeObservationStatus::Unsupported,
        ),
        (
            json!({"protocol_version": 2}),
            RuntimeObservationStatus::IncompatibleProtocol,
        ),
        (
            json!({"protocol_version": null}),
            RuntimeObservationStatus::IncompatibleProtocol,
        ),
        (
            json!({"scope": "dispatch"}),
            RuntimeObservationStatus::IncompatibleProtocol,
        ),
        (
            json!({"fresh": false}),
            RuntimeObservationStatus::StaleControl,
        ),
    ] {
        let mut observation = halted_observation(&halt);
        observation
            .as_object_mut()
            .unwrap()
            .extend(changes.as_object().unwrap().clone());
        report(&pool, leader, observation).await;
        let status = runtime_control::status(&pool).await.unwrap();
        assert_eq!(status.state, RuntimeControlState::Unconfirmed);
        assert_eq!(status.unobserving_runtimes.len(), 1);
        assert_eq!(status.unobserving_runtimes[0].instance_id, leader);
        assert_eq!(status.unobserving_runtimes[0].observation_status, expected);
    }

    for received_at in [json!("2000-01-01T00:00:00Z"), json!("invalid"), Value::Null] {
        report(&pool, leader, halted_observation(&halt)).await;
        sqlx::query(
            "UPDATE awa.runtime_control_observations
             SET observation = jsonb_set(observation, '{received_at}', $2) WHERE instance_id = $1",
        )
        .bind(leader)
        .bind(received_at)
        .execute(&pool)
        .await
        .unwrap();
        let status = runtime_control::status(&pool).await.unwrap();
        assert_eq!(status.state, RuntimeControlState::Unconfirmed);
        assert_eq!(status.unobserving_runtimes.len(), 1);
        assert_eq!(
            status.unobserving_runtimes[0].observation_status,
            RuntimeObservationStatus::StaleReport
        );
        assert!(!status.unobserving_runtimes[0].report_recent);
    }
}

#[sqlx::test(migrations = false)]
async fn resume_reports_running_even_when_runtime_still_reports_previous_halt(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let leader = runtime(&pool, "0.6.16", true).await;
    report(&pool, leader, halted_observation(&halt)).await;
    let resumed =
        runtime_control::resume(&pool, runtime_control::PRUNE_SCOPE, halt.halt_id.unwrap())
            .await
            .unwrap();
    let status = runtime_control::status(&pool).await.unwrap();
    assert_eq!(status.state, RuntimeControlState::Running);
    assert_eq!(status.requested.revision, resumed.revision);
    assert!(status.requested.halt_id.is_none());
    assert!(!status.runtimes[0].observes_current_request);
    assert_eq!(
        status.runtimes[0].observation.as_ref().unwrap()["state"],
        "halted"
    );
}

#[sqlx::test(migrations = false)]
async fn no_halt_with_unobserving_live_runtime_is_degraded_until_recovery(pool: PgPool) {
    migrations::run(&pool).await.unwrap();
    let requested = runtime_control::status(&pool).await.unwrap().requested;
    assert!(requested.halt_id.is_none());
    let leader = runtime(&pool, "0.6.16", true).await;
    let standby = runtime(&pool, "0.6.16", false).await;
    let mut running = halted_observation(&requested);
    running["state"] = json!("running");
    running["admission_allowed"] = json!(true);
    report(&pool, standby, running.clone()).await;

    let missing = runtime_control::status(&pool).await.unwrap();
    assert_eq!(missing.state, RuntimeControlState::Degraded);
    assert_eq!(missing.unobserving_runtimes.len(), 1);
    assert_eq!(missing.unobserving_runtimes[0].instance_id, leader);

    for (changes, expected) in [
        (
            json!({"fresh": false, "observed_revision": null, "state": "unknown", "admission_allowed": false}),
            RuntimeObservationStatus::StaleControl,
        ),
        (
            json!({"protocol_version": 2}),
            RuntimeObservationStatus::IncompatibleProtocol,
        ),
        (
            json!({"supported": false}),
            RuntimeObservationStatus::Unsupported,
        ),
    ] {
        let mut observation = running.clone();
        observation
            .as_object_mut()
            .unwrap()
            .extend(changes.as_object().unwrap().clone());
        report(&pool, leader, observation).await;
        let status = runtime_control::status(&pool).await.unwrap();
        assert_eq!(
            status.state,
            RuntimeControlState::Degraded,
            "changes: {changes}"
        );
        assert_eq!(status.unobserving_runtimes.len(), 1);
        assert_eq!(status.unobserving_runtimes[0].instance_id, leader);
        assert_eq!(status.unobserving_runtimes[0].observation_status, expected);
    }

    report(&pool, leader, running.clone()).await;
    assert_eq!(
        runtime_control::status(&pool).await.unwrap().state,
        RuntimeControlState::Running
    );
    sqlx::query(
        "UPDATE awa.runtime_control_observations
         SET observation = jsonb_set(observation, '{received_at}',
             to_jsonb(clock_timestamp() - interval '6 seconds'))
         WHERE instance_id = $1",
    )
    .bind(leader)
    .execute(&pool)
    .await
    .unwrap();
    let stale_report = runtime_control::status(&pool).await.unwrap();
    assert_eq!(stale_report.state, RuntimeControlState::Degraded);
    assert_eq!(
        stale_report.unobserving_runtimes[0].observation_status,
        RuntimeObservationStatus::StaleReport
    );

    sqlx::query(
        "UPDATE awa.runtime_instances SET last_seen_at = clock_timestamp() - interval '1 minute'
         WHERE instance_id = $1",
    )
    .bind(leader)
    .execute(&pool)
    .await
    .unwrap();
    let stale_runtime = runtime_control::status(&pool).await.unwrap();
    assert_eq!(stale_runtime.state, RuntimeControlState::Running);
    assert!(stale_runtime.unobserving_runtimes.is_empty());
}

#[sqlx::test(migrations = false)]
async fn empty_or_entirely_stale_fleet_does_not_confirm_halted(pool: PgPool) {
    let halt = requested_halt(&pool).await;
    let empty = runtime_control::status(&pool).await.unwrap();
    assert_eq!(empty.state, RuntimeControlState::Unconfirmed);
    assert!(empty.runtimes.is_empty());
    let instance = runtime(&pool, "0.6.16", true).await;
    report(&pool, instance, halted_observation(&halt)).await;
    sqlx::query(
        "UPDATE awa.runtime_instances SET last_seen_at = clock_timestamp() - interval '1 minute'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let stale = runtime_control::status(&pool).await.unwrap();
    assert_eq!(stale.state, RuntimeControlState::Unconfirmed);
    assert!(!stale.runtimes[0].live);
    assert!(stale.unobserving_runtimes.is_empty());
}

#[sqlx::test(migrations = false)]
async fn default_cadence_follower_report_stays_recent_without_relaxing_halt_confirmation(
    pool: PgPool,
) {
    let halt = requested_halt(&pool).await;
    let leader = runtime(&pool, "0.6.16", true).await;
    let follower = runtime(&pool, "0.6.16", false).await;
    sqlx::query(
        "UPDATE awa.runtime_instances SET snapshot_interval_ms = 10000 WHERE instance_id = $1",
    )
    .bind(follower)
    .execute(&pool)
    .await
    .unwrap();

    for (changes, expected) in [
        (json!({}), RuntimeControlState::Halted),
        (json!({"fresh": false}), RuntimeControlState::Unconfirmed),
        (
            json!({"admission_allowed": true}),
            RuntimeControlState::Unknown,
        ),
        (
            json!({"outstanding_callbacks": 1}),
            RuntimeControlState::Unknown,
        ),
        (
            json!({"settlement_unknown": true}),
            RuntimeControlState::Unknown,
        ),
    ] {
        report(&pool, leader, halted_observation(&halt)).await;
        let mut observation = halted_observation(&halt);
        observation
            .as_object_mut()
            .unwrap()
            .extend(changes.as_object().unwrap().clone());
        report(&pool, follower, observation).await;
        sqlx::query(
            "UPDATE awa.runtime_control_observations
             SET observation = jsonb_set(observation, '{received_at}',
                 to_jsonb(clock_timestamp() - interval '12 seconds'))
             WHERE instance_id = $1",
        )
        .bind(follower)
        .execute(&pool)
        .await
        .unwrap();
        let status = runtime_control::status(&pool).await.unwrap();
        assert_eq!(status.state, expected, "changes: {changes}");
        let observed = status
            .runtimes
            .iter()
            .find(|runtime| runtime.instance_id == follower)
            .unwrap();
        assert!(observed.live);
        assert!(observed.report_recent);
        assert_eq!(observed.report_freshness_ms, 20_000);
    }

    report(&pool, follower, halted_observation(&halt)).await;
    let next_halt = runtime_control::halt(&pool, runtime_control::PRUNE_SCOPE, "new halt", "drain")
        .await
        .unwrap();
    report(&pool, leader, halted_observation(&next_halt)).await;
    let pending = runtime_control::status(&pool).await.unwrap();
    assert_eq!(pending.state, RuntimeControlState::Pending);
    let observed = pending
        .runtimes
        .iter()
        .find(|runtime| runtime.instance_id == follower)
        .unwrap();
    assert!(observed.report_recent);
    assert!(!observed.observes_current_request);

    report(&pool, follower, halted_observation(&next_halt)).await;
    sqlx::query(
        "UPDATE awa.runtime_control_observations
         SET observation = jsonb_set(observation, '{received_at}',
             to_jsonb(clock_timestamp() - interval '21 seconds'))
         WHERE instance_id = $1",
    )
    .bind(follower)
    .execute(&pool)
    .await
    .unwrap();
    let stale = runtime_control::status(&pool).await.unwrap();
    assert_eq!(stale.state, RuntimeControlState::Unconfirmed);
    assert_eq!(stale.unobserving_runtimes.len(), 1);
    let observed = &stale.unobserving_runtimes[0];
    assert_eq!(observed.instance_id, follower);
    assert!(observed.live);
    assert!(!observed.report_recent);
    assert_eq!(observed.report_freshness_ms, 20_000);
    assert_eq!(
        observed.observation_status,
        RuntimeObservationStatus::StaleReport
    );
}
