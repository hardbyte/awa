//! Supplemental lock evidence; never changes the claim-ring oracle's verdict.

use super::abandon_oracle::RingEpisode;
use super::shared::Shared;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::collections::BTreeSet;
use std::io::Write;
use std::time::Duration;

const SQL: &str = include_str!("ring_lock_capture.sql");
const LOCK_ROWS: usize = 256;
const BLOCKER_PIDS: usize = 64;
const CLIENT_DEADLINE: Duration = Duration::from_millis(750);

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Stage {
    Suspected,
    Violation,
}

impl Stage {
    fn index(self) -> usize {
        match self {
            Self::Suspected => 0,
            Self::Violation => 1,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Suspected => "suspected",
            Self::Violation => "violation",
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Decision {
    Capture(Stage),
    Exhausted(Stage),
}

/// Four slots per stage reserve half the run budget for actual violations.
#[derive(Default)]
pub struct CaptureBudget {
    episode: Option<(i64, i64)>,
    seen: [bool; 2],
    used: [u8; 2],
    exhausted_reported: [bool; 2],
}

impl CaptureBudget {
    pub fn plan(
        &mut self,
        episode: &RingEpisode,
        now_ms: i64,
        bound_ms: i64,
        zero_live: bool,
    ) -> Option<Decision> {
        let key = (episode.generation, episode.start_ms);
        if self.episode != Some(key) {
            self.episode = Some(key);
            self.seen = [false; 2];
        }
        let stage = if episode.reported {
            Stage::Violation
        } else if zero_live && now_ms - episode.start_ms >= bound_ms / 2 {
            Stage::Suspected
        } else {
            return None;
        };
        let index = stage.index();
        if self.seen[index] {
            return None;
        }
        self.seen[index] = true;
        if self.used[index] < 4 {
            self.used[index] += 1;
            Some(Decision::Capture(stage))
        } else if !self.exhausted_reported[index] {
            self.exhausted_reported[index] = true;
            Some(Decision::Exhausted(stage))
        } else {
            None
        }
    }
}

fn names(slot: i32) -> Vec<String> {
    [
        format!("lease_claims_{slot}"),
        format!("lease_claim_batches_{slot}"),
        format!("lease_claim_closures_{slot}"),
        format!("lease_claim_closure_batches_{slot}"),
        "claim_ring_state".into(),
        "claim_ring_slots".into(),
    ]
    .into()
}

fn missing(error: impl std::fmt::Display) -> Value {
    json!({"status": "missing", "error": error.to_string().chars().take(512).collect::<String>()})
}

fn finish(mut value: Value, expected: &[String]) -> Value {
    let mut gaps = Vec::new();
    let resolved: BTreeSet<_> = value["resolved_relations"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|row| row["relation"].as_str())
        .collect();
    let unresolved: Vec<_> = expected
        .iter()
        .filter(|name| !resolved.contains(name.as_str()))
        .cloned()
        .collect();
    if !unresolved.is_empty() {
        gaps.push("unresolved_relations");
    }
    if let Some(rows) = value["locks"].as_array_mut() {
        if rows.len() > LOCK_ROWS {
            rows.truncate(LOCK_ROWS);
            gaps.push("lock_rows_truncated");
        }
        if rows.iter().any(|row| {
            row["blocking_pids_truncated"] == true
                || (row["wait_event_type"] == "Lock" && row["blocking_pids_observed"] != true)
                || row["activity_missing"] == true
                || row["visibility_limited"] == true
        }) {
            gaps.push("incomplete_activity_or_blocker_graph");
        }
    }
    if value["blocker_pid_lookup_truncated"] == true {
        gaps.push("blocker_pid_lookup_truncated");
    }
    if value["missing_blocker_activity"]
        .as_array()
        .is_some_and(|rows| !rows.is_empty())
    {
        gaps.push("missing_blocker_activity");
    }
    if value["blocker_activity"]
        .as_array()
        .is_some_and(|rows| rows.iter().any(|row| row["visibility_limited"] == true))
    {
        gaps.push("blocker_activity_visibility_limited");
    }
    value["status"] = json!(if gaps.is_empty() { "ok" } else { "partial" });
    value["missing"] = json!(gaps);
    value["unresolved_relations"] = json!(unresolved);
    value
}

async fn collect(pool: &PgPool, schema: &str, slot: i32, sql: &str) -> Value {
    let expected = names(slot);
    let result = tokio::time::timeout(CLIENT_DEADLINE, async {
        let mut tx = pool.begin().await?;
        sqlx::query("SET TRANSACTION READ ONLY")
            .execute(tx.as_mut())
            .await?;
        sqlx::query("SET LOCAL statement_timeout = '250ms'")
            .execute(tx.as_mut())
            .await?;
        sqlx::query("SET LOCAL lock_timeout = '100ms'")
            .execute(tx.as_mut())
            .await?;
        let mut value: Value = sqlx::query_scalar(awa::audited_sql(sql))
            .bind(schema)
            .bind(&expected)
            .fetch_one(tx.as_mut())
            .await?;
        let pids: BTreeSet<i32> = value["locks"]
            .as_array()
            .into_iter()
            .flatten()
            .flat_map(|row| row["blocking_pids"].as_array().into_iter().flatten())
            .filter_map(|pid| pid.as_i64().and_then(|pid| i32::try_from(pid).ok()))
            .filter(|pid| *pid > 0)
            .collect();
        value["blocker_pid_lookup_truncated"] = json!(pids.len() > BLOCKER_PIDS);
        let requested: Vec<_> = pids.into_iter().take(BLOCKER_PIDS).collect();
        let activity: Value = sqlx::query_scalar(
            "SELECT COALESCE(jsonb_agg(to_jsonb(a)), '[]'::jsonb) FROM (
                SELECT pid, leader_pid, backend_start, application_name, usename, state,
                       wait_event_type, wait_event, xact_start, query_start,
                       EXTRACT(EPOCH FROM(clock_timestamp()-xact_start))::float8 AS tx_age_s,
                       EXTRACT(EPOCH FROM(clock_timestamp()-query_start))::float8 AS query_age_s,
                       left(query,512) AS query,
                       query IS NULL OR query = '<insufficient privilege>' AS visibility_limited
                FROM pg_catalog.pg_stat_activity
                WHERE datid=(SELECT oid FROM pg_catalog.pg_database WHERE datname=current_database())
                  AND pid=ANY($1::int4[]) ORDER BY pid LIMIT 65
            ) a",
        )
        .bind(&requested)
        .fetch_one(tx.as_mut())
        .await?;
        let found: BTreeSet<_> = activity
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| row["pid"].as_i64())
            .collect();
        value["missing_blocker_activity"] = json!(requested
            .iter()
            .filter(|pid| !found.contains(&i64::from(**pid)))
            .collect::<Vec<_>>());
        value["blocker_activity"] = activity;
        tx.rollback().await?;
        Ok::<_, sqlx::Error>(value)
    })
    .await;
    match result {
        Ok(Ok(value)) => finish(value, &expected),
        Ok(Err(error)) => missing(error),
        Err(_) => missing("claim-ring metadata capture exceeded its 750ms client deadline"),
    }
}

pub async fn observe(
    shared: &Shared,
    pool: &PgPool,
    schema: &str,
    slot_count: i32,
    next_slot: Option<(i64, i64)>,
    budget: &mut CaptureBudget,
) {
    let Some(episode) = shared.abandon.current_ring_episode() else {
        return;
    };
    let now = shared.now_ms();
    let Some(decision) = budget.plan(
        &episode,
        now,
        shared.config.abandon_bound.as_millis() as i64,
        next_slot.is_some_and(|(live, _)| live == 0),
    ) else {
        return;
    };
    let stage = match decision {
        Decision::Capture(stage) => stage,
        Decision::Exhausted(stage) => {
            shared.finding(
                "claim_ring_lock_capture_missing",
                format!("{} lock-capture budget exhausted", stage.name()),
                vec![],
            );
            return;
        }
    };
    let incoming = (episode.slot + 1).rem_euclid(slot_count);
    let mut value = collect(pool, schema, incoming, SQL).await;
    value["stage"] = json!(stage.name());
    value["started_at_ms"] = json!(now);
    value["finished_at_ms"] = json!(shared.now_ms());
    value["current_slot"] = json!(episode.slot);
    value["next_slot"] = json!(incoming);
    value["generation"] = json!(episode.generation);
    value["next_slot_probe"] = json!(next_slot);
    shared.run.add("claim_ring_lock_capture_attempts", 1);
    if value["status"] != "ok" {
        shared.finding(
            "claim_ring_lock_capture_missing",
            "claim-ring lock evidence is missing or partial",
            vec![value.to_string()],
        );
    }
    let path = shared.config.report_dir.join("claim-ring-locks.jsonl");
    let written = (|| {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(file, "{value}")
    })();
    match written {
        Ok(()) => shared.run.add("claim_ring_lock_capture_records", 1),
        Err(error) => shared.finding(
            "claim_ring_lock_capture_missing",
            format!("could not write claim-ring lock evidence: {error}"),
            vec![],
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn episode(generation: i64, reported: bool) -> RingEpisode {
        RingEpisode {
            generation,
            reported,
            slot: 7,
            start_ms: 0,
            end_ms: None,
            last_live_ms: 0,
            expired_in_next_slot: 0,
        }
    }

    #[test]
    fn ring_lock_budget_waits_half_bound_and_reserves_violation_captures() {
        let mut budget = CaptureBudget::default();
        assert_eq!(budget.plan(&episode(0, false), 59_999, 120_000, true), None);
        assert_eq!(
            budget.plan(&episode(0, false), 60_000, 120_000, false),
            None
        );
        for generation in 1..=4 {
            assert_eq!(
                budget.plan(&episode(generation, false), 60_000, 120_000, true),
                Some(Decision::Capture(Stage::Suspected))
            );
            assert_eq!(
                budget.plan(&episode(generation, false), 70_000, 120_000, true),
                None
            );
        }
        assert_eq!(
            budget.plan(&episode(5, false), 60_000, 120_000, true),
            Some(Decision::Exhausted(Stage::Suspected))
        );
        assert_eq!(budget.plan(&episode(6, false), 60_000, 120_000, true), None);
        for generation in 7..=10 {
            assert_eq!(
                budget.plan(&episode(generation, true), 120_001, 120_000, true),
                Some(Decision::Capture(Stage::Violation))
            );
            assert_eq!(
                budget.plan(&episode(generation, true), 130_000, 120_000, true),
                None
            );
        }
        assert_eq!(
            budget.plan(&episode(11, true), 120_001, 120_000, true),
            Some(Decision::Exhausted(Stage::Violation))
        );
        assert_eq!(
            budget.plan(&episode(12, true), 120_001, 120_000, true),
            None
        );
        assert_eq!(budget.used, [4, 4]);
    }

    #[test]
    fn ring_lock_result_caps_and_missing_relations_stay_explicit() {
        let rows = vec![json!({"blocking_pids_truncated": true}); LOCK_ROWS + 1];
        let value = finish(json!({"resolved_relations": [], "locks": rows}), &names(0));
        assert_eq!(value["status"], "partial");
        assert_eq!(value["locks"].as_array().unwrap().len(), LOCK_ROWS);
        assert_eq!(value["unresolved_relations"].as_array().unwrap().len(), 6);
        let gaps = value["missing"].as_array().unwrap();
        assert!(gaps.contains(&json!("lock_rows_truncated")));
        assert!(gaps.contains(&json!("incomplete_activity_or_blocker_graph")));
    }

    #[test]
    fn ring_lock_hidden_second_lookup_is_partial_even_without_selected_locks() {
        let expected = names(0);
        let inventory: Vec<_> = expected
            .iter()
            .map(|name| json!({"relation": name}))
            .collect();
        let value = finish(
            json!({
                "resolved_relations": inventory,
                "locks": [],
                "blocker_activity": [{"pid": 42, "query": null, "visibility_limited": true}]
            }),
            &expected,
        );
        assert_eq!(value["status"], "partial");
        assert!(value["missing"]
            .as_array()
            .unwrap()
            .contains(&json!("blocker_activity_visibility_limited")));
    }

    struct Database {
        config: super::super::config::SoakConfig,
        pool: PgPool,
    }

    impl Database {
        async fn create() -> Self {
            use super::super::config::{replace_database_name, SoakConfig};
            let base = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:test@localhost:15432/awa_test".to_string()
            });
            let name = format!("awa_soak_ring_locks_{}", uuid::Uuid::new_v4().simple());
            let config = SoakConfig::for_test(replace_database_name(&base, &name));
            super::super::db::recreate_database(&config)
                .await
                .expect("isolated database");
            let pool = super::super::db::connect_pool(&config.database_url, "ring_capture_test", 6)
                .await
                .expect("fixture pool");
            Self { config, pool }
        }

        async fn drop(self) {
            self.pool.close().await;
            let admin =
                super::super::db::connect_pool(&self.config.admin_url(), "ring_capture_cleanup", 1)
                    .await
                    .expect("cleanup connection");
            sqlx::query(awa::audited_sql(format!(
                "DROP DATABASE {} WITH (FORCE)",
                self.config.database_name()
            )))
            .execute(&admin)
            .await
            .expect("drop fixture database");
            admin.close().await;
        }
    }

    #[tokio::test]
    async fn ring_lock_capture_identifies_holder_waiter_and_releases_cleanly() {
        let db = Database::create().await;
        let pool = &db.pool;
        let mut holder = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL application_name = 'ring_capture_holder'")
            .execute(holder.as_mut())
            .await
            .unwrap();
        sqlx::query("LOCK TABLE awa.lease_claims_0 IN ACCESS SHARE MODE")
            .execute(holder.as_mut())
            .await
            .unwrap();
        let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(holder.as_mut())
            .await
            .unwrap();
        let mut waiter = pool.begin().await.unwrap();
        sqlx::query("SET LOCAL application_name = 'ring_capture_waiter'")
            .execute(waiter.as_mut())
            .await
            .unwrap();
        let waiter_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(waiter.as_mut())
            .await
            .unwrap();
        let waiting = tokio::spawn(async move {
            sqlx::query("LOCK TABLE awa.lease_claims_0 IN ACCESS EXCLUSIVE MODE")
                .execute(waiter.as_mut())
                .await
                .unwrap();
            waiter.rollback().await.unwrap();
        });
        let deadline =
            tokio::time::Instant::now() + crate::ci_timing::scaled_timeout(Duration::from_secs(5));
        loop {
            let waiting: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=$1 AND NOT granted)",
            )
            .bind(waiter_pid)
            .fetch_one(pool)
            .await
            .unwrap();
            if waiting {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "exclusive waiter did not queue"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let value = collect(pool, "awa", 0, SQL).await;
        assert_eq!(value["status"], "ok", "{value}");
        assert_eq!(value["resolved_relations"].as_array().unwrap().len(), 6);
        let rows = value["locks"].as_array().unwrap();
        let held = rows
            .iter()
            .find(|row| row["pid"] == holder_pid && row["relation"] == "lease_claims_0")
            .expect("holder evidence");
        assert_eq!(held["mode"], "AccessShareLock");
        assert_eq!(held["granted"], true);
        assert_eq!(held["application_name"], "ring_capture_holder");
        assert!(held["tx_age_s"].as_f64().unwrap() >= 0.0);
        let blocked = rows
            .iter()
            .find(|row| row["pid"] == waiter_pid && row["granted"] == false)
            .expect("waiter evidence");
        assert_eq!(blocked["mode"], "AccessExclusiveLock");
        assert_eq!(blocked["application_name"], "ring_capture_waiter");
        assert_eq!(blocked["wait_event_type"], "Lock");
        assert!(blocked["blocking_pids"]
            .as_array()
            .unwrap()
            .contains(&json!(holder_pid)));
        assert!(value["blocker_activity"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["pid"] == holder_pid));
        holder.rollback().await.unwrap();
        tokio::time::timeout(
            crate::ci_timing::scaled_timeout(Duration::from_secs(5)),
            waiting,
        )
        .await
        .unwrap()
        .unwrap();
        let clean = collect(pool, "awa", 0, SQL).await;
        assert_eq!(clean["status"], "ok", "{clean}");
        assert!(clean["server_statement_at"].as_str().is_some());
        assert!(clean["server_observed_at"].as_str().is_some());
        assert_eq!(clean["resolved_relations"].as_array().unwrap().len(), 6);
        assert!(clean["locks"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["pid"] != holder_pid && row["pid"] != waiter_pid));
        db.drop().await;
    }

    #[tokio::test]
    async fn ring_lock_capture_failure_and_unresolved_inventory_are_not_empty_success() {
        let db = Database::create().await;
        let value = collect(&db.pool, "missing_ring_schema", 0, SQL).await;
        assert_eq!(value["status"], "partial");
        assert_eq!(value["resolved_relations"], json!([]));
        assert_eq!(value["locks"], json!([]));
        assert!(value["server_statement_at"].as_str().is_some());
        assert!(value["server_observed_at"].as_str().is_some());
        assert_eq!(value["unresolved_relations"].as_array().unwrap().len(), 6);
        let failed = collect(&db.pool, "awa", 0,
            "SELECT jsonb_build_object() FROM pg_sleep(1) WHERE $1::text IS NOT NULL AND $2::text[] IS NOT NULL").await;
        assert_eq!(failed["status"], "missing", "{failed}");
        assert!(
            failed.get("locks").is_none(),
            "failed capture must not look unlocked"
        );
        assert!(!failed["error"].as_str().unwrap().is_empty());
        db.drop().await;
    }
}
