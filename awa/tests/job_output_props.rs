//! ADR-048 property test: any value either fails to record as an output, or
//! reads back equal through both terminal paths: the compact receipt batch
//! and the general `done_entries` row.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test.
//! PROPTEST_CASES raises the case count (default 128).

use awa::model::queue_storage::QueueStorage;
use awa::{JobArgs, OutputSlot, MAX_OUTPUT_BYTES};
use awa_testing::setup::TestDatabase;
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::Cell;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Produce {
    case: u64,
}

fn json_string() -> impl Strategy<Value = String> {
    prop_oneof![
        30 => "\\PC{0,16}",
        1 => "[a-z]{0,4}\0[a-z]{0,4}",
        1 => Just("\u{FFFF}\u{10FFFF}".to_string()),
    ]
}

fn json() -> impl Strategy<Value = Value> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        any::<u64>().prop_map(Value::from),
        any::<f64>()
            .prop_filter("JSON has no NaN or infinity", |f| f.is_finite())
            .prop_map(Value::from),
        json_string().prop_map(Value::from),
    ];
    leaf.prop_recursive(3, 24, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(Value::from),
            prop::collection::btree_map(json_string(), inner, 0..6)
                .prop_map(|map| Value::Object(map.into_iter().collect())),
        ]
    })
}

/// Mostly small values, and strings right around the size limit.
fn output() -> impl Strategy<Value = Value> {
    prop_oneof![
        8 => json(),
        1 => (MAX_OUTPUT_BYTES - 8..MAX_OUTPUT_BYTES + 8)
            .prop_map(|n| Value::from("x".repeat(n.saturating_sub(2)))),
    ]
}

/// JSON equality as jsonb keeps it: jsonb stores numbers as `numeric`,
/// so a float with no fractional part reads back as an integer of the same
/// value.
fn same_json(left: Option<&Value>, right: Option<&Value>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => same_value(left, right),
        _ => false,
    }
}

fn same_value(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(l), Value::Number(r)) => l == r || l.as_f64() == r.as_f64(),
        (Value::Array(l), Value::Array(r)) => {
            l.len() == r.len() && l.iter().zip(r).all(|(l, r)| same_value(l, r))
        }
        (Value::Object(l), Value::Object(r)) => {
            l.len() == r.len()
                && l.iter()
                    .all(|(key, l)| r.get(key).is_some_and(|r| same_value(l, r)))
        }
        _ => left == right,
    }
}

fn runner() -> TestRunner {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(128);
    TestRunner::new(Config {
        cases,
        failure_persistence: None,
        ..Config::default()
    })
}

/// Record `value` on a fresh slot, then complete one claimed job on each
/// terminal path with whatever was recorded, and read both back.
async fn check(
    pool: &sqlx::PgPool,
    store: &QueueStorage,
    case: u64,
    value: Value,
) -> Result<bool, TestCaseError> {
    let slot = OutputSlot::for_testing();
    let recorded = match slot.set(&value) {
        Ok(()) => slot.seal_for_testing(),
        Err(_) => {
            prop_assert_eq!(
                slot.seal_for_testing(),
                None,
                "a failed set clears the slot"
            );
            return Ok(false);
        }
    };
    let queue = "output_props";
    for _ in 0..2 {
        awa::insert_with(
            pool,
            &Produce { case },
            awa::InsertOpts {
                queue: queue.into(),
                ..Default::default()
            },
        )
        .await
        .expect("insert");
    }
    let mut claimed = store
        .claim_runtime_batch(pool, queue, 2, Duration::from_secs(60))
        .await
        .expect("claim");
    prop_assert_eq!(claimed.len(), 2);
    for job in &mut claimed {
        job.job.output = recorded.clone();
    }
    store
        .complete_runtime_batch(pool, &claimed[..1])
        .await
        .expect("compact completion");
    let mut tx = pool.begin().await.expect("begin");
    store
        .complete_runtime_batch_slow_in_tx(&mut tx, &claimed[1..])
        .await
        .expect("general completion");
    tx.commit().await.expect("commit");

    for job in &claimed {
        let row = store
            .load_job(pool, job.job.id)
            .await
            .expect("load")
            .expect("job exists");
        prop_assert!(
            same_json(row.output.as_ref(), recorded.as_ref()),
            "job {}: stored {:?}, recorded {:?}",
            job.job.id,
            row.output,
            recorded
        );
    }
    Ok(recorded.is_some())
}

#[tokio::test(flavor = "multi_thread")]
async fn recorded_outputs_read_back_through_both_terminal_paths() {
    let db = TestDatabase::queue_storage().await;
    let pool = db.pool().clone();
    let store = QueueStorage::from_existing_schema("awa").unwrap();
    let handle = tokio::runtime::Handle::current();
    let (stored, rejected) = (Cell::new(0u32), Cell::new(0u32));
    let case = Cell::new(0u64);
    let result = runner().run(&output(), |value| {
        case.set(case.get() + 1);
        let outcome = tokio::task::block_in_place(|| {
            handle.block_on(check(&pool, &store, case.get(), value))
        })?;
        let counter = if outcome { &stored } else { &rejected };
        counter.set(counter.get() + 1);
        Ok(())
    });
    if let Err(err) = result {
        panic!("{err}");
    }
    let (stored, rejected) = (stored.get(), rejected.get());
    eprintln!("stored {stored} outputs and rejected or cleared {rejected}");
    assert!(
        stored > 0 && rejected > 0,
        "stored {stored}, rejected {rejected}"
    );

    let compact_batches: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM awa.receipt_completion_batches WHERE outputs IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(compact_batches > 0, "no output rode a compact batch");
}
