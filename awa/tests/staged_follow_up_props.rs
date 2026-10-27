//! ADR-046 property tests: whatever staging accepts, the completion
//! transaction can insert. A request that the database would reject must fail
//! at `enqueue_on_completion`, never later, when it would roll back the
//! completion and leave the job running until rescue.
//!
//! Set DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test.
//! PROPTEST_CASES raises the case count (default 256 per engine).

use awa::{FollowUps, InsertOpts, JobArgs, UniqueOpts};
use awa_testing::setup::TestDatabase;
use chrono::{DateTime, TimeZone, Utc};
use proptest::prelude::*;
use proptest::test_runner::{Config, TestCaseError, TestRunner};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cell::Cell;

#[derive(Debug, Clone, Serialize, Deserialize, JobArgs)]
struct Trigger {
    id: i64,
}

#[derive(Debug, Clone)]
struct Request {
    kind: String,
    args: Value,
    opts: InsertOpts,
}

/// Names around the 200-character limit, multibyte names whose byte length
/// exceeds it, NUL bytes, and ordinary names.
fn name() -> impl Strategy<Value = String> {
    prop_oneof![
        8 => "[a-z_]{1,24}",
        1 => "\\PC{0,8}",
        1 => (195usize..=205).prop_map(|n| "q".repeat(n)),
        1 => (95usize..=105).prop_map(|n| "é".repeat(n)),
        1 => (190usize..=210).prop_map(|n| "é".repeat(n)),
        1 => "[a-z]{0,4}\0[a-z]{0,4}",
    ]
}

fn json_string() -> impl Strategy<Value = String> {
    prop_oneof![
        30 => "\\PC{0,16}",
        1 => "[a-z]{0,4}\0[a-z]{0,4}",
        1 => Just("\u{FFFF}\u{10FFFF}".to_string()),
    ]
}

/// Mostly valid tags, so a request with many of them is not almost always
/// rejected for one bad tag.
fn tag() -> impl Strategy<Value = String> {
    prop_oneof![20 => "[a-z_]{1,12}", 1 => name()]
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

/// Mostly small values, sometimes one large enough to cross the byte budget.
fn json_payload() -> impl Strategy<Value = Value> {
    prop_oneof![
        8 => json(),
        1 => (60_000usize..70_000).prop_map(|n| Value::from("x".repeat(n))),
    ]
}

/// Anywhere chrono can represent, which is wider than Postgres's range.
fn run_at() -> impl Strategy<Value = Option<DateTime<Utc>>> {
    let min = DateTime::<Utc>::MIN_UTC.timestamp();
    let max = DateTime::<Utc>::MAX_UTC.timestamp();
    prop_oneof![
        3 => Just(None),
        2 => (-86_400i64 * 365..86_400 * 365)
            .prop_map(|offset| Some(Utc::now() + chrono::Duration::seconds(offset))),
        1 => (min..=max).prop_map(|secs| Utc.timestamp_opt(secs, 0).single()),
    ]
}

fn deadline() -> impl Strategy<Value = Option<chrono::Duration>> {
    prop_oneof![
        3 => Just(None),
        2 => (-3_600i64..86_400).prop_map(|secs| Some(chrono::Duration::seconds(secs))),
        1 => any::<i64>().prop_map(chrono::Duration::try_milliseconds),
    ]
}

fn unique() -> impl Strategy<Value = Option<UniqueOpts>> {
    let period = prop_oneof![
        Just(None),
        (-2i64..=2).prop_map(Some),
        any::<i64>().prop_map(Some),
    ];
    prop_oneof![
        Just(None),
        (any::<bool>(), any::<bool>(), period, any::<u8>()).prop_map(
            |(by_queue, by_args, by_period, states)| Some(UniqueOpts {
                by_queue,
                by_args,
                by_period,
                states,
            })
        ),
    ]
}

fn request() -> impl Strategy<Value = Request> {
    // Mostly in range, sometimes just outside it, occasionally anything.
    let int_field = |lo: i16, hi: i16| {
        prop_oneof![
            6 => lo..=hi,
            1 => prop_oneof![Just(lo - 1), Just(hi + 1)],
            1 => any::<i16>(),
        ]
    };
    (
        name(),
        json_payload(),
        name(),
        int_field(1, 4),
        int_field(1, 1000),
        run_at(),
        deadline(),
        json_payload(),
        prop::collection::vec(tag(), 0..23),
        unique(),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..300)),
    )
        .prop_map(
            |(
                kind,
                args,
                queue,
                priority,
                max_attempts,
                run_at,
                deadline_duration,
                metadata,
                tags,
                unique,
                ordering_key,
            )| Request {
                kind,
                args,
                opts: InsertOpts {
                    queue,
                    priority,
                    max_attempts,
                    run_at,
                    deadline_duration,
                    metadata,
                    tags,
                    unique,
                    ordering_key,
                },
            },
        )
}

fn runner() -> TestRunner {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(256);
    TestRunner::new(Config {
        cases,
        // Under queue storage some generated queue names are filtered out (#560).
        max_global_rejects: cases.saturating_mul(2),
        failure_persistence: None,
        ..Config::default()
    })
}

/// Stage `request` alone and, if staging accepts it, insert it the way the
/// completion transaction does, then roll back.
async fn check(
    pool: &sqlx::PgPool,
    trigger: &awa::JobRow,
    request: Request,
) -> Result<bool, TestCaseError> {
    let follow_ups = FollowUps::for_testing();
    let staged = follow_ups.enqueue_raw_on_completion(request.kind, request.args, request.opts);
    let rows = match (staged, follow_ups.seal_for_testing()) {
        (Ok(()), Ok(rows)) => rows,
        (Err(_), Err(_)) => return Ok(false),
        (staged, sealed) => {
            return Err(TestCaseError::fail(format!(
                "staging and sealing disagree: staged {staged:?}, sealed {:?}",
                sealed.map(|rows| rows.len())
            )))
        }
    };
    let mut tx = pool.begin().await.expect("begin");
    let inserted = awa_worker::insert_staged_follow_ups_in_tx(&mut tx, trigger, &rows).await;
    tx.rollback().await.expect("rollback");
    inserted.map(|()| true).map_err(|err| {
        TestCaseError::fail(format!("staging accepted a row the insert rejected: {err}"))
    })
}

/// Queue storage cannot yet insert into a queue whose name is longer than 59
/// bytes, because `pg_notify` rejects the channel name (#560). That limit
/// applies to every insert, not only staged ones, so this property leaves
/// such names out under queue storage until #560 is fixed.
const QUEUE_STORAGE_MAX_QUEUE_BYTES: usize = 59;

async fn accepted_requests_are_insertable(db: TestDatabase, max_queue_bytes: usize) {
    let pool = db.pool().clone();
    let trigger = awa::insert(&pool, &Trigger { id: 1 }).await.unwrap();
    let handle = tokio::runtime::Handle::current();
    let (accepted, rejected) = (Cell::new(0u32), Cell::new(0u32));
    let strategy = request().prop_filter("queue name too long for the engine (#560)", move |r| {
        r.opts.queue.len() <= max_queue_bytes
    });
    let result = runner().run(&strategy, |request| {
        let outcome =
            tokio::task::block_in_place(|| handle.block_on(check(&pool, &trigger, request)))?;
        let counter = if outcome { &accepted } else { &rejected };
        counter.set(counter.get() + 1);
        Ok(())
    });
    if let Err(err) = result {
        panic!("{err}");
    }
    let (accepted, rejected) = (accepted.get(), rejected.get());
    eprintln!("staging accepted {accepted} and rejected {rejected} generated requests");
    // A generator that only ever produced rejects would prove nothing.
    assert!(
        accepted > 0 && rejected > 0,
        "accepted {accepted}, rejected {rejected}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_requests_are_insertable_on_canonical() {
    accepted_requests_are_insertable(TestDatabase::canonical().await, usize::MAX).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn accepted_requests_are_insertable_under_queue_storage() {
    accepted_requests_are_insertable(
        TestDatabase::queue_storage().await,
        QUEUE_STORAGE_MAX_QUEUE_BYTES,
    )
    .await;
}
