//! Verifies that the CHECK constraints on awa.queue_descriptors /
//! awa.job_kind_descriptors (v009) actually reject bad data rather than
//! letting it slip through to the catalog.
//!
//! These are in the integration tests dir (not unit tests) because they
//! need a live Postgres. Requires DATABASE_URL to point at a reachable
//! database with the current schema applied.

use awa_model::admin::{
    cleanup_stale_descriptors, sync_job_kind_descriptors, sync_queue_descriptors,
    JobKindDescriptor, NamedJobKindDescriptor, NamedQueueDescriptor, QueueDescriptor,
};
use chrono::TimeDelta;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".into())
}

async fn pool() -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url())
        .await
        .expect("connect")
}

async fn with_clean_catalog<F, Fut, T>(prefix: &str, f: F) -> T
where
    F: FnOnce(PgPool) -> Fut,
    Fut: std::future::Future<Output = T>,
{
    let p = pool().await;
    awa_model::migrations::run(&p).await.unwrap();
    sqlx::query("DELETE FROM awa.queue_descriptors WHERE queue LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(&p)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.job_kind_descriptors WHERE kind LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(&p)
        .await
        .unwrap();
    let result = f(p.clone()).await;
    sqlx::query("DELETE FROM awa.queue_descriptors WHERE queue LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(&p)
        .await
        .unwrap();
    sqlx::query("DELETE FROM awa.job_kind_descriptors WHERE kind LIKE $1")
        .bind(format!("{prefix}%"))
        .execute(&p)
        .await
        .unwrap();
    result
}

fn is_check_violation(err: &awa_model::AwaError) -> bool {
    format!("{err:?}").contains("check")
}

#[tokio::test]
async fn queue_descriptor_rejects_empty_name() {
    with_clean_catalog("constraint_", |p| async move {
        let result = sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: String::new(),
                descriptor: QueueDescriptor::new().display_name("oops"),
            }],
            Duration::from_secs(10),
        )
        .await;
        let err = result.expect_err("empty queue name should violate CHECK");
        assert!(is_check_violation(&err), "unexpected error: {err:?}");
    })
    .await;
}

#[tokio::test]
async fn queue_descriptor_rejects_oversized_name() {
    with_clean_catalog("constraint_", |p| async move {
        let long_name = "q".repeat(201);
        let result = sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: long_name,
                descriptor: QueueDescriptor::new(),
            }],
            Duration::from_secs(10),
        )
        .await;
        let err = result.expect_err("201-char queue name should violate CHECK");
        assert!(is_check_violation(&err), "unexpected error: {err:?}");
    })
    .await;
}

#[tokio::test]
async fn queue_descriptor_rejects_too_many_tags() {
    with_clean_catalog("constraint_", |p| async move {
        let mut descriptor = QueueDescriptor::new();
        for i in 0..21 {
            descriptor = descriptor.tag(format!("tag_{i}"));
        }
        let result = sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: "constraint_too_many_tags".into(),
                descriptor,
            }],
            Duration::from_secs(10),
        )
        .await;
        let err = result.expect_err("21 tags should violate CHECK");
        assert!(is_check_violation(&err), "unexpected error: {err:?}");
    })
    .await;
}

#[tokio::test]
async fn queue_descriptor_rejects_nonpositive_sync_interval() {
    with_clean_catalog("constraint_", |p| async move {
        let result = sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: "constraint_zero_interval".into(),
                descriptor: QueueDescriptor::new(),
            }],
            Duration::from_secs(0),
        )
        .await;
        let err = result.expect_err("0 sync_interval should violate CHECK");
        assert!(is_check_violation(&err), "unexpected error: {err:?}");
    })
    .await;
}

#[tokio::test]
async fn queue_descriptor_accepts_valid_payload() {
    with_clean_catalog("constraint_", |p| async move {
        sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: "constraint_valid".into(),
                descriptor: QueueDescriptor::new()
                    .display_name("Valid")
                    .description("Valid descriptor")
                    .owner("team")
                    .docs_url("https://example.com/runbook")
                    .tags(vec!["a", "b", "c"])
                    .extra(serde_json::json!({"k": "v"})),
            }],
            Duration::from_secs(10),
        )
        .await
        .expect("valid descriptor should upsert cleanly");
    })
    .await;
}

#[tokio::test]
async fn job_kind_descriptor_rejects_oversized_description() {
    with_clean_catalog("constraint_", |p| async move {
        let long_description = "d".repeat(2001);
        let mut descriptor = JobKindDescriptor::new();
        descriptor.description = Some(long_description);
        let result = sync_job_kind_descriptors(
            &p,
            &[NamedJobKindDescriptor {
                kind: "constraint_long_desc".into(),
                descriptor,
            }],
            Duration::from_secs(10),
        )
        .await;
        let err = result.expect_err("2001-char description should violate CHECK");
        assert!(is_check_violation(&err), "unexpected error: {err:?}");
    })
    .await;
}

#[tokio::test]
async fn job_kind_descriptor_rejects_oversized_docs_url() {
    with_clean_catalog("constraint_", |p| async move {
        let long_url = format!("https://example.com/{}", "x".repeat(2048));
        let result = sync_job_kind_descriptors(
            &p,
            &[NamedJobKindDescriptor {
                kind: "constraint_long_url".into(),
                descriptor: JobKindDescriptor::new().docs_url(long_url),
            }],
            Duration::from_secs(10),
        )
        .await;
        let err = result.expect_err(">2048-char docs_url should violate CHECK");
        assert!(is_check_violation(&err), "unexpected error: {err:?}");
    })
    .await;
}

// ── Retention ────────────────────────────────────────────────────────────

async fn backdate_queue_last_seen(pool: &PgPool, queue: &str, days: i32) {
    sqlx::query(
        "UPDATE awa.queue_descriptors \
         SET last_seen_at = now() - make_interval(days => $2) \
         WHERE queue = $1",
    )
    .bind(queue)
    .bind(days)
    .execute(pool)
    .await
    .unwrap();
}

async fn backdate_kind_last_seen(pool: &PgPool, kind: &str, days: i32) {
    sqlx::query(
        "UPDATE awa.job_kind_descriptors \
         SET last_seen_at = now() - make_interval(days => $2) \
         WHERE kind = $1",
    )
    .bind(kind)
    .bind(days)
    .execute(pool)
    .await
    .unwrap();
}

async fn queue_exists(pool: &PgPool, queue: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM awa.queue_descriptors WHERE queue = $1)",
    )
    .bind(queue)
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn kind_exists(pool: &PgPool, kind: &str) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM awa.job_kind_descriptors WHERE kind = $1)",
    )
    .bind(kind)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn cleanup_stale_descriptors_deletes_beyond_window() {
    // Distinct prefix from `cleanup_stale_descriptors_preserves_fresh_rows`
    // — `with_clean_catalog` issues `DELETE … WHERE queue LIKE
    // 'retention_%'` at both ends of the closure, and `tokio::test`
    // runs siblings in parallel, so a shared prefix made the two
    // tests race: the second test's pre-clean wiped the first
    // test's freshly-synced row before the first reached its
    // `queue_exists` assertion. Locally tests are usually fast enough
    // to dodge this; CI saw it.
    with_clean_catalog("retention_old_", |p| async move {
        sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: "retention_old_queue".into(),
                descriptor: QueueDescriptor::new().display_name("ancient"),
            }],
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        sync_job_kind_descriptors(
            &p,
            &[NamedJobKindDescriptor {
                kind: "retention_old_kind".into(),
                descriptor: JobKindDescriptor::new().display_name("ancient"),
            }],
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        backdate_queue_last_seen(&p, "retention_old_queue", 40).await;
        backdate_kind_last_seen(&p, "retention_old_kind", 40).await;

        let q_deleted = cleanup_stale_descriptors(
            &p,
            "awa.queue_descriptors",
            TimeDelta::try_days(30).unwrap(),
        )
        .await
        .unwrap();
        let k_deleted = cleanup_stale_descriptors(
            &p,
            "awa.job_kind_descriptors",
            TimeDelta::try_days(30).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(q_deleted, 1, "stale queue descriptor should be deleted");
        assert_eq!(k_deleted, 1, "stale kind descriptor should be deleted");
        assert!(!queue_exists(&p, "retention_old_queue").await);
        assert!(!kind_exists(&p, "retention_old_kind").await);
    })
    .await;
}

#[tokio::test]
async fn cleanup_stale_descriptors_preserves_fresh_rows() {
    with_clean_catalog("retention_fresh_", |p| async move {
        sync_queue_descriptors(
            &p,
            &[NamedQueueDescriptor {
                queue: "retention_fresh_queue".into(),
                descriptor: QueueDescriptor::new().display_name("fresh"),
            }],
            Duration::from_secs(10),
        )
        .await
        .unwrap();

        // A just-synced descriptor is well inside the 30-day window.
        let deleted = cleanup_stale_descriptors(
            &p,
            "awa.queue_descriptors",
            TimeDelta::try_days(30).unwrap(),
        )
        .await
        .unwrap();

        assert_eq!(deleted, 0, "fresh descriptor must not be deleted");
        assert!(queue_exists(&p, "retention_fresh_queue").await);
    })
    .await;
}

#[tokio::test]
async fn cleanup_stale_descriptors_rejects_unknown_table() {
    with_clean_catalog("retention_unknown_", |p| async move {
        let result = cleanup_stale_descriptors(
            &p,
            "awa.jobs", // not a descriptor catalog
            TimeDelta::try_days(30).unwrap(),
        )
        .await;
        let err = result.expect_err("unknown table must be rejected");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("Validation") || msg.contains("unknown table"),
            "expected validation error, got {msg}"
        );
    })
    .await;
}
