//! Queue lanes under a split-role install: a schema owner, a migration role
//! that is a member of it, and several application roles that share a runtime
//! group role. Only the owner and the migration role have default privileges,
//! so a lane sequence is usable by every runtime role only if it is created as
//! the owner or granted explicitly, whichever role touches the lane first.

use awa::model::storage::LaneSequenceOutcome;
use awa::model::{migrations, storage, AwaError, InsertOpts, InsertParams, QueueStorage};
use awa::model::{JobState, QueueStorageConfig};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Executor, PgPool};
use std::str::FromStr;
use std::time::Duration;

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

fn sql(text: impl Into<String>) -> sqlx::AssertSqlSafe<String> {
    awa::audited_sql(text)
}

/// Roles, a database owned by the schema owner, and a pool per role.
struct Cluster {
    admin: PgPool,
    database: String,
    roles: Vec<String>,
    owner_name: String,
    migrator_name: String,
    runtime_name: String,
    connect_options: PgConnectOptions,
    owner: PgPool,
    migrator: PgPool,
    app_a: PgPool,
    app_b: PgPool,
    app_c: PgPool,
}

#[derive(Clone, Copy, PartialEq)]
enum Schema {
    /// Every migration, including the lane provisioning functions.
    Current,
    /// The release before lane provisioning (v052).
    BeforeProvisioning,
}

impl Cluster {
    /// `runtime_create` grants the runtime group CREATE on the schema, as a
    /// 0.6 install does so runtimes can create lane sequences themselves.
    async fn new(
        schema: Schema,
        runtime_create: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:test@localhost:15432/awa_test".to_string());
        let admin = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await?;
        let suffix = uuid::Uuid::new_v4().simple().to_string()[..12].to_string();
        let owner_name = format!("lane_owner_{suffix}");
        let migrator_name = format!("lane_migrator_{suffix}");
        let runtime_name = format!("lane_runtime_{suffix}");
        let app_names = ["a", "b", "c"].map(|app| format!("lane_app_{app}_{suffix}"));
        let database = format!("lane_roles_{suffix}");
        admin
            .execute(sqlx::raw_sql(sql(format!(
                r#"
                CREATE ROLE {owner_name} LOGIN PASSWORD 'lane_test';
                CREATE ROLE {migrator_name} LOGIN PASSWORD 'lane_test' IN ROLE {owner_name};
                CREATE ROLE {runtime_name} NOLOGIN;
                CREATE ROLE {a} LOGIN PASSWORD 'lane_test' IN ROLE {runtime_name};
                CREATE ROLE {b} LOGIN PASSWORD 'lane_test' IN ROLE {runtime_name};
                CREATE ROLE {c} LOGIN PASSWORD 'lane_test' IN ROLE {runtime_name};
                "#,
                a = app_names[0],
                b = app_names[1],
                c = app_names[2],
            ))))
            .await?;
        admin
            .execute(sqlx::raw_sql(sql(format!(
                "CREATE DATABASE {database} OWNER {owner_name}"
            ))))
            .await?;
        let mut roles = vec![
            owner_name.clone(),
            migrator_name.clone(),
            runtime_name.clone(),
        ];
        roles.extend(app_names.iter().cloned());
        let options = PgConnectOptions::from_str(&url)?
            .database(&database)
            .password("lane_test");
        let pool = |user: &str| {
            PgPoolOptions::new()
                .max_connections(3)
                .connect_with(options.clone().username(user))
        };
        let owner = pool(&owner_name).await?;
        let migrator = pool(&migrator_name).await?;

        owner
            .execute(sqlx::raw_sql(sql(format!(
                r#"
                CREATE SCHEMA awa;
                GRANT USAGE ON SCHEMA awa TO {runtime_name};
                "#
            ))))
            .await?;
        for (creator, creator_pool) in [(&owner_name, &owner), (&migrator_name, &migrator)] {
            creator_pool
                .execute(sqlx::raw_sql(sql(format!(
                    r#"
                    ALTER DEFAULT PRIVILEGES FOR ROLE {creator} IN SCHEMA awa
                      GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO {runtime_name};
                    ALTER DEFAULT PRIVILEGES FOR ROLE {creator} IN SCHEMA awa
                      GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {runtime_name};
                    ALTER DEFAULT PRIVILEGES FOR ROLE {creator} IN SCHEMA awa
                      GRANT EXECUTE ON FUNCTIONS TO {runtime_name};
                    "#
                ))))
                .await?;
        }
        // The migration role runs every migration with the owner as its
        // effective role, as the database-roles guide describes.
        let migrate_as_owner = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                options
                    .clone()
                    .username(&migrator_name)
                    .options([("role", owner_name.as_str())]),
            )
            .await?;
        let target = match schema {
            Schema::Current => migrations::CURRENT_VERSION,
            Schema::BeforeProvisioning => 52,
        };
        for (_, _, statement) in migrations::migration_sql_range(0, target) {
            migrate_as_owner
                .execute(sqlx::raw_sql(sql(statement)))
                .await?;
        }
        if schema == Schema::BeforeProvisioning {
            // Every migration bundle carries the current helper definitions,
            // so put the v052 helpers back and drop what v052 did not have.
            migrate_as_owner
                .execute(sqlx::raw_sql(sql(include_str!(
                    "fixtures/v052_lane_sequence_helpers.sql"
                ))))
                .await?;
            migrate_as_owner
                .execute(sqlx::raw_sql(sql(r#"
                    SELECT awa.v052_refresh_lane_sequence_helpers('awa');
                    DROP FUNCTION awa.v052_refresh_lane_sequence_helpers(text);
                    DROP FUNCTION awa.provision_lane(text, smallint, smallint);
                    DROP FUNCTION awa.provision_lane_sequences(text, smallint, smallint);
                    DROP FUNCTION awa.repair_lane_sequence(text, text);
                    DROP FUNCTION awa.lane_sequence_problems(text, text);
                    "#)))
                .await?;
        }
        migrate_as_owner.close().await;
        if runtime_create {
            owner
                .execute(sqlx::raw_sql(sql(format!(
                    "GRANT CREATE ON SCHEMA awa TO {runtime_name}"
                ))))
                .await?;
        }

        let app_a = pool(&app_names[0]).await?;
        let app_b = pool(&app_names[1]).await?;
        let app_c = pool(&app_names[2]).await?;
        Ok(Self {
            admin,
            database,
            roles,
            owner_name,
            migrator_name,
            runtime_name,
            connect_options: options,
            owner,
            migrator,
            app_a,
            app_b,
            app_c,
        })
    }

    async fn teardown(self) -> TestResult {
        for pool in [
            &self.owner,
            &self.migrator,
            &self.app_a,
            &self.app_b,
            &self.app_c,
        ] {
            pool.close().await;
        }
        self.admin
            .execute(sqlx::raw_sql(sql(format!(
                "DROP DATABASE {} WITH (FORCE)",
                self.database
            ))))
            .await?;
        for role in self.roles.iter().rev() {
            self.admin
                .execute(sqlx::raw_sql(sql(format!("DROP ROLE {role}"))))
                .await?;
        }
        self.admin.close().await;
        Ok(())
    }

    /// The migration role acting as the schema owner, as `awa migrate` runs
    /// under the database-roles guide.
    async fn migrator_as_owner(&self) -> Result<PgPool, sqlx::Error> {
        PgPoolOptions::new()
            .max_connections(1)
            .connect_with(
                self.connect_options
                    .clone()
                    .username(&self.migrator_name)
                    .options([("role", self.owner_name.as_str())]),
            )
            .await
    }

    /// The `queue_enqueue_seq_*` sequence of `queue`'s first lane.
    async fn enqueue_sequence(&self, queue: &str) -> Result<String, sqlx::Error> {
        Ok(self
            .lane_sequence_owners(queue)
            .await?
            .into_iter()
            .map(|(sequence, _)| sequence)
            .find(|sequence| sequence.starts_with("queue_enqueue_seq_"))
            .expect("lane has an enqueue sequence"))
    }

    /// `(sequence, owner)` for every lane sequence of `queue`.
    async fn lane_sequence_owners(
        &self,
        queue: &str,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        sqlx::query_as(
            r#"
            SELECT sequence.relname::text, pg_get_userbyid(sequence.relowner)::text
            FROM awa.queue_enqueue_heads AS heads
            CROSS JOIN LATERAL (VALUES (heads.seq_name), ((
                SELECT claims.seq_name FROM awa.queue_claim_heads AS claims
                WHERE claims.queue = heads.queue
                  AND claims.priority = heads.priority
                  AND claims.enqueue_shard = heads.enqueue_shard
            ))) AS lane(seq_name)
            JOIN pg_class AS sequence
              ON sequence.relname = lane.seq_name
             AND sequence.relnamespace = 'awa'::regnamespace
            WHERE heads.queue = $1
            ORDER BY 1
            "#,
        )
        .bind(queue)
        .fetch_all(&self.owner)
        .await
    }

    async fn assert_lanes_owned_by_owner(&self, queue: &str, lanes: usize) -> TestResult {
        let owners = self.lane_sequence_owners(queue).await?;
        assert_eq!(
            owners.len(),
            lanes * 2,
            "lane sequences of {queue}: {owners:?}"
        );
        for (sequence, owner) in owners {
            assert_eq!(owner, self.owner_name, "{sequence} of {queue}");
        }
        Ok(())
    }

    async fn set_enqueue_shards(&self, queue: &str, shards: i16) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO awa.queue_meta (queue, enqueue_shards) VALUES ($1, $2) \
             ON CONFLICT (queue) DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards",
        )
        .bind(queue)
        .bind(shards)
        .execute(&self.owner)
        .await?;
        Ok(())
    }
}

fn store() -> QueueStorage {
    QueueStorage::new(QueueStorageConfig::default()).expect("default queue storage config")
}

/// Enqueue `count` jobs through a fresh store, as a producer process would.
async fn enqueue(pool: &PgPool, queue: &str, priority: i16, count: i64) -> Result<i64, AwaError> {
    store().enqueue_batch(pool, queue, priority, count).await
}

/// Claim and complete everything on `queue`, as a worker does; returns how
/// many jobs completed.
async fn drain(pool: &PgPool, queue: &str) -> Result<usize, AwaError> {
    let worker = store();
    let mut completed = 0;
    loop {
        let claimed = worker
            .claim_runtime_batch(pool, queue, 100, Duration::from_secs(60))
            .await?;
        if claimed.is_empty() {
            return Ok(completed);
        }
        completed += worker.complete_runtime_batch(pool, &claimed).await?.len();
    }
}

/// Schedule jobs that become due shortly, one per priority.
async fn schedule(pool: &PgPool, queue: &str, priorities: &[i16]) -> Result<(), AwaError> {
    let run_at = chrono::Utc::now() + chrono::Duration::milliseconds(200);
    let jobs: Vec<InsertParams> = priorities
        .iter()
        .map(|&priority| InsertParams {
            kind: "lane_ownership_job".into(),
            args: serde_json::json!({}),
            opts: InsertOpts {
                queue: queue.into(),
                priority,
                run_at: Some(run_at),
                ..Default::default()
            },
        })
        .collect();
    store().enqueue_params_batch(pool, &jobs).await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    Ok(())
}

async fn first_touch_by_one_app_role(runtime_create: bool) -> TestResult {
    let cluster = Cluster::new(Schema::Current, runtime_create).await?;

    // Role A creates the lane; role B enqueues, claims and completes on it.
    assert_eq!(enqueue(&cluster.app_a, "fresh_queue", 3, 2).await?, 2);
    assert_eq!(enqueue(&cluster.app_b, "fresh_queue", 3, 2).await?, 2);
    assert_eq!(drain(&cluster.app_b, "fresh_queue").await?, 4);

    // The maintenance leader, running as role C, promotes scheduled jobs into
    // lanes nobody has created yet: two priorities over two shards.
    cluster.set_enqueue_shards("fresh_queue", 2).await?;
    schedule(&cluster.app_a, "fresh_queue", &[1, 4]).await?;
    assert_eq!(
        store()
            .promote_due(&cluster.app_c, JobState::Scheduled, 10)
            .await?,
        2
    );
    let promoted_shards: Vec<i16> = sqlx::query_scalar(
        "SELECT enqueue_shard FROM awa.queue_enqueue_heads \
         WHERE queue = 'fresh_queue' AND priority IN (1, 4) ORDER BY 1",
    )
    .fetch_all(&cluster.owner)
    .await?;
    assert_eq!(promoted_shards, vec![0, 1]);

    // A and B both use the lanes the leader created; each producer's rotor
    // visits both shards.
    let (producer_a, producer_b) = (store(), store());
    for _ in 0..2 {
        producer_a
            .enqueue_batch(&cluster.app_a, "fresh_queue", 4, 1)
            .await?;
        producer_b
            .enqueue_batch(&cluster.app_b, "fresh_queue", 1, 1)
            .await?;
    }
    assert_eq!(drain(&cluster.app_a, "fresh_queue").await?, 6);

    cluster
        .assert_lanes_owned_by_owner("fresh_queue", 5)
        .await?;
    assert!(storage::lane_sequence_problems(&cluster.owner, "awa")
        .await?
        .is_empty());
    cluster.teardown().await
}

#[tokio::test]
async fn lane_first_touched_by_an_app_role_with_schema_create_serves_every_runtime_role(
) -> TestResult {
    first_touch_by_one_app_role(true).await
}

#[tokio::test]
async fn lane_first_touched_by_an_app_role_without_schema_create_serves_every_runtime_role(
) -> TestResult {
    first_touch_by_one_app_role(false).await
}

#[tokio::test]
async fn raising_enqueue_shards_live_lets_the_leader_create_a_shard_first() -> TestResult {
    let cluster = Cluster::new(Schema::Current, true).await?;
    enqueue(&cluster.app_a, "sharded_queue", 2, 3).await?;

    cluster.set_enqueue_shards("sharded_queue", 2).await?;
    // The leader's store has already routed a promotion to shard 0, as a
    // long-running leader has, so its next pick is the new shard.
    let leader = store();
    schedule(&cluster.app_a, "sharded_queue", &[2]).await?;
    assert_eq!(
        leader
            .promote_due(&cluster.app_c, JobState::Scheduled, 10)
            .await?,
        1
    );
    schedule(&cluster.app_b, "sharded_queue", &[2]).await?;
    assert_eq!(
        leader
            .promote_due(&cluster.app_c, JobState::Scheduled, 10)
            .await?,
        1
    );
    let shards: Vec<i16> = sqlx::query_scalar(
        "SELECT enqueue_shard FROM awa.queue_enqueue_heads WHERE queue = 'sharded_queue' ORDER BY 1",
    )
    .fetch_all(&cluster.owner)
    .await?;
    assert_eq!(shards, vec![0, 1]);

    // Producers on both roles spread over both shards; B drains them.
    let producer_a = store();
    let producer_b = store();
    for _ in 0..4 {
        producer_a
            .enqueue_batch(&cluster.app_a, "sharded_queue", 2, 1)
            .await?;
        producer_b
            .enqueue_batch(&cluster.app_b, "sharded_queue", 2, 1)
            .await?;
    }
    assert_eq!(drain(&cluster.app_b, "sharded_queue").await?, 13);
    cluster
        .assert_lanes_owned_by_owner("sharded_queue", 2)
        .await?;
    cluster.teardown().await
}

#[tokio::test]
async fn prepare_queue_as_an_app_role_provisions_lanes_every_runtime_role_can_use() -> TestResult {
    let cluster = Cluster::new(Schema::Current, true).await?;
    store()
        .prepare_queue(&cluster.app_a, "prepared_queue", 2)
        .await?;
    cluster
        .assert_lanes_owned_by_owner("prepared_queue", 8)
        .await?;

    cluster.set_enqueue_shards("prepared_queue", 2).await?;
    let producer = store();
    for priority in 1..=4 {
        producer
            .enqueue_batch(&cluster.app_b, "prepared_queue", priority, 1)
            .await?;
        producer
            .enqueue_batch(&cluster.app_c, "prepared_queue", priority, 1)
            .await?;
    }
    assert_eq!(drain(&cluster.app_b, "prepared_queue").await?, 8);
    cluster
        .assert_lanes_owned_by_owner("prepared_queue", 8)
        .await?;
    cluster.teardown().await
}

#[tokio::test]
async fn mixed_fleet_shares_lanes_whichever_path_creates_them() -> TestResult {
    let cluster = Cluster::new(Schema::Current, true).await?;
    // Role A cannot call provision_lane, so its stores take the path a binary
    // that predates the function takes: they write the lane rows themselves.
    cluster
        .owner
        .execute(sqlx::raw_sql(sql(format!(
            "REVOKE EXECUTE ON FUNCTION awa.provision_lane(text, smallint, smallint) FROM PUBLIC, {}",
            cluster.runtime_name
        ))))
        .await?;
    cluster
        .owner
        .execute(sqlx::raw_sql(sql(format!(
            "GRANT EXECUTE ON FUNCTION awa.provision_lane(text, smallint, smallint) TO {}, {}",
            cluster.roles[4], cluster.roles[5]
        ))))
        .await?;
    let can_provision: bool = sqlx::query_scalar(
        "SELECT has_function_privilege('awa.provision_lane(text,smallint,smallint)', 'EXECUTE')",
    )
    .fetch_one(&cluster.app_a)
    .await?;
    assert!(!can_provision);

    enqueue(&cluster.app_a, "fleet_queue", 1, 2).await?;
    enqueue(&cluster.app_b, "fleet_queue", 2, 2).await?;
    enqueue(&cluster.app_b, "fleet_queue", 1, 2).await?;
    enqueue(&cluster.app_a, "fleet_queue", 2, 2).await?;
    assert_eq!(drain(&cluster.app_a, "fleet_queue").await?, 8);
    cluster
        .assert_lanes_owned_by_owner("fleet_queue", 2)
        .await?;

    // A provisioner that disappears after a store has used it: the next lane
    // falls back inside the same transaction instead of failing the enqueue.
    let producer = store();
    producer
        .enqueue_batch(&cluster.app_b, "fleet_queue", 3, 1)
        .await?;
    cluster
        .owner
        .execute("DROP FUNCTION awa.provision_lane(text, smallint, smallint)")
        .await?;
    producer
        .enqueue_batch(&cluster.app_b, "fleet_queue", 4, 1)
        .await?;
    enqueue(&cluster.app_c, "fleet_queue", 4, 1).await?;
    assert_eq!(drain(&cluster.app_c, "fleet_queue").await?, 3);
    cluster
        .assert_lanes_owned_by_owner("fleet_queue", 4)
        .await?;
    cluster.teardown().await
}

#[tokio::test]
async fn binary_ahead_of_the_schema_creates_lanes_as_before() -> TestResult {
    let cluster = Cluster::new(Schema::BeforeProvisioning, false).await?;
    let has_provisioner: bool = sqlx::query_scalar(
        "SELECT to_regprocedure('awa.provision_lane(text,smallint,smallint)') IS NOT NULL",
    )
    .fetch_one(&cluster.owner)
    .await?;
    assert!(!has_provisioner);

    // The owner creates lanes lazily, as on v052.
    enqueue(&cluster.owner, "ahead_queue", 2, 2).await?;
    enqueue(&cluster.app_b, "ahead_queue", 2, 1).await?;
    assert_eq!(drain(&cluster.app_b, "ahead_queue").await?, 3);
    // A runtime without schema CREATE still gets v052's provisioning hint.
    let error = enqueue(&cluster.app_a, "ahead_queue", 3, 1)
        .await
        .unwrap_err();
    let AwaError::Database(sqlx::Error::Database(db_error)) = &error else {
        panic!("expected a database error, got {error:?}");
    };
    assert_eq!(db_error.code().as_deref(), Some("42501"));
    assert!(db_error.message().contains("has not been provisioned"));
    cluster.teardown().await
}

#[tokio::test]
async fn concurrent_first_touch_of_one_lane_by_two_roles() -> TestResult {
    let cluster = Cluster::new(Schema::Current, true).await?;

    // A holds a brand-new lane's creation open; B's first touch waits for it
    // and then uses A's sequences.
    let mut holding = cluster.app_a.begin().await?;
    sqlx::query("SELECT awa.provision_lane('raced_queue', 2::smallint, 0::smallint)")
        .execute(holding.as_mut())
        .await?;
    let app_b = cluster.app_b.clone();
    let waiting = tokio::spawn(async move { enqueue(&app_b, "raced_queue", 2, 1).await });
    let mut blocked = false;
    for _ in 0..100 {
        blocked = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted \
             AND database = (SELECT oid FROM pg_database WHERE datname = current_database()))",
        )
        .fetch_one(&cluster.owner)
        .await?;
        if blocked {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(blocked, "B's first touch should wait on A's lane lock");
    holding.commit().await?;
    assert_eq!(waiting.await??, 1);

    // Unordered races on many fresh lanes.
    for round in 0..20 {
        let queue = format!("raced_queue_{round}");
        let (from_a, from_b) = tokio::join!(
            enqueue(&cluster.app_a, &queue, 1, 1),
            enqueue(&cluster.app_b, &queue, 1, 1)
        );
        assert_eq!(from_a? + from_b?, 2);
        cluster.assert_lanes_owned_by_owner(&queue, 1).await?;
        assert_eq!(drain(&cluster.app_c, &queue).await?, 2);
    }
    assert_eq!(drain(&cluster.app_c, "raced_queue").await?, 1);
    cluster
        .assert_lanes_owned_by_owner("raced_queue", 1)
        .await?;
    cluster.teardown().await
}

#[tokio::test]
async fn migration_repairs_lanes_it_may_and_reports_the_rest() -> TestResult {
    let mut cluster = Cluster::new(Schema::BeforeProvisioning, true).await?;
    // Lanes as a 0.6 fleet left them: one each created by an app role, by the
    // migration role acting as itself, and by the owner without the runtime's
    // grant.
    enqueue(&cluster.app_a, "app_lane", 1, 1).await?;
    enqueue(&cluster.migrator, "migrator_lane", 1, 1).await?;
    enqueue(&cluster.owner, "owner_lane", 1, 1).await?;
    for (sequence, _) in cluster.lane_sequence_owners("owner_lane").await? {
        cluster
            .owner
            .execute(sqlx::raw_sql(sql(format!(
                "REVOKE ALL ON SEQUENCE awa.{sequence} FROM {}",
                cluster.runtime_name
            ))))
            .await?;
    }
    for lane in ["owner_lane", "app_lane"] {
        let failed = drain(&cluster.app_b, lane).await.unwrap_err();
        assert!(
            failed
                .to_string()
                .contains("permission denied for sequence"),
            "{failed}"
        );
    }
    // 0.7 refuses to migrate while app roles hold CREATE on the schema.
    cluster
        .owner
        .execute(sqlx::raw_sql(sql(format!(
            "REVOKE CREATE ON SCHEMA awa FROM {}",
            cluster.runtime_name
        ))))
        .await?;

    // `awa migrate` connects as the migration role and acts as the owner. As
    // the owner it adds the grants the owner's sequences lack; the migration
    // role's own sequences need the migration role's privileges, so they are
    // reassigned as the login role. Neither can act as the app role.
    let migrate_pool = cluster.migrator_as_owner().await?;
    tokio::time::timeout(Duration::from_secs(120), migrations::run(&migrate_pool)).await??;
    assert_eq!(
        migrations::current_version(&cluster.owner).await?,
        migrations::CURRENT_VERSION
    );
    migrate_pool.close().await;
    cluster
        .assert_lanes_owned_by_owner("migrator_lane", 1)
        .await?;
    cluster.assert_lanes_owned_by_owner("owner_lane", 1).await?;
    assert_eq!(drain(&cluster.app_b, "migrator_lane").await?, 1);
    assert_eq!(drain(&cluster.app_b, "owner_lane").await?, 1);

    let app_a = cluster.roles[3].clone();
    let app_a = &app_a;
    let problems = storage::lane_sequence_problems(&cluster.app_c, "awa").await?;
    assert_eq!(problems.len(), 2, "{problems:?}");
    for problem in &problems {
        assert_eq!(&problem.owner, app_a);
        assert_eq!(problem.expected_owner, cluster.owner_name);
        assert_eq!(
            problem.missing_grants,
            vec![format!(
                "GRANT USAGE, UPDATE, SELECT ON SEQUENCE awa.{} TO {}",
                problem.sequence_name, cluster.runtime_name
            )]
        );
        assert_eq!(
            problem.reassign_sql.as_deref(),
            Some(
                format!(
                    "ALTER SEQUENCE awa.{} OWNER TO {}",
                    problem.sequence_name, cluster.owner_name
                )
                .as_str()
            )
        );
        assert!(problem.repair_roles.is_empty(), "{problem:?}");
    }
    let next_steps =
        storage::lane_sequence_next_steps(problems.iter().map(|problem| (problem, false)));
    assert_eq!(next_steps.len(), 1, "{next_steps:?}");
    assert!(
        next_steps[0].contains(&format!("no login role can both act as {app_a}")),
        "{next_steps:?}"
    );

    // Re-running the repair as the migration role changes nothing and fails
    // nothing.
    cluster
        .owner
        .execute(sqlx::raw_sql(sql(format!(
            "GRANT EXECUTE ON FUNCTION awa.repair_lane_sequence(text, text) TO {}",
            cluster.runtime_name
        ))))
        .await?;
    let rerun =
        storage::repair_lane_sequences(&mut *cluster.migrator.acquire().await?, "awa").await?;
    assert_eq!(rerun.len(), 2);
    assert!(rerun
        .iter()
        .all(|result| result.outcome == LaneSequenceOutcome::Unrepaired));

    // A role that can act as the app role but cannot become the owner adds
    // the grants and leaves the owner for someone who can.
    let delegate = format!("{}_delegate", cluster.runtime_name);
    cluster
        .admin
        .execute(sqlx::raw_sql(sql(format!(
            "CREATE ROLE {delegate} LOGIN PASSWORD 'lane_test' IN ROLE {app_a}; \
             GRANT CONNECT ON DATABASE {} TO {delegate}",
            cluster.database
        ))))
        .await?;
    let delegate_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(cluster.connect_options.clone().username(&delegate))
        .await?;
    let delegated =
        storage::repair_lane_sequences(&mut *delegate_pool.acquire().await?, "awa").await?;
    delegate_pool.close().await;
    assert_eq!(delegated.len(), 2);
    for result in &delegated {
        assert_eq!(result.outcome, LaneSequenceOutcome::OwnerUnchanged);
        assert_eq!(&result.owner, app_a);
        assert!(result.missing_grants.is_empty());
        assert!(result.reassign_sql.is_some());
    }
    assert_eq!(drain(&cluster.app_b, "app_lane").await?, 1);
    enqueue(&cluster.app_c, "app_lane", 1, 1).await?;

    // Once the migration role is a member of the app role too, the check names
    // it, and a repair connected as it reassigns.
    cluster
        .admin
        .execute(sqlx::raw_sql(sql(format!(
            "GRANT {app_a} TO {migrator}",
            migrator = cluster.migrator_name
        ))))
        .await?;
    let problems = storage::lane_sequence_problems(&cluster.app_c, "awa").await?;
    assert!(problems
        .iter()
        .all(|problem| problem.repair_roles == vec![cluster.migrator_name.clone()]));
    let repaired =
        storage::repair_lane_sequences(&mut *cluster.migrator.acquire().await?, "awa").await?;
    assert_eq!(repaired.len(), 2);
    assert!(
        repaired
            .iter()
            .all(|result| result.outcome == LaneSequenceOutcome::Repaired),
        "{repaired:?}"
    );
    assert!(storage::lane_sequence_problems(&cluster.app_c, "awa")
        .await?
        .is_empty());
    cluster.assert_lanes_owned_by_owner("app_lane", 1).await?;
    assert_eq!(drain(&cluster.app_b, "app_lane").await?, 1);

    cluster.roles.push(delegate);
    cluster.teardown().await
}

/// Reassigning a lane sequence waits on any open enqueue or claim on the lane,
/// so the repair bounds that wait and holds each sequence's lock only for its
/// own short transaction. A held sequence is skipped and reported; the rest
/// are repaired, and a later repair finishes it.
#[tokio::test]
async fn migration_skips_a_lane_sequence_an_open_transaction_holds() -> TestResult {
    let cluster = Cluster::new(Schema::BeforeProvisioning, true).await?;
    enqueue(&cluster.migrator, "held_lane", 1, 1).await?;
    enqueue(&cluster.migrator, "free_lane", 1, 1).await?;
    let held_sequence = cluster.enqueue_sequence("held_lane").await?;
    // 0.7 refuses to migrate while app roles hold CREATE on the schema.
    cluster
        .owner
        .execute(sqlx::raw_sql(sql(format!(
            "REVOKE CREATE ON SCHEMA awa FROM {}",
            cluster.runtime_name
        ))))
        .await?;

    // An enqueue in flight on held_lane holds its sequence's lock.
    let mut holder = cluster.app_b.begin().await?;
    sqlx::query("SELECT nextval($1::regclass)")
        .bind(format!("awa.{held_sequence}"))
        .execute(&mut *holder)
        .await?;

    let migrate_pool = cluster.migrator_as_owner().await?;
    // The migration completes while the lock is held.
    tokio::time::timeout(Duration::from_secs(120), migrations::run(&migrate_pool)).await??;
    for (sequence, owner) in cluster.lane_sequence_owners("held_lane").await? {
        let expected = if sequence == held_sequence {
            &cluster.migrator_name
        } else {
            &cluster.owner_name
        };
        assert_eq!(&owner, expected, "{sequence}");
    }
    cluster.assert_lanes_owned_by_owner("free_lane", 1).await?;

    // A repair while the transaction is still open reports the sequence busy.
    let mut conn = migrate_pool.acquire().await?;
    let busy = storage::repair_lane_sequences(&mut conn, "awa").await?;
    assert_eq!(busy.len(), 1, "{busy:?}");
    assert_eq!(busy[0].sequence_name, held_sequence);
    assert_eq!(busy[0].outcome, LaneSequenceOutcome::Busy);
    assert_eq!(busy[0].owner, cluster.migrator_name);
    let next_steps = storage::lane_sequence_repair_next_steps(&busy);
    assert_eq!(next_steps.len(), 1);
    assert!(next_steps[0].contains("busy"), "{next_steps:?}");
    assert!(
        next_steps[0].contains("awa storage lane-sequences --schema awa --repair"),
        "{next_steps:?}"
    );

    holder.rollback().await?;
    let repaired = storage::repair_lane_sequences(&mut conn, "awa").await?;
    assert_eq!(repaired.len(), 1, "{repaired:?}");
    assert_eq!(repaired[0].outcome, LaneSequenceOutcome::Repaired);
    drop(conn);
    migrate_pool.close().await;
    cluster.assert_lanes_owned_by_owner("held_lane", 1).await?;
    assert!(storage::lane_sequence_problems(&cluster.app_c, "awa")
        .await?
        .is_empty());
    assert_eq!(drain(&cluster.app_b, "held_lane").await?, 1);
    cluster.teardown().await
}
