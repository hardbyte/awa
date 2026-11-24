//! A caller's temporary objects cannot answer for the queue ring's cursor
//! tables inside `<schema>.enqueue_ring_cursor()` (v062), which runs as the
//! owner of the queue-storage tables for any caller.

use awa_model::{audited_sql, migrations, QueueStorage, QueueStorageConfig};
use sqlx::{Connection, PgConnection, PgPool};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Like `assert!`, but returns an error, so a test that created a
/// cluster-wide role still drops it before it reports the failure.
macro_rules! ensure {
    ($cond:expr, $($arg:tt)+) => {
        if !$cond {
            return Err(format!($($arg)+).into());
        }
    };
}

/// Like `assert_eq!`, but returns an error; see [`ensure!`].
macro_rules! ensure_eq {
    ($left:expr, $right:expr, $($arg:tt)+) => {{
        let (left, right) = (&$left, &$right);
        if left != right {
            return Err(format!("{}: left {:?}, right {:?}", format!($($arg)+), left, right).into());
        }
    }};
}

const CUSTOM: &str = "ring_cursor_custom";
const PASSWORD: &str = "ring_cursor_test";

async fn install_custom(pool: &PgPool) -> TestResult {
    QueueStorage::new(QueueStorageConfig {
        schema: CUSTOM.into(),
        queue_slot_count: 4,
        lease_slot_count: 2,
        claim_slot_count: 2,
        ..Default::default()
    })?
    .install(pool)
    .await?;
    Ok(())
}

/// A login role with only `USAGE` on each schema: what any runtime role has.
/// `EXECUTE` on `enqueue_ring_cursor()` comes from `PUBLIC`, and `TEMP` on
/// the database from `PUBLIC` by default.
async fn create_caller(pool: &PgPool, schemas: &[&str]) -> Result<String, sqlx::Error> {
    let role = format!("ring_cursor_caller_{}", uuid::Uuid::new_v4().simple());
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(pool)
        .await?;
    let mut sql = format!(
        "CREATE ROLE {role} LOGIN PASSWORD '{PASSWORD}';
         GRANT CONNECT, TEMP ON DATABASE \"{database}\" TO {role};"
    );
    for schema in schemas {
        sql.push_str(&format!("GRANT USAGE ON SCHEMA {schema} TO {role};"));
    }
    sqlx::raw_sql(audited_sql(sql)).execute(pool).await?;
    Ok(role)
}

async fn drop_caller(pool: &PgPool, role: &str) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(audited_sql(format!(
        "DROP OWNED BY {role}; DROP ROLE {role};"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

async fn connect_as(pool: &PgPool, role: &str) -> Result<PgConnection, sqlx::Error> {
    PgConnection::connect_with(
        &pool
            .connect_options()
            .as_ref()
            .clone()
            .username(role)
            .password(PASSWORD),
    )
    .await
}

/// As `role`, in one session, shadow `schema.ring_cursor_authority` with a
/// temporary view over a temporary function that records `current_user`, call
/// `schema.enqueue_ring_cursor()`, and return the cursor it gave and every
/// role the function ran as.
async fn attack(
    pool: &PgPool,
    role: &str,
    schema: &str,
) -> Result<((i32, i64), Vec<String>), Box<dyn std::error::Error>> {
    let mut session = connect_as(pool, role).await?;
    sqlx::raw_sql(
        "CREATE TEMP TABLE IF NOT EXISTS seen (who TEXT);
         TRUNCATE pg_temp.seen;
         CREATE OR REPLACE FUNCTION pg_temp.spy() RETURNS TEXT
         LANGUAGE sql VOLATILE
         AS $$ INSERT INTO pg_temp.seen VALUES (current_user) RETURNING 'columns' $$;
         CREATE OR REPLACE TEMP VIEW ring_cursor_authority AS
             SELECT TRUE AS singleton, pg_temp.spy() AS authority;",
    )
    .execute(&mut session)
    .await?;
    let cursor: (i32, i64) = sqlx::query_as(audited_sql(format!(
        "SELECT slot, generation FROM {schema}.enqueue_ring_cursor()"
    )))
    .fetch_one(&mut session)
    .await?;
    let seen: Vec<String> = sqlx::query_scalar("SELECT who FROM pg_temp.seen")
        .fetch_all(&mut session)
        .await?;
    session.close().await?;
    Ok((cursor, seen))
}

async fn committed_cursor(pool: &PgPool, schema: &str) -> Result<(i32, i64), sqlx::Error> {
    sqlx::query_as(audited_sql(format!(
        "SELECT slot, generation FROM {schema}.ring_cursor('queue')"
    )))
    .fetch_one(pool)
    .await
}

#[sqlx::test(migrations = false)]
async fn enqueue_ring_cursor_never_reads_a_callers_temporary_view(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    install_custom(&pool).await?;
    let caller = create_caller(&pool, &["awa", CUSTOM]).await?;
    let result = async {
        for schema in ["awa", CUSTOM] {
            let (cursor, seen) = attack(&pool, &caller, schema).await?;
            ensure!(
                seen.is_empty(),
                "{schema}: the caller's temporary view was read, as {seen:?}"
            );
            ensure_eq!(
                cursor,
                committed_cursor(&pool, schema).await?,
                "{schema}: the cursor still comes from the ring"
            );
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    drop_caller(&pool, &caller).await?;
    result
}

/// Every `SECURITY DEFINER` function awa installs, and `ring_cursor()`, which
/// a definer once called, pins a `search_path` that lists `pg_temp` last, so a
/// caller's temporary objects never take the place of an unqualified table.
#[sqlx::test(migrations = false)]
async fn definer_functions_list_pg_temp_last(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    install_custom(&pool).await?;
    let functions: Vec<(String, Option<Vec<String>>)> = sqlx::query_as(
        "SELECT p.oid::regprocedure::text, p.proconfig
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname IN ('awa', $1)
           AND (p.prosecdef OR p.proname = 'ring_cursor')
         ORDER BY 1",
    )
    .bind(CUSTOM)
    .fetch_all(&pool)
    .await?;
    assert!(
        functions
            .iter()
            .any(|(name, _)| name.contains("enqueue_ring_cursor")),
        "the query found the definer functions: {functions:?}"
    );
    let unpinned: Vec<_> = functions
        .iter()
        .filter(|(_, config)| {
            !config.iter().flatten().any(|setting| {
                setting
                    .strip_prefix("search_path=")
                    .is_some_and(|path| path.trim_end().ends_with("pg_temp"))
            })
        })
        .collect();
    assert!(
        unpinned.is_empty(),
        "functions whose search_path does not end in pg_temp: {unpinned:?}"
    );
    Ok(())
}
