//! A caller's temporary objects cannot answer for a table inside a function
//! that runs with its owner's privileges: `<schema>.enqueue_ring_cursor()`
//! (v062) and every other `SECURITY DEFINER` function, with everything they
//! call.

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

/// Functions in `awa` and `schema` that set a `search_path` which does not list
/// `pg_temp` last, or that lists `public`, with that setting.
async fn unpinned_functions(
    pool: &PgPool,
    schemas: &[&str],
) -> Result<Vec<(String, String)>, sqlx::Error> {
    let schemas: Vec<String> = schemas.iter().map(|schema| schema.to_string()).collect();
    sqlx::query_as(
        "SELECT p.oid::regprocedure::text, substr(setting, length('search_path=') + 1)
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         CROSS JOIN LATERAL unnest(p.proconfig) AS setting
         WHERE n.nspname = ANY($1)
           AND setting LIKE 'search_path=%'
           AND (setting !~ '(^|, )pg_temp$' OR setting ~ '(=|, )public(,|$)')
         ORDER BY 1",
    )
    .bind(&schemas)
    .fetch_all(pool)
    .await
}

async fn functions_with_a_search_path(pool: &PgPool, schemas: &[&str]) -> Result<i64, sqlx::Error> {
    let schemas: Vec<String> = schemas.iter().map(|schema| schema.to_string()).collect();
    sqlx::query_scalar(
        "SELECT count(*)
         FROM pg_proc p
         JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = ANY($1)
           AND EXISTS (SELECT 1 FROM unnest(p.proconfig) AS setting WHERE setting LIKE 'search_path=%')",
    )
    .bind(&schemas)
    .fetch_one(pool)
    .await
}

/// Every function awa installs that sets a `search_path`, in `awa` and in a
/// custom queue-storage schema, lists `pg_temp` last and leaves `public` out,
/// so no function that runs with its owner's privileges, at any depth below a
/// `SECURITY DEFINER` function or a trigger, resolves a table through a
/// caller's temporary schema.
#[sqlx::test(migrations = false)]
async fn every_function_search_path_lists_pg_temp_last_without_public(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    install_custom(&pool).await?;
    let checked = functions_with_a_search_path(&pool, &["awa", CUSTOM]).await?;
    assert!(checked > 50, "the query found awa's functions: {checked}");
    let unpinned = unpinned_functions(&pool, &["awa", CUSTOM]).await?;
    assert!(
        unpinned.is_empty(),
        "functions whose search_path does not end in pg_temp or lists public: {unpinned:#?}"
    );
    Ok(())
}

/// A database whose functions still carry the paths older releases gave them
/// gets them pinned by the next migration step, as an upgrade does.
#[sqlx::test(migrations = false)]
async fn a_migration_step_pins_older_search_paths(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    install_custom(&pool).await?;
    for schema in ["awa", CUSTOM] {
        sqlx::raw_sql(audited_sql(format!(
            "DO $$
             DECLARE v_function REGPROCEDURE;
             BEGIN
                 FOR v_function IN
                     SELECT p.oid::regprocedure
                     FROM pg_proc p
                     WHERE p.pronamespace = '{schema}'::regnamespace
                       AND EXISTS (SELECT 1 FROM unnest(p.proconfig) AS setting
                                   WHERE setting LIKE 'search_path=%')
                 LOOP
                     EXECUTE format(
                         'ALTER ROUTINE %s SET search_path = pg_catalog, {schema}, public',
                         v_function
                     );
                 END LOOP;
             END;
             $$;"
        )))
        .execute(&pool)
        .await?;
    }
    ensure!(
        !unpinned_functions(&pool, &["awa", CUSTOM])
            .await?
            .is_empty(),
        "the old paths were put back"
    );

    for (_, _, sql) in migrations::migration_sql_range(61, 62) {
        sqlx::raw_sql(audited_sql(sql)).execute(&pool).await?;
    }

    let unpinned = unpinned_functions(&pool, &["awa", CUSTOM]).await?;
    ensure!(
        unpinned.is_empty(),
        "after the v062 step, functions whose search_path does not end in pg_temp or lists public: {unpinned:#?}"
    );
    Ok(())
}

/// The call that exercises each `SECURITY DEFINER` function a runtime role may
/// execute, in `schema`. A new definer function must be added here.
fn definer_call(schema: &str, name: &str) -> Option<String> {
    Some(match name {
        "enqueue_ring_cursor" => format!("SELECT * FROM {schema}.enqueue_ring_cursor()"),
        "provision_lane" => {
            format!("SELECT {schema}.provision_lane('shadow_queue', 0::smallint, 0::smallint)")
        }
        "provision_lane_sequences" => format!(
            "SELECT {schema}.provision_lane_sequences('shadow_queue', 0::smallint, 1::smallint)"
        ),
        "ensure_claim_batch_job_index" => {
            format!("SELECT {schema}.ensure_claim_batch_job_index(0)")
        }
        _ => return None,
    })
}

/// A caller shadows every table and view in `awa` and a custom queue-storage
/// schema with a temporary view that records being read, then calls every
/// `SECURITY DEFINER` function it may execute. None of them, nor anything they
/// call or fire, may read a shadow: each would have run the caller's code with
/// the owner's privileges.
#[sqlx::test(migrations = false)]
async fn definer_functions_never_read_a_callers_temporary_relation(pool: PgPool) -> TestResult {
    migrations::run(&pool).await?;
    install_custom(&pool).await?;
    let caller = create_caller(&pool, &["awa", CUSTOM]).await?;
    let result = async {
        // One shadow per relation name, with that relation's columns, so a
        // query that reached it would plan and read it. Each column depends
        // on the read, so the planner cannot fold a filter on it to a
        // constant and skip the read.
        let shadows: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT ON (c.relname)
                 format(
                     'CREATE TEMP VIEW %I AS SELECT %s FROM (SELECT pg_temp.shadow_read(%L) AS done) AS read',
                     c.relname,
                     COALESCE(
                         (SELECT string_agg(
                                     format('CASE WHEN read.done THEN NULL::%s END AS %I',
                                            format_type(a.atttypid, a.atttypmod), a.attname),
                                     ', ' ORDER BY a.attnum)
                          FROM pg_attribute a
                          WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
                         ''
                     ),
                     c.relname
                 )
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname IN ('awa', $1)
               AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
             ORDER BY c.relname, n.nspname",
        )
        .bind(CUSTOM)
        .fetch_all(&pool)
        .await?;
        ensure!(shadows.len() > 50, "found awa's relations: {}", shadows.len());

        let definers: Vec<(String, String)> = sqlx::query_as(
            "SELECT n.nspname::text, p.proname::text
             FROM pg_proc p
             JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname IN ('awa', $1)
               AND p.prosecdef
               AND has_function_privilege($2, p.oid, 'EXECUTE')
             ORDER BY 1, 2",
        )
        .bind(CUSTOM)
        .bind(&caller)
        .fetch_all(&pool)
        .await?;
        ensure!(
            definers.iter().any(|(_, name)| name == "enqueue_ring_cursor"),
            "found the definer functions: {definers:?}"
        );

        let mut session = connect_as(&pool, &caller).await?;
        sqlx::raw_sql(
            "CREATE TEMP TABLE shadow_reads_log (who TEXT, relation TEXT);
             CREATE FUNCTION pg_temp.shadow_read(p_relation TEXT) RETURNS BOOLEAN
             LANGUAGE sql VOLATILE
             AS $$ INSERT INTO pg_temp.shadow_reads_log VALUES (current_user, p_relation) RETURNING TRUE $$;",
        )
        .execute(&mut session)
        .await?;
        for shadow in &shadows {
            sqlx::raw_sql(audited_sql(shadow.clone()))
                .execute(&mut session)
                .await?;
        }

        let mut findings = Vec::new();
        for (schema, name) in &definers {
            let Some(call) = definer_call(schema, name) else {
                findings.push(format!(
                    "{schema}.{name} is SECURITY DEFINER and executable by a runtime role; \
                     add a call for it to definer_call"
                ));
                continue;
            };
            if let Err(err) = sqlx::raw_sql(audited_sql(call.clone()))
                .execute(&mut session)
                .await
            {
                findings.push(format!("{call}: {err}"));
            }
            let reads: Vec<(String, String)> =
                sqlx::query_as("DELETE FROM pg_temp.shadow_reads_log RETURNING who, relation")
                    .fetch_all(&mut session)
                    .await?;
            if !reads.is_empty() {
                findings.push(format!("{call} read the caller's shadows as {reads:?}"));
            }
        }
        session.close().await?;
        ensure!(findings.is_empty(), "{}", findings.join("\n"));
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    drop_caller(&pool, &caller).await?;
    result
}
