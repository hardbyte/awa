//! awa's own transactions run at READ COMMITTED whatever the session's
//! default isolation level (#847).

use awa_model::isolation::BeginReadCommitted;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

#[sqlx::test(migrations = false)]
async fn own_transactions_ignore_a_repeatable_read_default(
    pool: PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repeatable_read = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(
            pool.connect_options()
                .as_ref()
                .clone()
                .options([("default_transaction_isolation", "repeatable read")]),
        )
        .await?;

    let mut inherited = repeatable_read.begin().await?;
    let level: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *inherited)
        .await?;
    inherited.rollback().await?;
    assert_eq!(level, "repeatable read", "begin() inherits the default");

    let mut own = repeatable_read.begin_read_committed().await?;
    let level: String = sqlx::query_scalar("SELECT current_setting('transaction_isolation')")
        .fetch_one(&mut *own)
        .await?;
    own.rollback().await?;
    assert_eq!(level, "read committed");
    Ok(())
}
