use serde::Serialize;
use sqlx::PgPool;

use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub read_only: bool,
}

impl AppState {
    pub fn new(pool: PgPool, read_only: bool) -> Self {
        Self { pool, read_only }
    }

    pub fn require_writable(&self) -> Result<(), ApiError> {
        if self.read_only {
            return Err(ApiError::read_only());
        }
        Ok(())
    }
}

pub async fn detect_read_only(pool: &PgPool) -> Result<bool, sqlx::Error> {
    let read_only =
        sqlx::query_scalar::<_, bool>("SELECT current_setting('transaction_read_only') = 'on'")
            .fetch_one(pool)
            .await?;
    Ok(read_only)
}

#[derive(Debug, Serialize)]
pub struct Capabilities {
    pub read_only: bool,
}
