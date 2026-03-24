use serde::Serialize;
use sqlx::PgPool;

use crate::error::ApiError;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

impl AppState {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn require_writable(&self) -> Result<(), ApiError> {
        if self.is_read_only().await? {
            return Err(ApiError::read_only());
        }
        Ok(())
    }

    pub async fn is_read_only(&self) -> Result<bool, ApiError> {
        let read_only =
            sqlx::query_scalar::<_, bool>("SELECT current_setting('transaction_read_only') = 'on'")
                .fetch_one(&self.pool)
                .await?;
        Ok(read_only)
    }
}

#[derive(Debug, Serialize)]
pub struct Capabilities {
    pub read_only: bool,
}
