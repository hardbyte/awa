use axum::extract::State;
use axum::Json;

use awa_model::admin;

use crate::error::ApiError;
use crate::state::AppState;

pub async fn list_kinds(
    State(state): State<AppState>,
) -> Result<Json<Vec<admin::JobKindOverview>>, ApiError> {
    let kinds = admin::job_kind_overviews(&state.pool).await?;
    Ok(Json(kinds))
}
