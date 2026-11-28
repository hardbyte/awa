use awa_model::batch_operations::{
    self, BatchOperation, BatchOperationFilter, BatchOperationKind, BatchOperationPreview,
    BatchOperationSpec, BatchOperationState, ListBatchOperationsFilter, SubmitBatchOperation,
};
use axum::extract::{Path, Query, State};
use axum::Json;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use uuid::Uuid;

use crate::error::ApiError;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct BatchOperationPayload {
    pub op_kind: BatchOperationKind,
    pub filter: BatchOperationFilter,
    pub spec: serde_json::Value,
    #[serde(default)]
    pub submitted_by: Option<String>,
    #[serde(default)]
    pub all: bool,
}

impl BatchOperationPayload {
    fn decode_spec(
        self,
    ) -> Result<
        (
            BatchOperationSpec,
            BatchOperationFilter,
            Option<String>,
            bool,
        ),
        ApiError,
    > {
        let mut spec = match self.spec {
            serde_json::Value::Object(spec) => spec,
            _ => {
                return Err(awa_model::AwaError::Validation(
                    "spec must be a JSON object".to_string(),
                )
                .into())
            }
        };
        if spec.contains_key("op_kind") {
            return Err(awa_model::AwaError::Validation(
                "spec must not repeat op_kind".to_string(),
            )
            .into());
        }
        spec.insert("op_kind".to_string(), self.op_kind.as_str().into());
        let spec: BatchOperationSpec = serde_json::from_value(serde_json::Value::Object(spec))?;
        Ok((spec, self.filter, self.submitted_by, self.all))
    }
}

/// `GET /api/batch-ops` parameters. `before_submitted_at` (RFC 3339) and
/// `before_id` (UUID) together are a keyset cursor matching the list's
/// `(submitted_at, id)` newest-first order: pass the last row's values to
/// read the page after it.
#[derive(Debug, Deserialize)]
pub struct ListBatchOperationsParams {
    pub state: Option<BatchOperationState>,
    pub limit: Option<i64>,
    pub before_submitted_at: Option<String>,
    pub before_id: Option<String>,
}

impl ListBatchOperationsParams {
    fn cursor(&self) -> Result<Option<(DateTime<Utc>, Uuid)>, ApiError> {
        let invalid = |name: &str, value: &str| {
            ApiError::from(awa_model::AwaError::Validation(format!(
                "invalid value for `{name}`: {value:?}"
            )))
        };
        match (&self.before_submitted_at, &self.before_id) {
            (None, None) => Ok(None),
            (Some(at), Some(id)) => {
                let at = DateTime::parse_from_rfc3339(at.trim())
                    .map_err(|_| invalid("before_submitted_at", at))?
                    .with_timezone(&Utc);
                let id = Uuid::parse_str(id.trim()).map_err(|_| invalid("before_id", id))?;
                Ok(Some((at, id)))
            }
            _ => Err(awa_model::AwaError::Validation(
                "`before_submitted_at` and `before_id` must be given together".to_string(),
            )
            .into()),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PatchBatchOperationPayload {
    pub state: BatchOperationState,
}

pub async fn preview_batch_operation(
    State(state): State<AppState>,
    Json(payload): Json<BatchOperationPayload>,
) -> Result<Json<BatchOperationPreview>, ApiError> {
    state.require_writable()?;
    let (spec, filter, _, _) = payload.decode_spec()?;
    let preview = batch_operations::preview_batch_operation(&state.pool, spec, filter).await?;
    Ok(Json(preview))
}

pub async fn submit_batch_operation(
    State(state): State<AppState>,
    Json(payload): Json<BatchOperationPayload>,
) -> Result<Json<BatchOperation>, ApiError> {
    state.require_writable()?;
    let (spec, filter, submitted_by, all) = payload.decode_spec()?;
    let operation = batch_operations::submit_batch_operation(
        &state.pool,
        SubmitBatchOperation {
            spec,
            filter,
            submitted_by,
            allow_all: all,
        },
    )
    .await?;
    state.invalidate_dashboard_caches();
    Ok(Json(operation))
}

pub async fn list_batch_operations(
    State(state): State<AppState>,
    Query(params): Query<ListBatchOperationsParams>,
) -> Result<Json<Vec<BatchOperation>>, ApiError> {
    let before = params.cursor()?;
    let operations = batch_operations::list_batch_operations(
        &state.pool,
        &ListBatchOperationsFilter {
            state: params.state,
            limit: params.limit,
            before,
        },
    )
    .await?;
    Ok(Json(operations))
}

/// `GET /api/batch-ops/{id}`: 404 for an unknown id, and for a path segment
/// that is not a UUID, since no operation can have it.
pub async fn get_batch_operation(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<BatchOperation>, ApiError> {
    let not_found = || ApiError::not_found(format!("batch operation not found: {id}"));
    let uuid = Uuid::parse_str(&id).map_err(|_| not_found())?;
    batch_operations::find_batch_operation(&state.pool, uuid)
        .await?
        .map(Json)
        .ok_or_else(not_found)
}

pub async fn patch_batch_operation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(payload): Json<PatchBatchOperationPayload>,
) -> Result<Json<BatchOperation>, ApiError> {
    state.require_writable()?;
    match payload.state {
        BatchOperationState::Cancelling => {
            let operation =
                batch_operations::request_batch_operation_cancellation(&state.pool, id).await?;
            state.invalidate_dashboard_caches();
            Ok(Json(operation))
        }
        _ => Err(awa_model::AwaError::Validation(
            "only state=cancelling is supported for batch operation PATCH".to_string(),
        )
        .into()),
    }
}
