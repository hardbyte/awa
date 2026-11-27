use awa_model::batch_operations::{
    self, BatchOperation, BatchOperationFilter, BatchOperationKind, BatchOperationPreview,
    BatchOperationSpec, BatchOperationState, ListBatchOperationsFilter, SubmitBatchOperation,
};
use axum::extract::{Path, Query, State};
use axum::Json;
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

#[derive(Debug, Deserialize)]
pub struct ListBatchOperationsParams {
    pub state: Option<BatchOperationState>,
    pub limit: Option<i64>,
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
    let operations = batch_operations::list_batch_operations(
        &state.pool,
        &ListBatchOperationsFilter {
            state: params.state,
            limit: params.limit,
        },
    )
    .await?;
    Ok(Json(operations))
}

pub async fn get_batch_operation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<BatchOperation>, ApiError> {
    Ok(Json(
        batch_operations::get_batch_operation(&state.pool, id).await?,
    ))
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
