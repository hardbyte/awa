use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// API error wrapper around AwaError, mapping to HTTP status codes.
pub enum ApiError {
    Awa(awa_model::AwaError),
    ReadOnly,
}

impl From<awa_model::AwaError> for ApiError {
    fn from(err: awa_model::AwaError) -> Self {
        ApiError::Awa(err)
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        ApiError::Awa(err.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::ReadOnly => (
                StatusCode::SERVICE_UNAVAILABLE,
                "awa serve is connected to a read-only database; admin actions are disabled"
                    .to_string(),
            ),
            ApiError::Awa(err) => match &err {
                awa_model::AwaError::JobNotFound { .. } => (StatusCode::NOT_FOUND, err.to_string()),
                awa_model::AwaError::CallbackNotFound { .. } => {
                    (StatusCode::NOT_FOUND, err.to_string())
                }
                awa_model::AwaError::Validation(_) => (StatusCode::BAD_REQUEST, err.to_string()),
                awa_model::AwaError::UniqueConflict { .. } => {
                    (StatusCode::CONFLICT, err.to_string())
                }
                awa_model::AwaError::Database(_) | awa_model::AwaError::Serialization(_) => {
                    tracing::error!(error = %err, "internal error");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal server error".to_string(),
                    )
                }
                _ => {
                    tracing::error!(error = %err, "internal error");
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal server error".to_string(),
                    )
                }
            },
        };

        let body = axum::Json(json!({ "error": message }));
        (status, body).into_response()
    }
}

impl ApiError {
    pub fn read_only() -> Self {
        ApiError::ReadOnly
    }
}
