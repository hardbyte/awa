use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// API error wrapper around AwaError, mapping to HTTP status codes.
pub struct ApiError(pub awa_model::AwaError);

impl From<awa_model::AwaError> for ApiError {
    fn from(err: awa_model::AwaError) -> Self {
        ApiError(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self.0 {
            awa_model::AwaError::JobNotFound { .. } => (StatusCode::NOT_FOUND, self.0.to_string()),
            awa_model::AwaError::CallbackNotFound { .. } => {
                (StatusCode::NOT_FOUND, self.0.to_string())
            }
            awa_model::AwaError::Validation(_) => (StatusCode::BAD_REQUEST, self.0.to_string()),
            awa_model::AwaError::UniqueConflict { .. } => {
                (StatusCode::CONFLICT, self.0.to_string())
            }
            awa_model::AwaError::Database(_) | awa_model::AwaError::Serialization(_) => {
                tracing::error!(error = %self.0, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
            _ => {
                tracing::error!(error = %self.0, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };

        let body = axum::Json(json!({ "error": message }));
        (status, body).into_response()
    }
}
