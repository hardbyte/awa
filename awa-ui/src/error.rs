use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// API error wrapper around AwaError, mapping to HTTP status codes.
#[derive(Debug)]
pub enum ApiError {
    Awa(awa_model::AwaError),
    ReadOnly,
    NotFound(String),
    Unauthorized(String),
    ObservationBusy,
    ObservationChanged,
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

impl From<serde_json::Error> for ApiError {
    fn from(err: serde_json::Error) -> Self {
        ApiError::Awa(awa_model::AwaError::Validation(err.to_string()))
    }
}

impl From<std::sync::Arc<crate::cache::CacheError>> for ApiError {
    fn from(err: std::sync::Arc<crate::cache::CacheError>) -> Self {
        // Moka wraps try_get_with errors in Arc. Extract the inner AwaError
        // if we're the only holder, otherwise create a database-level error
        // from the description.
        let cache_err = std::sync::Arc::try_unwrap(err).unwrap_or_else(|arc| (*arc).clone());
        match cache_err {
            crate::cache::CacheError::Busy => ApiError::ObservationBusy,
            crate::cache::CacheError::Changed => ApiError::ObservationChanged,
            crate::cache::CacheError::Database(error) => {
                ApiError::Awa(std::sync::Arc::try_unwrap(error).unwrap_or_else(|arc| {
                    awa_model::AwaError::Database(sqlx::Error::Protocol(arc.to_string()))
                }))
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let retry = matches!(&self, Self::ObservationBusy | Self::ObservationChanged);
        let (status, message) = match self {
            ApiError::ObservationBusy => (
                StatusCode::TOO_MANY_REQUESTS,
                "aggregate read admission is full".to_string(),
            ),
            ApiError::ObservationChanged => (
                StatusCode::SERVICE_UNAVAILABLE,
                "aggregate changed during read; retry".to_string(),
            ),
            ApiError::ReadOnly => (
                StatusCode::SERVICE_UNAVAILABLE,
                "awa serve is connected to a read-only database; admin actions are disabled"
                    .to_string(),
            ),
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, message),
            ApiError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
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
        let mut response = (status, body).into_response();
        if retry {
            response
                .headers_mut()
                .insert("retry-after", "1".parse().unwrap());
            response
                .headers_mut()
                .insert("cache-control", "private, no-store".parse().unwrap());
        }
        response
    }
}

impl ApiError {
    pub fn read_only() -> Self {
        ApiError::ReadOnly
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        ApiError::Unauthorized(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        ApiError::NotFound(message.into())
    }
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    #[test]
    fn transient_cache_errors_preserve_status_and_retry_after() {
        for (error, status) in [
            (
                crate::cache::CacheError::Busy,
                StatusCode::TOO_MANY_REQUESTS,
            ),
            (
                crate::cache::CacheError::Changed,
                StatusCode::SERVICE_UNAVAILABLE,
            ),
        ] {
            let response = ApiError::from(std::sync::Arc::new(error)).into_response();
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()["retry-after"], "1");
            assert_eq!(response.headers()["cache-control"], "private, no-store");
        }
    }
}
