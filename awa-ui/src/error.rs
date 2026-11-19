use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;

/// What a bulk call's committed batches did before an error stopped it.
pub mod bulk {
    #[derive(Debug, Default)]
    pub struct Committed {
        pub changed: u64,
        pub skipped: u64,
        pub conflicts: Vec<i64>,
    }
}

/// API error wrapper around AwaError, mapping to HTTP status codes.
#[derive(Debug)]
pub enum ApiError {
    Awa(awa_model::AwaError),
    ReadOnly,
    NotFound(String),
    Unauthorized(String),
    Conflict(String),
    /// A 409 whose JSON body carries `detail`'s fields beside `error`.
    ConflictDetail {
        message: String,
        detail: serde_json::Map<String, serde_json::Value>,
    },
    /// `error` stopped a bulk call after earlier batches committed. The
    /// response keeps `error`'s status and adds what those batches did:
    /// `changed`, `skipped` and `conflicts`.
    Partial {
        error: Box<ApiError>,
        done: bulk::Committed,
    },
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
        ApiError::Awa(
            std::sync::Arc::try_unwrap(cache_err.0).unwrap_or_else(|arc| {
                awa_model::AwaError::Database(sqlx::Error::Protocol(arc.to_string()))
            }),
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, detail) = self.status_and_body();
        (status, axum::Json(detail)).into_response()
    }
}

impl ApiError {
    /// The response status and JSON body, whose `error` field holds the
    /// message.
    fn status_and_body(self) -> (StatusCode, serde_json::Map<String, serde_json::Value>) {
        if let ApiError::ConflictDetail {
            message,
            mut detail,
        } = self
        {
            detail.insert("error".into(), message.into());
            return (StatusCode::CONFLICT, detail);
        }
        if let ApiError::Partial { error, done } = self {
            let (status, mut detail) = error.status_and_body();
            if let Some(serde_json::Value::String(message)) = detail.get_mut("error") {
                message.push_str(&format!(
                    "; changed {} row(s) before the error",
                    done.changed
                ));
            }
            detail.insert("changed".into(), done.changed.into());
            detail.insert("skipped".into(), done.skipped.into());
            detail.insert("conflicts".into(), done.conflicts.into());
            return (status, detail);
        }
        let (status, message) = match self {
            ApiError::ReadOnly => (
                StatusCode::SERVICE_UNAVAILABLE,
                "awa serve is connected to a read-only database; admin actions are disabled"
                    .to_string(),
            ),
            ApiError::NotFound(message) => (StatusCode::NOT_FOUND, message),
            ApiError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
            ApiError::Conflict(message) => (StatusCode::CONFLICT, message),
            ApiError::ConflictDetail { .. } | ApiError::Partial { .. } => {
                unreachable!("answered above")
            }
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

        let serde_json::Value::Object(body) = json!({ "error": message }) else {
            unreachable!("a JSON object literal")
        };
        (status, body)
    }

    /// `error` after a bulk call's earlier batches committed `done`, or
    /// `error` alone when they changed and skipped nothing.
    pub fn after_partial(error: impl Into<ApiError>, done: bulk::Committed) -> Self {
        let error = error.into();
        if done.changed == 0 && done.skipped == 0 {
            error
        } else {
            ApiError::Partial {
                error: Box::new(error),
                done,
            }
        }
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

    pub fn conflict(message: impl Into<String>) -> Self {
        ApiError::Conflict(message.into())
    }
}

#[cfg(test)]
mod partial_tests {
    use super::*;

    #[test]
    fn a_partial_failure_keeps_the_status_and_reports_what_changed() {
        let error = ApiError::after_partial(
            awa_model::AwaError::Validation("bad batch".into()),
            bulk::Committed {
                changed: 3,
                skipped: 1,
                conflicts: vec![42],
            },
        );
        let (status, body) = error.status_and_body();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["changed"], 3);
        assert_eq!(body["skipped"], 1);
        assert_eq!(body["conflicts"], serde_json::json!([42]));
        assert_eq!(
            body["error"],
            "validation error: bad batch; changed 3 row(s) before the error"
        );
    }

    #[test]
    fn a_failure_that_changed_nothing_is_the_plain_error() {
        let error = ApiError::after_partial(
            awa_model::AwaError::Validation("bad batch".into()),
            bulk::Committed::default(),
        );
        let (status, body) = error.status_and_body();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.get("changed").is_none());
    }
}
