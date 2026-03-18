use crate::{
    AwaError as PyAwaError, DatabaseError, SchemaNotMigrated, SerializationError, UniqueConflict,
    UnknownJobKind, ValidationError,
};
use pyo3::PyErr;

pub fn map_connect_error(err: sqlx::Error) -> PyErr {
    map_sqlx_error(err)
}

pub fn map_sqlx_error(err: sqlx::Error) -> PyErr {
    if let sqlx::Error::Database(ref db_err) = err {
        if db_err.code().as_deref() == Some("23505") {
            return UniqueConflict::new_err(err.to_string());
        }
    }

    DatabaseError::new_err(err.to_string())
}

pub fn map_awa_error(err: awa_model::AwaError) -> PyErr {
    match err {
        awa_model::AwaError::JobNotFound { id } => {
            PyAwaError::new_err(format!("job not found: {id}"))
        }
        awa_model::AwaError::UniqueConflict { existing_id } => {
            let detail = existing_id
                .map(|value| format!(" ({value})"))
                .unwrap_or_default();
            UniqueConflict::new_err(format!("unique conflict{detail}"))
        }
        awa_model::AwaError::SchemaNotMigrated { expected, found } => SchemaNotMigrated::new_err(
            format!("schema not migrated: expected version {expected}, found {found}"),
        ),
        awa_model::AwaError::UnknownJobKind { kind } => {
            UnknownJobKind::new_err(format!("unknown job kind: {kind}"))
        }
        awa_model::AwaError::Serialization(err) => SerializationError::new_err(err.to_string()),
        awa_model::AwaError::Database(err) => map_sqlx_error(err),
    }
}

pub fn validation_error(message: impl Into<String>) -> PyErr {
    ValidationError::new_err(message.into())
}

pub fn state_error(message: impl Into<String>) -> PyErr {
    PyAwaError::new_err(message.into())
}
