use crate::{
    AwaError as PyAwaError, CallbackNotFound, DatabaseError,
    LiveRuntimesRequireExclusiveWindow, RuntimeVersionFloorNotMet, SchemaNotMigrated,
    SerializationError, StorageNotFinalized, UniqueConflict, UnknownJobKind, ValidationError,
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
        awa_model::AwaError::CallbackNotFound { callback_id } => {
            CallbackNotFound::new_err(format!("callback not found: {callback_id}"))
        }
        awa_model::AwaError::UniqueConflict { constraint } => {
            let detail = constraint
                .map(|value| format!(" ({value})"))
                .unwrap_or_default();
            UniqueConflict::new_err(format!("unique conflict{detail}"))
        }
        awa_model::AwaError::SchemaNotMigrated { expected, found } => SchemaNotMigrated::new_err(
            format!("schema not migrated: expected version {expected}, found {found}"),
        ),
        // Same schema-version family as SchemaNotMigrated; the Display impl
        // carries the "upgrade the binaries" guidance — pass it through (#392).
        err @ awa_model::AwaError::SchemaNewerThanBinary { .. } => {
            SchemaNotMigrated::new_err(err.to_string())
        }
        // The Display impl carries the full operator guidance (finalize
        // steps + upgrade guide links) — pass it through verbatim.
        err @ awa_model::AwaError::StorageNotFinalized { .. } => {
            StorageNotFinalized::new_err(err.to_string())
        }
        err @ awa_model::AwaError::LiveRuntimesRequireExclusiveWindow { .. } => {
            LiveRuntimesRequireExclusiveWindow::new_err(err.to_string())
        }
        err @ awa_model::AwaError::RuntimeVersionFloorNotMet { .. } => {
            RuntimeVersionFloorNotMet::new_err(err.to_string())
        }
        awa_model::AwaError::UnknownJobKind { kind } => {
            UnknownJobKind::new_err(format!("unknown job kind: {kind}"))
        }
        awa_model::AwaError::Validation(msg) => ValidationError::new_err(msg),
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

#[cfg(test)]
mod tests {
    use super::*;
    use pyo3::Python;

    #[test]
    fn live_runtime_migration_gate_maps_to_dedicated_exception() {
        Python::initialize();
        let err = map_awa_error(awa_model::AwaError::LiveRuntimesRequireExclusiveWindow {
            migration_version: 42,
            count: 2,
            plural: "s",
            newest_secs: 3,
            instances: " Live runtimes: worker-a, worker-b.".to_string(),
        });

        Python::attach(|py| {
            assert!(err.is_instance_of::<LiveRuntimesRequireExclusiveWindow>(py));
            assert!(!err.is_instance_of::<StorageNotFinalized>(py));
            let message = err.to_string();
            assert!(message.contains("requires no live runtimes"));
            assert!(message.contains("stop or drain the workers"));
        });
    }

    #[test]
    fn runtime_version_floor_maps_to_dedicated_exception() {
        Python::initialize();
        let err = map_awa_error(awa_model::AwaError::RuntimeVersionFloorNotMet {
            migration_version: 42,
            minimum_version: "0.6.2",
            count: 1,
            instances: " Incompatible runtimes: worker-a: \"0.6.1\".".to_string(),
        });

        Python::attach(|py| {
            assert!(err.is_instance_of::<RuntimeVersionFloorNotMet>(py));
            assert!(!err.is_instance_of::<LiveRuntimesRequireExclusiveWindow>(py));
            let message = err.to_string();
            assert!(message.contains("version 0.6.2 or newer"));
            assert!(message.contains("--allow-live-runtimes"));
        });
    }
}
