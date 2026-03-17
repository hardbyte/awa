mod args;
mod client;
mod job;
mod transaction;

use pyo3::prelude::*;

// Python exception types for Awa errors.
pyo3::create_exception!(_awa, AwaError, pyo3::exceptions::PyException);
pyo3::create_exception!(_awa, UniqueConflict, AwaError);
pyo3::create_exception!(_awa, SchemaNotMigrated, AwaError);
pyo3::create_exception!(_awa, UnknownJobKind, AwaError);
pyo3::create_exception!(_awa, SerializationError, AwaError);
pyo3::create_exception!(_awa, TerminalError, AwaError);
pyo3::create_exception!(_awa, DatabaseError, AwaError);

/// Derive a kind string from a CamelCase class name.
#[pyfunction]
fn derive_kind(name: &str) -> String {
    args::derive_kind(name)
}

/// Run migrations against the given database URL.
#[pyfunction]
fn migrate<'py>(py: Python<'py>, database_url: String) -> PyResult<Bound<'py, PyAny>> {
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await
            .map_err(|e| pyo3::exceptions::PyConnectionError::new_err(e.to_string()))?;

        awa_model::migrations::run(&pool)
            .await
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(())
    })
}

/// Get the raw migration SQL as a list of (version, description, sql) tuples.
#[pyfunction]
fn migrations() -> Vec<(i32, String, String)> {
    awa_model::migrations::migration_sql()
        .into_iter()
        .map(|(v, d, s)| (v, d.to_string(), s.to_string()))
        .collect()
}

#[pymodule]
fn _awa(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Classes
    m.add_class::<client::PyClient>()?;
    m.add_class::<job::PyJob>()?;
    m.add_class::<job::PyJobState>()?;
    m.add_class::<transaction::PyTransaction>()?;
    m.add_class::<client::PyRetryAfter>()?;
    m.add_class::<client::PySnooze>()?;
    m.add_class::<client::PyCancel>()?;

    // Functions
    m.add_function(wrap_pyfunction!(derive_kind, m)?)?;
    m.add_function(wrap_pyfunction!(migrate, m)?)?;
    m.add_function(wrap_pyfunction!(migrations, m)?)?;

    // Exceptions
    m.add("AwaError", m.py().get_type::<AwaError>())?;
    m.add("UniqueConflict", m.py().get_type::<UniqueConflict>())?;
    m.add("SchemaNotMigrated", m.py().get_type::<SchemaNotMigrated>())?;
    m.add("UnknownJobKind", m.py().get_type::<UnknownJobKind>())?;
    m.add("SerializationError", m.py().get_type::<SerializationError>())?;
    m.add("TerminalError", m.py().get_type::<TerminalError>())?;
    m.add("DatabaseError", m.py().get_type::<DatabaseError>())?;

    Ok(())
}
