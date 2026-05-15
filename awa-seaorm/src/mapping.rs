use awa::{AwaError, JobRow, JobState};
use sea_orm::{DbErr, QueryResult, SqlErr};
use std::ops::Deref;
use std::sync::Arc;

/// Convert a SeaORM database error into Awa's public error type.
pub fn map_db_err(err: DbErr) -> AwaError {
    if matches!(err.sql_err(), Some(SqlErr::UniqueConstraintViolation(_))) {
        return AwaError::UniqueConflict {
            constraint: unique_constraint_name(&err),
        };
    }

    match err {
        DbErr::Exec(sea_orm::RuntimeErr::SqlxError(err))
        | DbErr::Query(sea_orm::RuntimeErr::SqlxError(err))
        | DbErr::Conn(sea_orm::RuntimeErr::SqlxError(err)) => match Arc::try_unwrap(err) {
            Ok(err) => awa::map_sqlx_error(err),
            Err(err) => AwaError::Database(sqlx::Error::Protocol(err.to_string())),
        },
        other => AwaError::Database(sqlx::Error::Protocol(other.to_string())),
    }
}

fn unique_constraint_name(err: &DbErr) -> Option<String> {
    match err {
        DbErr::Exec(runtime) | DbErr::Query(runtime) => match runtime {
            sea_orm::RuntimeErr::SqlxError(sqlx_err) => match sqlx_err.deref() {
                sqlx::Error::Database(db_err) => db_err.constraint().map(|c| c.to_string()),
                _ => None,
            },
            _ => None,
        },
        _ => None,
    }
}

/// Decode a SeaORM query result into an Awa job row.
pub fn job_row_from_query_result(row: &QueryResult) -> Result<JobRow, AwaError> {
    let state_str: String = col(row, "state_str")?;
    let state = state_str
        .parse::<JobState>()
        .map_err(AwaError::Validation)?;

    let unique_states = row
        .try_get::<Option<String>>("", "unique_states_str")
        .map_err(|err| decode_err("unique_states_str", err))?
        .map(|bits| parse_unique_states(&bits))
        .transpose()?;

    Ok(JobRow {
        id: col(row, "id")?,
        kind: col(row, "kind")?,
        queue: col(row, "queue")?,
        args: col(row, "args")?,
        state,
        priority: col(row, "priority")?,
        attempt: col(row, "attempt")?,
        run_lease: col(row, "run_lease")?,
        max_attempts: col(row, "max_attempts")?,
        run_at: col(row, "run_at")?,
        heartbeat_at: col(row, "heartbeat_at")?,
        deadline_at: col(row, "deadline_at")?,
        attempted_at: col(row, "attempted_at")?,
        finalized_at: col(row, "finalized_at")?,
        created_at: col(row, "created_at")?,
        errors: col(row, "errors")?,
        metadata: col(row, "metadata")?,
        tags: col(row, "tags")?,
        unique_key: col(row, "unique_key")?,
        unique_states,
        callback_id: col(row, "callback_id")?,
        callback_timeout_at: col(row, "callback_timeout_at")?,
        callback_filter: col(row, "callback_filter")?,
        callback_on_complete: col(row, "callback_on_complete")?,
        callback_on_fail: col(row, "callback_on_fail")?,
        callback_transform: col(row, "callback_transform")?,
        progress: col(row, "progress")?,
    })
}

/// Decode multiple SeaORM query results into Awa job rows.
pub fn job_rows_from_query_results(rows: Vec<QueryResult>) -> Result<Vec<JobRow>, AwaError> {
    rows.iter().map(job_row_from_query_result).collect()
}

fn col<T>(row: &QueryResult, name: &str) -> Result<T, AwaError>
where
    T: sea_orm::TryGetable,
{
    row.try_get("", name).map_err(|err| decode_err(name, err))
}

fn decode_err(name: &str, err: impl std::fmt::Debug) -> AwaError {
    AwaError::Validation(format!("failed to decode column {name}: {err:?}"))
}

fn parse_unique_states(bits: &str) -> Result<u8, AwaError> {
    u8::from_str_radix(bits, 2)
        .map_err(|err| AwaError::Validation(format!("failed to decode unique_states: {err}")))
}
