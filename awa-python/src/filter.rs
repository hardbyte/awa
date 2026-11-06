//! Python dicts to [`JobFilter`] (ADR-049), shared by job listing,
//! counting, the DLQ and batch operations.

use awa_model::{JobCount, JobFilter, JobState};
use chrono::{DateTime, Utc};
use pyo3::prelude::*;
use pyo3::types::PyDict;

const FIELDS: [&str; 8] = [
    "kind",
    "queue",
    "ids",
    "tag",
    "state",
    "dead_lettered",
    "created_at_gte",
    "created_at_lt",
];

fn item<'py>(dict: &Bound<'py, PyDict>, key: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    Ok(dict.get_item(key)?.filter(|value| !value.is_none()))
}

fn parse_state(value: &str) -> PyResult<JobState> {
    value
        .parse::<JobState>()
        .map_err(pyo3::exceptions::PyValueError::new_err)
}

/// A string or a list of strings, as the wire format's `tag` and `state` take.
fn one_or_many(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(one) = value.extract::<String>() {
        return Ok(vec![one]);
    }
    value.extract::<Vec<String>>()
}

/// A filter dict, or the empty filter for `None`. Unknown keys are an error,
/// so a typo cannot widen the jobs a purge or batch operation acts on.
pub(crate) fn parse_job_filter(py: Python<'_>, filter: Option<&Py<PyAny>>) -> PyResult<JobFilter> {
    let Some(filter) = filter else {
        return Ok(JobFilter::default());
    };
    let filter = filter.bind(py);
    if filter.is_none() {
        return Ok(JobFilter::default());
    }
    let dict: &Bound<'_, PyDict> = filter.cast().map_err(|_| {
        pyo3::exceptions::PyTypeError::new_err("a job filter must be a dict or None")
    })?;
    for key in dict.keys() {
        let key: String = key.extract()?;
        if !FIELDS.contains(&key.as_str()) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown job filter field {key:?}; expected one of {}",
                FIELDS.join(", ")
            )));
        }
    }
    let mut parsed = JobFilter {
        kind: item(dict, "kind")?.map(|v| v.extract()).transpose()?,
        queue: item(dict, "queue")?.map(|v| v.extract()).transpose()?,
        ids: item(dict, "ids")?.map(|v| v.extract()).transpose()?,
        dead_lettered: item(dict, "dead_lettered")?
            .map(|v| v.extract())
            .transpose()?,
        created_at_gte: item(dict, "created_at_gte")?
            .map(extract_datetime)
            .transpose()?,
        created_at_lt: item(dict, "created_at_lt")?
            .map(extract_datetime)
            .transpose()?,
        ..Default::default()
    };
    if let Some(tag) = item(dict, "tag")? {
        parsed = parsed.tags(one_or_many(&tag)?);
    }
    if let Some(state) = item(dict, "state")? {
        for state in one_or_many(&state)? {
            parsed = parsed.state(parse_state(&state)?);
        }
    }
    parsed.validate().map_err(crate::errors::map_awa_error)?;
    Ok(parsed)
}

pub(crate) fn extract_datetime(value: Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    if let Ok(datetime) = value.extract::<DateTime<Utc>>() {
        return Ok(datetime);
    }
    let text: String = value.extract()?;
    DateTime::parse_from_rfc3339(&text)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|err| pyo3::exceptions::PyValueError::new_err(err.to_string()))
}

/// The old keyword arguments, layered onto a filter. Every field narrows,
/// so a keyword that names a different kind or queue than the filter is an
/// error rather than a replacement.
pub(crate) fn with_legacy_fields(
    filter: JobFilter,
    kind: Option<String>,
    queue: Option<String>,
    tag: Option<String>,
) -> PyResult<JobFilter> {
    fn merge(
        name: &str,
        keyword: Option<String>,
        field: Option<String>,
    ) -> PyResult<Option<String>> {
        match (keyword, field) {
            (Some(keyword), Some(field)) if keyword != field => {
                Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "{name}={keyword:?} conflicts with the filter's {name} {field:?}"
                )))
            }
            (keyword, field) => Ok(keyword.or(field)),
        }
    }
    let kind = merge("kind", kind, filter.kind.clone())?;
    let queue = merge("queue", queue, filter.queue.clone())?;
    Ok(JobFilter {
        kind,
        queue,
        ..filter
    }
    .tags(tag))
}

/// `{"count": n, "exact": bool}`: `exact` is false when more than `n` jobs
/// match and counting stopped at the cap.
pub(crate) fn count_to_py(py: Python<'_>, count: JobCount) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("count", count.value())?;
    dict.set_item("exact", matches!(count, JobCount::Exact(_)))?;
    Ok(dict.into_any().unbind())
}
