//! Query-string form of a [`JobFilter`] (ADR-049), shared by the jobs and
//! DLQ endpoints. `tag` and `state` may repeat (all of the tags, any of the
//! states); `ids` is comma-separated.

use awa_model::filter::{MAX_FILTER_IDS, MAX_FILTER_TAGS};
use awa_model::{AwaError, JobFilter, JobState};
use chrono::{DateTime, Utc};

use crate::error::ApiError;

/// A filter plus the paging parameters that travel with it.
#[derive(Debug, Default)]
pub struct FilterQuery {
    pub filter: JobFilter,
    pub before_id: Option<i64>,
    pub before_dlq_at: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
}

fn invalid(name: &str, value: &str) -> ApiError {
    ApiError::Awa(AwaError::Validation(format!(
        "invalid value for `{name}`: {value:?}"
    )))
}

fn too_many(name: &str, cap: usize) -> ApiError {
    ApiError::Awa(AwaError::Validation(format!(
        "a job filter may take at most {cap} `{name}` values"
    )))
}

fn parse_time(name: &str, value: &str) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|_| invalid(name, value))
}

/// Parse a raw query string. Unknown parameters and empty values are errors,
/// as unknown fields are in the JSON filter: dropping either would return a
/// wider list than the one asked for. (A 0.6 server ignores unknown ones.)
pub fn parse(raw: Option<&str>) -> Result<FilterQuery, ApiError> {
    let mut query = FilterQuery::default();
    let Some(raw) = raw else {
        return Ok(query);
    };
    for (name, value) in url::form_urlencoded::parse(raw.as_bytes()) {
        let value = value.trim();
        // A parameter dropped here would widen the population, as an unknown
        // field would in the JSON filter.
        if value.is_empty() {
            return Err(ApiError::Awa(AwaError::Validation(format!(
                "`{name}` needs a value"
            ))));
        }
        let filter = &mut query.filter;
        match name.as_ref() {
            "kind" => filter.kind = Some(value.to_string()),
            "queue" => filter.queue = Some(value.to_string()),
            "tag" => {
                if !filter.tags.iter().any(|tag| tag == value) {
                    // Checked per parameter so a long query string is
                    // refused before it costs more than the cap.
                    if filter.tags.len() == MAX_FILTER_TAGS {
                        return Err(too_many("tag", MAX_FILTER_TAGS));
                    }
                    filter.tags.push(value.to_string());
                }
            }
            "state" => {
                let state = value
                    .parse::<JobState>()
                    .map_err(|_| invalid("state", value))?;
                if !filter.states.contains(&state) {
                    filter.states.push(state);
                }
            }
            "dead_lettered" => {
                filter.dead_lettered = Some(match value {
                    "true" => true,
                    "false" => false,
                    _ => return Err(invalid("dead_lettered", value)),
                })
            }
            "ids" => {
                let ids = filter.ids.get_or_insert_with(Vec::new);
                for id in value.split(',').map(str::trim).filter(|id| !id.is_empty()) {
                    if ids.len() == MAX_FILTER_IDS {
                        return Err(too_many("ids", MAX_FILTER_IDS));
                    }
                    ids.push(id.parse().map_err(|_| invalid("ids", id))?);
                }
            }
            "created_at_gte" => filter.created_at_gte = Some(parse_time("created_at_gte", value)?),
            "created_at_lt" => filter.created_at_lt = Some(parse_time("created_at_lt", value)?),
            "before_id" => {
                query.before_id = Some(value.parse().map_err(|_| invalid("before_id", value))?)
            }
            "before_dlq_at" => query.before_dlq_at = Some(parse_time("before_dlq_at", value)?),
            "limit" => query.limit = Some(value.parse().map_err(|_| invalid("limit", value))?),
            other => {
                return Err(ApiError::Awa(AwaError::Validation(format!(
                    "unknown parameter `{other}`; filters take kind, queue, tag, state, \
                     ids, dead_lettered, created_at_gte and created_at_lt"
                ))))
            }
        }
    }
    query.filter.validate()?;
    Ok(query)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_tags_and_states_accumulate() {
        let parsed = parse(Some(
            "tag=order%3A1&tag=tenant%3Aacme&state=failed&state=cancelled&ids=3,4&limit=50",
        ))
        .unwrap();
        assert_eq!(parsed.filter.tags, vec!["order:1", "tenant:acme"]);
        assert_eq!(
            parsed.filter.states,
            vec![JobState::Failed, JobState::Cancelled]
        );
        assert_eq!(parsed.filter.ids, Some(vec![3, 4]));
        assert_eq!(parsed.limit, Some(50));
    }

    #[test]
    fn empty_values_and_unknown_parameters_are_refused() {
        // Either would otherwise drop a term and widen the population.
        assert!(parse(Some("kind=&queue=q")).is_err());
        assert!(parse(Some("queue=q&tags=a")).is_err());
        assert_eq!(
            parse(Some("queue=q")).unwrap().filter,
            JobFilter::new().queue("q")
        );
    }

    #[test]
    fn caps_apply_while_parsing() {
        let tags: Vec<String> = (0..=MAX_FILTER_TAGS).map(|i| format!("tag=t{i}")).collect();
        assert!(parse(Some(&tags.join("&"))).is_err());
        // Repeats of one tag count once.
        let repeated = vec!["tag=t"; MAX_FILTER_TAGS * 2].join("&");
        assert_eq!(parse(Some(&repeated)).unwrap().filter.tags, vec!["t"]);
        let ids: Vec<String> = (0..=MAX_FILTER_IDS).map(|i| i.to_string()).collect();
        assert!(parse(Some(&format!("ids={}", ids.join(",")))).is_err());
    }

    #[test]
    fn malformed_values_are_rejected() {
        assert!(parse(Some("state=bogus")).is_err());
        assert!(parse(Some("ids=1,x")).is_err());
        assert!(parse(Some("created_at_gte=yesterday")).is_err());
    }
}
