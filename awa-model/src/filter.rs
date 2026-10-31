//! One description of "which jobs" for listing, counting, batch operations
//! and the DLQ (ADR-049).
//!
//! A [`JobFilter`] is a population: every field that is set narrows it, and
//! paging is passed separately ([`JobPage`], [`DlqPage`]) so a filter means
//! the same thing wherever it is stored or sent. Each storage engine compiles
//! a filter to SQL that narrows candidates using only the predicates the
//! filter sets; the batched loader then settles each candidate's current
//! state and [`JobFilter::matches`] re-checks it.

use crate::error::AwaError;
use crate::job::{JobRow, JobState};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Most job ids a submitted filter may name.
pub const MAX_FILTER_IDS: usize = 1000;
/// Longest tag, in bytes, a filter may search for.
pub const MAX_FILTER_TAG_BYTES: usize = 256;
/// Most tags a filter may require; a job carries at most this many.
pub const MAX_FILTER_TAGS: usize = 20;
/// Largest page `find_jobs` returns.
pub const MAX_PAGE_LIMIT: i64 = 1000;
/// Page size when none is given.
pub const DEFAULT_PAGE_LIMIT: i64 = 100;
/// `count_jobs` counts exactly up to this many matches and reports more as
/// [`JobCount::AtLeast`].
pub const COUNT_CAP: i64 = 10_000;

/// Which jobs. Build one with `..Default::default()` or the builder methods
/// so fields added later do not break the construction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobFilter {
    pub kind: Option<String>,
    pub queue: Option<String>,
    /// Any of these states; empty means any state the consumer allows.
    pub states: Vec<JobState>,
    /// All of these tags.
    pub tags: Vec<String>,
    /// `Some(true)` keeps only dead-lettered jobs, `Some(false)` drops them.
    pub dead_lettered: Option<bool>,
    pub ids: Option<Vec<i64>>,
    /// Original insert time, unchanged by retries, DLQ moves and priority or
    /// queue moves.
    pub created_at_gte: Option<DateTime<Utc>>,
    pub created_at_lt: Option<DateTime<Utc>>,
}

impl JobFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }

    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    pub fn state(mut self, state: JobState) -> Self {
        if !self.states.contains(&state) {
            self.states.push(state);
        }
        self
    }

    pub fn states(mut self, states: impl IntoIterator<Item = JobState>) -> Self {
        for state in states {
            self = self.state(state);
        }
        self
    }

    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        let tag = tag.into();
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
        }
        self
    }

    pub fn tags<T: Into<String>>(mut self, tags: impl IntoIterator<Item = T>) -> Self {
        for tag in tags {
            self = self.tag(tag);
        }
        self
    }

    pub fn dead_lettered(mut self, dead_lettered: bool) -> Self {
        self.dead_lettered = Some(dead_lettered);
        self
    }

    pub fn ids(mut self, ids: impl IntoIterator<Item = i64>) -> Self {
        self.ids = Some(ids.into_iter().collect());
        self
    }

    pub fn created_at_gte(mut self, at: DateTime<Utc>) -> Self {
        self.created_at_gte = Some(at);
        self
    }

    pub fn created_at_lt(mut self, at: DateTime<Utc>) -> Self {
        self.created_at_lt = Some(at);
        self
    }

    /// True when no field narrows the population.
    pub fn is_empty(&self) -> bool {
        self.kind.is_none()
            && self.queue.is_none()
            && self.states.is_empty()
            && self.tags.is_empty()
            && self.dead_lettered.is_none()
            && self.ids.as_ref().is_none_or(Vec::is_empty)
            && self.created_at_gte.is_none()
            && self.created_at_lt.is_none()
    }

    /// True when the filter needs more than ADR-030's shape: a runner from
    /// before ADR-049 cannot read it, or ignores `dead_lettered`.
    pub fn uses_extended_fields(&self) -> bool {
        self.tags.len() > 1 || self.states.len() > 1 || self.dead_lettered.is_some()
    }

    /// Bounds a submitted filter must respect. Stored filters are not
    /// re-validated when they load, so tightening a bound never strands an
    /// accepted batch operation.
    pub fn validate(&self) -> Result<(), AwaError> {
        if let Some(ids) = &self.ids {
            if ids.len() > MAX_FILTER_IDS {
                return Err(AwaError::Validation(format!(
                    "a job filter may name at most {MAX_FILTER_IDS} ids, got {}",
                    ids.len()
                )));
            }
        }
        if self.tags.len() > MAX_FILTER_TAGS {
            return Err(AwaError::Validation(format!(
                "a job filter may require at most {MAX_FILTER_TAGS} tags, got {}",
                self.tags.len()
            )));
        }
        if let Some(tag) = self
            .tags
            .iter()
            .find(|tag| tag.len() > MAX_FILTER_TAG_BYTES)
        {
            return Err(AwaError::Validation(format!(
                "a filter tag may be at most {MAX_FILTER_TAG_BYTES} bytes, got {}",
                tag.len()
            )));
        }
        if let (Some(gte), Some(lt)) = (self.created_at_gte, self.created_at_lt) {
            if gte >= lt {
                return Err(AwaError::Validation(
                    "created_at_gte must be earlier than created_at_lt".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Whether a loaded job, in its current state, belongs to the population.
    /// `dead_lettered` says whether the row came from the DLQ.
    pub fn matches(&self, job: &JobRow, dead_lettered: bool) -> bool {
        self.kind.as_ref().is_none_or(|kind| &job.kind == kind)
            && self.queue.as_ref().is_none_or(|queue| &job.queue == queue)
            && (self.states.is_empty() || self.states.contains(&job.state))
            && self.tags.iter().all(|tag| job.tags.contains(tag))
            && self
                .dead_lettered
                .is_none_or(|wanted| wanted == dead_lettered)
            && self.ids.as_ref().is_none_or(|ids| ids.contains(&job.id))
            && self.created_at_gte.is_none_or(|gte| job.created_at >= gte)
            && self.created_at_lt.is_none_or(|lt| job.created_at < lt)
    }

    /// The states this filter can select once a consumer's eligible states
    /// are applied. Empty means none can match.
    pub(crate) fn effective_states(&self, eligible: &[JobState]) -> Vec<JobState> {
        eligible
            .iter()
            .copied()
            .filter(|state| self.states.is_empty() || self.states.contains(state))
            .filter(|state| {
                // Dead-lettered rows are reported as failed.
                self.dead_lettered != Some(true) || *state == JobState::Failed
            })
            .collect()
    }
}

/// One value, or a list of them.
#[derive(Deserialize)]
#[serde(untagged, expecting = "a single value or a list of values")]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            OneOrMany::One(one) => vec![one],
            OneOrMany::Many(many) => many,
        }
    }
}

/// The ADR-030 JSON shape. Several tags or states go under the same singular
/// key as a list, never a new key: a server or runner from before ADR-049
/// ignores an unknown key, which would widen the filter, but refuses a list
/// where it expects one value.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireIn {
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    ids: Option<Vec<i64>>,
    #[serde(default)]
    tag: Option<OneOrMany<String>>,
    #[serde(default)]
    state: Option<OneOrMany<JobState>>,
    #[serde(default)]
    dead_lettered: Option<bool>,
    #[serde(default)]
    created_at_gte: Option<DateTime<Utc>>,
    #[serde(default)]
    created_at_lt: Option<DateTime<Utc>>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum OneOrManyOut<'a, T> {
    One(&'a T),
    Many(&'a [T]),
}

fn one_or_many<T>(values: &[T]) -> Option<OneOrManyOut<'_, T>> {
    match values {
        [] => None,
        [one] => Some(OneOrManyOut::One(one)),
        many => Some(OneOrManyOut::Many(many)),
    }
}

#[derive(Serialize)]
struct WireOut<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ids: Option<&'a Vec<i64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tag: Option<OneOrManyOut<'a, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<OneOrManyOut<'a, JobState>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dead_lettered: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at_gte: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_at_lt: Option<DateTime<Utc>>,
}

impl Serialize for JobFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        WireOut {
            kind: self.kind.as_ref(),
            queue: self.queue.as_ref(),
            ids: self.ids.as_ref(),
            tag: one_or_many(&self.tags),
            state: one_or_many(&self.states),
            dead_lettered: self.dead_lettered,
            created_at_gte: self.created_at_gte,
            created_at_lt: self.created_at_lt,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JobFilter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = WireIn::deserialize(deserializer)?;
        let filter = JobFilter {
            kind: wire.kind,
            queue: wire.queue,
            ids: wire.ids,
            dead_lettered: wire.dead_lettered,
            created_at_gte: wire.created_at_gte,
            created_at_lt: wire.created_at_lt,
            ..Default::default()
        };
        Ok(filter
            .tags(wire.tag.map(OneOrMany::into_vec).unwrap_or_default())
            .states(wire.state.map(OneOrMany::into_vec).unwrap_or_default()))
    }
}

/// The first version whose batch-operation runner reads every
/// [`JobFilter`] field. An older runner fails to load a filter with several
/// tags or states, and ignores `dead_lettered`.
pub const EXTENDED_FILTER_MIN_RUNTIME: &str = "0.7.0-alpha.2";

/// Refuse a filter that needs ADR-049's fields while a live runtime is older
/// than [`EXTENDED_FILTER_MIN_RUNTIME`]. Unknown and non-semver versions fail
/// closed.
pub(crate) async fn require_fleet_for_extended_filters(
    pool: &sqlx::PgPool,
) -> Result<(), AwaError> {
    let minimum = semver::Version::parse(EXTENDED_FILTER_MIN_RUNTIME)
        .expect("the extended-filter runtime floor is valid semver");
    let live: Vec<(String, String)> = sqlx::query_as(
        // The same liveness window cron reconciliation uses: three snapshot
        // intervals, and never under 30 seconds.
        "SELECT instance_id::text, version FROM awa.runtime_instances \
         WHERE last_seen_at \
             + make_interval(secs => GREATEST(30.0, snapshot_interval_ms * 0.003)) >= now()",
    )
    .fetch_all(pool)
    .await
    .map_err(crate::error::map_sqlx_error)?;
    let old: Vec<String> = live
        .into_iter()
        .filter(|(_, version)| {
            semver::Version::parse(version).map_or(true, |reported| reported < minimum)
        })
        .map(|(id, version)| format!("{id} ({version})"))
        .collect();
    if old.is_empty() {
        return Ok(());
    }
    Err(AwaError::Validation(format!(
        "this filter uses several tags or states, or dead_lettered, which runtimes older \
         than {EXTENDED_FILTER_MIN_RUNTIME} cannot run; {} live runtime(s) are older: {}",
        old.len(),
        old.join(", ")
    )))
}

/// A newest-first window over a filter's population.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobPage {
    /// Only jobs with a smaller id: the last id of the previous page.
    pub before_id: Option<i64>,
    /// Clamped to `1..=MAX_PAGE_LIMIT`.
    pub limit: i64,
}

impl Default for JobPage {
    fn default() -> Self {
        Self {
            before_id: None,
            limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

impl JobPage {
    pub fn first(limit: i64) -> Self {
        Self {
            before_id: None,
            limit,
        }
    }

    pub fn before(before_id: i64, limit: i64) -> Self {
        Self {
            before_id: Some(before_id),
            limit,
        }
    }

    pub(crate) fn clamped_limit(&self) -> i64 {
        self.limit.clamp(1, MAX_PAGE_LIMIT)
    }
}

/// A newest-first window over dead-lettered jobs, ordered by `(dlq_at, id)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqPage {
    pub before_dlq_at: Option<DateTime<Utc>>,
    pub before_id: Option<i64>,
    pub limit: i64,
}

impl Default for DlqPage {
    fn default() -> Self {
        Self {
            before_dlq_at: None,
            before_id: None,
            limit: DEFAULT_PAGE_LIMIT,
        }
    }
}

impl DlqPage {
    pub(crate) fn clamped_limit(&self) -> i64 {
        self.limit.clamp(1, MAX_PAGE_LIMIT)
    }
}

/// How many jobs a filter selects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum JobCount {
    Exact(i64),
    /// More than [`COUNT_CAP`] jobs match; this is the cap.
    AtLeast(i64),
}

impl JobCount {
    pub fn value(self) -> i64 {
        match self {
            JobCount::Exact(n) | JobCount::AtLeast(n) => n,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_adr030_filters_still_deserialize() {
        // Written by the 0.6 `BatchOperationFilter`, which serialized every
        // field including the empty ones.
        let stored = serde_json::json!({
            "kind": null, "queue": "default", "ids": null, "tag": "customer:acme",
            "state": "available", "created_at_gte": null, "created_at_lt": null
        });
        let filter: JobFilter = serde_json::from_value(stored).unwrap();
        assert_eq!(
            filter,
            JobFilter::new()
                .queue("default")
                .tag("customer:acme")
                .state(JobState::Available)
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err = serde_json::from_value::<JobFilter>(serde_json::json!({ "tagz": "x" }));
        assert!(err.is_err(), "a typo must not widen the population");
    }

    #[test]
    fn several_tags_or_states_share_the_adr030_keys() {
        let one = serde_json::to_value(JobFilter::new().tag("a").state(JobState::Failed)).unwrap();
        assert_eq!(one, serde_json::json!({ "tag": "a", "state": "failed" }));
        let many = JobFilter::new()
            .tags(["a", "b"])
            .states([JobState::Failed, JobState::Cancelled]);
        let json = serde_json::to_value(&many).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "tag": ["a", "b"], "state": ["failed", "cancelled"] })
        );
        assert_eq!(serde_json::from_value::<JobFilter>(json).unwrap(), many);
        assert!(many.uses_extended_fields());
        assert!(!JobFilter::new().tag("a").uses_extended_fields());
        assert!(
            serde_json::from_value::<JobFilter>(serde_json::json!({ "tags": ["a"] })).is_err(),
            "only the singular keys exist on the wire"
        );
    }

    #[test]
    fn a_pre_adr049_reader_refuses_several_tags_rather_than_widening() {
        // The 0.6 `BatchOperationFilter`: it ignores unknown keys.
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Adr030Filter {
            queue: Option<String>,
            tag: Option<String>,
            state: Option<JobState>,
        }
        let narrow = JobFilter::new().queue("q").tags(["a", "b"]);
        assert!(
            serde_json::from_value::<Adr030Filter>(serde_json::to_value(narrow).unwrap()).is_err()
        );
        let narrow = JobFilter::new()
            .queue("q")
            .states([JobState::Available, JobState::Scheduled]);
        assert!(
            serde_json::from_value::<Adr030Filter>(serde_json::to_value(narrow).unwrap()).is_err()
        );
    }

    #[test]
    fn validation_bounds_ids_tags_and_dates() {
        assert!(JobFilter::new().ids(0..1001).validate().is_err());
        assert!(JobFilter::new().ids(0..1000).validate().is_ok());
        assert!(JobFilter::new().tag("x".repeat(257)).validate().is_err());
        assert!(JobFilter::new()
            .tags((0..21).map(|n| format!("t{n}")))
            .validate()
            .is_err());
        assert!(JobFilter::new()
            .tags((0..20).map(|n| format!("t{n}")))
            .validate()
            .is_ok());
        let now = Utc::now();
        assert!(JobFilter::new()
            .created_at_gte(now)
            .created_at_lt(now)
            .validate()
            .is_err());
    }

    #[test]
    fn effective_states_apply_eligibility_and_dead_lettered() {
        let eligible = [JobState::Available, JobState::Scheduled];
        assert_eq!(
            JobFilter::new().effective_states(&eligible),
            eligible.to_vec()
        );
        assert!(JobFilter::new()
            .state(JobState::Running)
            .effective_states(&eligible)
            .is_empty());
        assert_eq!(
            JobFilter::new()
                .dead_lettered(true)
                .effective_states(&[JobState::Failed, JobState::Completed]),
            vec![JobState::Failed]
        );
    }

    #[test]
    fn page_limits_are_clamped() {
        assert_eq!(JobPage::first(0).clamped_limit(), 1);
        assert_eq!(JobPage::first(5000).clamped_limit(), MAX_PAGE_LIMIT);
    }
}
