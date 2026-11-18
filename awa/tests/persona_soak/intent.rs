//! Declared intent: states a persona puts the system in on purpose, which the checks
//! must allow. A persona declares an intent for a queue, or for the whole run, for as
//! long as the state holds; the checks read the log of declarations afterwards.
//!
//! ```ignore
//! // A queue paused on purpose, until the guard drops:
//! let _paused = shared.declare(Some(&queue), Intent::Paused, "alex pause");
//! // A queue no runtime serves, for the rest of the run:
//! shared.declare(Some(&queue), Intent::Unserved, "parked").keep();
//! // An operator operation in flight on a queue's lanes:
//! let op = shared.declare(Some(&queue), Intent::OperatorOp("move"), "move to b");
//! ```
//!
//! Queue scopes are logical queue names: a declaration for `q` covers its stripes
//! (`q#N`). A span with no queue covers every queue and ring.
//!
//! | Intent | Allows |
//! | --- | --- |
//! | `Paused` | a stalled claim cursor (with the existing hold count), a ready lane pinning the queue ring, the queue ring's generation standing still |
//! | `Unserved` | the same, for a queue no runtime serves |
//! | `OperatorOp(kind)` | the same while the operation runs; the bounded-release check measures from its end |
//! | `Disruption` | everything above for every queue and ring, prune `blocked` without `pruned`, and backends over a fleet budget |
//!
//! Every allowance extends `AWA_SOAK_PIN_RELEASE_SECONDS` past the end of the span, the
//! time the system has to recover once the state is lifted.

use super::registry::QueueInfo;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "op", rename_all = "snake_case")]
pub enum Intent {
    /// The queue is paused on purpose (an operator pause, or a hold that builds a backlog).
    Paused,
    /// No runtime serves the queue on purpose; its ready jobs wait.
    Unserved,
    /// An operator operation (`cancel`, `move`, `set_priority`, `drain`, ...) is in flight
    /// on the queue's lanes.
    OperatorOp(&'static str),
    /// A disruptive window (kill, restart, terminated connections, a completion stall).
    Disruption,
}

impl Intent {
    /// Paused and unserved queues also count as held by the stall check.
    pub fn holds(self) -> bool {
        matches!(self, Intent::Paused | Intent::Unserved)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct IntentSpan {
    pub id: u64,
    /// Logical queue name, or `None` for the whole run.
    pub queue: Option<String>,
    pub intent: Intent,
    pub detail: String,
    pub start_ms: i64,
    /// `None` while the state still holds.
    pub end_ms: Option<i64>,
}

impl IntentSpan {
    /// Whether this span applies to the physical queue `queue` (`q` or `q#N`).
    pub fn applies_to(&self, queue: &str) -> bool {
        match &self.queue {
            None => true,
            Some(scope) => logical(queue) == scope,
        }
    }

    /// Whether the span covers `at_ms`, allowing `grace_ms` after it ended.
    pub fn covers(&self, at_ms: i64, grace_ms: i64) -> bool {
        at_ms >= self.start_ms && self.end_ms.is_none_or(|end| at_ms <= end + grace_ms)
    }

    /// Whether the span overlaps `[from_ms, to_ms]`.
    pub fn overlaps(&self, from_ms: i64, to_ms: i64, grace_ms: i64) -> bool {
        self.start_ms <= to_ms && self.end_ms.is_none_or(|end| end + grace_ms >= from_ms)
    }
}

/// `q#N` to `q`; other names unchanged.
pub fn logical(queue: &str) -> &str {
    match queue.rsplit_once('#') {
        Some((name, suffix)) if suffix.parse::<u32>().is_ok() => name,
        _ => queue,
    }
}

#[derive(Default)]
pub struct IntentLog {
    next_id: AtomicU64,
    spans: Mutex<Vec<IntentSpan>>,
}

impl IntentLog {
    pub fn begin(
        &self,
        queue: Option<&str>,
        intent: Intent,
        detail: impl Into<String>,
        at_ms: i64,
    ) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.spans.lock().expect("intent log").push(IntentSpan {
            id,
            queue: queue.map(|queue| logical(queue).to_string()),
            intent,
            detail: detail.into(),
            start_ms: at_ms,
            end_ms: None,
        });
        id
    }

    /// Records a span whose end is known up front.
    pub fn window(
        &self,
        queue: Option<&str>,
        intent: Intent,
        detail: impl Into<String>,
        start_ms: i64,
        end_ms: i64,
    ) {
        let id = self.begin(queue, intent, detail, start_ms);
        self.end(id, end_ms);
    }

    pub fn end(&self, id: u64, at_ms: i64) {
        if let Some(span) = self
            .spans
            .lock()
            .expect("intent log")
            .iter_mut()
            .rev()
            .find(|span| span.id == id)
        {
            span.end_ms = Some(at_ms.max(span.start_ms));
        }
    }

    pub fn set_detail(&self, id: u64, detail: impl Into<String>) {
        if let Some(span) = self
            .spans
            .lock()
            .expect("intent log")
            .iter_mut()
            .rev()
            .find(|span| span.id == id)
        {
            span.detail = detail.into();
        }
    }

    pub fn spans(&self) -> Vec<IntentSpan> {
        self.spans.lock().expect("intent log").clone()
    }
}

/// Spans of `spans` that apply to `queue` and cover `at_ms`.
pub fn covering<'a>(
    spans: &'a [IntentSpan],
    queue: Option<&'a str>,
    at_ms: i64,
    grace_ms: i64,
) -> impl Iterator<Item = &'a IntentSpan> + 'a {
    spans.iter().filter(move |span| {
        span.covers(at_ms, grace_ms)
            && match queue {
                Some(queue) => span.applies_to(queue),
                None => span.queue.is_none(),
            }
    })
}

/// Ends its span when dropped. [`IntentGuard::keep`] leaves it open for the rest of the run.
pub struct IntentGuard {
    shared: Arc<super::shared::Shared>,
    id: Option<u64>,
    held: Option<Arc<QueueInfo>>,
}

impl IntentGuard {
    pub(super) fn new(
        shared: Arc<super::shared::Shared>,
        id: u64,
        held: Option<Arc<QueueInfo>>,
    ) -> Self {
        if let Some(queue) = &held {
            queue.hold();
        }
        Self {
            shared,
            id: Some(id),
            held,
        }
    }

    /// Leaves the span open until the end of the run.
    pub fn keep(mut self) {
        self.id = None;
        self.held = None;
    }

    /// Replaces the span's detail, for what is known only once the state ends (rows moved).
    pub fn set_detail(&self, detail: impl Into<String>) {
        if let Some(id) = self.id {
            self.shared.intents.set_detail(id, detail);
        }
    }
}

impl Drop for IntentGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            self.shared.intents.end(id, self.shared.now_ms());
        }
        if let Some(queue) = self.held.take() {
            queue.release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_cover_their_queue_and_its_stripes_with_grace() {
        let log = IntentLog::default();
        let paused = log.begin(Some("q#2"), Intent::Paused, "pause", 1_000);
        log.end(paused, 5_000);
        log.begin(None, Intent::Disruption, "kill", 20_000);
        let spans = log.spans();
        assert_eq!(spans[0].queue.as_deref(), Some("q"));
        assert!(spans[0].applies_to("q#7") && spans[0].applies_to("q"));
        assert!(!spans[0].applies_to("other"));
        assert_eq!(covering(&spans, Some("q#1"), 3_000, 0).count(), 1);
        assert_eq!(covering(&spans, Some("q"), 6_000, 0).count(), 0);
        assert_eq!(covering(&spans, Some("q"), 6_000, 2_000).count(), 1);
        assert_eq!(covering(&spans, Some("other"), 25_000, 0).count(), 1);
        assert_eq!(covering(&spans, None, 3_000, 0).count(), 0);
        assert!(spans[0].overlaps(4_000, 9_000, 0));
        assert!(!spans[0].overlaps(6_000, 9_000, 0));
        assert_eq!(logical("q#bad"), "q#bad");
    }
}
