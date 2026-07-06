# ADR-034: Job dependencies (single-parent A→B on ADR-029)

## Status

Proposed — number claimed; full design tracked in [#14](https://github.com/hardbyte/awa/issues/14) per roadmap decision D7 in [`docs/0.7-roadmap.md`](../0.7-roadmap.md).

## Context

"Run B after A completes" currently requires the handler to insert the next job manually.
ADR-029 already delivers transactional follow-up enqueues atomically with the parent's guarded
finalization — the mechanism exists; what is missing is a declarative parking state and a
failure policy.

## Decision (to be developed)

- `InsertOpts::after(JobRef)` parks B in the deferred backlog in a `waiting_on` state.
- A's guarded finalization transactionally promotes B (success) or applies B's declared
  `on_parent_failure` policy: `Cancel` (default) | `EnqueueWithContext` | `Discard`.
- Exactly one parent. Fan-in, fan-out, DAGs, and workflow state are **non-goals** (the PRD's
  "not a workflow engine" boundary; #81 guards it).
- TLA+ witness required for the promote-on-finalize race (parent finalizes concurrently with
  child cancel).

## Relationship to other ADRs

Builds directly on ADR-029 (transactional follow-up jobs); composes with ADR-019 deferred
storage; bounded by the ADR/PRD workflow non-goal.
