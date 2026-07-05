# ADR-035: Backpressure and producer flow control

## Status

Proposed — number claimed; full design tracked in [#341](https://github.com/hardbyte/awa/issues/341) and experiment E6 in [`docs/0.7-roadmap.md`](../0.7-roadmap.md).

## Context

Nothing stops producers from outrunning completion; the COPY path (ADR-008) makes it trivial
(measured: ~9,982/s enqueue vs ~4,016/s completion, depth 1.26M). Rejecting an enqueue inside a
user transaction (ADR-006) converts a queue-health condition into a business-transaction
failure, so hard rejection can never be the silent default.

## Decision (to be developed)

- **Soft-signal default, opt-in hard rejection**: enqueue paths can return a depth signal
  (`EnqueueOutcome::pressure`) sourced from lane-head cursors (index-only, no scans);
  `InsertOpts::backpressure: Off | Signal | Reject{limit}` with `Reject` returning a typed
  error and documented as changing transactional-enqueue semantics.
- `PacedProducer` helpers in Rust and Python (the pattern the benchmark harness already proved
  externally).
- Metrics: `awa.enqueue.backpressure.{signaled,rejected}`.

## Relationship to other ADRs

Tensions with ADR-006 (transaction surface) made explicit; reads via the #289/#330 cursor
signals; composes with ADR-025/031 throughput levers rather than replacing them.
