# ADR-038: Hot-reloadable per-queue runtime overrides

## Status

Accepted — Tier 1 implemented for 0.7 ([#396](https://github.com/hardbyte/awa/issues/396),
from [#385](https://github.com/hardbyte/awa/issues/385)); Tier 2 tracked in
[#397](https://github.com/hardbyte/awa/issues/397).

## Context

Worker runtime parameters are read from the builder at startup, so tuning a live fleet
(poll cadence, batch size, rate limits, attempt deadlines) requires a rolling restart.
Graceful drain makes restarts safe but slow, and online tuning during an incident — the
moment it matters most — is exactly when restarts are least welcome ([#385](https://github.com/hardbyte/awa/issues/385)).

awa already has one hot-reloadable control: `queue_meta.paused`, polled by dispatchers and
applied live. The database is already the fleet's control plane.

## Decision

Extend `awa.queue_meta` with **nullable override columns** (migration v041):
`override_poll_interval_ms`, `override_claim_batch_size`, `override_rate_limit`
(jobs/second), `override_deadline_ms`, plus `overrides_updated_at`.

Semantics:

- The **builder value is the default**; a non-NULL override wins; NULL (or clearing)
  reverts to the builder value. Whole-state writes: `set` replaces the full override row.
- Dispatchers refresh overrides on a **slow cadence (10s)** — one single-row read per
  claimer per cadence, not a per-poll control query — and apply them in the dispatch loop.
  A change is fleet-consistent within roughly one cadence.
- Mutation surfaces: `awa_model::admin::{set,clear,queue}_runtime_overrides` and
  `awa queue overrides set|clear|show`.

Two guard rails, both logged when they bite:

1. **Rate-limit overrides only retune an existing limiter.** The shared token bucket is
   created at build time only when the builder configured a rate limit; enabling limiting
   on an unlimited queue live (or disabling it) changes the hot claim path and is Tier 2.
2. **Deadline overrides never cross the zero boundary.** The receipts-vs-legacy claim mode
   is selected by zero/non-zero `deadline_duration` at startup; an override that would flip
   the mode is ignored with a warning. Retuning a non-zero deadline is fully supported and
   applies to the next claim.

## Consequences

**Positive**

- Incident-time tuning (slow a hot queue's polling, shrink batches, tighten deadlines)
  without restarts, consistently across the fleet, auditable via `overrides_updated_at`.
- No new infrastructure and no new hot mutable rows: `queue_meta` is one low-churn row per
  queue, written at operator frequency — the #169 dead-tuple discipline is unaffected.

**Negative / limits**

- Changes take up to one refresh cadence to apply. The cadence has a process-wide
  escape hatch (`AWA_OVERRIDE_REFRESH_MS`, floored at 50ms) but is deliberately not
  configurable per queue in Tier 1.
- Per-claimer application means a brief window where claimers of one queue disagree.
- Live concurrency (`max_workers`), correctness-coupled cadences (heartbeat/rescue), and
  Python/UI parity are explicitly Tier 2 ([#397](https://github.com/hardbyte/awa/issues/397)).

## Alternatives considered

- **SIGHUP / config-file reload** — awa config lives in builder code inside embedding
  applications; library workers have no file to reload. Rejected.
- **In-process update API only** — no fleet consistency; every embedder rebuilds the
  distribution story. The DB override path subsumes it.
- **Per-poll override reads** — simplest freshness, but adds a control query to every poll
  on every claimer; the 10s cadence costs almost nothing and is fast enough for tuning.

## Relationship to other ADRs

Extends the `queue_meta` control-plane pattern (pause/resume); respects ADR-026's
no-new-hot-rows discipline; interacts with ADR-023's claim-mode selection (hence the
zero-boundary guard); effective-value reporting pairs with the runtime-snapshot work in
[#391](https://github.com/hardbyte/awa/issues/391).
