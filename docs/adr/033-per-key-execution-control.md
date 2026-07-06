# ADR-033: Per-key execution control (tiered)

## Status

Proposed — number claimed; full design tracked in [#340](https://github.com/hardbyte/awa/issues/340) and experiment E5 in [`docs/0.7-roadmap.md`](../0.7-roadmap.md).

## Context

Awa controls execution at queue granularity only. Multi-tenant workloads need per-key
concurrency limits ("at most N in-flight per customer") and fairness across keys within a
queue. The binding constraint from #169: no per-key counter family may become a new hot
mutable table under a pinned MVCC horizon.

## Decision (to be developed)

Tiered delivery per roadmap decision D4:

- **Tier 1 (committed):** worker-local per-key rate limiting and concurrency caps via
  `InsertOpts::concurrency_key` + per-queue `KeyPolicy` — exact per worker, approximate across
  a fleet (documented formula), zero storage footprint.
- **Tier 2 (experiment-gated):** fleet-exact concurrency via claim-time key-window checks,
  only if E5 finds a design with zero new hot mutable rows and bounded claim-path cost.

## Relationship to other ADRs

Composes with ADR-005 (priority aging), ADR-010 (rate limiting), ADR-011 (weighted
concurrency), ADR-025 (enqueue shards — `concurrency_key` defaults to `ordering_key`), and
ADR-026 (the delta-ledger discipline any per-key state must follow).
