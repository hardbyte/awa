# Awa Positioning

## Short Version

Awa's slot: more capable than a Postgres event queue, less ecosystem-bound
than a language-specific job framework. It is a Postgres-native job queue for
teams that want:

- full job-queue behavior, not just enqueue/dequeue
- Rust and Python worker runtimes on the same queues
- transactional enqueue on the same Postgres you already run
- low dispatch latency without turning the main queue path into a vacuum trap

Typical scenarios where Awa fits:

- **Transactional side-effects**: webhook fan-out, email, payment confirmation
  enqueued inside the business transaction that causes them.
- **Mixed-runtime fleets**: Rust request-path services and Python ML / ETL
  workers sharing the same queues.
- **Long-running jobs with checkpoints**: batch imports, data pipelines that
  need to resume after retry without restarting from zero.
- **External orchestration**: jobs that park mid-execution for a webhook or
  external system to respond, then resume in the same handler.

## Best Fit

Awa is a strong fit when you want:

- priorities, retries, snoozes, cron, callbacks, DLQ, and UI in one system
- mixed Rust and Python worker fleets
- Postgres as the only required infrastructure dependency
- worker-owned dispatch, rescue, rotation, and prune instead of external
  tickers or `pg_cron`

## Nearby Categories

### Postgres event and message queues

PgQue (lightweight Postgres pub-sub/queue library) and its PgQ historical
lineage are the clearest reference points here.

That category is a good fit when you want:

- an event log
- independent consumer cursors
- a system optimized first around event-stream retention and rotation

That category is not the right fit when you need:

- per-job priorities
- unique jobs
- cron scheduling
- callback orchestration
- richer job lifecycle controls

### Language-specific Postgres job frameworks

River (Go-native Postgres job queue) and Oban (Elixir Postgres job queue;
Oban Pro is the commercial partitioned tier) are the clearest references here.

That category is a good fit when you want a job framework deeply shaped around
one host language and one surrounding ecosystem.

## Awa's Slot

Awa sits between those categories:

- it is a job queue, not just a stream
- it keeps Postgres as the only hard dependency
- it supports Rust and Python as first-class runtimes
- it uses segmented queue storage so queue history and lease churn do not sit
  in one mutable queue heap

That last point matters, but it is a differentiator, not the whole identity.
The product story should start with "Postgres job queue for Rust and Python"
and then explain why the storage engine matters operationally.

## What We Should Say

- Postgres-native job queue for Rust and Python.
- Full job-queue features without Redis or RabbitMQ.
- Runtime-owned maintenance; no `pg_cron` requirement.
- Segmented queue storage designed to keep the hot path lean under sustained
  load.
- Built for priorities, retries, cron, callbacks, DLQ, and operator tooling.

## What We Should Not Say

- Do not market "uniquely vacuum-aware."
- Do not frame Awa as "better PgQue."
- Do not claim "zero bloat."
- Do not make the strongest latency-plus-bloat claim until the head-to-head
  benchmark exists on the same hardware.

## Claims We Can Support Now

The current local validation supports these claims:

- queue storage keeps dead tuples out of the main ready path
- queue storage materially improves pickup-latency tails versus Awa's older
  mutable-row engine
- Awa keeps full job-queue behavior while using a segmented storage engine

Reference artifact:

- [ADR-019 validation bench](adr/bench/019-queue-storage-validation-2026-04-19.md)

## Proof Still Needed

The strongest public claim would be:

> Low dispatch latency and bounded dead tuples.

That should be backed by a same-hardware comparison against:

- Awa queue storage
- PgQue
- River
- optionally Oban Pro as a partitioned paid reference

The useful comparison set is:

- idle pickup latency
- sustained runtime throughput
- overlap readers / MVCC horizon pressure
- mixed workload soak
- terminal-failure burst
