# Where Awa Fits

Awa is a Postgres-native job queue for Rust and Python: more capable than
a Postgres event queue, less ecosystem-bound than a language-specific job
framework. It is for teams that want:

- full job-queue behavior, not just enqueue/dequeue
- Rust and Python worker runtimes on the same queues
- transactional enqueue on the same Postgres you already run
- low dispatch latency without turning the main queue path into a vacuum trap

Typical scenarios where Awa fits:

- **Transactional side-effects** — webhook fan-out, email, payment
  confirmation enqueued inside the business transaction that causes them.
- **Mixed-runtime fleets** — Rust request-path services and Python ML /
  ETL workers sharing the same queues.
- **Long-running jobs with checkpoints** — batch imports, data pipelines
  that need to resume after retry without restarting from zero.
- **External orchestration** — jobs that park mid-execution for a webhook
  or external system to respond, then resume in the same handler.

## Best fit

Awa is a strong fit when you want:

- priorities, retries, snoozes, cron, callbacks, DLQ, and UI in one system
- mixed Rust and Python worker fleets
- Postgres as the only required infrastructure dependency
- worker-owned dispatch, rescue, rotation, and prune instead of external
  tickers or `pg_cron`

## Nearby categories

### Postgres event and message queues

PgQue and its PgQ historical lineage are the clearest reference points
here. That category is a good fit when you want:

- an event log
- independent consumer cursors
- a system optimized first around event-stream retention and rotation

It is not the right fit when you need per-job priorities, unique jobs,
cron scheduling, callback orchestration, or richer job lifecycle controls.

### Language-specific Postgres job frameworks

River (Go) and Oban (Elixir; Oban Pro adds a partitioned tier) are the
clearest references here. That category is a good fit when you want a
job framework deeply shaped around one host language and one surrounding
ecosystem. If your stack is Go-only or Elixir-only, those are usually
the right answer.

## Awa's slot

Awa sits between those categories:

- it is a job queue, not just a stream
- it keeps Postgres as the only hard dependency
- it supports Rust and Python as first-class runtimes
- it uses segmented queue storage so queue history and lease churn do
  not sit in one mutable queue heap

The storage engine is a differentiator, not the whole identity. The
product story starts with "Postgres job queue for Rust and Python" and
then explains why the storage engine matters operationally.

## What you can rely on

- Queue storage keeps dead tuples out of the main ready path.
- Queue storage materially improves pickup-latency tails versus the
  pre-0.6 mutable-row engine.
- Awa keeps full job-queue behavior while using a segmented storage
  engine.

## See also

- [ADR-019 validation bench](adr/bench/019-queue-storage-validation-2026-04-19.md) — engine validation artifact
- [Cross-system benchmarks](https://github.com/hardbyte/postgresql-job-queue-benchmarking) — Awa vs pgque, procrastinate, pg-boss, river, oban, pgmq
