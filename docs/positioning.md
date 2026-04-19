# Awa Positioning

## Positioning

Awa is the Postgres job queue for teams that want full job-queue features and
care about Postgres vacuum pressure under sustained load.

The useful framing is:

- Awa is the job-queue option in the vacuum-aware tier.
- It is a job queue, not just a message queue.
- The interesting claim is low dispatch latency with bounded hot-path dead
  tuples.

Avoid turning "vacuum-aware" into the whole pitch. That is a design stance, not
the product category.

## Where Awa Fits

There are two nearby categories Awa should position against.

### Event and message queues

PgQue is the clearest reference point here. PgQue itself is new, but it
deliberately revives the older PgQ storage ideas from Skype-era Postgres queue
systems.

That category is a strong fit when you want:

- a shared event log
- independent consumer cursors
- stable sustained-load behavior

It is not the right category when you want:

- per-job priorities
- unique jobs
- cron scheduling
- rich per-job lifecycle
- low-latency dispatch

### Language-native Postgres job frameworks

River and Oban Pro are the clearest references here.

That category is a strong fit when you are all-in on one surrounding runtime
and want a mature framework shaped for that ecosystem.

## Awa's Slot

Awa should sit between those two categories:

- full Postgres job-queue semantics
- first-class Rust and Python workers
- transactional enqueue
- priorities, retries, snoozes, cron, callbacks, DLQ, UI
- a storage engine designed around bounded hot-path churn rather than a mutable
  queue heap
- runtime-owned dispatch, rescue, rotation, and prune, with no `pg_cron`
  requirement

That is the differentiator. Not "Awa is vacuum-aware." Instead:

> Awa is the Postgres job queue for teams that care about vacuum pressure.

## What We Should Say

- Postgres-native job queue for Rust and Python.
- Full job-queue features without Redis or RabbitMQ.
- Runtime-owned maintenance; no ticker or `pg_cron` required.
- Designed for low dispatch latency and bounded hot-path dead tuples.
- A real worker runtime in both Rust and Python, not just insert-only clients.

## What We Should Not Say

- Do not market "uniquely vacuum-aware."
- Do not say "zero bloat" unless the benchmark literally proves it for the
  named path.
- Do not frame Awa as "better PgQue."
- Do not make the strongest latency-plus-bloat claim until the head-to-head
  benchmark exists.

## Proof Burden

The strongest public claim would be:

> Low dispatch latency and bounded dead tuples.

That should only become headline copy after a clean same-hardware comparison
against:

- Awa queue storage
- PgQue
- River
- optionally Oban Pro as a paid partitioned reference

The comparison set should include:

- idle pickup latency
- sustained runtime throughput
- overlap readers / MVCC horizon pressure
- mixed workload soak
- terminal-failure burst

Commands and raw output should live in-repo.

## What Is Safe To Say Now

The current local validation already supports these weaker claims:

- queue storage keeps dead tuples out of the main ready path
- queue storage materially improves pickup-latency tails versus Awa's old
  mutable-row engine
- the redesign keeps full job-queue behavior rather than collapsing to a simple
  enqueue/dequeue system

Reference artifact:

- [ADR-019 validation bench](adr/bench/019-queue-storage-validation-2026-04-19.md)

## Candidate Lines

Use now:

- The Postgres job queue for the vacuum-aware tier.
- A Postgres job queue with priorities, callbacks, DLQ, and bounded hot-path
  dead tuples.

Use only after the head-to-head benchmarks hold:

- Low dispatch latency and bounded dead tuples.
