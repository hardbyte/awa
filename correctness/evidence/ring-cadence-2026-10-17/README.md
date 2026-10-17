# Ring rotation cadence comparison

This compares eager maintenance rotation with the observed-fill cadence in
issue [#518](https://github.com/hardbyte/awa/issues/518). Both policies use
the same executable: only the runtime policy environment variable differs.
`provenance.json` records the source revisions and executable digest.
`environment.json` records the image digest, resource limits and CPU topology.
The adapter descriptor flags the checkout as dirty because the report files
were untracked during execution; both recorded source-diff hashes are empty,
and every accepted cell verifies the same executable digest.

## Method

The runner copies the external
[benchmark harness](https://github.com/hardbyte/postgresql-job-queue-benchmarking)
into private directories. It adds the policy switch and catalog/ring metrics,
bypasses the harness's otherwise unconditional adapter rebuild, and verifies
the prebuilt executable digest before and after every cell. It leaves the
source harness checkout unchanged.

Each policy uses fresh PostgreSQL 18.6 data, its own Compose project and port,
a four-CPU database quota, 8 GiB database memory limit, and a disjoint set of
four physical CPU cores including their SMT siblings. Native adapters share
their policy's CPU set with its database. Eager uses CPUs `0-3,12-15` and port
15558; cadence uses `6-9,18-21` and port 15559. The policies run concurrently
on one development host, sharing storage and memory bandwidth. Each CPU set lies within one L3 cache group. Local database validation is
finished before the accepted matrix. Earlier exploratory cells overlapped
validation or used a candidate CPU set spanning both cache groups and are
excluded from the results. A warmed pair before the builder-order correction
was also rerun on the final executable. Core assignments were fixed, with no
crossover or repeated trials. These are single paired
engineering runs, not confidence intervals or general capacity claims. The
host is an AMD Ryzen 9 5900X with 12 physical cores and 24 hardware threads.

Configuration is 16 queue slots, eight claim slots, eight lease slots, one
stripe, compact receipts, approximately 256-byte payloads, and 1 ms synthetic handler work.
Queue and claim decision ticks are 1 s; the lease tick is 250 ms. PostgreSQL
durability and autovacuum settings come from the recorded harness manifest.

| Cell | Offered jobs/s | Workers | Warmup | Measured phase | Following phase |
| --- | ---: | ---: | ---: | --- | --- |
| True idle | 0 | 8 | 60 s | 180 s idle | — |
| Low load | 200 | 8 | 240 s | 240 s steady | — |
| Reference | 800 | 32 | 240 s | 240 s steady | — |
| Saturation | Depth target 4,000 (rate flag unused in this mode) | 64 | 240 s | 240 s steady | — |
| Retry storm | 200 | 16 | 240 s | 240 s with 30% transient, 1% poison | 120 s recovery |
| Logical stall | 400 | 16 | 240 s, then 60 s streaming | 240 s stalled consumer | 60 s catchup |

The claim batch size is 512 in every cell. The retry limit is five attempts. The logical publication covers all tables.
Every active cell must show at least 32 queue rotations and 16 claim
rotations during warmup: two actual wraps of each active ring. The unused
legacy lease ring remains idle with compact receipts. This check prevents a
fresh ring's initial reclamation delay from appearing as a steady-state
catalog improvement. True idle is measured separately from the earlier
programme sweep's 200 jobs/s cell labelled "idle".

Catalog update rates use differences in cumulative `pg_stat_sys_tables`
counters over the first-to-last sample interval within the measured phase.
The exporter uses recorded phase-start markers and declared durations to
exclude samples before a phase starts or during boundary snapshots and teardown.
Each run records these bounds and the excluded-sample count in `measurements.json`.
`excluded-samples.csv.gz` preserves the rejected rows, including a consumer
shutdown sample after the final catchup window.
Ring rotation rates use the same calculation on generation counters.
Dead-tuple values are PostgreSQL estimates; maxima include only the named
phase. Physical ring bytes include all partition children and their indexes,
TOAST and auxiliary forks. They include empty relation metadata and exclude
unpartitioned control/deferred tables, catalogs and retained WAL.

The harness's `completion_rate` counts successful handler returns, and its
`end_to_end_p99_ms` stops before durable completion acknowledgement. Those
are labelled **handler** metrics here. The histograms use 30-second rolling windows sampled every five seconds;
windows immediately after a phase change can include the preceding phase. A median of their p99s is not a whole-run p99. The PostgreSQL runtime regressions separately check
durable completion and safe reclamation; these measurements do not establish
a durable-completion latency distribution.

During the retry storm, the adapter's latency histogram covers only jobs
without an injected failure. Retried completions are included in the handler
rate and have separate scenario counters; their backoff latency is absent
from that histogram. The logical consumer discards decoded output, so the
catchup phase checks decoding progress rather than replica apply.

## Reproduction

From the Awa checkout, using the external harness's locked Python environment:

```bash
uv run --project /path/to/postgresql-job-queue-benchmarking --frozen python \
  scripts/benchmark-ring-cadence.py \
  --harness /path/to/postgresql-job-queue-benchmarking \
  --adapter-lock correctness/evidence/ring-cadence-2026-10-17/adapter-Cargo.lock.gz \
  --output /tmp/awa-ring-cadence

uv run --no-project python scripts/summarize-ring-cadence.py \
  /tmp/awa-ring-cadence /tmp/awa-ring-cadence-evidence
```

`--resume` skips cells that already have a successful record. The runner uses
the CPU sets and ports above; edit them for a different machine before running.
Full raw files, plots and logs remain in the selected output directory. The
compact evidence exports selected samples, aggregation inputs, per-run
manifests and raw-file digests.

## Saturation validation

The first 64-worker run on the cadence corrections, before the additional
prune fix, did not complete two queue wraps during warmup under either
policy. Those measurements are excluded. `rejected-pre-fix-saturation.json`
records the failed warmup gates. `pre-fix-lock-cycle.json` retains a live
read-only lock snapshot: pruning held the ready/attempt children exclusively
and waited for routing; a claimer held routing and waited for attempts.
The original order also exists on main.

Both policies in the accepted comparison include the fix: bounded exclusive
wait on routing first, `NOWAIT` on every later child, whole-transaction rollback
on contention, and next-tick retry for `Blocked`. Active-data gates retain
exponential backoff. The regression tests fail with the original acquisition
and pass with the fix. The lock model includes routing reads and a negative
witness for the old order. Progress still needs successful lock opportunities.

## Results

All 12 runs completed on runtime source `3f9fff9` using executable SHA-256
`ed6656893cb7b669dc0e0e7877952e1585652d2b1a279836f2f8d5c994b1b68a`.
Every active run passed the two-wrap warmup gate. Values below are **eager → cadence**.

### Catalog churn, handler throughput and retained ring storage

| Cell | `pg_class` updates/s | Reduction | Handler returns/s | Mean ring MiB | Maximum ring MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| True idle | 0.00 → 0.00 | — | 0.00 → 0.00 | 5.62 → 5.62 | 5.62 → 5.62 |
| 200 jobs/s | 29.86 → 5.67 | 81.0% | 199.98 → 200.01 | 13.46 → 27.22 | 16.16 → 28.53 |
| 800 jobs/s | 28.68 → 9.52 | 66.8% | 799.95 → 799.88 | 23.55 → 39.11 | 29.04 → 41.09 |
| Backlogged / 64 workers | 19.97 → 6.46 | 67.7% | 4,834.13 → 4,896.97 | 206.64 → 317.67 | 268.09 → 429.15 |
| Retry storm | 18.84 → 7.32 | 61.1% | 197.41 → 197.43 | 29.52 → 37.67 | 37.52 → 44.38 |
| Logical consumer stalled | 30.54 → 6.06 | 80.2% | 399.96 → 400.02 | 16.71 → 38.20 | 19.98 → 40.80 |

The catalog reduction comes with more retained ring storage. The size target does
not cap that storage. Handler throughput is similar in these paired runs; the
fixed core assignments and single trials do not establish a capacity or latency improvement.

### Handler latency windows

These are the median and worst **30-second rolling-window p99s**, sampled every
five seconds. They end at handler return, before durable acknowledgement. In the
retry row they cover only jobs without injected failures.

| Cell | Median window p99, ms | Worst window p99, ms |
| --- | ---: | ---: |
| 200 jobs/s | 53.52 → 39.01 | 61.02 → 56.03 |
| 800 jobs/s | 67.04 → 67.01 | 116.03 → 77.06 |
| Backlogged / 64 workers | 956.41 → 889.86 | 1,253.38 → 1,010.17 |
| Retry storm | 51.52 → 44.03 | 61.02 → 54.02 |
| Logical consumer stalled | 59.52 → 50.02 | 195.07 → 150.01 |

### CPU and actual warmup

CPU values are consumed CPU-seconds per wall-clock second. Generation counts
are the maximum observed during warmup, not the configured number of ticks.

| Cell | PostgreSQL CPU cores | Native adapter CPU cores | Warmup queue generations | Warmup claim generations |
| --- | ---: | ---: | ---: | ---: |
| True idle | 0.062 → 0.065 | 0.010 → 0.011 | 0 → 0 | 0 → 0 |
| 200 jobs/s | 0.675 → 0.636 | 0.091 → 0.095 | 184 → 36 | 236 → 36 |
| 800 jobs/s | 0.744 → 0.715 | 0.121 → 0.125 | 184 → 77 | 237 → 36 |
| Backlogged / 64 workers | 1.326 → 1.348 | 0.279 → 0.285 | 59 → 56 | 237 → 44 |
| Retry storm | 1.232 → 1.176 | 0.135 → 0.136 | 192 → 36 | 235 → 36 |
| Logical consumer stalled | 0.736 → 0.679 | 0.101 → 0.108 | 187 → 42 | 236 → 35 |

### Retry behavior

| Measured storm signal | Eager → cadence, per second |
| --- | ---: |
| Injected attempt failures | 68.09 → 68.10 |
| Retried successful handlers | 58.17 → 58.19 |
| Poison attempts exhausted | 1.67 → 1.67 |

At the end of the 120-second recovery, retryable depth was 0 → 0,
and injected failures/s were 0.00 → 0.00. The producer continued
offering clean jobs during recovery.

### Logical consumer pause and catchup

| Phase | Decoded output MiB/s | Retained WAL: first / maximum / last MiB, eager | Retained WAL: first / maximum / last MiB, cadence | Catalog xmin age: first → last, eager | Catalog xmin age: first → last, cadence |
| --- | ---: | ---: | ---: | ---: | ---: |
| Streaming | 0.27 → 0.27 | 0.91 / 15.81 / 11.11 | 0.78 / 8.08 / 2.03 | 348 → 721 | 307 → 730 |
| Paused | 0.00 → 0.00 | 13.84 / 139.58 / 139.58 | 4.56 / 125.00 / 125.00 | 1624 → 43926 | 1644 → 44017 |
| Catchup | 1.47 → 1.38 | 142.20 / 144.78 / 1.91 | 127.34 / 139.12 / 10.75 | 44823 → 672 | 44915 → 673 |

Mean paused flag during the stall: 1.000 → 1.000.
Consumer exits after catchup: 0 → 0;
disconnect counter: 1 → 1;
error counter: 1 → 1.

Both paused connections hit the server timeout. Each consumer logged exactly
one expected closed-connection error and one reconnect, without exiting.
Both slots were active at the final in-window sample, catalog-xmin age fell
back below 700 transactions, and retained WAL dropped substantially. Every
published table had replica identity. Decoded output is discarded: this checks
decoding recovery, not downstream replica apply.

## Evidence files

- `measurements.json`: phase bounds, counter inputs/rates, gauge summaries and raw-file digests for all 12 runs.
- `samples.csv.gz`: selected metric samples used for the summaries, including phase-start markers.
- `excluded-samples.csv.gz`: rows outside the declared phase windows, retained for audit.
- `*-manifest.json`: each run's CLI, phases, PostgreSQL settings and adapter metadata.
- `provenance.json`, `environment.json` and `adapter-Cargo.lock.gz`: source, executable, container and dependency identities.
- `validation.json`: completed-run, monotonic-counter, retry recovery and logical catchup checks.
- `pre-fix-lock-cycle.json` and `rejected-pre-fix-saturation.json`: diagnosis and rejected warmup evidence.
