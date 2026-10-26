# Durable finalization recovery: warmed load checks

The reviewed runtime retains saved results across connection loss, fences replay by the original attempt/lease, drains tracked finalizers, and bounds retry awaits by an absolute deadline. Its finalizer budget also permits a flushing completion batch and a queued batch per shard. The initial limit of one result per worker prevented configured full batches from forming; `initial-worker-cap/` preserves that investigation. The full-batch SQL regression rejects that limit and verifies durable drain after backpressure.

Baseline: `88f9a6906eb9a42f9ad8283ac1389ca64615d59f`. Candidate: `a8a5eb8cdc751439065815751cf446d9b339e10d`.

This packet accompanies [PR #521](https://github.com/hardbyte/awa/pull/521). The executable identities are frozen in `*-build.json`; the source verification is linked in the PR description.

## Results

Values are **baseline → candidate**. These are single paired runs on a shared host; they do not establish a capacity or latency ranking.

| Cell | Handler returns/s | Median window p99, ms | Worst window p99, ms | Mean queue depth |
| --- | ---: | ---: | ---: | ---: |
| 800 jobs/s, 32 workers | 800.03 → 800.03 | 68.54 → 72.06 | 83.01 → 95.04 | 12.79 → 13.44 |
| Depth target 4,000, 64 workers | 4034.07 → 4049.63 | 622.08 → 626.69 | 857.09 → 1204.22 | 713.81 → 804.71 |

Both cells completed without a `Failed to complete job` log entry. This is not a count of durable acknowledgements; the boundary and shutdown limitations are described below.

### Actual warmup generations

Every cell requires at least 32 queue and 16 claim generations before measurement.

| Build | Reference queue / claim | Saturation queue / claim |
| --- | ---: | ---: |
| Baseline | 73 / 36 | 54 / 37 |
| Candidate | 74 / 35 | 37 / 37 |

## Method and scope

Each build uses a frozen executable, checked by SHA-256 before and after
every cell. The same 274 dependency package versions and 21 identical
Python/Rust harness source files are used on each side; both effective adapter
lockfiles and source hashes are included. The #521 candidate enables
`tokio-util`'s `rt` feature, adding a dependency edge without changing versions. Both #524 builds already include that feature.
`runner-provenance.json` identifies the external harness revision and its
pre-existing modified adapter lockfile; the private copies use the published
locks. `*-build.json` identifies the Awa revision and records an empty runtime
diff. Build logs and local Cargo patches are included. Manifests may report `dirty: true` because of untracked evidence files; the recorded runtime diff is empty and each frozen executable is checked against its build digest. `build-environment.json`
records the current host toolchain and selected environment from the running
driver; it is not a historical trace of compiler invocations or a promise of
byte-for-byte executable reproduction.

The external harness runs PostgreSQL 18.6 with synchronous commit and
autovacuum enabled. Each PostgreSQL container has a four-CPU quota and 8 GiB memory limit.
Native adapters have CPU affinity without a cgroup CPU quota. Each side uses
fresh data and disjoint CPU sets: baseline `0-3,12-15`, candidate
`6-9,18-21`, including SMT siblings. Both the adapter and its database use
that side's CPU set. The host is a 12-core/24-thread Ryzen 9 5900X. Other
user workloads remained active, and the two runs share storage and memory
bandwidth. `host-load.csv` records load and CPU/IO/memory pressure during the
phase windows. Assignments are fixed, without crossover or repeated trials.
These are descriptive engineering regression checks; they do not establish
confidence intervals, a capacity ranking or a latency improvement.

Configuration: 16 queue slots, eight claim slots, eight lease slots, one
stripe, compact receipts, claim batches of 512, approximately 256-byte
payloads and 1 ms synthetic handler work. Rotation uses the default cadence,
with a 1 s queue decision tick and 250 ms lease tick. Each cell warms for
240 s and measures for 240 s. Warmup must record at least 32 queue and 16
claim generations: two actual wraps of each active ring. The unused legacy
lease ring stays idle. Reference offers 800 jobs/s to 32 workers; the
64-worker saturation cell maintains a queue-depth target of 4,000 (the
50,000 rate flag is unused in depth-target mode).

The harness counts successful **handler returns**, before durable completion
acknowledgement. The `end_to_end_p99_ms` latency values summarize overlapping **30-second rolling
handler-end p99 windows**, sampled every five seconds. Their median and
maximum are not whole-run p99s; initial measure snapshots can include the
preceding warmup under the same load. PostgreSQL regressions separately test
durable finalization and replay correctness.

## Evidence interpretation

`*-raw.csv.gz` preserves each complete raw CSV and matches its recorded digest.
`*-samples.csv.gz` and `*-excluded.csv.gz` partition the selected metrics using
the raw phase-start markers and nominal durations; `*.json` retains aggregation
inputs and sample intervals. Counter rates use the first-to-last in-window
delta, gauges use the recorded snapshots. `results.json` contains the report's
values. Logs include adapter stderr, so the zero `Failed to complete job`
count is checked in the actual native-output log. It does not prove that
every handler return was durably acknowledged before harness shutdown.

The inherited manifest's `started_at` field is its export time, not the run
start. Use raw phase markers and sample timestamps for timing. The final
runner records every observed replacement database in `observed_containers`;
its last `container` identifies the instance used after preflight. Separate
`*-measured-container.json` snapshots directly check resource limits during
raw phase windows. Compose configuration, PostgreSQL settings and image
identity are retained. In the initial investigation packet only, reference
container limits were inferred from identical unchanged Compose settings
after direct preflight validation; its saturation instances were observed
directly.

## Reproduction

Use the external harness revision in `runner-provenance.json`, a checkout of
each Awa source revision in `*-build.json`, and its frozen Python environment:

```bash
uv run --project /path/to/postgresql-job-queue-benchmarking --frozen python \
  runner.py \
  --baseline /path/to/baseline-awa \
  --candidate /path/to/candidate-awa \
  --harness /path/to/postgresql-job-queue-benchmarking \
  --output /tmp/awa-review-reproduction \
  --adapter-lock baseline-adapter-Cargo.lock.gz \
  --pr 521 --priorities 1
```

Use `521` with priorities `1`, or `524` with priorities `1,2`. Edit the runner's
CPU sets, ports and native build-cache path for another host. Keep validation builds/tests off the
benchmark host while sampling. The runner applies the existing cadence
helper's private-copy instrumentation and exports the audit packet.


## Report export and validation

`package.py` re-exports the completed driver output, checks the raw digests and
warmup gates, and selects host-load rows from the raw phase timestamps.
`report.py` renders this report using `METHOD.md`. From this packet directory:

```bash
uv run --no-project python package.py 521 /tmp/awa-review-report \
  --root /tmp/awa-review-reproduction --host-load /path/to/host-load.csv
uv run --no-project python report.py 521 /tmp/awa-review-report
```

`host-monitor.py` is the observation script used for these runs. Adjust its
output, tag and stop-file paths when reproducing. `validation/` preserves
local gate logs and negative-regression evidence; the PR links the full CI
run on the exact tested runtime source.
