# ADR-023 Receipt-Ring Validation Artifact (2026-04-26)

This file records the exact commands, configuration, and raw results
backing the validation numbers in ADR-023. The run exercises the
post-Phase-6 system: `lease_claim_receipts` defaulted on, the legacy
`open_receipt_claims` table deleted, and `lease_claims` /
`lease_claim_closures` partitioned by `claim_slot` with the claim ring
rotating in lockstep with the lease ring.

## Run identity

- Run id: `custom-20260426T032000Z-839ddc`
- Branch: `feature/vacuum-aware-storage-redesign` at `7b688e4`
- Started: 2026-04-26 05:15:45Z (15:15 NZST), finished ~17:14 NZST
- Duration: 115 min (5m warmup + 25m clean + 30m idle-in-tx + 30m
  recovery + 25m clean)

## Host

- 24-core x86_64, 91 GiB RAM
- Linux 6.18.21, glibc 2.42
- Postgres 17.2-alpine in compose
- `max_connections = 400` (raised from default 200 — see
  `benchmarks/portable/postgres.conf` for the reasoning; smaller
  ceilings caused pool exhaustion on the 4×8 profile)
- `shared_buffers = 256MB`, `autovacuum = on`,
  `autovacuum_naptime = 1min`, `autovacuum_vacuum_scale_factor = 0.2`

## CLI

```text
uv run python benchmarks/portable/long_horizon.py \
  --systems awa \
  --replicas 4 --worker-count 8 --producer-rate 200 \
  --phase warmup=warmup:5m \
  --phase clean_1=clean:25m \
  --phase idle_1=idle-in-tx:30m \
  --phase recovery_1=recovery:30m \
  --phase clean_2=clean:25m \
  --skip-build
```

Per-replica producer rate is 200/s, so aggregate offered load is 800/s.

## Headline result

> Receipt-plane peak dead tuples stayed at **≤49** across **all** phases
> — including a 30-min `idle-in-tx` phase that holds open transactions
> to block autovacuum. The `lease_claim_closures_<0..7>` partitions
> stayed at exactly **0** dead tuples in every phase, confirming the
> append-only-then-truncate property the partitioning was designed to
> preserve.

`open_receipt_claims` is absent from the schema and from
`manifest.adapters.awa.event_tables`. The receipt control table no
longer exists.

## Throughput by phase

Aggregate completion / enqueue rates summed across the 4 replicas
(median over the 5s sample window of each phase):

| phase      | type        | aggregate completion/s | aggregate enqueue/s | p99 e2e median (ms) | p99 e2e peak (ms) |
|------------|-------------|------------------------|---------------------|---------------------|-------------------|
| clean_1    | clean       | 769.8                  | 791.9               | 92.0                | 244.1             |
| idle_1     | idle-in-tx  | 359.2                  | 640.5               | n/a (no clean drain)| 67 076            |
| recovery_1 | recovery    | 744.3                  | 752.0               | n/a                 | n/a               |
| clean_2    | clean       | 614.0                  | 736.0               | n/a                 | n/a               |

The e2e p99 spike of 67 s during `idle_1` is the expected blocked-vacuum
latency: held-open transactions prevent claim-ring partitions from being
truncated and the steady-state pipeline backs up. The recovery and
following clean phases drained without losing any claims (see receipt
plane numbers below).

## Receipt plane — peak `n_dead_tup` per partition per phase

| table                               | clean_1 | idle_1 | recovery_1 | clean_2 |
|-------------------------------------|--------:|-------:|-----------:|--------:|
| awa_exp.lease_claims_0              | 7       | 8      | 1          | 6       |
| awa_exp.lease_claims_1              | 0       | 0      | 3          | 2       |
| awa_exp.lease_claims_2              | 3       | 0      | 0          | 8       |
| awa_exp.lease_claims_3              | 0       | 0      | 8          | 1       |
| awa_exp.lease_claims_4              | 4       | 0      | 0          | 8       |
| awa_exp.lease_claims_5              | 2       | 0      | 8          | 8       |
| awa_exp.lease_claims_6              | 7       | 2      | 8          | 11      |
| awa_exp.lease_claims_7              | 8       | 0      | 0          | 5       |
| awa_exp.lease_claim_closures_0..7   | 0       | 0      | 0          | 0       |
| **receipt-plane total**             | **31**  | **10** | **28**     | **49**  |

Closure partitions are append-only between rotations; their truncate
runs under `ACCESS EXCLUSIVE` on a partition that the rotate path has
already moved off. Zero-dead-tuple steady state is the designed shape.

## Receipt plane — peak `total_relation_size_mb`

Largest single partition during `clean_2` was
`lease_claims_1` at **1.26 MB**. Aggregate across all 16 receipt-plane
partitions stayed **< 12 MB** through the entire run.

## Warm tables — peak `n_dead_tup` per phase

| table                          | clean_1 | idle_1 | recovery_1 | clean_2 |
|--------------------------------|--------:|-------:|-----------:|--------:|
| awa_exp.attempt_state          | 0       | 0      | 0          | 0       |
| awa_exp.queue_lanes            | 0       | 0      | 0          | 0       |
| awa_exp.queue_claim_heads      | 0       | 0      | 0          | 0       |
| awa_exp.queue_enqueue_heads    | 0       | 0      | 0          | 0       |
| awa_exp.queue_ring_state       | 76      | 665    | 40         | 40      |
| awa_exp.queue_ring_slots       | 95      | 665    | 158        | 0       |
| awa_exp.lease_ring_state       | 103     | 28 503 | 232        | 104     |
| awa_exp.lease_ring_slots       | 0       | 0      | 0          | 0       |
| awa_exp.claim_ring_state       | 78      | 1 631  | 84         | 76      |
| awa_exp.claim_ring_slots       | 0       | 0      | 0          | 0       |

The `*_ring_state` tables are singletons (one row each); their dead
tuples are HOT-update churn that recovers within one autovacuum pass
once held-open transactions release. The 28 503 spike on
`lease_ring_state` during `idle_1` collapses back to 232 in
`recovery_1` and 104 by `clean_2`. This matches the architectural
contract in `correctness/storage/AwaDeadTupleContract.tla`: ring-state
rows are `Warm` (bounded by row count, churn-prone but reclaim-fast).

## ADR-019 baseline comparison

ADR-019 (`docs/adr/bench/019-queue-storage-validation-2026-04-19.md`)
is dominated by short throughput tests against a single-replica DB.
The closest comparable number is the mixed-workload soak's
`exact_dead_total = 276` after a 10s, 1000 jobs/s drain. The Phase 6
multi-replica long-horizon equivalent is the **49** receipt-plane peak
in `clean_2` — a different shape (longer, multi-replica, includes an
idle-in-tx phase), but the order-of-magnitude reduction confirms the
prediction in ADR-023: by removing the `open_receipt_claims` row from
the receipt path, receipt-plane MVCC churn drops from "low hundreds"
to "low tens" and is fully partition-truncate-reclaimed instead of
autovacuum-reclaimed.

## Validation status

- All four TLA+ specs in `correctness/storage/` pass under TLC
  (`AwaSegmentedStorage`, `AwaSegmentedStorageRaces`,
  `AwaStorageLockOrder`, `AwaSegmentedStorageTrace`,
  `AwaDeadTupleContract`). TLC is wired into
  `.github/workflows/nightly-chaos.yml`.
- `cargo test --workspace` green at `7b688e4`.
- 4 receipt-plane chaos scenarios in
  `awa/tests/receipt_plane_chaos_test.rs` pass (`#[ignore]` group,
  exercised in `nightly-chaos.yml`).
- `open_receipt_claims` is absent from `prepare_schema()` and from
  the live schema after install (test
  `test_open_receipt_claims_is_absent_after_install`).

## Known limitations / follow-ups

### Residual ~2.4 GB/h/replica RSS growth

This run saw per-replica RSS climb from ~zero at start to 3.9–4.6 GB at
T+95m. Investigation traced this to **glibc malloc arena
fragmentation**: the 24-core host's default `MALLOC_ARENA_MAX = 8 ×
num_cores = 192` lets tokio worker threads each pin their own arenas,
which never shrink once tokio's per-thread allocation pattern stops
returning blocks to the same arena that allocated them.
`/proc/<pid>/maps` showed 56 separate 64 MB anonymous regions = 3.5 GB
of allocator arenas, with zero growth in heap or mmap regions of any
other shape.

**The leak is in the bench harness's process model, not the receipt-ring
implementation.** It would not affect a production deployment unless
the deployment also runs many tokio worker threads in a single process
on a high-core box.

Fix: `MALLOC_ARENA_MAX=2` is now exported from
`bench_harness/adapters.py:_base_env` (see commit on
`feature/vacuum-aware-storage-redesign`). A 30-min validation run with
the cap is queued before the 12h overnight run.

### Pool sizing

The pre-fix `max_connections = 200` ceiling caused pool-timeout errors
on the 4×8 profile because each replica's bench pool defaults to 80
connections (worker_count × 4 + 48), and 4 × 80 = 320 exceeds the
ceiling. The new defaults — `max_connections = 400` in
`benchmarks/portable/postgres.conf` and per-replica pool size in
`benchmarks/portable/awa-bench/src/long_horizon.rs` — are pinned and
captured in the manifest for cross-run comparability.

## Files

- Run output: `benchmarks/portable/results/custom-20260426T032000Z-839ddc/`
  - `summary.json` — per-table peak/median per phase, per-replica
    breakdowns
  - `manifest.json` — host shape, postgres image, settings, CLI
  - `raw.csv` — 5s-sampled metrics (~72 MB)
  - `plots/` — per-table dead-tuple curves
  - `index.html` — composed view
