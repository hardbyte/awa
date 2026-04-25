# ADR-023 branch review — 2026-04-25

Mid-branch review of the ADR-023 work on `feature/vacuum-aware-storage-redesign`
conducted 2026-04-25. Five specialist passes covered:

1. Hot-path SQL + race matrix (`awa-model/src/queue_storage.rs` — claim, complete,
   rescue, materialize, cancel, progress, heartbeat)
2. Schema lifecycle (prepare_schema, install, reset, rotate, prune, partition DDL)
3. TLA+ specifications (`correctness/storage/*.tla` + `MAPPING.md`)
4. Benchmark harness + chaos tests
5. Docs (ADRs, architecture, positioning, plan docs)

Findings are organised by severity. Wave 1 is being actively fixed; waves 2–4
are tracked here for follow-up PRs before Phase 5 merges.

---

## Wave 1 — must-fix before Phase 5 (ship-stoppers)

These combine to silently grow `lease_claims` without bound on the branch as
it stands.

- **`rotate_claims` has no busy-check.** (`awa-model/src/queue_storage.rs:9183`)
  Phase 3 made partitions real; the Phase-2-era "partitions don't exist yet"
  comment is stale. The ring advances onto partitions that still contain live
  rows, and the cursor lap wraps silently.

- **`complete_runtime_batch` never deletes `lease_claims` rows.** The
  receipt-completion path writes only a closure row and relies on partition
  TRUNCATE at prune time. With prune a no-op (next finding), every completed
  receipt accumulates both a claim row and a closure row for the lifetime of
  the runtime.

- **`prune_oldest_claims` is a no-op** (`queue_storage.rs:9242`) while
  `rotate_claims` advances every tick. The `claim_ring_slots` state machine
  rotates but never reclaims, so every receipt plane query at steady state
  scans an ever-growing append log.

- **Stale "Phase 2 note" comments** on both `rotate_claims` and
  `prune_oldest_claims` promise Phase 3 / Phase 5 would fill in the behaviour.
  Phase 3 shipped the partitions without wiring them to rotate, and Phase 5
  hasn't landed.

The fix is a real prune body (rescue-if-needed, `ACCESS EXCLUSIVE` on both
children, TRUNCATE both) plus a busy-check in `rotate_claims` that mirrors
`rotate_leases` (count rows in the target `lease_claims_<slot>` and
`lease_claim_closures_<slot>` children; skip if either has rows).

---

## Wave 2 — correctness fixes before merge

### Lock-order violations vs TLA+ `AwaStorageLockOrder`

- **`prune_oldest_leases` is missing `FOR UPDATE`** on `lease_ring_state` and
  `lease_ring_slots[slot]` (`queue_storage.rs:9089-9133`). `PruneLeasesPlan`
  in `AwaStorageLockOrder.tla:150` requires both. Concurrent rotate can
  race the prune's liveness check.

- **`rotate_leases` reads `lease_ring_state` without `FOR UPDATE`**
  (`queue_storage.rs:8899-8908`). Two rotators can both pass the busy-check,
  waste a child scan, and the loser ends up pointed at the current slot.

### Receipt-plane races

- **`close_receipt_tx` takes no `FOR UPDATE`** (`queue_storage.rs:5378-5392`).
  Under concurrent re-claim the non-unique `(job_id, run_lease)` lookup can
  pick the wrong `claim_slot`, writing the closure to the wrong partition
  or racing another rescue to duplicate the write. Accept `claim_slot` from
  the caller (already on `ClaimedRuntimeJob`) instead of looking it up here.

- **`rescue_stale_receipt_claims_tx` can rescue a materialized claim.**
  (`queue_storage.rs:6593-6606`) The stale predicate only checks
  `heartbeat_at`; it does not exclude claims already materialized into
  `leases`. Mirror the pattern in `load_job` (`queue_storage.rs:6814`):
  `AND NOT EXISTS (SELECT 1 FROM leases WHERE leases.job_id = claims.job_id
  AND leases.run_lease = claims.run_lease)`.

- **`cancel_job_tx` receipt-only branch can leave an orphan lease.**
  (`queue_storage.rs:5536-5680`) Between the "no lease row found" check and
  the commit, a concurrent `ensure_running_leases_from_receipts_tx` can
  materialize a `leases` row. The cancel writes a done row + closure, but a
  lease is now live for a cancelled job. Add a defensive `DELETE FROM leases
  WHERE job_id = $1 AND run_lease = $run_lease` at the tail of the branch,
  or lock the `(job_id, run_lease)` row in `leases` at the top.

### Schema-install hygiene

- **`open_receipt_claims` backfill runs every `prepare_schema`**
  (`queue_storage.rs:2310-2355`) — scans all of `lease_claims` on every
  runtime start, and after Phase 4 nothing reads the table. Gate it on the
  legacy-table `EXISTS` check the earlier migration uses, or delete it
  entirely (Phase 5 drops the table).

- **Anti-join inside that same backfill is missing `claim_slot`**
  (`queue_storage.rs:2338-2343`). It filters closures by `(job_id, run_lease)`
  only — the only lax anti-join in the file. If a future re-claim ever
  lands the same pair in a different partition, the anti-join treats a live
  claim as closed.

- **Slot-0 backfill for `open_receipt_claims.claim_slot` is unguarded when
  `current_slot = 0`.** (`queue_storage.rs:2196-2204`) The guard
  `current_slot <> 0` means legacy `claim_slot = 0` rows never get their
  real slot assigned when the ring happens to be at slot 0 — and new claims
  landing at slot 0 while the backfill is running are indistinguishable
  from unmigrated rows. Track migration via an explicit sentinel.

- **In-place `INSERT ... SELECT ... DROP` is not transactional**
  (`queue_storage.rs:2128-2153`). A crash between the copy and the drop
  leaves both the legacy table and the partitioned parent populated; the
  next run's `ON CONFLICT DO NOTHING` preserves correctness, but wrap the
  copy + drop in `pool.begin() ... commit()` to make recovery explicit.

- **`reset()` TRUNCATE list doesn't include the `_legacy` tables**
  (`queue_storage.rs:3099-3116`). On a partial-migration schema `reset()`
  fails.

### Admin cancel not modeled

- **Admin cancel of a running job has no TLA+ action or invariant.**
  (`AwaSegmentedStorage.tla:847, 1581-1620`) Commit `0334490` ships a new
  `Running → Terminal (cancelled)` path with `pg_notify`; the spec only has
  `CancelWaitingToTerminal`. Any bug in `cancel_job_tx`'s close-receipt +
  delete-lease + insert-done ordering passes the model. Add
  `CancelRunningToTerminal(j)` and `CancelReceiptOnlyToTerminal(j)` to
  `Next` and `TraceStep`.

---

## Wave 3 — formal-model fidelity

- **`AwaStorageLockOrder.tla` models `FOR SHARE` locks that Rust doesn't
  take.** `ClaimPlan` (`AwaStorageLockOrder.tla:120-126`) includes shared
  reads of `lease_ring_state` and `claim_ring_state`. `grep "FOR SHARE"
  awa-model/src/queue_storage.rs` → 0 hits. Rust uses subquery + CAS. Spec
  proves a stronger plan than what ships. Either model CAS explicitly or
  document that the spec asserts a desired, not current, lock plan.

- **`RotateClaimsPlan` overstates locking.** Models child reads; actual
  `rotate_claims` is pure ring-state CAS. Fix after Wave 1 adds the real
  busy-check (at which point the spec becomes accurate).

- **`PruneClaimsPlan` models Phase-5 target, not Phase-2 no-op.** The cfg
  should be commented to say which phase it's checking.

- **Spec claim bookkeeping is indexed by `job`, not `(job, run_lease)`.**
  (`AwaSegmentedStorage.tla:370-389, 1530-1543`) On re-claim the binding to
  the old partition's closure is wiped. `PruneClaimSegment(oldSeg)` sees the
  old partition as empty — can't reach the violating state where a closure
  row still needs rescue-before-truncate.

- **Trace harness missing `RescueStaleReceipt`**
  (`AwaSegmentedStorageTrace.tla:58-81`). Tests that exercise this path
  can't be transcribed.

- **`MAPPING.md` symbol and line-number drift.** Claim ring rows still say
  "pending Phase 4" while the same doc's section heading says "Phase 4
  landed". Multiple Rust function names in the table no longer exist or
  have moved (`insert_ready`, `insert_deferred`, `wait_external`,
  `resume_external`, `fail_to_dlq_terminal`, `retry_runtime`, `purge_dlq_job`,
  `upsert_attempt_state`). Line numbers are 175 commits stale. Either drop
  line numbers or add a `// MAPPING` marker pattern in the Rust and grep for
  it.

- **README.md coverage counts are pre-ADR-023** (`README.md:172`, says
  "2,076 distinct states"). MAPPING says 9,680 after claim-ring additions.

---

## Wave 4 — observability, bench hygiene, docs

### Benchmark harness

- **`compute_summary` aggregates across replicas without `instance_id`.**
  (`benchmarks/portable/bench_harness/writers.py:264-294`) With `--replicas
  N`, `summary.json -> systems.<s>.phases.<p>.metrics.queue_depth.{median,
  peak, count}` mixes `N` sample streams. Peaks are correctly max-over-all
  but medians and counts are wrong. Either include `instance_id` in the
  bucket or pre-aggregate with `aggregate_replica_metric_series`.

- **Non-awa adapters don't honour `PRODUCER_ONLY_INSTANCE_ZERO`.** Only
  `awa-bench/src/long_horizon.rs` gates observer-polled metrics on
  `instance_id == 0`. Every other adapter (`pgmq`, `pgque`, `procrastinate`,
  `awa-python`, `oban`, `pg-boss`, `river`) emits queue-depth etc. from
  every replica. Multi-replica cross-system comparisons are broken for
  these.

- **`check-bench-regression.py:256-258` has `sys.exit(1)` commented out.**
  Combined with `continue-on-error: true` on every bench step in
  `nightly-chaos.yml:159-192`, the nightly regression gate isn't actually
  enforced.

- **`benchmarks/baseline.json` has no Phase-4 entries** for
  `queue_counts_cached`, `lease_claim_closures` size, or per-table
  `n_dead_tup`. Phase 5/6 land into a vacuum.

- **`_autovacuum_count_delta` uses `values[-1] - values[0]`** based on
  raw.csv row order. Order is best-effort (queue → CSV) and not sorted by
  `elapsed_s`. `autovacuum_count` is monotonic; use `max - min` instead.

- **Producer rebuilds `QueueStorage` every batch**
  (`awa-bench/src/long_horizon.rs:577-585`). Allocator pressure at high
  producer rates; make it once outside the loop.

### Chaos coverage

The reviewer flagged three chaos gaps. The first two are documented
caveats; the third (receipt-plane chaos coverage) is **deferred to a
follow-up branch** before Phase 5 merges. Status:

- **"Outage" tests are connection-drop, not real PG restart.**
  Documented as a known limitation. `chaos_suite_test.rs` uses
  `pg_terminate_backend`; only `postgres_failover_smoke_test.rs` does
  a real `docker compose restart`. The distinction matters because a
  TCP drop preserves the server's WAL state, while a process restart
  exercises crash recovery. **Acceptance for Phase 5:** document this
  caveat in `docs/testing.md` (does not exist; add or fold into
  `architecture.md`'s testing section). A second-mode chaos test that
  does a real Postgres restart is desirable but not blocking.

- **No network partition / SIGSTOP leader scenario.** Documented as
  a known limitation. The asymmetric "leader alive to itself, dead
  to followers" failure mode requires either iptables fakery or
  process-pause primitives that don't fit cleanly into the
  containerised CI stack. **Acceptance for Phase 5:** track in an
  issue; not a merge blocker.

- **Zero chaos coverage of receipt-plane / claim-ring / partition
  truncate.** Deferred to a follow-up branch. The four scenarios that
  need coverage before Phase 5 merges:
  1. Receipt rescue under overload — flood `lease_claims` past the
     active partition's size threshold and confirm
     `rescue_stale_receipt_claims_tx` makes progress without lapping
     the partition (relies on the Wave 2c `leases` anti-join).
  2. Claim-ring prune racing live claims — fire `prune_oldest_claims`
     concurrently with steady claim/complete traffic and confirm
     `SkippedActive` returns rather than truncating an open claim.
     The Wave 1 `test_prune_oldest_claims_refuses_to_truncate_open_claim`
     covers the static case; the chaos version drives concurrent
     traffic.
  3. Partition `TRUNCATE` racing a closure write — confirm the
     `LOCK TABLE ACCESS EXCLUSIVE` in `prune_oldest_claims` makes
     `INSERT INTO lease_claim_closures` block (or fail with the
     50ms `lock_timeout`), not silently lose the closure write.
  4. Admin cancel during completion — confirm the receipt-only
     branch's defensive `DELETE FROM leases` (Wave 2d) leaves no
     orphan when materialize and cancel race over the same claim.
  These scenarios should land in a new
  `awa/tests/receipt_plane_chaos_test.rs` or as additional cases in
  `chaos_suite_test.rs`. Tracked under "ADR-023 Phase 5 acceptance"
  rather than this review's wave numbering.

### Docs

**Blocker (operator will be misled):**

- `docs/architecture.md:21, 34, 47, 74, 313` still describes
  `open_receipt_claims` as a live hot-plane table.
- `docs/architecture.md:273-328` claim-path description says
  "`FOR SHARE` on `lease_ring_state`" — directly contradicts `MAPPING.md`.

**Serious:**

- `docs/adr/019-queue-storage-redesign.md` validation section names the
  pre-ADR-023 frontier; no "superseded for the receipt plane by ADR-023"
  pointer.
- `docs/perf-review-2026-04-22.md` needs a top banner noting it's a
  pre-ADR-023 snapshot and the `open_receipt_claims` finding triggered
  Phase 1–4 work.
- `docs/lease-plane-redesign-spike.md` needs a "superseded by ADR-023 for
  the receipt plane" status banner.
- `docs/queue-striping-plan.md`, `docs/bounded-claimers-plan.md`,
  `docs/sticky-shard-leasing-plan.md` — no status blocks distinguishing
  "current design" from "reverted experiment".
- `correctness/storage/MAPPING.md` has a "Phase 4 landed" heading above
  per-row claims that still say "pending Phase 4 cutover". Internal
  contradiction.
- ADR-023 Phases 5 and 6 marked `(pending)` with no acceptance criteria.
- No operator documentation for the cancel-listener failure mode. The
  listener logs a warning and exits; nothing in `troubleshooting.md`,
  `deployment.md`, or `architecture.md` says how to detect or recover.
- No Phase-3 migration recovery doc in `migrations.md`.
- `README.md:371-393` ADR list stops at 021 — missing 022 (descriptor
  catalog) and 023 (this ADR).
- `docs/configuration.md` missing `claim_slot_count`, `CLAIM_SLOT_COUNT`
  env var, and `ClientBuilder::claim_rotate_interval`.
- `docs/migrations.md` missing a drafted 0.6 upgrade note (partition
  migration; Phase 6 default flip).

---

## Fix tracking

- **Wave 1:** fixed in commit `137e9ef` (real rotate-busy + prune-truncate).
- **Wave 2:** fixed in the commit alongside this update.
  - 2a: `FOR UPDATE` on `lease_ring_state` in `rotate_leases`; full
    `state → slot row → child` lock sequence in `prune_oldest_leases`.
  - 2b: `close_receipt_tx` now serialises via a `FOR UPDATE` CTE on the
    target `lease_claims` row, blocking concurrent materialization.
  - 2c: `rescue_stale_receipt_claims_tx` excludes claims already
    materialized into `leases`.
  - 2d: `cancel_job_tx` receipt-only branch defensively `DELETE`s any
    `leases` row that materialized between the top-of-function leases
    check and the FOR UPDATE on claims.
  - 2e: `prepare_schema` legacy migration (claims and closures) wraps
    copy + drop in a transaction; the `open_receipt_claims` backfill is
    gated on `legacy_exists`, the anti-join now matches on
    `claim_slot`, and the `claim_slot` ALTER uses `-1` as an
    unambiguous sentinel rather than conflating with a legitimate
    slot 0. `reset()` drops legacy tables before TRUNCATE.
- **Wave 3:** TLA+ fidelity + MAPPING.md cleanup landed alongside this
  update.
  - 3a: `AwaStorageLockOrder.tla` — dropped the bogus `FOR SHARE` on
    `lease_ring_state` / `claim_ring_state` from `ClaimPlan` (Rust
    takes no such lock — the rotator's CAS UPDATE is what serialises
    them); split the claim plan into receipts vs legacy modes; added
    plans for `close_receipt_tx`, `rescue_stale_receipt_claims_tx`,
    `ensure_running_leases_from_receipts_tx`, and the two
    `cancel_job_tx` branches; re-classified table-level INSERTs as
    `ModeShared` so two concurrent inserts to the same partition no
    longer report a false-positive deadlock. TLC now passes 39,040
    distinct states (was 9,680). The discovery: with the old "INSERT
    is exclusive" abstraction, complete-vs-cancel raced into a real
    AB-BA pattern in the model — but Postgres `RowExclusive` is
    compatible with itself, so it was a model error rather than a
    code bug. Moot for now since the Rust code never hit it; logged
    as part of the spec hardening.
  - 3b: `MAPPING.md` swept of stale function names, line numbers, and
    "pending Phase N" hedges that contradicted the post-Phase-4
    headings; same for `correctness/{,storage/}README.md`'s state-count
    references.
  - 3c: Added `CancelRunningToTerminal(j)` and
    `CancelReceiptOnlyToTerminal(j)` to `AwaSegmentedStorage.tla` so
    the model covers the running-lease and receipt-only branches of
    `cancel_job_tx` (in addition to the pre-existing
    `CancelWaitingToTerminal`); both clear every worker's
    `taskLease[w][j]` snapshot since admin cancel has no worker
    context. Wired into both `Next` and the trace harness's
    `TraceStep`. TLC base spec passes 2,326,528 distinct states.
  - Deferred: re-keying `claimOpen` / `claimClosed` from `Jobs` to
    `Jobs × RunLease` (the per-attempt bookkeeping). The reviewer's
    concern about losing the closure binding on re-claim is real but
    only fires if rescue-and-re-claim aren't atomic — they are
    (single `rescue_stale_receipt_claims_tx` transaction), so the
    spec gap doesn't currently mask a code bug. Logging as a
    nice-to-have for a future refinement.
- **Wave 4:** separate follow-up PR before Phase 5 merges to main.
  Each wave closes a finding; this file is updated as findings resolve.
  Delete this file (or fold it into an ADR-023 validation artifact)
  before the 0.6 release.
