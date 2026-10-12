> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# ADR-044: Gate A — storage evolution for 0.7; the segment-engine RFC graduates to 0.8

## Status

Accepted — decided 2026-08-22 against the evidence listed below. This is the
**Gate A** scope decision defined in the [0.7 roadmap §5](../../0.7-roadmap/index.md)
and required by the [#383](https://github.com/hardbyte/awa/issues/383)
performance contract ("Record the #295 Gate A decision"). It resolves
[#295](https://github.com/hardbyte/awa/issues/295) for the 0.7 scope: no
segment-storage restructuring migrations enter 0.7; the RFC graduates to 0.8
with its evidence attached.

## Context

[#295](https://github.com/hardbyte/awa/issues/295) proposes replacing
queue_storage's mutable lifecycle shape with append-only rotation segments plus
a cursor allocator, motivated by the #169 finding that a pinned MVCC horizon
degraded a 0.6-era engine from 799 → 387 jobs/s at 800 jobs/s offered over a
2-hour pin. The roadmap's decision D1 defaulted to *evolution of the existing
engine via staged migrations* and made that default reversible only by Gate A:
restructuring migrations enter 0.7 **only if all three** hold:

1. a prototype is ≥ parity clean-phase and strictly better at the 1,600/s
   pinned-horizon shape;
2. ≥40% of WAL/job is attributable to removable architecture (E3), i.e. the
   redesign has proven headroom;
3. the change is deliverable as staged in-place migrations (per D1) with TLA+
   deltas identified.

Since that rule was written, the 0.7 performance campaign landed the allocator
ideas *inside* the current engine as staged migrations, and measured them:

- **v027/v039** — sequence-backed lane cursors and cache-free ready-segment
  routing: the claim path walks the `ready_segments` control plane ordered by
  `next_lane_seq` with an index short-circuit at `LIMIT 1` (the "cursor
  allocator" of the RFC, in situ).
- **#409 / v043 (#371)** — idle rotation skip, then append-only ring-rotation
  ledgers: the ring singletons stopped being hot mutable rows entirely.
  Pinned-horizon soak: dead tuples flat at ~6 versus 145–298 accrual on the
  pre-ledger intermediate ([2026-07-11 gate](https://github.com/hardbyte/postgresql-job-queue-benchmarking/tree/main/results/2026-07-11-awa-07-alpha-gate)).
- **#410 / v042** — compact deadline receipt claims removed ~83k steady-state
  live `lease_claims` rows at 256-worker saturation (the E2/#246 fix).
- **Release-candidate cells (2026-08-22)** — main @ `8c27951` vs v0.6.0 at
  W=256 depth-target saturation: **11,945/s @ p99 317 ms vs 10,568/s @ p99
  532 ms** (+13% throughput, −40% tail), ref800 parity at p99 21 ms.
- **E3 attribution** (recorded on #415): ~52% of WAL bytes at saturation are
  B-tree/index maintenance — above the 40% headroom threshold. The landed work
  did **not** reduce it (WAL byte-parity with v0.6.0 per 5k cell: 1,142 vs
  1,143 MB); E9.4b confirmed BRIN cannot serve the ordered-LIMIT claim
  contract, so no in-place migration captures this share.

## Decision

**Gate A resolves to evolution: no segment-storage restructuring migrations
enter 0.7.** The RFC's remaining scope graduates to 0.8 with this evidence
attached. Concretely:

1. **Criteria (i) fails as specified, and its question is answered anyway.**
   No side-by-side prototype (P-b/P-c) was built. Instead the strongest
   candidate allocator shape — segments + cursors + append-only ledgers — was
   implemented inside the engine behind six individually-tested migrations and
   measured better at every recorded shape. The motivating degradation
   mechanism (dead-tuple accumulation on hot control rows under a pinned MVCC
   horizon) is eliminated at the representation level, not mitigated.
2. **Criterion (ii) passes the threshold but not the delivery test in (iii).**
   The ~52% index-maintenance WAL share is real headroom, but capturing it
   requires changing what the lifecycle rows *are* (index-avoiding segment
   storage), which D1 correctly prices as a third engine identity or an
   expand→flip→contract lifecycle swap of the core tables — exactly the cost
   0.5→0.6 paid, with restore-only rollback. No staged-migration path to that
   reduction has been identified (BRIN rejected; the composite lane indexes
   are load-bearing for the ordered-LIMIT claim contract).
3. **Criterion (iii) held for everything actually shipped** — v016 through
   v043 restructured cursors, routing, receipts, terminal history, and ring
   bookkeeping in place, each individually benchmarked, without a new engine
   identity. That delivery record is itself evidence the engine absorbs
   structural change; it does not evidence a ceiling.

What 0.7 ships instead of restructuring: the landed stack above, plus the E9
deployment guidance (`wal_compression=lz4`, leave `commit_delay` at 0, do not
pin `plan_cache_mode`) routed to the operations handbook (#379) and
`awa doctor` advisories (#373).

## Answers to the RFC's five questions

1. **Claim allocator** — answered by construction: per-lane sequence cursors
   (`queue_claim_heads.seq_name`), a non-overlapping ready-segment control
   plane for O(1) routing, `FOR UPDATE SKIP LOCKED` on the head row for
   fairness, and append-only rotation ledgers so the allocator's bookkeeping
   is vacuum-cold under any MVCC horizon. Per-row retries/heartbeats/
   cancellation compose unchanged (ADR-023 receipt plane, ADR-003 rescue).
2. **Receipt plane integration** — survived and strengthened: ring-partitioned
   receipts (ADR-023) now write compact batch claims (v038) and compact
   deadline claims (v042); terminal history folds into narrow rollups
   (ADR-026 + v043 deltas). No redesign needed to keep them.
3. **Migration story** — the staged expand→flip→contract pattern with a
   released stepping-stone (ADR-037, ADR-040, ADR-041) is now proven twice.
   A third engine identity would repeat the 0.5→0.6 operator cost without a
   measured win; if 0.8 takes up the segment design, it should reuse this
   machinery rather than a hard cutoff.
4. **Comparable designs** — pgque holds flat under pinned horizons by trading
   away per-job state. Awa kept the full job-queue contract and removed the
   degradation mechanism (hot-row churn) surgically; the residual WAL gap vs
   pgque (~2 KiB/job) is dominated by contract evidence — per-attempt identity,
   receipts, terminal batches — plus index maintenance, not by avoidable
   bookkeeping.
5. **TLA+ coverage** — the ledger migration updated `AwaStorageLockOrder`,
   `AwaDeadTupleContract` (ledgers modeled as cold `RowVacuum` with
   horizon-gated folds), added the `MixedFleet` staged-upgrade invariant, and
   TLC caught a real authority-read TOCTOU during #371. A segment redesign
   re-models all of this; that cost belongs to whatever release takes it.

## Consequences

- 0.7's performance story rests on measured parity-or-better cells against
  v0.6.0 and the elimination of the pinned-MVCC degradation mechanism, not on
  a storage replacement.
- The known residual ceilings are explicit and tracked: **#418** (claim yield
  capped at one generation per call under extreme fragmentation — latent,
  requires deeper fragmentation than bench shapes produce; the candidate fix
  is a claim-CTE change, deliverable in place) and the **~52% WAL share** of
  index maintenance (documented as the price of the claim contract until an
  index-avoiding shape exists).
- The 60-minute pinned-MVCC reference soak in **ledger authority** remains an
  open #383 performance-contract item for the release candidate; it validates
  the shipped design and does not gate this decision (its mechanism-level
  evidence is already recorded).

### Reversal conditions (what reopens the redesign for 0.8)

Any of these, evidenced on released artifacts, reopens #295 with priority:

- sustained WAL-flush-bound ceilings at documented operator shapes after the
  tuning presets ship;
- #418-class fragmentation wedges observed in a real fleet (not only bench
  shapes);
- long-horizon latency drift or bloat reappearing in ledger authority during
  the pinned-MVCC soak or nightly chaos runs.

## References

- [#295](https://github.com/hardbyte/awa/issues/295) — the RFC this gate closes for 0.7
- [#383](https://github.com/hardbyte/awa/issues/383) — performance contract naming Gate A
- [0.7 roadmap](../../0.7-roadmap/index.md) — D1, §5 experiments, Gate A rule
- [#169 spike](https://github.com/hardbyte/awa/blob/main/docs/archive/0.6-storage-design/issue-169-storage-spike.md) — original degradation evidence
- [bench repo 2026-07-11 gate](https://github.com/hardbyte/postgresql-job-queue-benchmarking/tree/main/results/2026-07-11-awa-07-alpha-gate)
  and 2026-08-22 RC cells — measured comparisons cited above
