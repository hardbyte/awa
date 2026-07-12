# ADR-041: Rolling-upgrade policy — no stop-the-world

## Status

Accepted — standing policy for 0.7 and every later release. The implementing
precedent is the v042 rolling redesign
([#425](https://github.com/hardbyte/awa/pull/425)) and the migrate fail-safe +
exclusive-window gate ([#426](https://github.com/hardbyte/awa/pull/426),
[#392](https://github.com/hardbyte/awa/issues/392)). The authoring checklist
derived from this ADR lives in
[`docs/development.md`](../development.md#authoring-schema-migrations);
rehearsal automation is tracked in
[#427](https://github.com/hardbyte/awa/issues/427).

## Context

Awa is infrastructure other systems run their traffic through. An upgrade that
requires stopping every worker — however briefly — is a cost the project
imposes on every operator, on every deployment, on every release, forever.
Postgres itself, and the queues Awa positions against (Oban, River), treat
online upgrades as table stakes; a Postgres-native queue that asks for a
maintenance window to change its own bookkeeping undercuts the reason to pick
it.

The project has now been on both sides of this trade inside one release cycle,
which is what this ADR generalizes from:

- The original v042 (ADR-040, ring-rotation ledgers) shipped as a **lockstep
  stop-the-world migration**: it dropped the mutable ring-cursor columns, so
  binaries and migration had to move together and no mixed 0.6/0.7 fleet was
  allowed.
- The 0.6.1 → 0.7 upgrade rehearsal (2026-07-12, recorded in the benchmarking
  repository under `results/2026-07-12-upgrade-rehearsal/LOG.md`) then showed
  what surrounds that assumption in practice. A 0.6.1 binary pointed at the
  v042 schema did not stop at a clean error: its legacy version normalization
  **rewrote `awa.schema_version` from 42 down to 4, re-applied migrations 5–22
  on top of the v042 layout, and crashed mid-loop**, leaving the version
  ledger at 22 while the physical schema stayed at 42 — a destructive,
  non-atomic split-brain ([#392](https://github.com/hardbyte/awa/issues/392)).
  The lesson is not only that the window was expensive; it is that everything
  *outside* the window had no fail-safe.
- The same rehearsal demonstrated the alternative is achievable and cheap to
  verify: against a database mid-workload (~50k banked ready entries, 32
  outstanding in-flight claims, 50 future-scheduled jobs), migrate v39→v42
  took 15.5s; all 32 `kill -9`'d in-flight jobs were rescued by the ADR-003
  heartbeat path (~34s into recovery) and re-run at attempt=2; every scheduled
  job fired; and terminal accounting reconciled **exactly** — 50,772 jobs
  enqueued, 50,772 accounted for, zero lost, zero stuck.

v042 was subsequently redesigned as a staged rolling upgrade
(expand → flip → contract, [#425](https://github.com/hardbyte/awa/pull/425)),
and `migrate` gained the old-binary fail-safe and a pre-flight live-runtime
gate ([#426](https://github.com/hardbyte/awa/pull/426)). This ADR converts
that pair from a one-off correction into policy, so the next representation
change starts rolling-by-construction instead of arriving there after review.

## Decision

**1. Rolling upgrades are the default. A stop-the-world moment is a design
defect to be engineered away, not an operational instruction to be
documented.**

Any schema or on-disk-representation change ships as
**expand → flip → contract**, spread across releases:

- **Expand** migrations are strictly additive and old-binary-compatible: new
  tables, nullable or defaulted columns, new functions. They may create and
  seed the new representation, but the old representation stays authoritative
  — selected by an explicit per-schema control row (v042's
  `ring_cursor_authority`), with fresh installs starting directly on the new
  representation (no old binary can exist there).
- The **flip** is a capability-gated *runtime* action — a SQL function, a CLI
  command, optionally a maintenance-leader auto-flip — never part of
  `migrate`. It promotes the new representation, one-way.
- **Contract** — dropping the old representation — lands **no earlier than the
  next minor** after the flip is universal, as its own migration (v042's
  compat columns are dropped in 0.8, not 0.7).

**2. The N−1 compatibility contract.** The schema produced by release N must
be **fully operable by release N−1 binaries — correct, not merely
non-crashing — until the fleet flips**. "Operable" means claims route to the
right slots, cursors advance under the same serialization discipline, and
counts stay exact; a mixed fleet must not corrupt state it will later agree
on. Consequently migrations may run before, during, or after the binary
rollout, in any order, and auto-migrate at worker startup stays safe.

**3. Capability signaling.** Flip-readiness is decided from
`awa.runtime_instances.binary_version` + `awa.semver_rank()` + a per-feature
minimum-version SQL constant (the pattern of
`awa.ring_authority_min_flip_version()`). New capability gates reuse this
mechanism rather than inventing another. Its load-bearing property is the
fail-safe default: `binary_version` is a column only capability-aware binaries
know exists, so a fresh heartbeat with `binary_version IS NULL` is *proof* of
a pre-capability binary — the signal is a write the old binary **cannot**
make, not a value we hope it sets correctly. `awa.semver_rank()` returns NULL
for anything unparseable, and NULL is treated as not-capable, so the gate
fails safe in both directions.

Two existing signals were considered and rejected as the discriminator:

- **`runtime_instances.version`** is an observability string written by *all*
  binaries — including old releases whose behavior is frozen and can no longer
  be fixed. Nothing ties the string to the presence of feature code (the
  rehearsal's 0.7 build reported `0.6.1` because the version bump lived on a
  release branch). A gate keyed on it would be keyed on a convention, not a
  capability.
- **The v010/v014 `storage_capability` / `transition_role` enums** are
  CHECK-constrained to the legacy→queue-storage engine transition's specific
  state machine, and their values are per-snapshot and dynamic — an auto-role
  runtime reports `queue_storage` capability and then *downgrades* to
  drain-only after the routing flip, which already produced a real gate bug
  (v014's header comment; caught by `AwaStorageTransition.tla`). Extending
  that vocabulary to unrelated features would couple every future flip to the
  0.6 transition's semantics.

**4. Fencing.** A flip must make a returning pre-flip binary **fail loudly**
— a poisoned sentinel (v042 sets the stale `current_slot = -1`, a slot with no
partition), a removed function signature, a refused precondition — never
silently misroute or half-work. Silent wrongness under version skew is the
one outcome this policy exists to make structurally impossible; ADR-040's
original column drop chose loud failure over a stale-cursor hazard for the
same reason, and the fence keeps that property without the lockstep coupling.

**5. Fail-safe direction everywhere.** Version-skew handling always errs
toward refusal:

- An old binary against a **newer** schema refuses to run `migrate` (or
  anything destructive) with an actionable error — it never "normalizes",
  rewrites version history, or deletes what it does not understand (#392's
  fix in [#426](https://github.com/hardbyte/awa/pull/426)).
- An unknown capability is **not** a capability; an unparseable version is
  **not** capable; an absent heartbeat column is **not** flip-aware.
- Gates refuse and name the remedy; overrides are explicit operator flags
  (`--allow-live-runtimes`, `--force`), never defaults.

**6. The exclusive-window escape hatch.** `EXCLUSIVE_WINDOW_MIGRATIONS`
(awa-model/src/migrations.rs, [#426](https://github.com/hardbyte/awa/pull/426))
exists for migrations a pre-migration binary genuinely cannot survive: the
pre-flight gate refuses to apply a flagged version while any runtime has
heartbeated within 90s, overridable only by an explicit flag. Adding a version
to that list is an **exceptional decision**, requiring explicit justification
in the migration's header comment and an operator callout in the CHANGELOG. A
release SHOULD NOT ship a flagged migration unless a rolling path is
impossible or grossly disproportionate — the precedent cuts the other way:
v042 was initially flagged, and redesign to expand → flip → contract removed
the flag. The legitimate residual use is contract migrations (the 0.8 column
drop), where the window asserts what the fence already guarantees.

**7. Adversarial validation before release.** Any migration that touches
hot-path structures or changes an on-disk representation MUST pass a
**mixed-version rehearsal** before the release ships. The rehearsal runs
against real traffic and hostile conditions, not a quiescent database:

- live enqueue and workers throughout, with a banked backlog;
- failing/retrying jobs and scheduled/cron jobs in the mix;
- in-flight jobs held across the migrate step;
- hard kills (`kill -9`) during the window, with rescue asserted;
- old and new binaries claiming concurrently (the N−1 contract, live);
- and an **exact zero-loss reconciliation** afterwards: every enqueued job
  has exactly one terminal outcome in its designed state, killed jobs are
  terminal at attempt=2, every scheduled fire happened, nothing is stuck.

Where a state machine changes, the TLA+ models MUST cover the mixed-version
interleavings, not just the end states — the `AwaStorageLockOrder` flip model
caught a real authority-read TOCTOU in the v042 design (rotator reads
`columns`, flip commits, rotator advances under the stale discipline) that no
single-version model could express. Evidence is recorded like other
validation artifacts (`docs/adr/bench/` or the benchmarking repository);
automating the rehearsal in CI is
[#427](https://github.com/hardbyte/awa/issues/427).

## Consequences

**Positive**

- Operators never schedule downtime for an Awa minor. Migrations and binary
  rollouts compose in any order, so Awa upgrades fit inside whatever deploy
  machinery the operator already has.
- The rollback story is crisp and uniform: before the flip, rolling back to
  N−1 is safe; after the flip, a pre-flip binary is fenced and fails loudly.
  No release needs a bespoke "reverse migration recipe in your runbook".
- Capability gating is one reusable mechanism (`binary_version` +
  `semver_rank` + a min-version constant per feature) instead of a new
  vocabulary per transition.
- The rehearsal requirement converts "should be fine" into measured evidence
  — the same discipline ADR-019/023 established for performance claims,
  applied to upgrade correctness.

**Negative / costs — accepted deliberately**

- Representation changes take **two releases** to finish: compat shims, dual
  authority branches, and shadow writes live for a whole minor, and the old
  representation (plus its poisoning cruft after the flip) occupies the schema
  until the contract migration lands.
- The N−1 operability bar constrains design: it is not enough to stop writing
  an old structure — a mixed fleet must keep it *correct* (v042's compat mode
  shadows every advance into the ledger and serializes 0.6 and 0.7 rotators on
  the same row lock precisely for this).
- Rehearsals cost real engineering time per qualifying migration, and TLA+
  models grow mixed-version state spaces. This is the price of selling a
  queue that upgrades under load;
  [#427](https://github.com/hardbyte/awa/issues/427) amortizes it.
- Upgrades gain a second operator-visible phase (roll, then flip). Auto-flip
  reduces this to observation in the common case.

## Alternatives considered

- **Documented stop-the-world windows** (the original v042 posture): precise
  CHANGELOG procedures, version pinning, a short window per breaking release.
  Rejected — the cost multiplies across every operator and release, the
  rehearsal showed the failure mode when any step runs out of order is
  destructive rather than inconvenient, and the v042 redesign proved the
  rolling path costs one release of compat shims, not a rewrite.
- **Gate flips at migrate time** (schema version implies capability). A schema
  version records which SQL ran, not which binaries are live; under this
  policy migrations may run before the rollout, so migrate-time gating is
  either wrong or forces ordering back into the procedure. Rejected — flips
  gate on runtime heartbeat evidence.
- **Reuse `runtime_instances.version` or the v010/v014 transition enums** for
  capability signaling. Rejected for the reasons in Decision 3: the former is
  a frozen-binary convention, not a capability proof; the latter is welded to
  one transition's state machine and already misfired once.
- **Blue-green / parallel fleet upgrades as the documented procedure.** Works,
  but demands double capacity and still needs a correctness story for the
  handover moment; it treats the symptom operators-side instead of fixing the
  compatibility contract product-side. Not precluded — merely not required.

## Relationship to other ADRs

Generalizes the staged rollout ADR-040 adopted for v042 into standing policy,
and constrains every future storage ADR the way ADR-026's ledger discipline
constrains hot-path writes. Extends ADR-036: `docs/stability.md`'s
binary/schema skew statement is the public face of the N−1 contract, and this
ADR is its implementation discipline. ADR-037's 0.7 migrate gate and the
v010/v014 transition framework are ancestors — engine-transition-specific
machinery whose lessons (capability gating, refusal-first) this ADR keeps and
whose vocabulary it deliberately does not reuse. The heartbeat freshness
window (90s) is shared with the ADR-037 finalize gate.
