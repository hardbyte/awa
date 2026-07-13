# ADR-037: Canonical engine deprecation and removal

## Status

Accepted — implemented for 0.7 ([#370](https://github.com/hardbyte/awa/issues/370),
roadmap decision D2). Removal itself lands in 0.8.

## Context

The canonical (row-mutating) engine has existed since 0.1 as the original storage model.
Since queue-storage became the default and supported substrate (ADR-019, 0.6.0), canonical's
only remaining production role is as the *source* of the staged 0.5→0.6 transition: a place
existing work drains from while queue-storage takes over.

Keeping it alive is not free. Every feature pays a dual-engine tax — implementation, tests,
and docs — and [#360](https://github.com/hardbyte/awa/issues/360) documented how that tax
ships defects: the queue-storage `cancel_by_unique_key` branch referenced a column that does
not exist and passed CI because the harness only exercised canonical
([#359](https://github.com/hardbyte/awa/issues/359)). The dual-engine test matrix added for
0.7 catches this class, but it doubles integration cost for as long as both engines are
claimable.

## Decision

1. **0.7 migrate gate.** `awa migrate` (and library callers of
   `awa_model::migrations::run`) refuses to apply pending migrations unless the cluster's
   storage transition is finalized (`state = 'active'`) or the install is fresh. "Fresh"
   mirrors exactly what `awa.storage_auto_finalize_if_fresh` accepts at worker startup — an
   unprepared canonical state with no jobs and no runtimes seen in the last 90 seconds — so
   the migrate gate and the startup auto-finalize door admit the same clusters. The refusal
   applies migrations atomically-in-intent: nothing is applied on refusal, and the error
   names the exact finalize steps (`prepare → enter-mixed-transition → finalize --wait`) and
   the upgrade guides.
2. **Formal deprecation in 0.7.** Any runtime whose effective storage resolves to canonical
   logs a startup warning naming the transition steps and this ADR. Release notes and
   [`docs/upgrade-0.6-to-0.7.md`](../upgrade-0.6-to-0.7.md) state the deprecation.
3. **Removal in 0.8.** Canonical claim, execution, and trigger paths are deleted. Upgrades
   step 0.5 → 0.6 (finalize) → 0.7 → 0.8. A read-only drain/inspection check may remain so
   0.8 can still refuse legibly rather than misbehave against a canonical remnant.
4. **Model coverage.** `correctness/storage/AwaStorageTransition.tla` gains a `Migrate07`
   action gated by `Migrate07GateOpen`, with the invariant
   `Migrate07OnlyOnQuiescedCanonical`: a 0.7 migration never lands while canonical work
   exists or a canonical-only runtime is live. A finalized cluster may retain an idle
   `canonical_drain_only` runtime because supported writes already route exclusively to
   queue storage. The
   `AwaStorageTransitionMigrate07Ungated.cfg` expected-counterexample config witnesses why
   the gate is load-bearing.

## Consequences

**Positive**

- 0.7 features (per-key control, dependencies, backpressure, tracing storage) need exactly
  one storage implementation, one test leg, one docs story.
- The #360 dual-engine matrix becomes a bounded compatibility suite: full matrix until 0.8
  removes canonical, then it retires into the forward-compat matrix
  ([#367](https://github.com/hardbyte/awa/issues/367)).
- Operators get one unambiguous instruction at the boundary, and the refusal is a no-op —
  it never leaves a half-migrated cluster.

**Negative**

- Clusters that ignored the 0.6 transition cannot jump straight to 0.7; they must run a 0.6
  binary long enough to finalize. This is deliberate (stepping-stone upgrades), but it is a
  hard stop for anyone tracking `main` from a canonical cluster.
- The "effectively fresh" arm shares auto-finalize's 90-second runtime-staleness window: in
  principle an old canonical worker could start immediately after the gate passes and write
  canonical work into a newer schema. The window is identical to the one 0.6 already
  accepts at worker startup, and the forward-compat matrix (#367) is the backstop.

## Alternatives considered

- **Gate only migrations numbered above the 0.6 ceiling (v40+).** Would let a 0.7 binary
  bring a 0.5 schema up to v39 before refusing. Rejected: it splits the operator experience
  into "migrated some, then stopped", and the applied prefix duplicates what the supported
  0.6 binary already does. Refusing up front keeps intent atomic.
- **Auto-finalize during migrate.** Rejected: finalize is an operator decision with
  documented one-way-door semantics ([#180](https://github.com/hardbyte/awa/issues/180));
  `awa migrate` silently crossing it would violate that contract. The fresh-install arm is
  the only auto-promotion, and it already exists at worker startup.
- **Keep canonical indefinitely behind a feature flag.** Rejected: the dual-engine tax is
  per-feature and permanent; #359 shows it ships bugs even with good intentions.

## Relationship to other ADRs

Completes ADR-019's supersession of the pre-0.6 storage model; bounds the #360 dual-engine
test matrix; interacts with the staged-transition decisions recorded on #180 (explicit
finalize, no reverse migrator). The maintenance-only role (ADR-028) and callback ingress
(ADR-027) deployment shapes assume queue-storage-only runtimes from 0.8.
