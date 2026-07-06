# Upgrading from 0.6 to 0.7

The 0.6 → 0.7 upgrade is short: **the storage step is "be finalized."** Everything else in
0.7 is additive.

## The one gate

`awa migrate` on a 0.7 binary refuses to apply migrations unless one of these holds:

- the cluster's storage transition is **finalized** (`awa storage status` reports
  `state = active`, `current_engine = queue_storage`), or
- the install is **fresh** — a brand-new database, or a schema with no jobs and no
  recently-live runtimes (the same conditions that let a fresh install auto-finalize at
  worker startup).

Any other shape — canonical with work, `prepared`, `mixed_transition` — is refused with an
error naming the steps below. Nothing is applied on refusal; re-run `awa migrate` after
finalizing.

This is the [ADR-037](adr/037-canonical-engine-deprecation.md) deprecation gate: the
canonical (row-mutating) engine is deprecated in 0.7 and its claim/execution/trigger paths
are removed in 0.8. Requiring finalization at the 0.7 boundary means no 0.7 feature ever
needs a canonical implementation, and gives operators one unambiguous instruction.

## If you are on 0.6, finalized

You are done with the storage step. Deploy 0.7 binaries and run `awa migrate` as usual.

## If you are on 0.6, not yet finalized

Complete the staged transition **on your 0.6 binaries** first
(full procedure: [upgrade-0.5-to-0.6.md](upgrade-0.5-to-0.6.md)):

```bash
awa storage prepare --engine queue_storage
awa storage enter-mixed-transition
awa storage finalize --wait
```

Then upgrade binaries to 0.7 and run `awa migrate`.

Remember the transition is a one-way door once queue-storage work is accepted; the 0.5→0.6
guide covers the abort boundaries.

## If you are on 0.5.x

Step through 0.6: upgrade to the latest 0.6.x, run `awa migrate`, walk the staged
transition to `finalize`, then upgrade to 0.7. A 0.7 binary will not migrate a 0.5-era
schema directly — stepping-stone upgrades are the supported path (the same pattern Oban,
River, and Postgres itself use).

## Runtime deprecation warning

Any 0.7 worker whose effective storage resolves to the canonical engine — an unfinalized
cluster, an explicit `canonical_drain` role, or a builder configured for canonical storage —
logs a startup warning naming this guide. Treat it as a to-do, not an emergency: canonical
still works in 0.7, and is gone in 0.8.

## Rollback

0.7's gate itself changes nothing in the database. If you deploy 0.7 binaries against a
finalized cluster and need to roll back to 0.6 binaries, that is supported within the
documented skew window: a binary one minor version behind the schema must work or fail
loudly. There is no schema downgrade path (unchanged from previous releases).
