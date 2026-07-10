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

## The v042 ring-rotation ledger migration — lockstep, no mixed fleet

Migration **v042** ([#371](https://github.com/hardbyte/awa/issues/371),
[ADR-040](adr/040-append-only-ring-rotation-ledger.md)) is the one 0.7 migration that
**breaks the normal binary/schema skew window**. It moves the queue/lease/claim ring
cursors out of the mutable `{ring}_ring_state` singleton columns (`current_slot`,
`generation`) into append-only `{ring}_ring_rotations` ledgers, and then **drops those
columns** after seeding the ledger from them. This is a deliberate exception to the
otherwise additive-only migration policy: a pre-v042 binary that read a now-frozen cursor
would silently misroute writes into sealed slots, so the migration removes the column to
turn that into a loud, safe failure instead.

**Consequence: binaries and v042 must move together. Do not run a mixed fleet of pre-
and post-v042 binaries against one database.** Because the cursor columns are gone, a
0.6 (or any pre-v042) binary cannot claim, enqueue, or rotate against a v042 schema — it
fails loudly. This one migration needs a brief **stop-the-world** window:

1. Stop or fully drain all workers (no pre-v042 binary left connected).
2. Run `awa migrate` (applies v042; seeds the ledgers before dropping the columns, so the
   current cursor survives exactly).
3. Start the 0.7 binaries.

Fresh installs are unaffected — they get the ledger shape directly. Rolling upgrades that
keep old binaries live across this migration are **not** supported for v042.

## Runtime deprecation warning

Any 0.7 worker whose effective storage resolves to the canonical engine — an unfinalized
cluster, an explicit `canonical_drain` role, or a builder configured for canonical storage —
logs a startup warning naming this guide. Treat it as a to-do, not an emergency: canonical
still works in 0.7, and is gone in 0.8.

## Rollback

0.7's migrate *gate* itself changes nothing in the database. However, once migration
**v042** has run (see above), the one-minor-version binary/schema skew window no longer
applies: a 0.6 binary against a v042 schema fails loudly (the ring-cursor columns it reads
are gone), so rolling back to 0.6 binaries after applying v042 is **not** supported. Roll
back the binary only to a build that also carries the v042 ledger cursor (i.e. another 0.7
build). Before v042 is applied, the normal skew guarantee holds. There is no schema
downgrade path (unchanged from previous releases).
