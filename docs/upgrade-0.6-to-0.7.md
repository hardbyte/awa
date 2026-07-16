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

## The v043 ring-rotation ledger migration

Migration **v043** ([#371](https://github.com/hardbyte/awa/issues/371),
[ADR-040](adr/040-append-only-ring-rotation-ledger.md)) moves the queue/lease/claim ring
cursors out of the mutable `{ring}_ring_state` singleton columns (`current_slot`,
`generation`) into append-only `{ring}_ring_rotations` ledgers (dead-tuple-free rotation
under a pinned MVCC horizon). It is delivered as a staged **expand → flip → contract**
upgrade supporting a mixed 0.6.2/0.7 fleet.

> **Required stepping-stone:** first roll the whole fleet to **0.6.2 or later**.
> 0.6.2 is the first 0.6 build that recognizes v043 as forward-compatible only
> while ring authority is `columns`, refuses unknown newer schemas without
> modifying them, and refuses to restart after the authority flip. Do not run a
> 0.7 migration while a 0.6.0 or 0.6.1 binary can invoke `awa migrate`; those
> releases contain the destructive newer-schema misclassification fixed by
> [#392](https://github.com/hardbyte/awa/issues/392).

`awa migrate` enforces this stepping-stone: v043 is refused while any runtime with a
fresh heartbeat reports a version below 0.6.2 or an unparseable version. The
`--allow-live-runtimes` override is available for operators who have independently verified
the fleet; it should not be needed in the normal rollout.

**v043 is additive (the expand phase).** It creates and seeds the three ledgers and the
rollup-delta landing table, and it **keeps** the compat `current_slot` / `generation`
columns in place. Each queue-storage schema gets a `ring_cursor_authority` control row
that selects which representation is authoritative:

- `columns` (compat) — the pre-0.7 singleton columns are authoritative, exactly as 0.6
  wrote them, so a live 0.6.2 binary keeps working. 0.7 rotators additionally shadow every
  advance into the ledger, keeping it a ready-to-promote copy.
- `ledger` — the append-only ledgers are authoritative (the #371 dead-tuple win).

An **upgrade** starts in `columns`; a **fresh install** starts directly in `ledger` (no
old binary can exist). The flip is one-way.

### Procedure (either order works after the 0.6.2 stepping-stone)

First roll 0.6.2 across the fleet as a normal patch release. After that, both
orders are supported:

1. **Roll binaries, then migrate**, or **migrate, then roll binaries** — either way the
   fleet runs mixed 0.6.2/0.7 against one database once v043 is applied. In
   compat mode a 0.6.2 rotator and a 0.7 rotator serialize on the same
   `{ring}_ring_state` row lock, so the cursor stays correct. A 0.6.2 restart also
   recognizes v043 while authority remains `columns`; it refuses rather than mutating the
   schema once authority is `ledger`.

   If you roll binaries first, a 0.7 worker **refuses to start** on the
   pre-migration v040 schema — it fails closed at startup naming `awa migrate`
   as the fix, and completes no work. Under a rolling deployment the new pods
   crash-loop harmlessly while the remaining 0.6.2 workers keep draining
   traffic, and come up unaided the moment the migration commits. Plan
   capacity for that window, or migrate first to avoid it.
2. Once **every** worker is on 0.7, promote to ledger authority to unlock the full #371
   dead-tuple benefits — either:
   - manually: `awa storage flip-ring-authority` (add `--schema <name>` for a custom
     schema; `--check` prints the fleet flip-readiness and exits without changing
     anything), or
   - automatically: the maintenance leader auto-flips once every fresh-heartbeat runtime
     has reported a 0.7+ `binary_version` continuously for a stable period (default 10
     minutes; a builder knob). It logs the flip loudly.

If you migrate first, roll 0.7 workers promptly. An all-0.6.2 fleet on v043 remains safe:
crash and heartbeat rescue are batch-aware, but 0.6.2's deadline-rescue sweep does not read
the v042 compact claim batches used for newly claimed deadline jobs. Deadline-based rescue
for those jobs resumes when a 0.7 runtime takes maintenance leadership. Roll the
0.6 maintenance leader promptly after migrating. Awa's built-in
migrator applies v041-v043 in one transaction, so other sessions see v040 or v043, never an
intermediate version. An external migration runner that commits each version separately can
expose v041/v042; a restarting 0.6.2 worker refuses those transient schemas and connects
normally once v043 is committed.

The manual flip **refuses** (without `--force`) while any fresh-heartbeat runtime is not
known to be flip-aware — i.e. a 0.6 (or pre-flip 0.7) binary might still be reading the
compat columns. Roll the whole fleet first, or pass `--force` only once you have confirmed
no pre-flip binary is live.

### After the flip — do not roll back to a pre-flip binary

The flip **fences returning pre-flip binaries**: under the three cursor locks it first
reconciles and verifies every shadow ledger, then poisons both compat cursor fields and
the legacy prune metadata. A database trigger rejects any later old-style cursor advance,
including the `-1 -> 0` rotation a pre-ledger binary would otherwise compute. A 0.6.2 (or
pre-flip 0.7) binary that reconnects therefore fails loudly instead of misrouting writes
or pruning an authoritative ledger slot.
Rolling a binary back **across the flip** is therefore not supported; roll back only to
another 0.7 build (which reads the ledger). **Before** the flip, the normal one-minor
skew guarantee holds and rollback to the 0.6.2 stepping-stone is safe.

### Contract (0.8)

Dropping the compat columns for good is deferred to **0.8** (the contract phase), after
ledger authority is universal and the release's capability gate excludes pre-flip binaries.
Until then the columns remain as a cheap, cold safety net.

Fresh installs are unaffected — they get ledger authority directly and never touch the
compat path.

## Runtime deprecation warning

Any 0.7 worker whose effective storage resolves to the canonical engine — an unfinalized
cluster, an explicit `canonical_drain` role, or a builder configured for canonical storage —
logs a startup warning naming this guide. Treat it as a to-do, not an emergency: canonical
still works in 0.7, and is gone in 0.8.

## Rollback

0.7's migrate *gate* itself changes nothing in the database. Migration **v043** is
additive (it keeps the compat cursor columns), so applying it does **not** break the
one-minor-version skew window: a 0.6.2 binary keeps working against a v043 schema in compat
authority. The skew window only ends at the **ring-authority flip** (see above): after the
flip the stale compat columns are poisoned, so a pre-flip binary fails loudly and rolling
back across the flip is **not** supported — roll back only to another 0.7 build. Before
the flip, rollback to the 0.6.2 stepping-stone is safe. There is no schema downgrade path (unchanged from
previous releases).
