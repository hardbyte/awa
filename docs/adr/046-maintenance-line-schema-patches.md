# ADR-046: Maintenance-line schema patches

## Status

Accepted. Tracked with [#492](https://github.com/hardbyte/awa/issues/492). Implemented by the `SchemaPatch` registry, `awa.schema_patches` ledger (migration v048), and the patch export in `awa migrate --sql` / `--extract-to` / `awa.schema_patches()`.

## Context

Awa records schema state as one integer in `awa.schema_version`, and every line shares that sequence. Once a minor is cut, its maintenance line and the next development line both want to append migrations, and the numbers they would append collide: a 0.6.x `v041` and a 0.7 `v041` cannot both exist, because a database that took one and later upgrades to the other would either skip a migration or re-apply onto the wrong shape.

The obvious remedy, renumbering the development line, stops being available the moment a maintenance release ships forward-compatibility logic that names higher versions. awa 0.6.2 shipped exactly that: `forward_compatible_v043_columns` accepts a schema at `v043` only when the ring-cursor authority is still `columns`, so released 0.6.x binaries bind the meaning of v041 to v043 into code nobody can change. The dirty-key deadlock fix (ADR-045) then had to reach 0.6 without a migration version, and did so as an ad hoc idempotent patch whose applied state was inferred by probing object shapes.

That works once. It does not scale: probes are per-patch, invisible to operators, and silent about partial application by statement-at-a-time runners; external runners had no stable identity to apply; and the development line had no record that the change had already landed.

## Decision

1. **A maintenance line never adds a migration version past its last release.** The numbers above it belong to the next minor from the moment any release on the line ships a forward-compatibility guard that names them.
2. **Changes that must reach a maintenance line ship as named schema patches.** A patch is idempotent SQL (only `IF NOT EXISTS` / `OR REPLACE` forms, no `schema_version` row) registered in `SCHEMA_PATCHES` with a stable name. The runner applies pending patches after the last migration, only when the schema is exactly at the line's `CURRENT_VERSION`; a newer schema belongs to a newer binary whose migrations may have superseded the patch.
3. **Applied state lives in `awa.schema_patches`.** Every path that applies a patch records its name: the built-in runner, the exported `R__<name>.sql` script (which embeds the ledger table and row), and the development line's numbered migration that carries the same change. A patch may carry an `applied_probe` to recognise a database that received it before the ledger existed; a true probe back-fills the ledger row instead of re-running the SQL.
4. **The development line carries the change forward as an ordinary migration.** That migration is written idempotently, so it applies cleanly over a patched database, and it records the patch name in the ledger, so the two arrival paths converge. It does not re-register the patch in `SCHEMA_PATCHES`.
5. **External runners receive patches as repeatable scripts.** `awa migrate --sql` prints them after the versioned migrations as `-- Schema patch R__<name>`, `--extract-to` writes `R__<name>.sql`, and Python exposes `awa.schema_patches()`. `awa migrate --pending` lists every patch when the database is below `CURRENT_VERSION` (the migrations exported with it leave pre-patch objects in place) and only the unrecorded ones at `CURRENT_VERSION`.
6. **Versions stay the compatibility contract.** Patches never change what a version number means. A patch that a released binary must understand (for example, one that alters a table the binary queries by shape) is not a patch; it needs a new minor and a version.

## Consequences

- Maintenance releases can fix schema-level defects (trigger bodies, maintenance functions, additive tables) without a version bump and without touching released binaries' guards.
- Operators see applied patches with `SELECT name, applied_at, applied_by FROM awa.schema_patches` and through `awa migrate --pending --sql`.
- Each maintenance-line patch costs one numbered migration on the development line that carries the same SQL and ledger row.
- The ledger is created twice in source: migration v048 and `SCHEMA_PATCH_LEDGER_DDL`; a unit test keeps the texts identical.

## Alternatives considered

- **Renumber the development line to make room.** Breaks released maintenance binaries whose forward-compatibility guards name specific versions.
- **Register the same version on both lines with a gap.** A maintenance database would record a version whose lower neighbours it never applied; teaching the runner to fill gaps means non-monotonic migrations, which ADR-041 rules out.
- **Per-line version prefixes (`6041`, `7041`).** Solves collisions but changes the meaning of `MAX(schema_version)`, which every released binary compares against `CURRENT_VERSION`.
- **Probe-only detection (the 0.6.8 shape).** Kept as an optional back-fill for databases patched before the ledger existed, not as the source of truth.
