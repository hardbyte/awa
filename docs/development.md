# Development Guide

## Release Process

Use pre-release tags before publishing a final version. Both crates.io and PyPI treat published versions as immutable — a botched release cannot be overwritten and the version number is burned.

### Workflow

1. **Alpha** — early integration testing:
   ```
   v0.x.0-alpha.1 → v0.x.0-alpha.2 → ...
   ```
2. **Release candidate** — feature-complete, verifying in staging:
   ```
   v0.x.0-rc.1 → v0.x.0-rc.2 → ...
   ```
3. **Final release** — CI green, all checks pass:
   ```
   v0.x.0
   ```

### Steps

1. Bump version in these release manifests:
   - `Cargo.toml` (workspace `[workspace.package].version` and every workspace dependency whose version points at an Awa crate being released)
   - `awa/Cargo.toml` (`awa-testing` dev-dependency version)
   - `awa-cli/Cargo.toml` (`awa-ui` dependency version)
   - `awa-cli/pyproject.toml` (`[project].version` — controls CLI wheel version on PyPI)
   - `awa-python/Cargo.toml` (`version`, `awa-model`, `awa-worker` dep versions)
   - `awa-python/pyproject.toml` (`[project].version` — controls SDK wheel version on PyPI)
2. Commit: `Bump version to 0.x.0-alpha.1`
3. Push to a branch, wait for CI green.
4. Tag and push: `git tag v0.x.0-alpha.1 && git push origin v0.x.0-alpha.1`
5. The Release workflow builds wheels, publishes to crates.io and PyPI, and creates a GitHub Release with binary assets.
6. When ready for final: bump version to `0.x.0`, merge to main, tag `v0.x.0`.

### Why pre-releases matter

v0.2.0 was published directly. The GitHub Release workflow tried to attach binary assets to an already-published release, which GitHub blocks. Pre-release tags avoid this because:

- Draft releases are created by the workflow, not manually
- If a pre-release has problems, you bump to `-alpha.2` instead of fighting immutable registries

## Crate Dependencies

```
awa-macros  (proc-macro, no runtime deps)
    │
    ▼
awa-model   (core types + SQL, re-exports awa-macros::JobArgs)
    │
    ├──────────────┬──────────────┐
    ▼              ▼              ▼
awa-worker     awa-ui          awa-cli
    │          (axum API +       (depends on awa-ui)
    │           embedded UI)
    ├──────────────┐
    ▼              ▼
awa (facade)   awa-testing
                   │
                   ▼
              awa-python (PyO3 bridge, separate workspace)
```

Key dependencies per crate:

| Crate        | Key deps                                         |
| ------------ | ------------------------------------------------ |
| `awa-model`  | sqlx, blake3, serde, chrono, chrono-tz, croner   |
| `awa-worker` | awa-model, tokio, opentelemetry                  |
| `awa-ui`     | awa-model, axum, rust-embed                      |
| `awa-cli`    | awa-model, awa-ui, axum, clap                    |
| `awa-python` | awa-model, awa-worker, pyo3, pyo3-async-runtimes |

## Running Tests

```bash
# Start Postgres
docker run -d --name awa-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=awa_test \
  -p 15432:5432 postgres:17-alpine

# Rust
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --workspace

# Python
cd awa-python
uv run maturin develop
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/ -v

# TLA+ correctness models
./correctness/run-tlc.sh core/AwaCore.tla
./correctness/run-tlc.sh protocol/AwaExtended.tla
```

## Authoring Schema Migrations

Policy: [ADR-041 — rolling-upgrade policy](adr/041-rolling-upgrade-policy.md). Rolling deployment is the default: schema changes must remain operable by the previous release's binaries until the fleet flips, and representation changes ship as expand → flip → contract across releases. Migration version floors, exclusive migrations, and the newer-schema fail-safe live in `awa-model/src/migrations.rs`.

Checklist for any new `awa-model/migrations/vNNN_*.sql`:

**Every migration**

- [ ] Additive only: new tables, nullable/defaulted columns, indexes, functions. No drops, type changes, or tightened constraints on anything an N−1 binary touches.
- [ ] Idempotent and safe to re-run (`IF NOT EXISTS`, guarded `DO` blocks) — the runner may reapply amended earlier files before yours.
- [ ] Header comment states the issue link, what changes, and **explicitly what an N−1 binary does against the migrated schema** (not just "it works" — which paths it exercises and why they stay correct).
- [ ] Safe under live load: no long `ACCESS EXCLUSIVE` holds on hot tables; note the expected wall time on realistic data volumes.
- [ ] The current binary remains operable before migration, or startup applies the migration before any changed path runs. Test binary-first as well as migrate-first ordering.
- [ ] External migration tooling requirements are documented. Rust preflights do not run when operators apply `migration_sql()` themselves.

**If compatibility first ships in an earlier-release patch**

- [ ] Add `(migration_version, minimum_runtime_version)` to `MIGRATION_RUNTIME_VERSION_FLOORS`. The floor names a released version verified against the expanded schema; old or unparseable fresh runtimes must refuse, stale runtimes must not block, and `--allow-live-runtimes` must be tested.
- [ ] Keep the preflight race-free: runtime registration and heartbeat writes must not cross between the version check and migration commit. Record the expected observability-snapshot stall from the table lock; job and lease heartbeats must remain unaffected.
- [ ] Add the patch prerequisite to the CHANGELOG and upgrade guide before the release that introduces the migration.

**If it changes an on-disk representation or hot-path structure (expand → flip → contract)**

- [ ] The migration is the **expand** phase only: create and seed the new representation, keep the old one authoritative, and select authority via an explicit control row (pattern: v042's `ring_cursor_authority`). Fresh installs start on the new representation.
- [ ] The **flip** is a runtime action gated on fleet capability: a per-feature min-version SQL constant (pattern: `awa.ring_authority_min_flip_version()`) checked against `awa.semver_rank(runtime_instances.binary_version)` for every fresh-heartbeat runtime. The expand migration installs this schema-owned constant; binaries query rather than compile it. NULL or unparseable ⇒ not capable. Manual CLI command plus (optionally) a maintenance auto-flip; refusal names the remedy; `--force` is the only override.
- [ ] Under the old-writer locks, the flip treats the old representation as source of truth, reconciles the complete new representation, verifies exact equivalence, and changes authority atomically. Shadow writes alone do not satisfy this requirement.
- [ ] The flip **fences** returning pre-flip binaries at the database boundary. Exercise the actual N−1 write path; a sentinel is insufficient if old code can advance through it.
- [ ] The **contract** migration (dropping the old representation) is deferred to a later minor, tracked as its own issue, and independently checked against that release's N−1 contract.
- [ ] TLA+ models cover the **mixed-version interleavings** where a state machine changes (precedent: `AwaStorageLockOrder`'s flip model caught the v042 authority-read TOCTOU).
- [ ] A **mixed-version adversarial rehearsal** passes before release: live enqueue + workers, failing/retrying jobs, scheduled/cron jobs, in-flight jobs across the migrate step, `kill -9` during the window, deadline rescue, N−1-only operation on the expanded schema, old + new binaries claiming concurrently, both rollout orders, the flip/fence, and an exact zero-loss reconciliation. Use a released N−1 artifact, not only a source compatibility shim. Record the evidence (`docs/adr/bench/` or the benchmarking repo); CI automation is [#427](https://github.com/hardbyte/awa/issues/427).

**If no rolling-compatible design is practical**

- [ ] Record why expand/flip/contract and a runtime version floor are insufficient in an ADR or equivalent design review.
- [ ] Add the version to `EXCLUSIVE_WINDOW_MIGRATIONS` with the justification in the migration header, refusal/override/stale-heartbeat tests, a CHANGELOG operator callout, and the procedure in that release's upgrade guide.

**Docs**

- [ ] CHANGELOG entry; `docs/upgrade-X-to-Y.md` if any operator action is required; `docs/stability.md` if the skew statement is affected. State known mixed-version limitations explicitly; do not present a non-mixed rehearsal as proof of mixed-version operation.

## Pre-commit Checks (Rust)

Always run before committing Rust changes:

```bash
cargo fmt --all
SQLX_OFFLINE=true cargo clippy --all-targets --all-features -- -D warnings
SQLX_OFFLINE=true cargo build --workspace
```

The Python crate lives in a separate workspace:

```bash
cd awa-python
cargo fmt --all
SQLX_OFFLINE=true cargo clippy --all-targets -- -D warnings
```
