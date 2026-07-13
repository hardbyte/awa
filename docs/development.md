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

Policy: [ADR-041 — rolling-upgrade policy](adr/041-rolling-upgrade-policy.md). Use this checklist before opening a migration PR; version floors, exclusive migrations, and the newer-schema fail-safe live in `awa-model/src/migrations.rs`.

Checklist for any new `awa-model/migrations/vNNN_*.sql`:

**Every migration**

- [ ] Keep every object used by N−1 binaries compatible: no drops, type changes, or tightened constraints; make new objects and columns additive.
- [ ] Make the migration safe to re-run with guards such as `IF NOT EXISTS` and guarded `DO` blocks.
- [ ] In the header, link the issue and state how N−1 binaries operate against the migrated schema.
- [ ] Safe under live load: no long `ACCESS EXCLUSIVE` holds on hot tables; note the expected wall time on realistic data volumes.
- [ ] The current binary remains operable before migration, or startup applies the migration before any changed path runs. Test binary-first as well as migrate-first ordering.
- [ ] Document requirements for external runners, which do not execute Rust preflights.

**If compatibility first ships in an earlier-release patch**

- [ ] Add the released, verified patch to `MIGRATION_RUNTIME_VERSION_FLOORS`; test old, unparseable, and stale runtimes plus `--allow-live-runtimes`.
- [ ] Keep the preflight race-free and record any observability-snapshot stall from its lock. Job and lease heartbeats must remain unaffected.
- [ ] Publish the patch prerequisite before the migration and document it in the CHANGELOG and upgrade guide.

**If it changes an on-disk representation or hot-path structure (expand → flip → contract)**

- [ ] Make the migration the **expand** phase only: seed the new representation, keep the old one authoritative, and store authority explicitly. Fresh installs may start on the new representation.
- [ ] Gate the runtime **flip** on fresh fleet capability. Install the schema-owned per-feature capability constant with the expand migration; treat missing or unparseable evidence as incapable and make any override explicit.
- [ ] Under the old-writer locks, the flip treats the old representation as source of truth, reconciles the complete new representation, verifies exact equivalence, and changes authority atomically. Shadow writes alone do not satisfy this requirement.
- [ ] The flip **fences** returning pre-flip binaries at the database boundary. Exercise the actual N−1 write path; a sentinel is insufficient if old code can advance through it.
- [ ] The **contract** migration (dropping the old representation) is deferred to a later minor, tracked as its own issue, and independently checked against that release's N−1 contract.
- [ ] Model mixed-version interleavings in TLA+ when a state machine or lock order changes.
- [ ] Rehearse migrate-first, binary-first, and overlapping rollouts with a released N−1 artifact. Include concurrent old/new workers, failures and retries, scheduled work, in-flight work, hard-kill and deadline rescue, flip/fence behavior, and exact job accounting; record the evidence. CI automation is [#427](https://github.com/hardbyte/awa/issues/427).

**If no rolling-compatible design is practical**

- [ ] Explain in an ADR why expand/flip/contract and a version floor are insufficient, then add the migration to `EXCLUSIVE_WINDOW_MIGRATIONS` with refusal, override, and stale-heartbeat tests plus explicit operator documentation.

**Docs**

- [ ] Update the CHANGELOG, the release upgrade guide when operator action is required, and `docs/stability.md` when the skew contract changes. Link compatibility claims to rehearsals of the claimed version topology; describe narrower evidence only by the behavior it covers.

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
