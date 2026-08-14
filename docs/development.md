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
   - `awa-seaorm/Cargo.toml` (`awa-testing` dev-dependency version)
   - `awa-cli/Cargo.toml` (`awa-ui` and `awa-testing` dependency versions)
   - `awa-cli/pyproject.toml` (`[project].version` — controls CLI wheel version on PyPI)
   - `awa-python/Cargo.toml` (`version`, `awa-model`, `awa-worker` dep versions)
   - `awa-python/pyproject.toml` (`[project].version` — controls SDK wheel version on PyPI)
   - `Cargo.lock`, `awa-python/Cargo.lock`, and `awa-python/uv.lock`
2. Finalize the matching changelog section and commit the release preparation.
3. Push a branch and open a PR against the branch being released. Apply the
   `full-ci` label and wait for every check, including the TLA+ storage models.
4. After merge, manually dispatch the **CI** workflow against the release
   branch. Record its exact commit SHA and wait for the complete run to pass.
   The Release workflow refuses to publish unless this exact-SHA manual run
   succeeded; a PR run against a pre-merge SHA is not sufficient.
5. Tag and push the validated commit, for example:
   `git tag v0.x.0-alpha.1 && git push origin v0.x.0-alpha.1`.
6. Do not create or publish the GitHub Release manually. The Release workflow
   creates a draft first, builds and uploads every asset, publishes crates,
   wheels, and the container image, then publishes the completed draft. This
   ordering is required when immutable releases are enabled.
7. When ready for final: bump version to `0.x.0`, repeat the exact-SHA CI gate,
   merge to the release branch, and tag `v0.x.0`.

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
