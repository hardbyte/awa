# Development Guide

## Release Process

Use pre-release tags before publishing a final version. Both crates.io and PyPI
treat published versions as immutable — a botched release cannot be overwritten
and the version number is burned.

### Workflow

1. **Alpha** — early integration testing:
   ```
   v0.3.0-alpha.1 → v0.3.0-alpha.2 → ...
   ```
2. **Release candidate** — feature-complete, verifying in staging:
   ```
   v0.3.0-rc.1 → v0.3.0-rc.2 → ...
   ```
3. **Final release** — CI green, all checks pass:
   ```
   v0.3.0
   ```

### Steps

1. Bump version in `Cargo.toml` (workspace), `awa/Cargo.toml` (awa-testing
   ref), `awa-python/Cargo.toml`, and `awa-python/pyproject.toml`.
2. Commit: `Bump version to 0.3.0-alpha.1`
3. Push to a branch, wait for CI green.
4. Tag and push: `git tag v0.3.0-alpha.1 && git push origin v0.3.0-alpha.1`
5. The Release workflow builds wheels, publishes to crates.io and PyPI, and
   creates a GitHub Release with binary assets.
6. When ready for final: bump version to `0.3.0`, merge to main, tag `v0.3.0`.

### Why pre-releases matter

v0.2.0 was published directly. The GitHub Release workflow tried to attach
binary assets to an already-published release, which GitHub blocks. Pre-release
tags avoid this because:
- Draft releases are created by the workflow, not manually
- If a pre-release has problems, you bump to `-alpha.2` instead of fighting
  immutable registries

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
./correctness/run-tlc.sh AwaCore.tla
./correctness/run-tlc.sh AwaExtended.tla
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
