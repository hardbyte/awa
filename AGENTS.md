# AGENTS.md

Guidance for AI coding agents working in this repository. See
[`docs/development.md`](docs/development.md) for the full contributor workflow;
this file records the always-on rules.

## Build, Lint, Test

The workspace is Rust; the Python bindings (`awa-python`) are a separate
workspace. Always run the pre-commit checks before committing Rust changes:

```bash
cargo fmt --all
SQLX_OFFLINE=true cargo clippy --all-targets --all-features -- -D warnings
SQLX_OFFLINE=true cargo build --workspace
```

Tests need a live PostgreSQL:

```bash
docker run -d --name awa-pg -e POSTGRES_PASSWORD=test -e POSTGRES_DB=awa_test \
  -p 15432:5432 postgres:17-alpine
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test cargo test --workspace

cd awa-python && uv run maturin develop
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test uv run pytest tests/ -v
```

Read the redirected log and check the real exit status; do not trust a green
exit code from a piped or backgrounded command.

## Schema Migrations

Migrations are forward-only and must stay rolling-upgrade compatible. Follow the
migration checklist and rolling-upgrade policy in
[`docs/development.md`](docs/development.md#authoring-schema-migrations) and
[ADR-041](docs/adr/041-rolling-upgrade-policy.md) before opening a migration PR.
Version floors, exclusive migrations, and the newer-schema fail-safe live in
`awa-model/src/migrations.rs`.

## Agent Skills

Canonical, portable [Agent Skills](https://agentskills.io/) live under
`skills/<name>/SKILL.md` and follow the Agent Skills specification.

- Keep product workflows and version-aware semantics in skills. Keep always-on
  repository contribution rules in this file.
- `awa-jobs` covers authoring jobs and workers (Rust and Python); `awa-operations`
  covers deploying and operating the fleet.
- Validate every skill locally with
  `uvx --from skills-ref==0.1.1 agentskills validate skills/<name>`; CI runs the
  same check.
- Skills ship in the release binary archives. Do not copy them into crates,
  runtime images, or the container build.
- Update the affected skill when public behavior or a documented workflow
  changes, and avoid duplicating skill content elsewhere.
