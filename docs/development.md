# Development

This page is a short orientation for contributors building Awa locally. Release procedure, migration-author checklists, and CI policy are maintained in the repository's contributor files and workflow definitions because they change with the codebase and are not part of the user documentation contract.

## Repository layout

| Path | Purpose |
| --- | --- |
| `awa-model` | Core types, SQL, and migrations |
| `awa-worker` | Dispatch, execution, maintenance, and telemetry |
| `awa` | Public Rust facade |
| `awa-cli` / `awa-ui` | Command-line tools and embedded admin interface |
| `awa-python` | PyO3 Python package and Python worker API |
| `awa-testing` | Test support |
| `correctness` | Executable TLA+ models and trace checks |
| `docs` | MkDocs site and source ADRs |

## Local checks

Start a supported PostgreSQL instance and provide its URL to the test suite:

```bash
docker run -d --name awa-pg \
  -e POSTGRES_PASSWORD=test \
  -e POSTGRES_DB=awa_test \
  -p 15432:5432 postgres:17-alpine

export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
cargo test --workspace
```

The Python package uses `uv`:

```bash
cd awa-python
uv run maturin develop
uv run pytest tests/ -v
```

Run the core correctness models with the repository wrapper:

```bash
./correctness/run-tlc.sh core/AwaCore.tla
./correctness/run-tlc.sh protocol/AwaExtended.tla
```

Before submitting Rust changes, format and run the same offline checks used by CI:

```bash
cargo fmt --all
SQLX_OFFLINE=true cargo clippy --all-targets --all-features -- -D warnings
SQLX_OFFLINE=true cargo build --workspace
```

`awa-python` is a separate Rust workspace; run formatting and clippy from that directory too.

## Where contributor policy lives

- [Repository README](https://github.com/hardbyte/awa) — project overview and workspace entry point.
- [GitHub Actions workflows](https://github.com/hardbyte/awa/tree/main/.github/workflows) — the current CI and release gates.
- [ADR-041](adr/041-rolling-upgrade-policy.md) — architectural policy for rolling-compatible migrations.
- [Benchmarking](benchmarking.md) — reproducible performance suites and how to interpret their results.

When a schema change affects users or operators, update the relevant upgrade guide and [stability policy](stability.md). Detailed migration implementation checklists belong beside the migration code and review process rather than in the public product guide — they live in [`AGENTS.md`](https://github.com/hardbyte/awa/blob/main/AGENTS.md#schema-migrations).

## Authoring schema migrations

Schema changes must follow [ADR-041's expand, capability-gated flip, and later contract policy](adr/041-rolling-upgrade-policy.md). The implementation checklist lives in [`AGENTS.md`](https://github.com/hardbyte/awa/blob/main/AGENTS.md#schema-migrations) alongside the migration tests that enforce it; upgrade guides document any action required from operators.

## Maintaining the interactive failure lab

The [failure lab](concepts/failure-lab.md) embeds the canonical animation hosted by
[`hardbyte/postgresql-job-queue-benchmarking`](https://github.com/hardbyte/postgresql-job-queue-benchmarking).
Its source is `simulations/awa-failure-windows/index.html` there, and the
comparison-site build copies it to `lab/awa.html`. Change and test the animation
in that repository; do not add a second JavaScript/model copy to these docs.

When Awa's enqueue, attempt ownership, recovery or claim-durability behavior
changes, review the lab's source links and assumptions against the new behavior.
Update the lab's displayed revision with its model, rather than changing the
version label alone. Its model and browser checks live beside the canonical
source. The docs page uses an absolute HTTPS URL so project-site and preview
base paths do not change the embed target; verify the full-page link and iframe
after a hosting-path change.
