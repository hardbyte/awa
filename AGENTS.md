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

### Version matrix

CI runs the sharded Rust suite and the queue-storage leg against **Postgres 17
and 18** — both majors run the whole suite, because the schema, its
partitioning, and the planner behaviour the claim path depends on are all
version-sensitive.

The Python suite runs on the **newest two CPython releases**. The support
window is whatever `requires-python` says (3.10+), and the `abi3-py310` wheel
covers all of it from one artifact, so the legs are not there to prove the
ABI — they exercise the asyncio integration (`pyo3-async-runtimes`), which is
where version skew actually lands. The `pyproject.toml` classifiers therefore
track the support window, not the test matrix. When a new CPython ships, add it
to the matrix and drop the oldest.

### Contention-sensitive assertions in the nightly suites

The nightly chaos and benchmark suites run on shared runners whose CPU
allocation varies run to run. Three assertion shapes are sensitive to that and
must go through `awa/tests/ci_timing.rs` rather than hard-coding a bound:

| Shape | Helper | Failure if too tight |
| --- | --- | --- |
| Wall-clock wait for a state | `scaled_timeout` | Spurious timeout |
| `heartbeat_staleness` on a chaos client | `scaled_staleness` | The runtime rescues a *live* attempt and the test sees a genuine duplicate completion — a margin bug that reads as a correctness bug |
| "at least N of these happened" floor | `contention_floor` | Gate fires while every invariant it exists for is intact |

All three only ever loosen a bound, and only when `CI` is set, so a local run
keeps the strict values and a real regression still fails fast on a developer
machine. `AWA_CHAOS_TIMEOUT_MULTIPLIER` overrides the factor (clamped to
`>= 1.0`).

A minimum-progress floor must sit below the *observed* operating point, not the
nominal one. The receipt-plane rotation gate is the cautionary example (#399):
its floor came from the 1s rotate interval's tick rate, but rotation is driven
by the maintenance loop reaching a rotate decision, so the healthy steady state
sat flush against the floor and the gate fired at 41-vs-45 with every
architectural bound perfect.

Measurement windows are *not* timeouts — `recv_until`'s duration defines what a
benchmark samples, so it stays unscaled.

### Connection budget in the Python suite

`awa-python/tests/conftest.py` defaults test clients to a small pool and fails
the session if backend count grows across it. Two conventions follow (#420):

- A fixture that builds a client must `yield` it and close it in a `finally`,
  never `return` it. Returning leaves pool teardown to GC timing, which parks
  server connections for up to sqlx's 10-minute idle timeout.
- Pass `max_connections` explicitly only when the test is *about* pool sizing.
  Otherwise take the conftest default, so one test cannot starve the next.

## Schema Migrations

Migrations are forward-only and must stay rolling-upgrade compatible. Version
floors, exclusive migrations, and the newer-schema fail-safe live in
`awa-model/src/migrations.rs`.

The implementation checklist lives here rather than in `docs/`: it is
contributor-internal, and `docs/` is published as the public documentation site.
[`docs/development.md`](docs/development.md#authoring-schema-migrations) carries
the user-facing summary and points back here.

Policy: [ADR-041 — rolling-upgrade policy](docs/adr/041-rolling-upgrade-policy.md). Use this checklist before opening a migration PR; version floors, exclusive migrations, and the newer-schema fail-safe live in `awa-model/src/migrations.rs`.

Checklist for any new `awa-model/migrations/vNNN_*.sql`:

**Every migration**

- [ ] Keep every object used by N−1 binaries compatible: no drops, type changes, or tightened constraints; make new objects and columns additive.
- [ ] Make the migration safe to re-run: `IF NOT EXISTS` on `CREATE TABLE` / `SEQUENCE` / `INDEX`, `CREATE OR REPLACE` for functions and views, `DROP TRIGGER IF EXISTS` before each `CREATE TRIGGER`, guarded `DO` blocks for anything with no `IF NOT EXISTS` form (`CREATE TYPE`), and `ON CONFLICT (version) DO NOTHING` on the `awa.schema_version` row. `migrations::tests::every_migration_guards_its_ddl` enforces the top-level cases; `test_every_migration_is_individually_re_runnable` proves it against a real database.
- [ ] Keep every step transaction-safe — the runner applies the whole pending range in one transaction, so no `CREATE INDEX CONCURRENTLY`, `VACUUM`, or statement-level `BEGIN` / `COMMIT` / `ROLLBACK` / `SAVEPOINT`. `migrations::tests::every_migration_step_is_transaction_safe` enforces this.
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
- The repository is the source of truth; skills also ship in the CLI release
  binary archives. Do not copy them into crates, runtime images, or the container
  build — files buried in a wheel or cargo registry are not agent-discoverable,
  and duplicated copies drift. Consumers install from the repo pinned to the
  release tag matching their awa version (see the README).
- Update the affected skill when public behavior or a documented workflow
  changes, and avoid duplicating skill content elsewhere.
