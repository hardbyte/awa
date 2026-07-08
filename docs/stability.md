# Public Surface Stability Policy

> **Status: Accepted** ([ADR-036](adr/036-public-surface-stability-policy.md), from
> [`0.7-roadmap.md`](0.7-roadmap.md) decision D6). This document is normative: release notes
> list breaking changes against its surface list, and changes to the promises below are made
> by amending this document in an ordinary reviewed PR.

Awa is consumed through several distinct surfaces. This document states, per surface, what is
covered by a compatibility promise, what is explicitly internal, and what each release type may
change. Anything not listed here is **internal** and may change in any release without notice.

## Release types (pre-1.0)

While Awa is pre-1.0, "minor" releases (0.6 → 0.7) play the semver-major role:

| Release type | May contain |
| --- | --- |
| **Patch** (0.7.0 → 0.7.1) | Bug fixes only. No breaking changes to any listed surface. No new migrations except to fix a defective migration. |
| **Minor** (0.7 → 0.8) | Breaking changes to listed surfaces are allowed **only** with: a changelog entry under "Breaking changes", a migration path in the upgrade guide, and — where feasible — a deprecation period per the policy below. |
| **Beta / RC** | Surfaces may change between pre-release tags of the same line (the 0.6 `QueueFanout` → `PartitionedQueue` rename is the precedent). Pre-release tags promise nothing to each other; the stable tag promises everything below. |

## Deprecation policy

Announce in release N (changelog + doc note), warn at runtime in N+1 where the surface makes a
runtime warning possible, remove in N+2. Security fixes may compress this schedule; the
changelog will say so explicitly.

## Surfaces

| Surface | What is covered | What is not |
| --- | --- | --- |
| **Rust API** (`awa`, `awa-model`, `awa-macros`, `awa-testing`, `awa-seaorm`) | All `pub` items documented on docs.rs. Semver enforcement in CI is planned ([#402](https://github.com/hardbyte/awa/issues/402), `cargo-semver-checks`); until it lands, breaks are caught by review and the changelog rule below. | `#[doc(hidden)]` items; anything behind an unstable feature flag; `awa-worker`/`awa-ui` internals not re-exported by `awa`. |
| **Python API** (`awa-pg`) | Everything exported by `awa/__init__.py` and typed in the `.pyi` stubs (stub/API drift gated in CI per [#378](https://github.com/hardbyte/awa/issues/378)). Async and `_sync` counterparts per ADR-009. | Underscore-prefixed members; the raw PyO3 module layout. |
| **SQL producer contract** | `awa.insert_job_compat(...)` per [#342](https://github.com/hardbyte/awa/issues/342): signature, semantics, BLAKE3 `unique_key` derivation, `ordering_key` → shard hash. Versioned against `awa.schema_version`; conformance script provided. | Every other function, table, view, and trigger in the `awa.*` schema. Direct DML against storage tables is unsupported. |
| **Terminal read surface** | `{schema}.terminal_jobs` (per ADR-026) and the documented public views. | `done_entries` physical layout and all segment/ring/ledger internals — compact batches mean not every completed job is physically a `done_entries` row. |
| **HTTP admin API** | The documented endpoints and their response types (a typed, non-DAL response module in `awa-ui` — [#403](https://github.com/hardbyte/awa/issues/403); schemas listed in `ui-design.md`). The worker probe bodies (`/healthz`, `/readyz` field names, per `deployment.md`). A standalone `awa-api` crate is deferred until a second consumer exists ([#143](https://github.com/hardbyte/awa/issues/143), expected with the MCP server). | Handler internals; the embedded UI's asset paths; any endpoint not documented in `ui-design.md`. |
| **Callback receiver contract** | The signed callback endpoints (ADR-018/021/027): paths under the configured prefix, signature scheme, payload shapes, error mapping. Identical between embedded router and `awa callbacks serve`. | — |
| **CLI** | Documented commands, their exit codes, and `--json` output schemas (today: `awa health`, `awa queue overrides show`, `awa storage status`; `awa doctor` when it ships). Fatal errors render their `Display` guidance. | Human-readable (non-`--json`) output formatting. |
| **Metrics** | Metric names, types, and attribute keys listed in the telemetry docs. | Attribute *values* cardinality; bucket boundaries (may be tuned in minors with a changelog note). |
| **Storage schema / migrations** | The migration *path*: `awa migrate` upgrades any supported prior version's schema (support window per [#367](https://github.com/hardbyte/awa/issues/367) and ADR-037's gate). | Schema contents. Tables, indexes, functions, and triggers are internal; migrations may reshape them freely. There is no downgrade path. |
| **Configuration** | Documented `QueueConfig`/builder options, Python `start()` kwargs, and `AWA_*` environment variables in `configuration.md`. | Undocumented env vars; internal tuning defaults (may change in minors with a changelog note). |

**Reserved metadata namespace.** Job `metadata` keys prefixed `awa:` are
reserved for awa itself (today: `awa:traceparent`,
[ADR-039](adr/039-trace-propagation.md)). A caller-supplied value under a
documented reserved key is honored as authoritative, but awa may add,
interpret, or stop writing reserved keys in any release. Keys without the
prefix are user-owned and pass through untouched.

## What "supported" means for binary/schema skew

Rolling deploys create windows where an older binary runs against a newer schema. The support
statement, asserted nightly by `scripts/compat-matrix.sh` against **pinned release
artifacts** ([#367](https://github.com/hardbyte/awa/issues/367)):

- **One minor behind (0.6.x binary, 0.7 schema):** full lifecycle supported — enqueue,
  claim, complete, cancel — on a finalized cluster. Asserted with the published
  `awa-pg==0.6.0` wheel.
- **Two minors behind (0.5.x binary, finalized schema):** asymmetric. *Producers* keep
  working — the `awa.jobs` compatibility routing sends their inserts to the active engine.
  *Workers* are inert: they claim from the canonical hot table, which is empty on a
  finalized cluster. Upgrade workers before or with the finalize step.
- **Newer binary, older unfinalized schema:** `awa migrate` refuses loudly (exit non-zero,
  message names the exact finalize steps) rather than upgrading over live canonical work —
  the [ADR-037](adr/037-canonical-engine-deprecation.md) gate. Asserted against a 0.5.7-era
  schema carrying a canonical job; the pre-migrate-0.6 variant of this leg activates
  automatically once the 0.7 line ships its first migration.
- The 0.7 boundary additionally requires a finalized queue-storage transition (ADR-037).

## Relationship to release notes

Every stable release's changelog lists breaking changes against this document's surface list.
If a change breaks something *not* listed here, it is not a breaking change — but if that
surprises users repeatedly, the fix is to amend this document, not to argue.
