# ADR-036: Public surface stability policy

## Status

Accepted — the policy text is [`docs/stability.md`](../stability.md), which is normative
([#369](https://github.com/hardbyte/awa/issues/369), roadmap decision D6).

## Context

Awa is consumed through at least seven distinct surfaces — the Rust API, the Python API, the
SQL producer contract, the HTTP admin API, the callback receiver contract, metric
names/attributes, and the CLI — and 0.6's beta-series churn (`QueueFanout` →
`PartitionedQueue`) showed the cost of having no written boundary: consumers cannot tell a
supported surface from an internal one, and maintainers cannot tell a breaking change from a
refactor. Polyglot producers ([#342](https://github.com/hardbyte/awa/issues/342)) and API
consumers cannot exist without a written contract.

## Decision

Publish and maintain [`docs/stability.md`](../stability.md) as the single normative statement
of what is stable:

- A **surface-by-surface map**: what each surface promises, what is explicitly internal.
  Anything unlisted is internal and may change in any release without notice.
- **Release-type rules** while pre-1.0: patches fix bugs only (no new migrations except to
  repair a defective one); minors (0.x → 0.x+1) play the semver-major role and may break
  listed surfaces only with a changelog entry, an upgrade path, and — where feasible — a
  deprecation period; pre-release tags of one line promise nothing to each other.
- A **deprecation policy**: announce in N, warn at runtime in N+1 where possible, remove in
  N+2; security fixes may compress the schedule explicitly.
- A **binary/schema skew statement**, asserted nightly by the compat matrix
  (`scripts/compat-matrix.sh`, [#367](https://github.com/hardbyte/awa/issues/367)) against
  pinned release artifacts.

Changes to the policy are made by amending the document in an ordinary reviewed PR; release
notes list breaking changes against its surface list. If a change breaks something unlisted
and that surprises users repeatedly, the fix is to amend the document, not to argue.

## Consequences

**Positive**

- Consumers get an explicit contract per surface; maintainers get an explicit licence to
  change everything else.
- Enforcement has concrete hooks: `cargo-semver-checks` for the Rust rows
  ([#402](https://github.com/hardbyte/awa/issues/402)), the compat matrix for skew, `--json`
  schema tests for the CLI, and the release checklist in the
  [#383](https://github.com/hardbyte/awa/issues/383) tracker.

**Negative**

- The document must move with the code: every PR that adds a `--json` command, a probe
  field, or a public type either updates it or implicitly leaves that surface internal.
- Promising the HTTP response types constrains handler refactors until the response-type
  module ([#403](https://github.com/hardbyte/awa/issues/403)) fully separates them from the
  DAL.

## Alternatives considered

- **Semver alone** — says *that* something broke, not *which surface was ever promised*;
  pre-1.0 semver carries no information for consumers.
- **A stability attribute per item** (Rust `#[stable]`-style annotations) — heavier tooling
  than a seven-row table justifies, and it cannot describe SQL, HTTP, metric, or CLI
  surfaces at all.
- **Per-crate CHANGELOG discipline without a map** — documents changes without defining the
  boundary; the boundary is the thing consumers need.

## Relationship to other ADRs

Governs the ADR-016 adapter contract and the [#342](https://github.com/hardbyte/awa/issues/342)
SQL producer contract; constrains every future ADR that touches a listed surface; the skew
row is enforced by the ADR-037 migrate gate plus the compat matrix.
