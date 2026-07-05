# ADR-036: Public surface stability policy

## Status

Proposed — number claimed; policy text drafted in [`docs/stability.md`](../stability.md) (Draft),
tracked in [#369](https://github.com/hardbyte/awa/issues/369) per roadmap decision D6.

## Context

Awa has at least six consumable surfaces — Rust API, Python API, the SQL enqueue contract
(#342), the HTTP admin API (#143), metric names/attributes, and the CLI — with no written
statement of what is stable. Polyglot producers and API consumers cannot exist without one.

## Decision (to be developed)

Publish and maintain `docs/stability.md`: a surface-by-surface compatibility map, a
deprecation policy (announce in N, warn in N+1, remove in N+2 barring security), and explicit
internal/unstable designations (storage schema internals, `done_entries` physical layout,
undocumented SQL).

## Relationship to other ADRs

Governs the ADR-016 adapter contract and the #342 SQL producer contract; constrains all future
ADRs that touch a listed surface.
