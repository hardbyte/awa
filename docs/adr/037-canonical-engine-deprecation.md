# ADR-037: Canonical engine deprecation and removal

## Status

Proposed — number claimed; tracked in [#370](https://github.com/hardbyte/awa/issues/370) per roadmap decision D2 in [`docs/0.7-roadmap.md`](../0.7-roadmap.md).

## Context

The canonical (row-mutating) engine exists only as the 0.5→0.6 transition source since
queue-storage became the default (ADR-019, 0.6.0). Keeping it alive taxes every feature with
dual-engine implementation, tests (#360), and docs — the tax that shipped the #359 defect.

## Decision (to be developed)

- 0.7 `awa migrate` refuses to run unless storage state is `active` (or the install is fresh),
  with a refusal message naming the exact 0.6 finalize steps.
- Canonical formally deprecated in 0.7 (release notes, docs, startup warning).
- Claim/execution/trigger paths removed in 0.8; upgrades step 0.5 → 0.6 (finalize) → 0.7.
- Reversal condition: beta feedback showing a meaningful population stuck mid-transition for
  reasons Awa can fix.

## Relationship to other ADRs

Completes the ADR-019 supersession of the pre-0.6 storage model; bounds the #360 dual-engine
test matrix; interacts with the staged-transition tooling decisions recorded on #180.
