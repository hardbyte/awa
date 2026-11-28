# ADR-054: A claim reads its target slot only below the next segment of another slot

## Status

Accepted. Ships in migration v064. Tracked in [#885](https://github.com/hardbyte/awa/issues/885), a prerequisite for moving a stalled lane's rows between slots ([#779](https://github.com/hardbyte/awa/issues/779)).

## Context

`claim_ready_runtime` picks the lane to claim, locks its `queue_claim_heads` row with `FOR UPDATE ... SKIP LOCKED`, and in the same statement resolves the target: the segment of `ready_segments` with the smallest `next_lane_seq` past the lane's claim cursor. It then finds the first visible row of that `(slot, generation)` at or after the cursor and moves the cursor to it, and `claim_ready_slot_N` reads up to `max_batch` rows of the same slot at or after the cursor, in `lane_seq` order. The cursor then moves past the last row claimed, through the claim's evidence and the post-commit advance.

Neither read has an upper bound. The claim is therefore correct only while the lane's unclaimed rows in its target slot are one contiguous run, and none of them leaves the slot between the statement that resolves the target and the statements that read it. Two things break that:

- **An interleaved layout.** If a lane holds positions 1-10 and 21-30 in slot A and 11-20 in slot B, a claim of 15 on A takes 1-10 and 21-25. The cursor passes 11-20. They are never claimed, and the prune gate on B, which looks at the cursor, truncates them. [#847](https://github.com/hardbyte/awa/issues/847) was this layout: a producer above `READ COMMITTED` wrote a row into an older ring generation behind rows of a newer one, and claims skipped it. v062 stops current binaries writing it. Binaries before v062 still can in a mixed fleet, so for them the bound is the claim-side fix.
- **A stale target.** The head lock is granted after the first statement's snapshot, and a lock-only holder causes no recheck. If a mover relocates positions 1-5 to another slot and commits in between, the claim still targets slot 0, reads 6-10 and moves the cursor past 1-5. If the mover relocated 1-20 and the slot's next row is 21, the head step moves the cursor to 21.

No release moves a lane's rows, so the stale target needs a mover that does not exist yet. The interleaved layout is not hypothetical: it is what #847 reported, and a mixed fleet can still write it. `claim_target_bound_test` builds each with direct SQL and fails on the unmodified claim. A mover that holds the lane's head lock does not close the race, because the claim's first statement can start before the mover and be granted the lock after it commits.

Moving a lane's whole range in one transaction was safe in the stale-target race: the stale claim then finds nothing and leaves the cursor alone. The `AwaClaimTargetBoundUnboundedWholeMoves` witness shows it is not safe once a new row can land in the slot the range left, which is the current slot for a lane that is still being enqueued: the stale claim finds that row and moves the cursor to it, past the rows that moved.

## Decision

1. **Re-resolve the target after the head lock**, in a fresh statement. A mover that committed before the lock is seen, and the claim targets the slot the cursor's row is in now.
2. **Bound every read of the target slot.** The read stops below the smallest `first_lane_seq` of any segment of the lane that is past the cursor and in a different `(slot, generation)`. Segments are non-overlapping and ordered by `next_lane_seq`, so each read scans only the nearest few of them in that order and takes the smallest `first_lane_seq` among those of another slot. Every position a segment covers holds a ready row, since a segment is written in the transaction that writes its rows and tombstones never remove one, so the first segment past the cursor has a row at or after it. That bounds how far each read has to look:
   - **The head step** reads one row, which lies in the first segment past the cursor when that is in the target slot, so it scans two segments: the first, and the next, which is all it takes to see where another slot begins. When the first is in another slot (a stale target), it is the bound.
   - **`claim_ready_slot_N`** takes up to `max_batch` rows, which cannot reach a segment further out than the `max_batch + 1` nearest. A lane with one segment per row stays cheap and still fills a batch when its segments share a slot.

   A mover must keep the invariant true: it moves a segment's rows and the segment together, and never leaves a segment covering positions whose rows have gone.
3. **Compute the bound in the statement that reads the rows.** Both the head step in `claim_ready_runtime` and the `selected` CTE of `claim_ready_slot_N` carry the bound as a subquery, so the segments the bound reads and the rows it limits come from one snapshot. A mover that commits segments and rows together is seen by both or by neither. No signature changes: the claim functions keep their arguments, owners and grants.

At worst the bound turns a silent loss into a short or empty claim: the next claim re-resolves and targets the slot the cursor's row is in. A lane whose segments alternate between two slots claims one segment's rows per call. That is slower than a full batch and loses nothing; it is the cost of an interleaved layout, which the bound makes safe rather than avoids.

`AwaClaimTargetBound` models the claim as resolve, lock, re-resolve, head and read statements, with enqueues into either slot, movers that relocate a prefix or the whole of a lane's unclaimed rows, and prune gated on the cursor, all interleaving between any two statements. It checks that the cursor never passes an unclaimed row and that prune never removes one. The hardened claim passes with prefix moves. The current claim fails with interleaved enqueues alone, with prefix moves, and with whole-range moves. In the model the bound alone is sufficient; the re-resolve keeps the claim from starting on a target that is already stale. The model's head step scans two segments and its read `Limit + 1`, as the SQL does. It does not cover everything a mover meets; see [Mover obligations](#mover-obligations).

### Mixed versions

The change replaces function bodies only; arguments and results are unchanged, so binaries before it call the functions as before. An older `prepare_schema` can reinstall an older claim body, which drops the bound. A mover that depends on the bound (batched moves in #779) must therefore check that the installed `claim_ready_runtime` and `claim_ready_slot_N` carry it, and fall back to whole-range moves, or not move, when they do not. Even whole-range moves are only safe for a stale claim while no new row can land in the source slot.

### Mover obligations

The cursor does not move in the claim's own step. It moves after the claim commits, or at the next claim, which folds it over committed claim evidence: `max(next_lane_seq)` of the evidence batches in the target slot past the cursor, with no bound. Claim evidence and tombstones are keyed by `ready_slot`. A mover relocating rows must therefore:

- **Fold the cursor first.** Fold it over all committed evidence for the lane before it moves rows, so no evidence for a moved row is left keyed to the slot the row left, where a later fold in that slot could carry the cursor over unclaimed rows.
- **Not split a multi-row segment** unless it rewrites both halves' segments, and the evidence and tombstones of the rows in them, in the same transaction. A claim that straddles the split would otherwise record evidence under one slot for rows that now belong to the other.
- **Act on the current cursor.** A mover working from a cursor read before a concurrent claim committed can relocate rows that claim has taken.

`AwaClaimTargetBound` models none of these (its cursor moves in the claim's own step, and it has no evidence). They are covered by the obligations above and by `claim_target_bound_test` only for the layouts it builds, not by a proof.

## Alternatives considered

- **Row locks on the ready rows.** A claim that locks the rows it reads (`FOR UPDATE SKIP LOCKED`) would make a mover wait or skip them. It does not stop the cursor passing a row the claim never read, which is the interleaved case, and it puts a row lock on every claimed row of the hot path.
- **An atomic claim.** Resolving the target and reading the rows in one statement closes the stale target but not the interleaved layout, and the plans for the claim's CTEs are tuned around separate statements (the per-slot functions exist so each plans against one partition). A model with an atomic claim would pass while the code loses rows, which is why the model keeps the statements separate.
- **Requiring movers to move whole ranges only.** It leaves the claim correct only under a rule every future mover must keep, fails against a row landing in the source slot as above, and rules out the batched prefix moves #779 wants. The claim stays correct for any non-overlapping layout instead.

## Consequences

- Each claim costs one more probe of `ready_segments` for the re-resolve, and the head step and the slot function each scan a bounded number of segments for the bound: two in the head step, `max_batch + 1` in the slot function. The probes use the existing `(queue, priority, enqueue_shard, next_lane_seq, first_lane_seq)` index.
- A lane whose rows are interleaved across slots claims one run per call, and a claim can come back empty when it met a stale target; neither loses a row.
- The spent-prefix path already stops at another slot's segment (v061); it is unchanged.
