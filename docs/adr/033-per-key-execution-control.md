# ADR-033: Per-key execution control

## Status

Accepted. Tracked in [#340](https://github.com/hardbyte/awa/issues/340). The worker-local baseline
and the fleet-exact protocol remain experiment-gated by E5 in
[`docs/0.7-roadmap.md`](../0.7-roadmap.md); acceptance fixes the contract and candidate constraints,
not evidence that either tier has shipped.

## Context

Awa controls execution at queue granularity. Multi-tenant and resource-oriented workloads also
need a limit such as "at most one in-flight event for this provider operation" or "at most four
jobs for this tenant across the fleet."

`InsertOpts::ordering_key` does not provide that guarantee. It chooses an enqueue shard and
therefore preserves FIFO claim order inside that shard, but it does not prevent two already
claimed jobs from executing concurrently. It is also a routing input rather than durable job
identity: retry, callback-resume, and DLQ-retry paths do not currently retain the original key.

Three constraints make fleet-exact control a storage protocol rather than another local
semaphore:

1. A read-only "count live leases, then claim" check is racy. Two claimers can both observe zero
   and commit a claim unless the check and grant are serialized.
2. The claim cursor may only advance over work for which committed attempt evidence exists.
   Skipping a capacity-blocked row as if it were claimed would violate the cursor-monotonicity
   and prune proofs in ADR-019/023.
3. The #169 lesson remains binding: a per-key counter row updated on every claim and completion
   becomes a high-cardinality hot mutable table under a pinned MVCC horizon.

The public delivery contract also remains at-least-once. A durable Awa lease can be rescued while
the old process is partitioned and still executing application code. Per-key control can make the
database's active-attempt set exact; it cannot fence an external provider that does not accept a
fencing token.

## Decision

Deliver per-key control in two explicit tiers and keep fairness separate from the concurrency
safety invariant.

### Tier 1: worker-local control

Tier 1 uses an in-process `KeyPolicy` in each dispatcher. It is exact within one runtime and
approximate across a fleet:

```text
fleet upper bound ~= per-runtime limit x runtimes able to claim the queue
```

It is useful for noisy-neighbour damping and per-runtime API budgets, but its API and metrics must
always name the approximation. Worker-local rate limiting remains in this tier; this ADR does not
define an exact distributed rate-window protocol. The concurrency-key digest still travels
durably with the job so claim, retry, callback, and DLQ paths retain the dispatch identity. Tier 1's
storage claim is that it adds no fleet-wide grant, closure, counter, or permit family, not that the
key occupies no bytes in existing job representations.

### Tier 2: fleet-exact execution grants

Tier 2 defines an execution grant as the database authority for one keyed active attempt. For each
`(key_policy_id, concurrency_key_digest)`:

```text
count(unclosed execution grants) <= max_in_flight
```

That invariant is fleet-wide and independent of the number of worker processes or claimers.

#### Durable key identity and policy authority

`InsertOpts::concurrency_key` is distinct from `ordering_key`. When it is absent and an
`ordering_key` is present, the enqueue path copies the ordering key into the concurrency-key input
before storage. A domain-separated SHA-256 digest, rather than the raw caller key, is stored on
every live representation and every wide representation that can later be re-enqueued: ready,
deferred, lease, retry, and DLQ rows. SHA-256 rather than ADR-002's BLAKE3 is deliberate: the
public SQL contract computes this digest authoritatively inside PostgreSQL, and `sha256()` is a
PostgreSQL built-in, while BLAKE3 would require an extension or an impractical procedural
implementation. `unique_key` remains client-side BLAKE3 under ADR-002; the two key families
intentionally use different algorithms and computation venues, and the #342 vectors cover both. Narrow terminal rows and compact successful completions hydrate it from
their retained ready backing row under ADR-026 instead of widening the terminal hot path. The
fallback is evaluated once at enqueue; it is not recomputed from routing state later.

Enqueue-shard routing has explicit precedence:

1. an explicit `ordering_key` selects the shard;
2. otherwise a `concurrency_key` selects a stable shard using its stored digest; and
3. only jobs with neither key use the enqueue rotor.

The second rule localizes keyed-but-unordered work instead of scattering one saturated key across
every enqueue shard. It does not promote `concurrency_key` to an ordering API: Awa still makes no
completion-order promise, and jobs with explicit, differing ordering keys may share one concurrency
key across several lanes. Physical `PartitionedQueue` routing is unchanged; the policy id remains
the cross-partition concurrency namespace.

Shard locality is part of the producer contract, and the public SQL boundary enforces it rather than
trusting each polyglot client. Every supported producer path uses the same domain-separated
concurrency-key digest and routing precedence above. The `awa.insert_job` contract in #342 receives
the logical keys and computes the digest and `enqueue_shard` authoritatively in PostgreSQL; it does
not accept a client digest or shard as authoritative. If a conformance/debug request supplies an
expected value, a mismatch fails with a stable contract error. Binary-coupled and bulk paths perform
the equivalent validation before publishing staged rows. The contract freezes the digest and
`concurrency_key`-to-shard vectors alongside the existing `ordering_key` vectors, converting a
non-compliant producer from a silent queue-wide probe-cost regression into a boundary error.

The selected `enqueue_shard` becomes durable routing metadata on deferred/retry, callback, and DLQ
representations and is reused when those rows return to ready. It is not recomputed through the
unkeyed rotor. This also closes the current retry-path gap where an original ordering-key route is
not retained. Lowering `enqueue_shards` follows ADR-025's drain/compatibility rules for lanes that
were selected under the old width. A same-queue priority move retains the shard. A cross-queue
administrative move resolves the retained shard against the destination width as
`retained_enqueue_shard % destination_enqueue_shards`; it must not copy an out-of-range shard or
fall back to the rotor. Jobs that shared a source route therefore remain co-located after the move.

The limit and scope are database-authoritative, represented by a bounded policy catalog rather
than independently configured worker values. A policy has a stable `key_policy_id` and a
`max_in_flight` value. Multiple physical partitions of one `PartitionedQueue` may reference the
same policy, so a width change or cross-partition administration cannot accidentally create a
second concurrency namespace. Jobs without a concurrency key do not consume a grant.

Lowering a limit must preserve the invariant at the instant the new value becomes authoritative,
without holding a claim-conflicting row lock during an unbounded all-key scan. A successful change
uses a three-step, epoch-guarded transition:

1. A short transaction exclusively locks the policy row, records the requested target, increments
   its change epoch, and moves admission to `draining`. Acquiring that lock waits for claimers that
   already hold the shared policy lock; after commit, no new grant can open under that policy. A
   policy carries at most one pending transition: step 1 refuses while another change is already
   `draining`.
2. Outside the exclusive window, Awa scans the fixed claim-ring children for the maximum open count
   under a bounded, operator-visible verification timeout. Closures may continue and can only lower
   the result. A timeout or resource-limit failure returns `verification_incomplete`, keeps the old
   limit authoritative, and leaves admission drained until the operator resumes or cancels.
3. A final short transaction locks the policy row, verifies the same epoch, target, and drained
   state, and activates the lower value only if the observed maximum is within it. Otherwise it
   reports the observed maximum, leaves the old limit unchanged, and keeps the transition pending
   and drained so open grants keep falling toward the target.

Retrying after `verification_incomplete` is a `resume` of the pending transition, not a new step 1
— step 1 refuses while a change is `draining`, so resume is the only forward path. Resume carries
the change epoch and target returned by step 1, validates them against the pending transition in a
short policy-row transaction without incrementing the epoch, keeps admission closed throughout, and
reruns step 2's bounded verification followed by step 3. A resume with a stale epoch or mismatched
target is rejected without changing policy state; resume never creates a second pending transition.

Cancellation is also an epoch-guarded state transition, not an out-of-band flag clear. The request
carries the change epoch and target returned by step 1. A short transaction exclusively locks the
policy row, requires that exact epoch/target and `draining` state, increments the epoch, clears the
pending target, and restores the prior active limit/state atomically. Admission remains closed until
that transaction commits. A stale cancel is rejected without changing policy state; an in-flight
verifier or activator from the cancelled operation observes the new epoch and cannot commit. After
commit, new claims may use only the restored prior limit.

This protocol never exposes a committed policy state where the lower limit is authoritative while
an over-limit governed set exists, and claimers do not queue behind the verification scan. The
fail-closed residue is that an abandoned transition — an operator tool that crashed after step 1 —
leaves the policy `draining` indefinitely; nothing re-opens admission automatically. Draining
state, its pending target/epoch, and its age are therefore first-class health and metrics signals,
not just rows an operator can query. The operator waits for open grants to fall within the target
and resumes; Awa never "grandfathers" an over-limit active set under a lower authoritative value. Disabling or changing a policy's identity
while it has open grants uses the same drain/epoch discipline; silently changing namespace would
weaken the established guarantee.

#### Append-only grant and closure evidence

An execution grant is a semantic role of the attempt's existing claim evidence, not necessarily a
second ledger family. Grant open-ness and claim open-ness have the same lifecycle: materializing a
receipt claim into `leases` or entering `waiting_external` does not close either, while completion,
retry, snooze, cancellation, DLQ, and rescue close both.

The preferred physical prototype therefore routes keyed attempts through the retained row-local
`lease_claims` shape and adds `key_policy_id` plus the full key digest to that row. Existing
`lease_claim_closures` / `lease_claim_closure_batches` evidence closes the grant. The open set is
keyed `lease_claims` anti-joined with those existing closure shapes. Per-child indexes on
`(key_policy_id, key_digest)` limit the search to one key across the fixed claim-ring children;
same-key history retained within those children remains an E5 cost to measure.

This row-local shape is necessary because compact `lease_claim_batches` stores members in arrays;
PostgreSQL cannot provide the per-member policy/key index needed by the claim-time capacity query
without unnesting retained batches. Reusing claim evidence avoids an extra close write on every
transition, makes grant/claim divergence structurally impossible, and reuses the existing claim
prune proof.

It is not free. Keyed throughput that would otherwise use compact claim batches writes one
row-local claim per attempt, and closed rows remain until claim-ring rotation. E5 must compare this
preferred shape with a separate indexed grant/closure ledger that lets the ordinary receipt claim
remain compact. Both are append/truncate designs; neither may introduce a mutable per-key counter.
The accepted physical shape is the one that passes the #246, pinned-MVCC, WAL, and claim-p99 gates.

#### Serialized claim decision

`claim_ready_runtime` is already a PL/pgSQL transaction-scoped allocator, so the protocol remains
one database round trip. It probes eligible lane heads in scheduler order, bounded by the queue's
finite `(priority, enqueue_shard)` lane registry. For each candidate lane it:

1. reads the ordinary FIFO candidate window;
2. takes a shared row lock on each referenced policy, compatible with other claimers but conflicting
   with an administrative policy update;
3. on the first occurrence of each keyed value, attempts a transaction-scoped advisory lock;
4. counts committed, unclosed grants for that key;
5. chooses the longest contiguous candidate prefix that stays within every limit; and
6. appends ordinary claim evidence, including the grant fields or separate grant prototype, in the
   same transaction.

The function keeps per-call memos for policy row locks and for
`(key_policy_id, key_digest)`: it locks each policy once, acquires and counts a key once, then tracks
grants tentatively added by earlier admitted prefixes. This matters when an explicit ordering key,
priority, or compatibility row places the same concurrency key in more than one probed lane. The
database-authoritative count remains the source of truth; the memo only avoids repeating work
inside one transaction.

The default prototype uses `pg_try_advisory_xact_lock`: contention ends the admissible prefix
without waiting on another key decision. A 64-bit lock key is derived with a domain that includes
the Awa schema identity, policy id, and full digest; advisory locks are database-scoped, so omitting
the schema would make independent Awa schemas contend accidentally. A hash collision only causes
conservative under-admission because capacity is still counted by the full stored digest. The
existing per-attempt receipt-lock helper is the implementation precedent, while the exact blocking
versus try-lock acquisition plan remains part of E5 and `AwaStorageLockOrder` validation.

If a lane head is at capacity or its key lock is busy, the function does not advance that lane's
cursor or write a tombstone; it probes the next eligible lane. The current allocator selects only
one lane per call, and `SKIP LOCKED` helps only when concurrent transactions overlap, so returning
immediately here could repeatedly pick the same gated lane and make a ready queue appear idle. The
new result contract must distinguish `idle`, `key_gated`, and `key_lock_contended` even when it
returns no jobs. Dispatch idle backoff and idle-prune heuristics must treat the latter two as busy.

Relation locks for the eventual claim insert still interact with claim-ring prune; try-locking the
key does not make that insert non-blocking. The existing bounded prune `lock_timeout` remains, and
E5 plus the lock-order model must cover claim-holds-key-lock versus prune-holds-child-lock. If an
implementation caps probes below the fixed lane count, it must rotate the starting lane across
calls so lanes outside one probe window remain live.

#### Grant-close wakeup and gated backoff

Closing a keyed grant changes ready-work eligibility even when it does not insert a ready row.
Every completion, retry/snooze, cancellation, DLQ, callback resolution, caller-owned completion,
and rescue transaction that closes one or more keyed grants therefore emits the ordinary
transactional queue notification for each distinct affected logical queue. PostgreSQL delivers the
notification only after commit, so a woken claimer sees the closure. Batched closure paths dedupe
queues before calling `pg_notify`.

`key_gated` is its own dispatcher wait class. It is busy for idle-prune/health purposes but does not
hot-spin: the dispatcher waits for a queue notification plus a bounded safety poll for lost
notifications. `key_lock_contended` uses a shorter jittered retry because the competing claim
transaction should be brief. With a working session listener, post-closure pickup is
notify-bounded; poll-only or transaction-pooler deployments use the explicit gated safety cadence.
E5 measures pickup latency and notification wake amplification in both modes.

#### Grant lifetime

The grant opens with the committed claim and stays open while the attempt is `running` or
`waiting_external`. It closes atomically when the attempt:

- completes, fails terminally, enters the DLQ, or is cancelled;
- snoozes or becomes retryable/deferred;
- is rescued; or
- otherwise leaves the active-attempt set.

A later retry is a new attempt and acquires a new grant. Consequently, `max_in_flight = 1`
prevents overlap in Awa's admitted active-attempt set but does not promise completion order across
backoff: after event A becomes deferred, event B may run before A's retry. Holding a key barrier
across retry is a different strict-serialization feature and is not implied by this ADR.

Callback parking and resumption within `waiting_external` retain the grant and do not append or
reuse closure evidence. Only callback resolution that completes, retries/defers, cancels, enters the
DLQ, or otherwise leaves the active-attempt set closes it. Normal executor finalization, batched
completion, such callback resolution, admin cancellation, and maintenance rescue append or reuse
the claim closure in the same transaction as their guarded state transition.
[ADR-042](042-caller-owned-finalization-transactions.md) extends the same rule to a caller-owned
transaction: application rows, terminal evidence, and the grant closure commit together or all
roll back.

Capacity-release latency is therefore part of policy sizing. A crashed attempt holds its grant until
heartbeat/deadline rescue; `waiting_external` holds it until callback resolution or
`callback_timeout_at`, extended by callback heartbeats. There is no independent timer that silently
releases a live grant. Operators choose attempt deadlines and callback timeouts consistent with the
key's availability objective, and health/metrics expose the oldest open grant. At limit 1, a crash
blocks later admission for that key until rescue, and an unresolved one-hour callback can hold the
key for the full hour. Forced rescue still cannot prove that a partitioned zombie stopped calling
an external provider.

### Policy and operator surface

`max_in_flight` must be at least 1. A zero value would pause every key attached to a policy, not one
tenant, and queue pause already expresses that operation honestly. Per-key incident overrides are a
future design because a high-cardinality mutable override table could recreate the hot-row problem
this ADR avoids.

The admin API and CLI expose policy create/show/update, limit lowering, drain, and force-disable
operations using the same preview/confirm conventions as other operator mutations. Limit changes
are audited with an explicit authenticated actor supplied by the admin boundary, or with PostgreSQL
`session_user` for direct operator SQL; a definer must not record `current_user`, which would name
its execution owner. The UI and SQL inspection surface provide:

- open grant count and oldest age by policy and key digest;
- top saturated keys without exposing raw caller keys;
- a derived `key_gated` reason on an available job when its current policy/key is saturated;
- policies in `draining`, with their pending target, change epoch, and age, surfaced through
  health/metrics so an abandoned transition pages rather than lingers; and
- queue metrics for gated outcomes, lock contention, closure notifications, wake amplification,
  and post-closure pickup latency.

These are derived views over authoritative evidence, not a new mutable live-counter family.

### Fairness is a separate allocator decision

Issue #340 also asks for fairness across keys. Exact admission and cross-key fair scheduling have
different proof obligations. The contiguous-prefix protocol above intentionally preserves FIFO and
cursor monotonicity, so a saturated key at a lane head blocks other keys behind it in that lane.
Bounded lane probing prevents that lane from masquerading as an idle physical queue, but it is not
round-robin scheduling across keys.

Removing within-lane head-of-line blocking requires a later design: for example durable per-key
sub-lanes or append-only hole/skip evidence that the allocator and prune model can prove. Tier 2 may
ship without claiming cross-key fairness. Metrics must expose gated lanes, lock contention, oldest
gated age, and ready-but-gated outcomes so the trade-off is visible.

## Guarantees and non-guarantees

Tier 2 guarantees:

- no more than the configured number of committed, unclosed Awa attempts for a key across the
  fleet;
- rollback-safe admission: a failed claim transaction creates neither a claim nor a grant;
- attempt-specific release: a stale completion cannot close a successor's grant;
- no release before durable finalization acknowledgement; and
- a gated lane alone cannot produce an idle result while an admissible lane is within the configured
  probe set.

It does not guarantee:

- exactly-once handler execution;
- that a rescued/zombie process has stopped running application code;
- exactly-once effects in an external provider;
- per-key completion order across retry/snooze; or
- cross-key fairness.

Provider-facing handlers must still use provider idempotency keys and reconcile authoritative
provider state. Where the provider accepts a fencing token, `(job_id, run_lease)` or a derived
attempt token can be supplied, but acceptance is outside Awa's contract.

## Rolling upgrade

The schema expansion is disabled by default. Older workers may operate the additive schema only
while no exact policy is enabled; they cannot retain the new key through every retry/rescue path.
Enabling a policy therefore acts as a capability-gated flip under ADR-041: every fresh runtime that
can claim, retry, rescue, or administratively move the affected queues must advertise support first.

Runtime capability is necessary but not sufficient. Rows enqueued before the expansion may have an
`ordering_key`-selected shard but no recoverable concurrency-key digest: many keys map to one shard,
so `enqueue_shard` is not an authoritative backfill source. Before enabling complete coverage, the
same guarded flip must prove that every affected ready, deferred, retry, callback, DLQ, receipt, and
lease representation either carries the new policy/key identity or has drained. An
application-supplied authoritative key mapping may backfill rows before the flip; otherwise the
operator drains the affected queues. A legacy row without key identity never silently shares a
policy with new keyed work, because Awa cannot know which user-intent key it should consume.

For a queue that cannot drain, the operator may instead select an explicit
`permit_legacy_ungoverned` compatibility activation. Legacy rows then run without a grant while rows
with authoritative identity receive exact Tier 2 admission. This does not violate
`OpenGrantsPerKey <= Limit` for governed rows, but it weakens the end-to-end user-intent promise: a
legacy row may run concurrently with governed key-mates. The policy reports
`coverage=partial_legacy`, `awa doctor` remains non-green with representation counts and remediation,
and health/metrics expose ungoverned admission totals and remaining backlog. No raw shard is treated
as a key. Promotion to `coverage=complete` is a separate epoch-guarded flip after every
representation has drained or been authoritatively backfilled.

The implementation release must either provide a compatible N-1 patch that recognizes the expand
schema or defer the migration to the next minor. This ADR does not reopen the current 0.7 release
gate by itself.

## Validation

Before Tier 2 ships:

- a focused TLA+ model proves `OpenGrantsPerKey <= Limit`, the
  drain/epoch/verify/resume/activate-or-cancel lowering protocol — including that resume validates
  the pending epoch/target, never opens admission, and never creates a second pending transition —
  the governed-only scope of partial-legacy coverage, attempt-specific closure, rollback safety,
  completion-versus-rescue, no cursor advance over a gated head, and conditional liveness when
  capacity eventually becomes available;
- `AwaStorageLockOrder` covers the shared policy-row lock before key-lock acquisition, both short
  exclusive policy-transition windows with verification outside them, receipt completion, and both
  directions of the claim/prune partition-lock interaction;
- `AwaDeadTupleContract` classifies the selected row-local or separate-ledger shape as
  append/truncate and forbids per-key UPDATE/DELETE;
- integration races run at least two claimers on different runtimes against the same key and prove
  one winner at limit 1;
- limit lowering fences admission in a short epoch transition, performs its all-key verification
  outside the exclusive policy-row window under a bounded timeout, refuses incomplete or
  over-target verification, and never exposes a committed policy state where
  `OpenGrantsPerKey > Limit`; concurrent claimers do not wait behind the verification scan. A
  matching cancel restores the prior limit atomically, stale cancel/activate/resume requests fail,
  and no grant opens while rollback still reports `draining`; a second transition refuses while one
  is pending; resume validates the pending epoch/target, keeps admission closed, and reruns
  verification to activation; and an abandoned `draining` policy is reported through health/metrics
  with its age;
- a gated top-priority lane cannot make an admissible lane in the same queue report idle, and the
  storage result distinguishes idle, gated, and lock-contended outcomes;
- keyed jobs without an ordering key remain shard-local across enqueue, retry, callback, and DLQ
  replay; differing explicit ordering keys exercise the per-call same-key memo across lanes;
- complete-coverage enablement refuses while an affected legacy representation lacks authoritative
  key identity, including banked backlog and in-flight N-1 attempts; a drain or authoritative
  backfill plus the final capability/row reconciliation makes the flip succeed atomically. The
  explicit partial-legacy mode admits those rows ungoverned, reports non-green coverage and metrics,
  and cannot promote to complete coverage until the same reconciliation passes;
- closing a grant wakes a gated session-listening dispatcher after commit without hot polling;
  poll-only mode stays within its documented gated safety interval;
- completion, caller-owned completion, retry, snooze, cancel, DLQ, rescue, rotation, and prune each
  prove exact grant lifetime; callback park/resume retains the grant and only resolution that leaves
  the active-attempt set closes it;
- E5 compares keyed row-local claim evidence with a separate grant/closure ledger under uniform and
  Zipf key distributions at limits 1 and N. It records claim p99, oldest gated age, throughput,
  WAL/job, retained rows, and storage footprint, and stays within the roadmap's 10% claim-p99 gate;
  and
- the ADR-041 mixed-version rehearsal proves the disabled expand phase and the capability-gated
  enablement boundary with real N-1 artifacts.

## Alternatives considered

### BLAKE3 for the concurrency digest

Rejected. Server-authoritative digest computation is part of the producer contract, and PostgreSQL
has no native BLAKE3 — it would need an extension or a procedural implementation on the insert hot
path. Computing BLAKE3 client-side only would reopen the silent key-scatter hole the authoritative
boundary closes.

### Read live leases/receipts without serialization

Rejected. Two concurrent claimers can observe the same open count and both admit work.

### Separate execution-grant and closure ledgers

Retained as an E5 physical-layout candidate, not the default. It preserves compact receipt claims
but duplicates one grant and one closure fact for every keyed attempt and adds a second prune proof.
It wins only if the row-local claim shape repeats the #246 regression badly enough to justify that
write and correctness surface.

### Put key arrays on compact claim batches

Rejected for the capacity-query index. It keeps the batch compact, but PostgreSQL cannot index each
member by `(key_policy_id, key_digest)` without expanding retained arrays on the hot claim path.

### Park gated jobs and promote them on grant closure

This is the strongest alternative to lane-head gating and is retained as E5 prototype (d). A
committed park transition could tombstone the ready lane, advance its cursor with durable evidence,
and move the job to a keyed deferred-like structure. Grant closure would promote parked work and
reuse ordinary ready-row notification, removing repeated probes and within-lane head-of-line
blocking.

The costs are substantial: every gated arrival adds a move; the parking table churns in proportion
to the hot key's backlog; closure gains promotion work; and FIFO moves from lane order to parking
order. More importantly, promotion without a reservation can lose the next capacity slot to a new
ready arrival and re-park the same job, while reserving capacity before dispatch introduces the
pre-start reservation state ADR-023 deliberately avoided. E5 may select parking only with an exact
handoff/liveness proof, no hot mutable per-key row, and better measured results than the simpler
lane-probe shapes. [Solid Queue's concurrency controls](https://github.com/rails/solid_queue#concurrency-controls)
are direct semaphore-parking prior art; [GoodJob's concurrency controls](https://github.com/bensheldon/good_job#concurrency-controls)
and [Oban Pro's Smart Engine](https://oban.hexdocs.pm/2.11.0/smart_engine.html) are broader
Postgres-queue comparisons. Their choices are inputs, not evidence that the same storage trade-offs
fit Awa's claim ring.

### Mutable per-key counter or permit row

Rejected as the default design. It gives a simple unique/atomic lock target but recreates the
high-cardinality hot mutable state that #169 and ADR-026 require Awa to avoid.

### Session advisory lock held for the whole handler

Rejected. It consumes one dedicated Postgres session per keyed execution and releases on database
disconnect even if the handler process is still running, so it does not improve the zombie-worker
boundary.

### Shard pinning plus a per-shard cap

Rejected as fleet-exact per-key control. A cap of one serializes unrelated keys sharing the shard;
a larger cap permits the same key to overlap.

### Post-claim worker-local gating

Retained only for Tier 1. Across a fleet it is approximate and holds durable claims that are not
executing, wasting claim/receipt capacity.

## Relationship to other ADRs

- **ADR-005/010/011:** priority aging, rate limiting, and worker capacity continue to decide when a
  dispatcher asks for work; the execution grant is an additional storage admission condition.
- **ADR-013:** `(job_id, run_lease)` is the attempt identity attached to and allowed to close a
  grant.
- **ADR-019/023/026:** the preferred grant representation reuses the append-only claim/closure ring
  discipline and prune proof; E5 may select a separate append/truncate ledger only with its added
  proof cost recorded.
- **ADR-025/031:** ordering and concurrency keys are distinct. An explicit ordering key owns shard
  routing; otherwise the concurrency key provides shard locality without adding an ordering
  promise. The policy id defines the fleet-exact cross-partition namespace.
- **ADR-041:** enabling exact enforcement is a capability-gated representation flip.
- **ADR-042:** caller-owned completion closes the exact attempt's grant in the caller's atomic
  business/finalization transaction.
