# ADR-033: Per-key execution control

## Status

Proposed. Tracked in [#340](https://github.com/hardbyte/awa/issues/340). The
worker-local baseline and the fleet-exact protocol remain experiment-gated by E5 in
[`docs/0.7-roadmap.md`](../0.7-roadmap.md); this ADR is a design, not evidence that either tier
has shipped.

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
define an exact distributed rate-window protocol.

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
before storage. A domain-separated BLAKE3 digest, rather than the raw caller key, is stored on every
live representation and every wide representation that can later be re-enqueued: ready, deferred,
lease, retry, and DLQ rows. Narrow terminal rows and compact successful completions hydrate it from
their retained ready backing row under ADR-026 instead of widening the terminal hot path. The
fallback is evaluated once at enqueue; it is not recomputed from routing state later.

The limit and scope are database-authoritative, represented by a bounded policy catalog rather
than independently configured worker values. A policy has a stable `key_policy_id` and a
`max_in_flight` value. Multiple physical partitions of one `PartitionedQueue` may reference the
same policy, so a width change or cross-partition administration cannot accidentally create a
second concurrency namespace. Jobs without a concurrency key do not consume a grant.

Lowering a limit prevents new grants until the open count falls below the new value. Disabling or
changing a policy's identity while it has open grants requires an explicit drain/force operation;
silently changing namespace would weaken the established guarantee.

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
2. on the first occurrence of each keyed value, attempts a transaction-scoped advisory lock;
3. counts committed, unclosed grants for that key;
4. chooses the longest contiguous candidate prefix that stays within every limit; and
5. appends ordinary claim evidence, including the grant fields or separate grant prototype, in the
   same transaction.

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

Normal executor finalization, batched completion, callback transitions, admin cancellation, and
maintenance rescue append or reuse the claim closure in the same transaction as their guarded state
transition.
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

The implementation release must either provide a compatible N-1 patch that recognizes the expand
schema or defer the migration to the next minor. This ADR does not reopen the current 0.7 release
gate by itself.

## Validation

Before Tier 2 is accepted:

- a focused TLA+ model proves `OpenGrantsPerKey <= Limit`, attempt-specific closure, rollback
  safety, completion-versus-rescue, no cursor advance over a gated head, and conditional liveness
  when capacity eventually becomes available;
- `AwaStorageLockOrder` covers key-lock acquisition, receipt completion, and both directions of the
  claim/prune partition-lock interaction;
- `AwaDeadTupleContract` classifies the selected row-local or separate-ledger shape as
  append/truncate and forbids per-key UPDATE/DELETE;
- integration races run at least two claimers on different runtimes against the same key and prove
  one winner at limit 1;
- a gated top-priority lane cannot make an admissible lane in the same queue report idle, and the
  storage result distinguishes idle, gated, and lock-contended outcomes;
- completion, caller-owned completion, retry, snooze, callback wait/resume, cancel, DLQ, rescue,
  rotation, and prune each prove exact grant lifetime;
- E5 compares keyed row-local claim evidence with a separate grant/closure ledger under uniform and
  Zipf key distributions at limits 1 and N. It records claim p99, oldest gated age, throughput,
  WAL/job, retained rows, and storage footprint, and stays within the roadmap's 10% claim-p99 gate;
  and
- the ADR-041 mixed-version rehearsal proves the disabled expand phase and the capability-gated
  enablement boundary with real N-1 artifacts.

## Alternatives considered

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
- **ADR-025/031:** ordering and concurrency keys are distinct. Partition and enqueue-shard routing
  co-locate ordered work; the policy id defines the fleet-exact concurrency namespace.
- **ADR-041:** enabling exact enforcement is a capability-gated representation flip.
- **ADR-042:** caller-owned completion closes the exact attempt's grant in the caller's atomic
  business/finalization transaction.
