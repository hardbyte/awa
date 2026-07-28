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
job representation and carried through ready, deferred, retry, callback, DLQ, and terminal moves.
The fallback is therefore evaluated once at enqueue; it is not recomputed from routing state later.

The limit and scope are database-authoritative, represented by a bounded policy catalog rather
than independently configured worker values. A policy has a stable `key_policy_id` and a
`max_in_flight` value. Multiple physical partitions of one `PartitionedQueue` may reference the
same policy, so a width change or cross-partition administration cannot accidentally create a
second concurrency namespace. Jobs without a concurrency key do not consume a grant.

Lowering a limit prevents new grants until the open count falls below the new value. Disabling or
changing a policy's identity while it has open grants requires an explicit drain/force operation;
silently changing namespace would weaken the established guarantee.

#### Append-only grant and closure evidence

The queue-storage schema gains append-only execution-grant and execution-grant-closure evidence,
partitioned with the claim ring. A grant records at least:

- policy id and full key digest;
- `(job_id, run_lease)`;
- the claim/receipt identity and claim-ring slot; and
- grant timestamp.

A closure references the exact grant and records the transition that released it. The open set is
`grants` anti-joined with `closures`. Per-child indexes on `(key_policy_id, key_digest)` make the
lookup proportional to the fixed claim-ring width rather than retained job history.

This deliberately writes one extra grant and closure fact per keyed attempt. It does not update or
delete a per-key counter. Claim-ring prune may truncate the evidence only after its existing
receipt proof and the new proof that every grant in the slot is closed.

#### Serialized claim decision

The claim transaction:

1. selects the ordinary FIFO candidate window from one
   `(queue, priority, enqueue_shard)` lane;
2. acquires transaction-scoped advisory locks for the distinct
   `(key_policy_id, key_digest)` values in deterministic order;
3. counts committed, unclosed grants for each key;
4. chooses the longest contiguous candidate prefix that stays within every limit; and
5. appends the ordinary claim evidence and execution grants in the same transaction.

The advisory lock key may be a 64-bit reduction of the full digest. A lock collision only causes
conservative serialization because capacity is still counted by the full stored digest; it cannot
allow over-admission. Deterministic multi-key lock order is part of the storage lock-order model.

If the head row is already at capacity, the claim emits no work from that lane. It does not advance
past the row or write a tombstone. This preserves the current cursor and prune contracts, at the
cost of head-of-line blocking for unrelated keys that hash into the same shard.

#### Grant lifetime

The grant opens with the committed claim and stays open while the attempt is `running` or
`waiting_external`. It closes atomically when the attempt:

- completes, fails terminally, enters the DLQ, or is cancelled;
- snoozes or becomes retryable/deferred;
- is rescued; or
- otherwise leaves the active-attempt set.

A later retry is a new attempt and acquires a new grant. Consequently, `max_in_flight = 1`
prevents execution overlap but does not promise completion order across backoff: after event A
becomes deferred, event B may run before A's retry. Holding a key barrier across retry is a
different strict-serialization feature and is not implied by this ADR.

Normal executor finalization, batched completion, callback transitions, admin cancellation, and
maintenance rescue append the closure in the same transaction as their guarded state transition.
[ADR-042](042-caller-owned-finalization-transactions.md) extends the same rule to a caller-owned
transaction: application rows, terminal evidence, and the grant closure commit together or all
roll back.

### Fairness is a separate allocator decision

Issue #340 also asks for fairness across keys. Exact admission and cross-key fair scheduling have
different proof obligations. The contiguous-prefix protocol above intentionally preserves FIFO and
cursor monotonicity, so a saturated key at a shard head can block other keys behind it.

Removing that head-of-line blocking requires a later design: for example durable per-key sub-lanes
or append-only hole/skip evidence that the allocator and prune model can prove. Tier 2 may ship
without claiming cross-key fairness. Metrics must expose key-gated claims and oldest gated age so
the trade-off is visible.

## Guarantees and non-guarantees

Tier 2 guarantees:

- no more than the configured number of committed, unclosed Awa attempts for a key across the
  fleet;
- rollback-safe admission: a failed claim transaction creates neither a claim nor a grant;
- attempt-specific release: a stale completion cannot close a successor's grant; and
- no release before durable finalization acknowledgement.

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
  safety, completion-versus-rescue, and no cursor advance over a gated head;
- `AwaStorageLockOrder` includes deterministic key locks and the grant/closure partitions;
- `AwaDeadTupleContract` classifies both tables as append/truncate and forbids per-key
  UPDATE/DELETE;
- integration races run at least two claimers on different runtimes against the same key and prove
  one winner at limit 1;
- completion, caller-owned completion, retry, snooze, callback wait/resume, cancel, DLQ, rescue,
  rotation, and prune each prove exact grant lifetime;
- E5 runs uniform and Zipf key distributions at limits 1 and N, records claim p99, oldest gated
  age, throughput, WAL/job, and storage footprint, and stays within the roadmap's 10% claim-p99
  gate; and
- the ADR-041 mixed-version rehearsal proves the disabled expand phase and the capability-gated
  enablement boundary with real N-1 artifacts.

## Alternatives considered

### Read live leases/receipts without serialization

Rejected. Two concurrent claimers can observe the same open count and both admit work.

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
- **ADR-019/023/026:** grant evidence follows the append-only claim/closure ring discipline and
  extends claim-prune proofs without adding a mutable counter family.
- **ADR-025/031:** ordering and concurrency keys are distinct. Partition and enqueue-shard routing
  co-locate ordered work; the policy id defines the fleet-exact concurrency namespace.
- **ADR-041:** enabling exact enforcement is a capability-gated representation flip.
- **ADR-042:** caller-owned completion closes the exact attempt's grant in the caller's atomic
  business/finalization transaction.
