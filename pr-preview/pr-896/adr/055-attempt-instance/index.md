> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# ADR-055: Runtimes report the attempts they hold

## Status

Proposed. Nothing is implemented. The admin UI wants to show which runtime instance is executing a running job, link the job to that instance, and list an instance's in-flight jobs on its `/runtime/:id` page.

## Context

Two records exist today and nothing joins them.

**Runtime instances.** Each runtime generates its `instance_id` (a UUID) when the client is built and upserts one `awa.runtime_instances` row per snapshot interval, 10 s by default. The row carries health flags, version, and a `queues` JSONB array whose entries hold a per-queue `in_flight` count (`QueueRuntimeSnapshot`), but no attempt identities. An instance is shown as stale when its `last_seen_at` is older than three snapshot intervals or 30 s, whichever is longer, and long-stale rows are deleted by `cleanup_runtime_snapshots`. The snapshot is written in a transaction that first takes the cron protocol lock, and a statement trigger on `runtime_instances` takes the same cluster-wide advisory lock on every write (`cron_lock_evidence`, migration v045), so every instance's snapshot write is serialised with every other.

**Claims.** Under queue storage ([ADR-019](../019-queue-storage-redesign/index.md), [ADR-023](../023-receipt-plane-ring-partitioning/index.md)) with receipts on, the default, one `claim_ready_runtime` call writes one `lease_claim_batches` row per `(ready slot, generation, queue, priority, shard)` group, holding arrays of job ids, run leases, receipt ids, lane positions, attempts and max attempts, plus one `ready_claim_attempt_batches` row. It writes no `lease_claims` row and no `leases` row. Row-local `lease_claims` rows exist for legacy in-flight claims and materialisation paths; `leases` rows are written when an attempt materialises for a callback or other mutable lease state, or for every claim when `lease_claim_receipts` is off. All four tables are partitioned by ring slot and reclaimed by `TRUNCATE` once every claim in the slot has closure evidence. `run_lease` is the per-attempt identity ([ADR-013](../013-run-lease-and-guarded-finalization/index.md)): every finalize matches `(job_id, run_lease)`, and the runtime keys its local in-flight registry by the same pair.

**Heartbeats.** The heartbeat loop sends, every heartbeat interval (30 s by default), a heartbeat for every attempt in the in-flight registry. For a compact receipt claim it upserts `attempt_state (job_id, run_lease, heartbeat_at)`; for a materialised lease it updates `leases.heartbeat_at`. A receipt attempt therefore has no stored heartbeat until the first heartbeat pass after its claim, and admin reads report `heartbeat_at` as null for it until then. That covers every job shorter than the heartbeat interval and the first part of every longer one, not only zero-deadline jobs.

**What the claim knows.** The dispatcher calls `claim_runtime_batch_with_aging_for_instance` with a `claimer_owner_id`, which is the runtime's `instance_id` for the first dispatcher of a queue and a random UUID for each further one (bounded-claimer lease tokens in `queue_claimer_leases`). The runtime's `instance_id` is in scope at the call, but `claim_ready_runtime(p_queue, p_max_batch, p_deadline_secs, p_aging_secs)` and the per-slot `claim_ready_slot_N` functions take no instance argument. Recording the instance in the claim means a new argument on those functions.

**Constraints on the claim path.** [ADR-053](../053-coordination-off-the-commit-path/index.md) requires that a job transition not wait on coordination work and that coordination that changes nothing writes nothing. [ADR-054](../054-claim-target-bound/index.md) changed the claim bodies without changing signatures, owners or grants precisely so that N-1 binaries keep calling the same functions, and notes that an older `prepare_schema` can reinstall an older claim body. [ADR-041](../041-rolling-upgrade-policy/index.md) requires every release's schema change to work with N-1 binaries on the N schema, N binaries on the N-1 schema, and mixed fleets.

**What the admin reads have.** Admin reads of running queue-storage jobs already collect the open claims, slot by slot, as `(job_id, run_lease, queue)` (`queue_storage_open_claims`, `open_receipt_running_claims_sql`). `JobRow` is `#[non_exhaustive]` and the UI receives it flattened into `JobResponse`, which already carries computed fields that are not stored in the database. The UI's `/runtime/:id` page has no endpoint of its own: it fetches the fleet overview from `/api/runtime` and picks the instance. The queue overview lists the instances serving a queue, which is the only job-to-instance link the UI has today.

## Decision (proposed)

Each runtime publishes the identities of the attempts it holds, off every job path, and admin reads join them to running jobs by `(job_id, run_lease)`. Claim, heartbeat and finalize rows are not changed.

### Storage

A new table in the `awa` schema, one row per runtime instance:

```sql
CREATE TABLE awa.runtime_attempts (
    instance_id     UUID PRIMARY KEY,
    published_at    TIMESTAMPTZ NOT NULL,
    in_flight_total INTEGER NOT NULL,
    job_ids         BIGINT[] NOT NULL,
    run_leases      BIGINT[] NOT NULL,
    CHECK (cardinality(job_ids) = cardinality(run_leases))
);
```

It has no foreign key to `runtime_instances` and no trigger, so writing it takes no lock on `runtime_instances` and not the cron protocol lock. It is not a hot-path table under ADR-053: no job transition reads or writes it.

### Publishing

- The runtime reporter reads the in-flight registry (`InFlightRegistry::claim_refs`, the same set the heartbeat loop sends) on the snapshot cadence and writes its row in its own transaction, separate from the runtime snapshot.
- It writes only when the set has changed since its last write, or when its last write is older than half the instance's stale cutoff, so the readers' freshness check on `published_at` holds and an idle or steady instance writes rarely. The row is written only by its own instance, so the upsert meets no other writer.
- It publishes at most 1,000 attempts, oldest claim first, and records the full count in `in_flight_total`. In-flight attempts per instance are bounded by its worker capacity, so the cap is reached only by very large instances; at the cap the row is about 16 KB of array data and is TOASTed.
- On clean shutdown the runtime deletes its row. `cleanup_runtime_snapshots` deletes `runtime_attempts` rows whose instance row it deletes, and rows whose `published_at` is older than the same age.
- An attempt leaves the set when the registry drops it, which is after its finalize is acknowledged, so the set includes attempts whose handler has returned and whose completion is still in flight.

### Reading

An attribution is the instance whose row lists `(job_id, run_lease)` and whose `published_at` is within that instance's stale cutoff. Keying by `run_lease` means a stale row can never attribute a newer attempt of the same job: a rescued and reclaimed job has a new `run_lease`. Admin reads show an attribution only for a job whose current stored state is `running` or `waiting_external` with that `run_lease`, so a published attempt that has since completed, failed or been rescued is not shown on the job.

The table holds one row per live instance, so lookups scan it without an index: a job page reads every row whose `job_ids` contains the job; a job list does the same for the running ids on the page.

### Admin API

- `JobResponse` gains `instance_id: Option<Uuid>` and `instance_reported_at: Option<DateTime<Utc>>`, filled for running and `waiting_external` jobs on the job detail and job list endpoints. The field goes on the API response rather than on `JobRow` because it is an observation with a freshness, not stored job state, and `JobRow` is also what handlers and the Python bindings receive. The JSON name is `instance_id`, as the UI plan uses. A model helper, `admin::attempt_instances(pool, &[(job_id, run_lease)])`, serves both endpoints and Rust callers of the admin API.
- A new `GET /api/runtime/{instance_id}` returns the `RuntimeInstance` and an `in_flight` object: `reported_at`, `total`, `truncated`, and the published attempts, each with `job_id`, `run_lease`, `queue` and `kind` from storage and `current: bool`, false when storage no longer has that attempt running (it was rescued, cancelled or finalised while the runtime still held it). It returns 404 for an unknown instance and an `in_flight` of `null` for an instance that has never published.

### UI

- Job detail, for a running or waiting job: an "Instance" row linking to `/runtime/:id` with the instance label and how long ago it reported. With no attribution it says why: "Not reported yet" when the attempt started less than a snapshot interval ago; "Not reported by this version" when every live instance serving the queue predates reporting; otherwise "Unknown".
- Runtime instance page: switch from the fleet overview to `GET /api/runtime/{instance_id}` and add an "In-flight jobs" card listing the attempts with job links, queue, kind and run lease; attempts with `current: false` are marked as held after rescue or finalisation, which is a useful sign of a stuck handler; "Showing 1,000 of N" when truncated; and an empty state naming the version for an instance that does not report.
- The job list may show the short instance id for running jobs; it is not required by this decision.

### Migration and mixed fleets

One additive migration (v065 or the next free number) creates `awa.runtime_attempts`. It takes no lock on any existing table, scans nothing, and needs no runtime version floor. It does not touch the claim, heartbeat or finalize functions, their signatures, owners or grants.

- **N-1 runtimes on the N schema** do not know the table and do not publish. Their jobs show no instance and their runtime pages show the "does not report" state, decided from the absence of a row and the instance's `version`. Nothing else changes for them.
- **N runtimes on the N-1 schema**, while the migration is pending, find the table missing (`to_regclass`) and skip publishing; they check again on each snapshot, so they start publishing once the migration commits.
- **Mixed fleets** show attributions for jobs held by N runtimes and none for jobs held by N-1 runtimes. No reader treats a missing attribution as evidence of anything.
- **N admin on the N-1 schema** treats the missing table as no data. **Rolling back** binaries leaves the table in place and unused.

## Consequences

- No cost on the claim, heartbeat, finalize, rescue or prune paths, and no change to any claim-ring or lease-ring table, so ADR-053 and ADR-054 are untouched and no TLA+ model changes.
- Each instance writes one small row at most once per snapshot interval, outside the cron protocol lock. At 100 instances that is at most 10 writes per second, each a few kilobytes.
- Attribution lags the claim by up to one snapshot interval (10 s by default) plus the admin cache. Jobs shorter than that are usually never attributed, which leaves the receipt short-job case that also lacks a heartbeat unattributed. The UI's purpose, finding where a long-running or stuck job runs, is served.
- After an instance dies its row ages out with its snapshot, so an attribution disappears within the stale cutoff and is not kept for post-mortems; who held a rescued attempt is not recorded durably.
- Instances with more than 1,000 attempts in flight are listed partially; their remaining jobs show no instance.
- Canonical-storage runtimes publish too, since the registry is storage-independent, but canonical storage is deprecated ([ADR-037](../037-canonical-engine-deprecation/index.md)) and the admin join is only required for queue storage.

## Alternatives considered

### Instance id on the claim rows

Add `instance_id UUID` to `lease_claim_batches`, `lease_claims` and `leases`, written by the claim. Attribution is then exact, immediate, covers short jobs, and survives the instance's death until the claim slot is pruned.

- Row width is not the problem: the compact path writes one batch row per claim call, so 16 bytes is a small fraction of a batch row that already carries six arrays. On row-local `lease_claims` and `leases` rows it adds roughly 15%. Prune and rescue would ignore the column, and the slot's `TRUNCATE` reclaims it. Reading by job uses the existing claim-batch `job_ids` GIN path; listing by instance filters the open-claim read the admin already does.
- The claim functions need a new argument. The old `claim_ready_runtime` and `claim_ready_slot_N` signatures must stay for N-1 binaries, so every claim body exists twice until a contract release, ADR-054's "no signature changes" property is lost for this change, and an older `prepare_schema` reinstalling older bodies must not drop the new overload. With [ADR-043](../043-postgresql-capability-functions/index.md) it is also a new capability entry point.
- `ALTER TABLE ... ADD COLUMN` on the partitioned parents is catalog-only but takes `ACCESS EXCLUSIVE` on the parent and every partition, which every claim conflicts with; it needs a bounded `lock_timeout` and retries on a live fleet.
- The dispatcher passes a lease token, not the instance id, for its extra claimers, so the call has to pass the runtime's `instance_id` separately.

This is the path to take if exact attribution of short jobs, or a durable record of which instance held a rescued attempt, becomes a requirement. It does not belong in an observability feature.

### A small numeric instance slot on the claim rows

The same as above with a `SMALLINT` mapped to instances through a registry. It saves 14 bytes per row, which matters little on batch rows, and adds an allocator whose slots cannot be reused until every claim naming the slot has been pruned, coupling the instance registry to claim-ring prune. `queue_claimer_leases.claimer_slot` is not usable: it is per queue, held only while claiming, and owned by a lease token. It keeps every cost of the claim-row option.

### Instance id written by heartbeats

Add `instance_id` to `attempt_state` and `leases` and set it in each heartbeat write, which already rewrites the row. It stays off the claim path and is stored beside `heartbeat_at`, which admin reads already join. But every attempt is unattributed until its first heartbeat, up to 30 s by default rather than 10 s; it needs columns on a hot mutable table and on the partitioned lease table; and it changes four heartbeat paths (compact claims, legacy leases, progress, callback heartbeats). It is more change for a staler answer.

### In-flight identities inside the runtime snapshot

Put the attempt list in the existing `runtime_instances` row, for instance in the `queues` JSON. It needs no new table, but it lengthens a write that holds the cluster-wide cron protocol lock and rewrites a wider row on every snapshot whether or not the set changed. A separate table keeps that write as it is.

### One row per in-flight attempt

A table keyed by `(instance_id, job_id, run_lease)`, inserted at start and deleted at finish. It gives an indexable, exact set, but it is per-attempt insert and delete churn, the MVCC source ADR-023 removed when it retired `open_receipt_claims`, and it puts a write on every attempt's start and finish.

### Status quo

The UI shows the instances serving a job's queue, as the queue overview does. It costs nothing and answers "where could this run" but not "where is it running", and the runtime page cannot list jobs.

## Open questions

- Should the table record the queue-storage schema? A runtime and an admin server each use one schema, and job ids are not unique across schemas; one schema per deployment is assumed here.
- Is 1,000 the right cap, and should it be configurable?
- Should the Python admin bindings expose the attribution?
- Should the job list filter by instance (`?instance=`)? It would read `runtime_attempts` first and fetch those jobs, but it is not needed for the runtime page.
