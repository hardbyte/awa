# Issue 169: Storage Redesign Spike

This spike document captures the earlier exploratory work. The current concrete
design proposal and prototype results now live in
[ADR-019](/Users/brian/dev/awa/docs/adr/019-vacuum-aware-storage-redesign.md#L1).

## Question

Can Awa close the Issue 169 gap with **one** new storage model, rather than
shipping two user-facing modes?

The specific question in this follow-up was whether Awa could adopt a
`pgq`/`pgque`-style rotation scheme, let maintenance own pruning, and avoid
the earlier dual-mode recommendation.

## Short answer

Yes, one public model is credible, but only if we are willing to replace the
current `jobs_hot` row-state machine with a new storage engine.

What does **not** work:

- rotating the current mutable rows across buckets/tables
- moving payload/history into rotated tables while keeping the same mutable
  `available -> running -> done` lifecycle in one hot table

What does look credible:

- append-only rotated queue segments
- a much smaller mutable in-flight lease / claim table
- append-only done / retry / callback segment tables
- maintenance-led best-effort rotation and prune
- cached queue stats so long-running readers do not scan the prunable segments

The spike results strongly suggest that **rotation only pays off once the queue
row stops being the lifecycle state machine**.

## Why the earlier 2-mode idea existed

The previous 2-mode recommendation was a risk hedge:

- the current per-row model maps cleanly to Awa's rich job semantics
- the rotated append-only model maps cleanly to low-bloat queue storage

Given the updated constraint that a big migration is acceptable for `0.6`, I
do **not** think we need two public modes. I think we need one new engine.

## What `pgq` is really buying

The useful `pgq` property is not "partitioning" by itself. It is this:

- queue data lives in a small ring of append-only event tables
- the active table rotates on an interval
- old tables are reclaimed with `TRUNCATE`
- retry handling is separate from the main queue tables
- maintenance owns the rotation/prune loop

That is very different from Awa's current model, where the main queue row is
mutated repeatedly as it moves through claim, heartbeat, retry, callback,
completion, failure, and cleanup.

## Experiments

These were database spikes against local Postgres 17, not drop-in Awa
implementations.

Common setup:

- `pgbench` custom script, 4 clients, 4 threads, 6s run
- one `REPEATABLE READ READ ONLY` reader held for 8s
- maintenance loop rotated/pruned every 750ms
- autovacuum disabled on experimental tables to make MVCC effects obvious
- measurements use `pgstattuple`, not just `pg_stat_user_tables`

### 1. Mutable row ring

Model:

- four rotating queue tables
- jobs still mutate in-place: `available -> running -> completed`
- reader counts a `UNION ALL` view over all buckets
- maintenance rotates insert target and tries to `TRUNCATE` the oldest bucket

Result:

| Scenario | TPS | Avg latency | Prune ok | Prune blocked | Dead tuples |
|---|---:|---:|---:|---:|---:|
| Mutable ring + history reader | 2157.0 | 1.854 ms | 0 | 10 | 25,890 |

Dead tuples by bucket:

- `mutable_0`: 3,652 dead / 1,826 live
- `mutable_1`: 8,638 dead / 4,319 live
- `mutable_2`: 7,414 dead / 3,707 live
- `mutable_3`: 6,186 dead / 3,093 live

Interpretation:

- direct rotation of mutable Awa-style rows does **not** solve the problem
- the reader touching the bucket view blocked every prune attempt
- dead tuples still accumulated heavily across the ring

### 2. Append-only history ring + mutable runtime table

Model:

- append-only rotated event tables for queue/history
- separate mutable runtime table still carrying the lifecycle
- maintenance prunes the event ring

#### 2a. Reader hits runtime only

| Scenario | TPS | Avg latency | Prune ok | Prune blocked | Event dead | Runtime dead |
|---|---:|---:|---:|---:|---:|---:|
| Append ring + runtime reader | 2779.7 | 1.439 ms | 11 | 0 | 0 | 33,356 |

Interpretation:

- rotation/prune works fine when readers stay off the ring
- but the hot mutable table is still terrible
- moving payload/history out of `jobs_hot` does **not** close the gap by itself

#### 2b. Reader hits history ring

| Scenario | TPS | Avg latency | Prune ok | Prune blocked | Event dead | Runtime dead |
|---|---:|---:|---:|---:|---:|---:|
| Append ring + history reader | 2710.6 | 1.476 ms | 0 | 10 | 0 | 32,542 |

Interpretation:

- the event tables stayed append-only and clean
- but prune blocked again as soon as the reader touched the prunable ring
- maintenance can only own pruning if hot readers stop scanning those tables

### 3. Claim-ledger model

Model:

- append-only rotated event ring
- append-only rotated done ring
- tiny mutable `running` claim table only
- completion = delete claim row + append to done ring
- no `available -> running -> completed` state stored on the queue row

#### 3a. Reader hits running claims only

| Scenario | TPS | Avg latency | Prune ok | Prune blocked | Event dead | Done dead | Running dead |
|---|---:|---:|---:|---:|---:|---:|---:|
| Claim ledger + running reader | 590.9 | 6.770 ms | 11 | 0 | 0 | 0 | 250 |

Interpretation:

- this is the first spike that actually collapsed dead tuples
- the mutable hot table became tiny
- but the naive claim query was much slower than the earlier models

#### 3b. Reader hits event ring

| Scenario | TPS | Avg latency | Prune ok | Prune blocked | Running dead |
|---|---:|---:|---:|---:|---:|
| Claim ledger + history reader | 294.3 | 13.593 ms | 0 | 10 | 1,766 |

Interpretation:

- same pruning rule: if readers touch the prunable ring, maintenance blocks
- the storage shape is right, but the query strategy still matters

## What these spikes say

### 1. Rotation is necessary but not sufficient

If the hot lifecycle still lives in a mutable state table, dead tuples stay
high even when payload/history is append-only.

### 2. Maintenance can own pruning, but only under strict rules

The current maintenance leader is the right owner for this work.

However, pruning must be:

- best effort
- short lock timeout
- retry later on conflict

And the design must guarantee that long readers are **not** routinely touching
the prunable segments.

### 3. The real break point is the claim model

The dead tuple collapse only happened when I stopped treating the queue row as
the mutable lifecycle record and turned the mutable part into a small claim
ledger.

That is the important result from this spike.

### 4. A naive claim-ledger implementation is too slow

The spike used anti-joins over event/done/running views to find the next
claimable job. That cut dead tuples sharply, but it also cut throughput
sharply.

So the right conclusion is **not** "ship the spike". The conclusion is:

- the storage direction is right
- the claim algorithm is wrong

The real implementation needs a queue-local cursor/tick/range allocator, not a
global anti-join over all history.

## Revised recommendation for `0.6`

Ship **one** new storage engine, not two public modes.

### The engine should look like this

1. `queue segments`

- append-only rotated tables
- hold enqueued payload + static metadata
- reclaim with `TRUNCATE` once fully cold

2. `in-flight leases`

- small mutable table for active claims only
- heartbeat / deadline / callback waiting live here or in adjacent small
  control tables

3. `done / retry / callback segments`

- append-only segment tables, not terminal rows left in the hot queue table

4. `queue stats cache`

- maintenance-updated or event-updated counters
- admin/UI reads queue depth and state from cache tables, not by scanning the
  prunable segment ring in long transactions

5. `claim allocator metadata`

- per-queue cursor / tick / range metadata
- the dispatcher must not discover work by anti-joining across all history

### What this means for Awa specifically

The following current assumptions likely need to change:

- `jobs_hot` stops being the canonical lifecycle table
- `completed`, `failed`, and `cancelled` stop living as long-retained rows in
  the claim table
- `count(*) FROM jobs_hot` style analytics queries stop being the source of
  truth for queue visibility
- callback / retry / rescue flows need their own control-plane rows rather
  than repeated rewrites of the queue row itself

## What I would not do

I would **not** spend `0.6` on any of these as the headline answer:

- declarative partitioning of the current `jobs_hot`
- rotating mutable `jobs_hot` buckets without changing the lifecycle model
- moving only payload/history to rotated tables while keeping the same mutable
  runtime state machine

Those may help around the edges, but the spike says they do not close the gap.

## Practical `0.6` plan

1. Treat this as a breaking storage migration.
2. Keep the current maintenance leader, but extend it to own:
   - segment rotation
   - prune with low `lock_timeout`
   - cold-segment eligibility checks
   - queue stats refresh / reconciliation
3. Design the new claim allocator before writing schema migrations.
4. Benchmark the new allocator under the same idle-in-tx reader harness before
   merging the storage redesign.
5. Keep admin/history queries off the prunable ring, or pruning will stall.

## Bottom line

If the goal is to entirely close the gap, I no longer think the right answer is
"two modes".

I think the right answer is:

- one new storage engine
- rotated append-only segments
- tiny mutable claim/control tables
- maintenance-led best-effort pruning
- cached admin visibility
- a non-naive claim allocator

That is a bigger migration than the earlier dual-mode idea, but it is the
first direction from the spike that looks capable of attacking the real
problem instead of reshuffling the same row churn.
