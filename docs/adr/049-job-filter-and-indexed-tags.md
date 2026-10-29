# ADR-049: One job filter, and tags as the indexed handle

## Status

Proposed. Amends [ADR-030](030-batch-operations.md): its batch-operation filter becomes the filter for job listing, counting, batch preview, batch execution and the DLQ. No migration: new queue-storage schemas install the tag indexes, and existing ones get them through an online build that the migrator runs.

## Context

Operators and applications ask Awa one question in several places: *which jobs?* The admin UI's jobs page, `awa job list`, Python's `list_jobs`, a batch operation's preview, the batch operation itself and the DLQ pages each answer it, and each answers it differently.

- **Several filter types.**
  - `ListJobsFilter`: kind, queue, state, tag and paging.
  - ADR-030's `BatchOperationFilter`: kind, queue, state, tag, ids and created-at bounds.
  - `ListDlqFilter`, which mixes paging into the filter that `purge_dlq` and `bulk_retry_from_dlq` destroy with.
  - The UI's batch-operations form, which accepts only queue, kind and ids.

  So the population an operator found on the jobs page can't be carried into a batch operation.
- **Two compilers per engine.**
  - **Listing:** builds a candidate query over every place a job's row can live and fetches 300 to 5,000 candidate ids. It then loads each job with its own seven-query round trip, filters again in Rust, and loops until the page is full.
  - **Batch operations:** keep four hand-copied SQL predicates per engine.

  ADR-030 requires preview and execution to share one compiler. The list the operator was reading when they asked for the preview shares none of it.
- **Tags are the only handle on "jobs about X", and queue storage doesn't index them.**
  - **Queue storage** keeps tags inside each row's `payload`. The listing path filters tags in Rust after loading each candidate, and the DLQ and batch paths use `payload -> 'tags' ? $tag`, which no index can serve. A rare tag therefore walks every retained job.
  - **The canonical engine** has had GIN indexes on tags since v001.

Every other way to find a job is narrower:
- **A job id** is exact, but the caller has to store it.
- **A unique key** finds only unique kinds, and only while the job's state is inside its unique-states mask.
- **Searching args** is unindexed. Indexing args would tax every job on every table its row passes through.

### Measurements

The setup: 50,000 jobs on queue storage, inserted through `insert_many` (the `insert_job_compat` path), each with two tags. One tag is `tenant:N`, shared by one job in 50. The other is `order:N`, unique to the job. All jobs completed. The run used Postgres 16, a debug client build, and `awa/tests/tag_filter_bench.rs`. A tagged job never takes the compact completion path, so its lifecycle (insert, claim, completion) writes 1,725 B of WAL.

| Lookup | Time | Rows |
| --- | ---: | ---: |
| `list_jobs` by kind, no tag, limit 100 | 7.7 s to >120 s | 100 |
| `list_jobs`, common tag, limit 100 | >120 s | — |
| `list_jobs`, rare tag, limit 20 | >120 s | — |
| GIN over payload tags, common tag, 100 rows loaded with `get_job` | 4.8 s | 100 |
| GIN over payload tags, rare tag | 49 ms | 1 |
| `(tag, job_id)` side table, common tag, 100 rows loaded with `get_job` | 4.6 s | 100 |
| `(tag, job_id)` side table, rare tag | 45 ms | 1 |

Three things explain the table:

- **The listing query degrades as a prepared statement.** One execution with literal values took 2.3 s and touched 2.5 million buffers. It binds every filter field and writes `($n IS NULL OR column = $n)`. Once a prepared statement switches to a generic plan, Postgres can't drop those terms, and the same query ran for minutes.
- **Loading dominates once lookup is indexed.** `get_job` on queue storage runs seven queries, one per place a job's row can live, so a page of 100 jobs costs about 4.5 s.
- **The two indexes cost about the same:**

  | Index | Extra WAL per tagged job (two tags) | Other costs |
  | --- | --- | --- |
  | Side table | +386 B | Its cleanup costs more WAL later |
  | GIN over payload tags | +634 B | About 200 B of index per job |

  This run indexed only ready rows that carry tags. Completed rows store a NULL payload ([Lookup paths](#lookup-paths)), so they add nothing.

## Decision

### One filter

`awa_model::filter::JobFilter` describes "which jobs" for every path in [Surfaces](#surfaces):

```rust
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct JobFilter {
    pub kind: Option<String>,
    pub queue: Option<String>,
    /// Any of these states. Empty means any state the consumer allows.
    pub states: Vec<JobState>,
    /// All of these tags.
    pub tags: Vec<String>,
    /// `Some(true)`: only dead-lettered jobs. `Some(false)`: none.
    pub dead_lettered: Option<bool>,
    pub ids: Option<Vec<i64>>,
    pub created_at_gte: Option<DateTime<Utc>>,
    pub created_at_lt: Option<DateTime<Utc>>,
}
```

- **Every field narrows.** A field that's present narrows the population, and an absent or empty field doesn't. `ids` intersects with the other fields, as ADR-030 specifies.
- **`created_at` is the original insert time.** It doesn't change across retries, DLQ moves, or priority and queue moves.
- **No `#[non_exhaustive]`.** That would also forbid `..Default::default()`, which is how every existing caller builds a filter. Instead, the documented way to construct one is `JobFilter { kind: ..., ..Default::default() }` or the builder methods. A field added later then breaks no caller that follows it.
- **The JSON shape stays ADR-030's.** A filter with one tag serializes as `"tag": "..."`, and one state as `"state": "..."`. Only a filter that needs more uses `"tags": [...]`, `"states": [...]` or `"dead_lettered"`.
- **Deserialization is strict.** An unknown field is an error, not an ignored key, so a typo can't widen a stored batch operation.
- **New fields wait for the fleet.** A runner that predates this ADR would ignore a field it doesn't know. So submitting a batch operation whose filter uses a field ADR-030 didn't define is refused while any live runtime reports an older `binary_version`. `awa migrate` refuses the same way while a live runtime is too old.

`BatchOperationFilter` becomes an alias of `JobFilter`, so batch operations accept the new fields once the fleet is upgraded.

A filter describes a population, and paging is separate from it. So a filter means the same thing wherever it's stored or sent:
- `find_jobs(pool, &JobFilter, JobPage { before_id, limit })` takes its window separately.
- A batch scan takes its id bounds separately.
- `list_dlq` takes `DlqPage { before_dlq_at, before_id }`.

`list_jobs(&ListJobsFilter)` and `list_dlq(&ListDlqFilter)` keep working, marked `#[deprecated]`, through 0.8. They're thin conversions.

Each consumer applies the filter, then its own eligibility:

| Consumer | Eligible states | Order and window |
| --- | --- | --- |
| `find_jobs`: the admin UI jobs page, `awa job list`, Python `find_jobs` | all | id descending, `before_id` keyset, `limit` clamped to 1–1000 |
| `count_jobs`: the UI's match count | all | exact up to 10,000; above that, "10,000+" |
| Batch preview and scan (`set_priority`, `move_queue`) | `available`, `scheduled` (ADR-030) | id ascending, up to the scan's upper bound |
| `list_dlq`, `purge_dlq`, `bulk_retry_from_dlq` | dead-lettered | `(dlq_at, id)` descending |

Preview and execution select from the same population, which is ADR-030's rule. The jobs page shows a different population when it isn't filtered to `available` or `scheduled`. So when it offers a batch operation on "everything matching", it shows the eligible count beside the listed count. It disables the action for filters that can match no eligible state, and in read-only mode.

### One compiler per engine

Each engine has one function that turns `(filter, eligible states, window)` into SQL. It replaces the listing loop, the batch-operation predicates and the DLQ predicates.

- **Only present predicates are written.** The compiler emits a term only for a field the filter sets. The plan therefore matches the filter, and a generic plan keeps nothing it can't use.
- **Only reachable storage is read.** On queue storage, a job's row can be in any of:
  - the ready lanes;
  - `deferred_jobs`;
  - leases and receipt claims;
  - `done_entries` and compact completions;
  - `dlq_entries`.

  The compiler includes only the branches where the filter's states, intersected with the consumer's eligible states, can occur.
- **SQL narrows; the loader decides.** The branches aren't provably disjoint while a claim is in flight. The available branch compares lane sequences against a cursor that sits outside MVCC (ADR-044). So a claimed job can appear in neither branch, or in two, until its claim and cursor both settle. The compiler therefore returns candidate ids, de-duplicated and ordered, with a bounded over-fetch.
- **Rows load in a batch.** The batched loader reads the page's candidates with `job_id = ANY($1)`, seven queries per page. It picks each job's current row by the rule `get_job` uses, which is `get_job` with one id, and re-checks the filter against that row. The compiler fetches further only while the page is short.
- **Within a page, each job appears once, in its current state.** A job in the middle of a claim may be missed or shown in its previous state. That's the same advisory sense in which ADR-030 calls the preview advisory. Keyset paging on ids never repeats a job across pages, but a job can change state between pages.
- **The count is capped and timed out.** `count_jobs` counts distinct candidate ids in SQL, stops at 10,001, and runs under a server-side `statement_timeout`. List reads get the same timeout. The UI fetches the count when the filter changes, debounced, and doesn't poll it.
- **The filter has bounds.**
  - `ids` is capped at 1,000 when a filter is submitted. Rows already stored still load.
  - A filter tag is capped at 256 bytes. Producers aren't affected.
  - `limit` is clamped in the compiler on both engines.
- **Listing without a tag has no new index.** Ready and done partitions are already indexed by `job_id`, so an id-descending page with a kind, queue or state filter walks recent rows and stops at the page. A rare kind or queue walks further, bounded by the timeout. An index on `(kind, job_id)` or `(queue, job_id)` would tax every job, so it's left until measurements call for one.
- **Canonical** compiles to one statement over `awa.jobs`, with the same rule about present predicates. The canonical engine is deprecated ([ADR-037](037-canonical-engine-deprecation.md)) and isn't optimised further.

### Tags are the indexed handle for "jobs about X"

A tag is the supported way to find the jobs about something an application cares about: `order:123`, `tenant:acme`, `import:2026-10-29`.
- **Attach it at insert,** with `InsertOpts::tags` or Python's `tags=`. Insert already allows at most 20 tags per job, and the canonical engine enforces the same limit.
- **Every tag must match.** A filter's `tags` finds every retained job carrying all of them, in any state.

**The index.** Each queue-storage table where a job's row outlives its claim gets a partial GIN index on every partition: `ready_entries` and `done_entries` (each partitioned by ring slot), `deferred_jobs` and `dlq_entries`.

```sql
CREATE INDEX {partition}_tags_idx ON {schema}.{partition}
    USING gin ((payload -> 'tags') jsonb_path_ops)
    WITH (fastupdate = off)
    WHERE (payload -> 'tags') <> '[]'::jsonb;
```

- **The predicate excludes untagged jobs.** Untagged jobs are written two ways: without a `tags` key (the Rust storage paths), or with `"tags": []` (`insert_job_compat`). Either way they fail the predicate and add no index entry.
- **`fastupdate` is off.** A partition's GIN pending list would otherwise be scanned linearly by every lookup, then flushed synchronously by whichever insert overflows it. That's a latency spike on enqueue.
- **One index per partition, no parent index.** The ring partitions are created up front, so a parent index would buy nothing. Creating one would take a `SHARE` lock on the parent, blocking writes to every partition.
- **Old and new binaries both maintain it.** Postgres maintains the index on every write. Producers on any version and any insert path add entries, and pruning's truncation clears them.

#### Lookup paths

A tag predicate is written `(payload -> 'tags') <> '[]'::jsonb AND payload -> 'tags' @> $tags`. A job is then found where its tags are physically stored:

- **Ready, scheduled, retrying, running and dead-lettered jobs** are found by index hits on `ready_entries`, `deferred_jobs` and `dlq_entries`. Leases and claims carry no payload; they're found through their ready row.
- **Completed, cancelled and failed jobs** usually have a `done_entries` row with a NULL payload, because the row would be identical to its ready row's. Their tags exist only on the ready row, in the same ring slot, which is truncated with it. So the terminal branch has two parts:
  - done rows that carry their own payload, through the done index;
  - ready-index hits joined to their done row, for done rows with a NULL payload.

  The second part can't double count, because it only takes done rows with a NULL payload.
- **The prune carry is covered.** It moves a retained failed row with `COALESCE(done.payload, ready.payload)`, so the carried row carries its own tags.
- **The DLQ, purge and bulk-retry predicates change.** They move from `?` to the compiler's `@>` form; `jsonb_path_ops` can't serve `?`.

#### Building the index

There's no migration. A plain `CREATE INDEX` blocks writes for the whole build, and Awa's migrations run in one transaction, which rules out `CONCURRENTLY`.

- **New schemas.** Queue-storage schemas installed by a binary from this ADR on create each partition's index when the partition is created.
- **Existing schemas: `awa storage build-tag-index`.** It runs as the migrator, because since v049 the runtime role need not own Awa's tables, and only an owner may create an index. It builds one index at a time:
  - **A ring partition** is indexed right after pruning empties it, while it isn't the current slot. The build takes a `SHARE` lock with `NOWAIT`, confirms the partition is empty, creates the index (instant on an empty table) and commits. A populated partition is retried on a later pass, so the whole ring is covered within one ring cycle.
  - **`--now`** builds populated partitions with `CREATE INDEX CONCURRENTLY` instead, and so does every build on `deferred_jobs` and `dlq_entries`. Its costs:
    - **It holds back vacuum.** A partial expression index isn't exempt from the vacuum horizon, so the build holds `backend_xmin` while it runs. That delays vacuum on the HOT-updated lane tables and terminal-delta folding.
    - **It blocks pruning.** `prune_oldest`'s `NOWAIT` lock on that partition fails, so prune reports `Blocked` until the build finishes.
    - **It can lose a deadlock.** A build that loses one leaves an invalid index, which the next run repairs.
- **It's resumable.** Each index has a pinned name (`{partition}_tags_idx`). An existing invalid index is rebuilt with `REINDEX INDEX CONCURRENTLY`, never skipped by `IF NOT EXISTS`. Lock attempts use `lock_timeout` and retry. `awa storage status` reports how many partitions are indexed.
- **Callers can trigger it.** It runs from the CLI, from Rust as `storage::build_tag_indexes`, and from Python as `build_tag_indexes()`. `awa migrate` prints a reminder to run it but doesn't run it. External migration runners run the CLI; a static SQL file can't enumerate each database's partitions or repair invalid indexes.
- **Lookups stay correct during the build.** Until the build finishes, the planner uses the indexes that exist and scans the partitions that have none.

### Surfaces

| Surface | Change |
| --- | --- |
| Rust | `JobFilter`, `JobPage` and `DlqPage`. `admin::find_jobs` and `admin::count_jobs`. Batch-operation functions take `JobFilter`. `list_jobs` and `list_dlq` stay, deprecated. |
| HTTP | `GET /api/jobs` accepts every `JobFilter` field as a query parameter, plus `before_id` and `limit`. Repeated `tag=` and `state=` mean all-of and any-of; `ids` is comma-separated, up to 1,000. `GET /api/jobs/count` gives the capped count. Batch-operation bodies are unchanged. An older server ignores parameters it doesn't know and returns a wider list, which `docs/ui-design.md` documents. |
| Admin UI | The search bar accepts several `tag:` terms, plus `id:`, `created>=` and `created<`, and shows the capped match count. The batch-operations form takes the same filter, and the jobs page links to it pre-filled, as in [One filter](#one-filter). |
| CLI | `awa job list` and `awa dlq list` gain `--tag` (repeatable), `--state` (repeatable), `--id`, `--created-at-gte` and `--created-at-lt`. |
| Python | `find_jobs(...)`, `count_jobs(...)` and `list_dlq(...)` take the same fields as keyword arguments or as a `filter=` dict, which is the shape batch operations take. `list_jobs` gains `tag` and `before_id`. |

These paths stay outside `JobFilter`, because each acts on an explicit selection rather than a population:
- bulk retry and cancel by selected ids;
- `bulk_move_failed_to_dlq(kind, queue)`.

## What a tag can't tell you

- **A tag finds only jobs whose producers attached it.** It can't distinguish "never tagged", "pruned", and "never inserted": each is an empty result. Retained jobs inserted before a producer started tagging aren't backfilled.
- **A tag lives as long as the job's rows.** Retention prunes both together. Status history beyond retention belongs in change data capture (v047 keeps every Awa table replicable) or in traces.
- **Tags must not carry secrets or personal data.** Tags are shown in the admin UI, written to logs, and replicated by change data capture, and now they're indexed too. Use an opaque identifier (`order:123`), not a value such as an email address.

## Deferred: deriving tags from args

A kind could declare tags derived from its args, for example `#[awa(tag = "order:{order_id}")]` in Rust or `task(tags=lambda a: [...])` in Python.

For:
- **One declaration replaces every insert site.** A forgotten `tags:` at one call site makes those jobs silently unfindable.
- **It's discoverable.** The descriptor can advertise the tag's prefix to the UI.
- **Rust can check the field exists** at compile time.

Against:
- **Coverage would be partial.** Derivation runs only where the typed kind is known. Raw inserts by kind name, the bridge, `insert_job_compat` and other services wouldn't apply it, and a lookup that silently misses some jobs is worse than one that needs a tag passed.
- **Producers in two languages must render identical strings.** Otherwise the population splits.
- **Template changes aren't retroactive.**
- **It would copy arg values into tags without the caller seeing it,** which makes the privacy rule above easy to break.
- **It's a small template language.** Every rule in it becomes a compatibility promise.

It's deferred because the index serves explicit tags today, and derivation can be added later without changing storage. If it's added, it should be plain code rather than a template: a `JobArgs::tags(&self)` method and the equivalent Python hook, with a golden test that the two languages agree.

## Non-goals

- **Searching args or metadata.** No index covers them, and a filter field without an index path can't be bounded. ADR-030's rule stands: add a field when an operator needs it and storage can answer it predictably.
- **CEL or free-form predicates.** ADR-030's reasons hold.
- **An exact census of jobs mid-transition.** Listing and counting are advisory for jobs whose claim is in flight.

## Alternatives considered

- **A `(tag, job_id)` side table.** It writes less WAL before its cleanup is counted. But:
  - pruning truncates partitions without reading job ids, so its rows would need a cleanup sweep, or a check that each job still exists on every read;
  - every insert path in every binary version would have to write it, and producers on an older version would leave gaps during an upgrade;
  - retained jobs would need a backfill.
- **A partial predicate of `payload ? 'tags'`.** It indexes every job inserted through `insert_job_compat`, which writes `"tags": []`, and so taxes untagged traffic.
- **A parent index created by a migration.** `CREATE INDEX ON ONLY` takes a `SHARE` lock on the parent, held until the migration commits, which blocks every insert. On the unpartitioned tables it does a full blocking build. The ring partitions already exist, so a parent index adds nothing.
- **A GIN index on `args`.** Every job would pay on every table, to answer a question tags answer for the jobs that opt in.
- **Keep the filters separate and just index tags.** That fixes neither the generic-plan collapse nor the per-row loads. It also leaves previews selecting a different population from the list the operator was reading.
- **External search** (Elasticsearch, a warehouse). Change data capture already supports it for history. Operators still need live state, which only Awa's tables have.

## Consequences

### Positive

- **Every listing, count, batch and DLQ path agrees** on which jobs a filter selects, and the UI can carry a search into a batch operation.
- **Finding jobs by tag becomes an indexed lookup** in every state, including dead-lettered jobs, and listing stops degrading under generic plans.
- **The tag index needs no code on any insert path** and no cleanup, and covers producers on older versions.

### Negative

- **Tagged jobs write more WAL.** With two tags per job, the lifecycle writes about a third more. Untagged jobs are unaffected.
- **Upgrading gains an online step** for existing schemas. Until it finishes, tag lookups on unindexed partitions still scan. `--now` holds back vacuum and blocks pruning of the partition it's building.
- **Some names are deprecated.** `list_jobs`, `ListJobsFilter`, `list_dlq` and `ListDlqFilter` are deprecated, and `BatchOperationFilter` gains fields. Code that builds it with an exhaustive struct literal adds `..Default::default()`.

## Rollout

- **No schema migration.** Binaries before and after this ADR read and write the same tables, and the indexes are invisible to them.
- **A binary from this ADR on uses the new compiler** whether or not the indexes exist. Without them, a tag predicate scans in SQL, which is still one statement and one batched load per page instead of a loop of per-row loads.
- **Filters with new fields wait for the fleet.** Batch operations whose filter uses a field ADR-030 didn't define are refused until no live runtime is older than this ADR.

## Validation

- **Every job is in exactly one branch.** A property test checks this across every transition, with claims racing, on both engines. The only exception is the documented in-flight window.
- **Same population, both engines.** For filters drawn from every field combination, `find_jobs`, `count_jobs`, the batch preview and the batch scan all agree, after eligibility.
- **Tag lookups across a job's life.** Every state and transition is covered:
  - a completed job with a NULL done payload;
  - a retry;
  - a DLQ move and a DLQ retry;
  - priority and queue moves;
  - a rescue;
  - pruning, with the failed-row carry;
  - jobs inserted through `insert_job_compat`, Rust COPY and the bridge.
- **Untagged jobs add no index entries,** on every insert path.
- **The build is safe to interrupt and to run alongside pruning.** Specifically:
  - an interrupted `--now` build is repaired by the next run;
  - a ring build never queues an exclusive lock that blocks a lane;
  - pruning during a build reports `Blocked` and then succeeds.
- **A mixed fleet.** A 0.6.9 producer's tagged jobs are found by tag once the build has run. A batch operation using `tags` is refused while a 0.6.9 runtime is live.
- **Benchmark.** `awa/tests/tag_filter_bench.rs`, on a realistic mix of tagged and untagged jobs, checks:
  - a 100-row page loads in one batched read;
  - a rare-tag lookup stays under 100 ms at 50,000 retained jobs;
  - with `fastupdate = off`, p99 enqueue latency for tagged jobs stays within 10% of today.

## Relationship to other ADRs

- **ADR-030.** Its filter becomes `JobFilter`, and its rule that preview and execution share a compiler extends to listing, counting and the DLQ. Its planned tag-editing operation stays compatible, because Postgres re-indexes any row it rewrites.
- **ADR-026 / ADR-048.** The compact completion path is unchanged. Tagged jobs already leave it.
- **ADR-044.** The lane cursor sits outside MVCC, which is why listing re-checks each job's current row instead of trusting the branches.
- **ADR-045.** No trigger is added.
