# AWA Web UI

The `awa-ui` crate provides a built-in web admin UI for monitoring and managing AWA job queues. It ships as an axum router with an embedded React/TypeScript frontend — no separate Node.js runtime or build step needed at deployment time.

```bash
awa --database-url $DATABASE_URL serve --host 127.0.0.1 --port 3000
```

The admin UI is **one of several HTTP surfaces** Awa exposes. The other two are the standalone callback receiver (`awa callbacks serve` — see [HTTP callbacks](http-callbacks.md)) and user-owned callback APIs ([callback receivers](callback-receivers.md)). The deployable-role split is covered end-to-end in [Security](security.md#deployable-roles).

## Architecture

`awa-ui` exposes an `axum::Router` that can be run standalone via `awa-cli serve` or embedded into an existing axum application:

```rust
// Standalone — admin UI + admin API + callback receiver in one router
let app = awa_ui::router(pool.clone(), Duration::from_secs(5)).await?;
axum::serve(listener, app).await?;

// Embedded in your app
let app = my_app_router.nest("/awa", awa_ui::router(pool.clone(), Duration::from_secs(5)).await?);

// Callback-only — no admin UI, no admin API, no static fallback
// Use this when callbacks are externally reachable but admin must stay private.
let app = awa_ui::callback_router(
    pool.clone(),
    awa_ui::CallbackReceiverConfig::new(awa_ui::CallbackAuth::Signed(secret)),
)
.await?;
```

Give the UI its own pool built with `awa_ui::admin_pool_options` and `awa_ui::admin_connect_options`, as `awa serve` does. They set a statement timeout and a release hook. Without them, a request the client abandons leaves its query running and holds its connection, and over TLS sqlx can lose that connection for good (see [configuration](configuration.md#abandoned-requests)):

```rust
let pool = awa_ui::admin_pool_options(PgPoolOptions::new())
    .max_connections(10)
    .connect_with(awa_ui::admin_connect_options(
        database_url.parse()?,
        Some(Duration::from_secs(60)),
    ))
    .await?;
```

The frontend is a React 19 SPA built with Vite and embedded into the binary via `rust-embed`. The axum handler serves `index.html` for all non-API routes (SPA fallback), and JSON REST endpoints under `/api/`.

### Frontend Stack

- **React 19 + TypeScript** with strict mode
- **IntentUI** — component library built on React Aria Components + Tailwind CSS v4
- **TanStack Router** — type-safe, URL-driven routing
- **TanStack Query** — server state with auto-refetch and stale-while-revalidate

### API Endpoints

```
GET  /api/stats                         Job counts by state
GET  /api/stats/outcomes?minutes=60&bucket=60s&queue=&kind=
                                        { covered_since, dead_lettered_covered_since, buckets: [{ bucket,
                                        completed, failed, cancelled, dead_lettered }] }: jobs reaching
                                        each terminal outcome per bucket, by when they reached it.
                                        `minutes` is 1 to 1440 and a whole number of buckets; `bucket`
                                        is 60s, 300s, 900s or 3600s. Buckets are aligned to the Unix
                                        epoch, oldest first, the last holding the current time, and
                                        start at the earlier of the two coverage times. Completed,
                                        failed and cancelled are null before `covered_since`, dead
                                        letters before `dead_lettered_covered_since` (see below);
                                        covered empty buckets are zeros. Retries are not counted.
                                        Unknown or empty parameters are a 400.
GET  /api/stats/timeseries?minutes=60   Deprecated: jobs by creation minute and current state.
                                        Unused by the UI; use /api/stats/outcomes.

GET  /api/jobs?kind=&queue=&state=&tag=&ids=&dead_lettered=&created_at_gte=&created_at_lt=&limit=&before_id=
                                        The shared job filter (ADR-049): `state` and `tag` repeat
                                        (any of the states, all of the tags), `ids` is comma-separated.
                                        Rows omit output. Unknown or empty parameters are a 400; a 0.6
                                        server ignores unknown parameters and returns a wider list.
GET  /api/jobs/count?<same filter>      { kind: "exact" | "at_least", value }; capped at 10,000
GET  /api/jobs/:id                      Includes the job's output (ADR-048)
POST /api/jobs/:id/retry
POST /api/jobs/:id/cancel
POST /api/jobs/bulk-retry               { ids: [...] }
POST /api/jobs/bulk-cancel              { ids: [...] }

POST /api/batch-ops/preview             { op_kind, filter, spec }
POST /api/batch-ops                     { op_kind, filter, spec, all?, submitted_by? }
GET  /api/batch-ops?state=&limit=
GET  /api/batch-ops/:id
PATCH /api/batch-ops/:id                { state: "cancelling" }

GET  /api/queues
GET  /api/queues/:name
GET  /api/queues/runtime
POST /api/queues/:name/pause
POST /api/queues/:name/resume
POST /api/queues/:name/drain

GET  /api/kinds                         Job kinds with descriptor metadata

GET  /api/runtime                       Worker instances and health
GET  /api/storage                       Storage transition status
GET  /api/stats/kinds                   Distinct job kinds
GET  /api/stats/queues                  Distinct queues

GET  /api/cron
POST /api/cron/:name/trigger
POST /api/cron/:name/pause              { paused_by? }
POST /api/cron/:name/resume

GET  /api/dlq?<job filter>&limit=&before_id=&before_dlq_at=
GET  /api/dlq/:id
GET  /api/dlq/depth
POST   /api/dlq/:id/retry
DELETE /api/dlq/:id
POST   /api/dlq/retry-matching          { filter, all? }   all only with an empty filter; 202 with a retry_from_dlq batch operation
POST   /api/dlq/purge-matching          { filter, all? }   all only with an empty filter; 202 with a purge_dlq batch operation
POST   /api/dlq/bulk-retry              { kind?, queue?, tag?, all? }   the 0.6 form
POST   /api/dlq/bulk-purge              { kind?, queue?, tag?, all? }   the 0.6 form
POST   /api/dlq/bulk-move               { kind?, queue?, reason? }

POST /api/callbacks/:callback_id/complete   { payload?, output? }
POST /api/callbacks/:callback_id/fail
POST /api/callbacks/:callback_id/heartbeat

GET  /api/capabilities                  Feature flags / read-only detection
```

`GET /api/stats/outcomes` counts the terminal rows the database still holds. On queue storage those are `done_entries` and completion batches, bucketed by `finalized_at`, and dead letters by `dlq_at`. Queue-ring prune reclaims terminal rows a generation at a time, so `covered_since` is the time the generation after the oldest sealed one opened (from the queue ring's rotation ledger), or the window start if that is later: every job enqueued and finished since then is counted. A job enqueued before it and finished after it can be missing, since it belonged to a generation prune may have reclaimed. Ring prune does not touch dead letters; DLQ cleanup removes them past a retention the database does not record (30 days by default, configurable per queue). So `dead_lettered_covered_since` is the later of the window start and the oldest dead letter retained on the queue (or on any queue, without a `queue` filter): a retention shorter than the window shows there, and a queue's first dead letter within the window reads as the start of its coverage. On the canonical engine the counts come from `awa.jobs_hot`, which cleanup trims only past its retention (24 hours for completed jobs by default), both coverage times are the window start, and dead letters, which exist only on queue storage, are zero. The response is cached server-side for the dashboard cache TTL, per parameter set. A server without the endpoint answers 404.

## Pages

The sidebar groups the pages: Overview; Investigate (Jobs, Dead letters); Configure (Queues, Job kinds, Schedules); Operate (Fleet, Bulk operations). Dead letters, Fleet and Bulk operations carry a count badge for dead letters, unhealthy live instances and active operations. Page names changed in 0.7; the paths did not, so Fleet is still `/runtime`, Schedules `/cron` and Bulk operations `/batch-ops`.

### Overview (`/`)

A health strip and a ranked list of what needs attention, built from reads the other pages already make.

- **Health strip**: ready jobs in unpaused queues with the oldest one's wait (jobs held in paused queues are counted apart), retrying jobs and the queue with the most, failed jobs with the share of finished jobs that failed in the last hour and in the hour before (or, when the outcome counts do not cover both hours, the completions of the last hour), dead letters with how many were added in the last hour (when covered) and the queue with the most, stuck running jobs (past their deadline or without a heartbeat for 90 seconds, among the 100 newest running jobs), and bulk operations in flight with the latest failure. Each figure links to the jobs behind it. A figure whose read failed shows "—" and the error, never 0.
- **Fleet and deferred work** each get one line: how many live instances are healthy, which one leads and how many are stale; and how many jobs are scheduled or waiting for a callback, with the next schedule fire. Work that isn't due yet is not counted as backlog.
- **Needs attention** ranks problems, each linking to a scoped view: unhealthy instances, no live workers, stuck running jobs, failure groups (kind and error class across the 50 newest failed jobs, dead letters left out), recently failed bulk operations, a failure rate that rose sharply over the hour before (at least 20 failed jobs, 5% of finished jobs and twice the previous hour's rate), paused queues holding ready jobs, more or fewer than one maintenance leader, queues whose oldest ready job has waited a minute, dead letters per queue, queue config drift between workers, overdue and paused schedules, descriptor drift, mixed worker versions and running bulk operations. Red marks what needs an operator now, amber what needs one soon, grey what's worth knowing. With nothing to report it says "Nothing needs attention" and when it last checked; checks whose reads failed are named.
- **Queues needing attention** lists up to five queues with failures, dead letters, retries or held work, with a link to every queue.

The failure rate and the dead letters added come from `GET /api/stats/outcomes` over the last two hours in one-minute buckets; the hours end with the current minute. Each figure uses an hour only when its own series' coverage reaches back to the hour's start, since a null bucket is not a zero: the failure rate follows `covered_since`, the dead letters added follow `dead_lettered_covered_since`. Dead letters stay out of the failure rate. On queue storage under steady load, ring prune keeps terminal rows for minutes rather than hours, so the Failed tile there usually shows the completions of the last hour instead, while the dead-letter trend still shows. A server without the endpoint answers 404: the strip stays as it was and the endpoint is not read again. The read is one of the Overview's 11 observation keys, against the limit of 16.

### Jobs (`/jobs`)

Filterable, paginated job list with state filter pills (showing counts), a search bar and a bulk-action bar. The search takes `kind:`, `queue:`, `tag:`, `id:`, `created>=` and `created<` terms. The bar acts on the selected rows (Retry and Cancel, at once) or on all the jobs the filter matches (Retry, Cancel, Discard and Move to DLQ as bulk operations, plus Set priority and Move queue through the new-operation form); an action that can't apply to the filter is disabled and says why. The jobs list includes dead-lettered rows, but job actions never change them, so the "all matching" scope counts only live jobs ("All 18 matching live jobs") and agrees with the count the action's preview shows.

Kind and queue cells show declared display names, with the raw name as a tooltip. A "why" column gives a failed, retryable or cancelled job's last error, when a retryable or scheduled job runs next and when a waiting job's callback times out; its header follows the state filter (Last error, Next attempt, Runs, Callback or Detail). Priority shows only when it isn't the default or was aged. Badges mark exceptions only.

Desktop uses a table; phones get cards whose title links to the job, so Tab and Enter open it, with a labelled checkbox for selection. Cursor-based pagination uses `before_id` on the primary key.

### Job Detail (`/jobs/:id`)

Full inspection of a single job. A line under the title says where it stands: how long a running job has run, its heartbeat age ("no heartbeat yet" for a short-job claim, which has none), deadline, attempt and progress; when a retryable or scheduled job runs next; how long a waiting job has waited and when its callback times out; how long an available job has been ready; how long a completed job took and waited; when a failed or cancelled job ended and after how many attempts. Which runtime instance holds an attempt isn't recorded, so it isn't shown. Below it: state badge, queue (linked), priority, attempt count, creation time, a timeline reconstructed from timestamps and error history, syntax-highlighted JSON arguments, and retry/cancel actions. Errors show once: the latest formatted (class, message and traceback, with library frames dimmed), then a table of every failed attempt with when it failed, the gap since the one before and its outcome. Runs are split where the attempt number goes down (a manual retry), identical consecutive errors collapse into one row, and the raw JSON is a toggle away. Entries record when an attempt failed but not when it started, so only the final attempt's duration is shown. The dead-letter page uses the same card. Declared queue and job-kind descriptors render alongside the raw fields so operators can jump from a job instance to its owning team or runbook without leaving the page.

### Queues (`/queues`)

Per-queue table: ready jobs with the oldest one's wait, running, retrying, failed, dead letters, deferred (scheduled, plus jobs waiting for a callback), completions per hour and capacity, with Pause, Resume and Drain in each row's Actions menu. Badges beside the name mark exceptions only: Paused, DLQ on, a stale or drifted descriptor and config mismatch between workers. Declared-but-empty queues appear alongside active ones because the descriptor catalog is authoritative.

### Queue hub (`/queues/:name`)

Each queue's page, with Pause, Resume and Drain in its header and four tabs: Overview (counts linking into the jobs tab, failing kinds, the schedules that enqueue into it and serving instances), Jobs (the jobs list locked to the queue), Dead letters (the dead-letter list locked to the queue) and Config (the runtime configuration each worker reports). Tabs are deep links: `/queues/:name/jobs`, `/queues/:name/dlq` and `/queues/:name/config`.

### Job kinds (`/kinds`, `/kinds/:kind`)

Catalog of every declared job kind, with job and queue counts, completions per hour, owner and declared output type. Declared-but-never-enqueued kinds still appear because the descriptor catalog is authoritative. A kind's page shows its descriptor, its recent failures grouped by error, the schedules that enqueue it and the queues it runs in, with a "Retry all failed" bulk action.

### Fleet (`/runtime`, `/runtime/:instanceId`)

Cluster overview: instance count, liveness, leader status, and an attention panel for unexpected states (no live workers, a missing or extra leader, unhealthy instances, queue config mismatch). Healthy instances carry no badge; a stale or unhealthy one does, and each worker loop shows its state in words. An instance's page shows its summary, loop health and the queues it serves.

### Schedules (`/cron`, `/cron/:name`)

Table of registered schedules: the schedule in words and as a cron expression, next fire, last enqueue, target kind and queue, and badges for Paused, Retired, Queue paused and Overdue. Each row has Pause or Resume, Trigger now and Retire or Restore. "Trigger now" fires the job immediately without affecting the next scheduled run, and is allowed on a paused schedule after a confirmation, because pause stops automatic fires only. Pausing leaves `last_enqueued_at` untouched, so resume respects the schedule's existing missed-fire policy. A schedule's page shows its definition, its recent runs and its ownership.

When the target queue is itself paused, the row shows a "Queue paused" badge: fires continue to enqueue jobs into the paused queue, and those jobs dispatch only after the queue is resumed. Cron pause and queue pause are independent — one is "stop scheduling," the other is "stop dispatching."

### Bulk operations (`/batch-ops`, `/batch-ops/:id`, `/batch-ops/new`)

Operator surface for durable async mutations recorded in `awa.batch_operations`.

- **History** lists recent operations with kind, state, progress, processed, skipped and errored counts, the last error and submitted age. An operation that skipped every row says it changed nothing.
- **An operation's page** shows its filter with a link to the matching jobs, its spec, progress, last error, submitter and runner. Active operations offer Cancel, which requests cooperative cancellation at chunk boundaries.
- **New operation** (`/batch-ops/new`) chooses `set_priority`, `move_queue`, `retry`, `cancel`, `discard`, `move_to_dlq`, `retry_from_dlq` or `purge_dlq`, filters by queue, kind, tags, ids, state or created time, and previews the exact count and sample rows before submitting. Preview is advisory; execution rechecks eligibility while the maintenance leader runs the operation. The Jobs and Dead letters lists open this form with their filter for Set priority and Move queue.

### Dead Letter Queue (`/dlq`, `/dlq/:id`)

Operator surface for queue-storage `dlq_entries`.

- **List (`/dlq`)** — filterable table showing ID, kind, queue, reason, last error, attempts and age; phones get cards.
- **Bulk actions** — Retry and Purge on all the rows a filter matches, each previewed and then submitted as a `retry_from_dlq` or `purge_dlq` bulk operation, whose page then opens. Empty-filter actions require explicit `all=true` confirmation through the API.
- **Detail (`/dlq/:id`)** — single-row inspection with retry and purge actions.
- **Queue counts** — `/queues` links each queue's non-zero dead-letter count to its Dead letters tab.

The shared `/jobs/:id` detail page also carries DLQ metadata when a job has already moved into the DLQ, so direct links keep working after routing.

## Interaction Patterns

**Data freshness:** Views poll through one scheduler per tab (see [admin observation](admin-observation.md)). A pill in the header says how current the page is: "Live · updated 3s ago" (the age of the least recently loaded view), amber "Last updated 2m ago — reconnecting" while some reads fail, red "Server unreachable" when every read fails, which also dims the page, "Paused while rows are selected" while a list holds its rows still for a selection, and "A view's request was refused — refresh to retry" when the server refused a view's request with a 4xx, which is not retried until a refresh. Not-found and refused views don't count as failing. Polling stops while the tab is in the background, and the header's refresh button reads everything again.

**URL-driven state:** All filters live in URL search params (`/jobs?state=failed&q=kind:send_email`). Back/forward and bookmarks work naturally. The older form `/jobs?state=failed&kind=send_email` is folded into `q`.

**Confirming actions:** Confirmation scales with what an action can break. Pausing or resuming a queue or schedule takes one click and its toast offers Undo. An action on one job with side effects (Retry, Cancel, retrying or purging a dead letter, triggering a paused schedule, cancelling a bulk operation) opens a dialog that names the object and the risk. Acting on several selected jobs lists them in the dialog. An action on everything a filter matches previews it first: the dialog shows the exact count, the filter in words and some of the matching jobs, and submitting opens the bulk operation's page. Discarding failed jobs, purging dead letters, and cancelling or retrying 1,000 or more jobs or dead letters (or a count the server could only bound from below) also ask you to type the count, since a retry runs handlers again and repeats their side effects, and draining a queue asks for its name after showing how many jobs it cancels. A dialog's confirm button names the action ("Cancel job #918"), its dismiss button is "Keep", it stays open until the server answers and shows the server's error in place, and it says "This can't be undone" only when that is true.

**Jump to:** ⌘K (Ctrl+K elsewhere) or the header's "Jump to…" button opens a palette that goes to a job by id (a dead-lettered one opens its DLQ row), a queue, job kind or schedule by name or display name, a bulk operation by id or its first eight characters, a jobs search (`kind:send_email`, or `tenant:acme` as a tag), or any page. It loads names when it opens and doesn't poll.

**Dark mode:** Three modes — Light, Dark, System (follows OS). Persisted in localStorage. Theme toggle cycles through all three.

**Responsive:** Desktop table layouts; on phones the Jobs, Dead letters, Queues, Job kinds, Bulk operations and Schedules lists become cards whose titles are links. Nav collapses to a drawer on small screens.

**Reduced motion:** With the system's reduced-motion setting on, animations and transitions settle at once.

**Read-only:** When the server is read-only (a read replica, or `--read-only`), admin actions stay in place but disabled, each giving the reason "Read-only server — admin actions are off" on hover and to assistive technology.

## Development

```bash
cd awa-ui/frontend
npm install
npm run dev          # Vite dev server on :5173, proxies API to :3000
```

Run the backend separately:

```bash
awa --database-url $DATABASE_URL serve
```

To build the frontend for embedding:

```bash
cd awa-ui/frontend
npm run build        # Output to awa-ui/static/, picked up by rust-embed
```


### Cron ownership API

`GET /api/cron/reconciliation` returns owner plans with declarations, blockers,
conflicts, additions/updates/retirements, and grace evidence.
`POST /api/cron/owner-action` accepts an action (`adopt`, `retire`, `retire_owner`,
`restore`), its `name`/`owner_id` and adoption `expected_owner`, an `actor`, and
`apply` (defaults false). It returns `{action, schedules, applied}`. Committing
requires writable mode. Existing `/api/cron` rows add owner and retirement audit
fields; retired rows have no next-fire prediction. The UI previews operations
before an explicit Apply and distinguishes paused from retired schedules.
