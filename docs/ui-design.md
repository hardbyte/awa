# AWA Web UI

The `awa-ui` crate provides a built-in web dashboard for monitoring and managing AWA job queues. It ships as an axum router with an embedded React/TypeScript frontend — no separate Node.js runtime or build step needed at deployment time.

```bash
awa --database-url $DATABASE_URL serve --host 127.0.0.1 --port 3000
```

## Architecture

`awa-ui` exposes an `axum::Router` that can be run standalone via `awa-cli serve` or embedded into an existing axum application:

```rust
// Standalone
let app = awa_ui::router(pool.clone());
axum::serve(listener, app).await?;

// Embedded in your app
let app = my_app_router.nest("/awa", awa_ui::router(pool.clone()));
```

The frontend is a React 19 SPA built with Vite and embedded into the binary via `rust-embed`. The axum handler serves `index.html` for all non-API routes (SPA fallback), and JSON REST endpoints under `/api/`.

### Frontend Stack

- **React 19 + TypeScript** with strict mode
- **IntentUI** — component library built on React Aria Components + Tailwind CSS v4
- **TanStack Router** — type-safe, URL-driven routing
- **TanStack Query** — server state with auto-refetch and stale-while-revalidate

### API Endpoints

```
GET  /api/stats                         Dashboard summary (state counts)
GET  /api/stats/timeseries?minutes=60   Bucketed counts for sparklines

GET  /api/jobs?state=&kind=&queue=&tag=&limit=&before_id=
GET  /api/jobs/:id
POST /api/jobs/:id/retry
POST /api/jobs/:id/cancel
POST /api/jobs/bulk-retry               { ids: [...] }
POST /api/jobs/bulk-cancel              { ids: [...] }
POST /api/jobs/retry-failed             { kind?, queue? }
POST /api/jobs/discard-failed           { kind? }

GET  /api/queues
POST /api/queues/:name/pause
POST /api/queues/:name/resume
POST /api/queues/:name/drain

GET  /api/kinds                         Job kinds with descriptor metadata

GET  /api/runtime                       Worker instances and health
GET  /api/runtime/:instance_id

GET  /api/cron
POST /api/cron/:name/trigger

GET  /api/dlq?kind=&queue=&tag=&limit=&before_id=
GET  /api/dlq/:id
GET  /api/dlq/depth
POST   /api/dlq/:id/retry
DELETE /api/dlq/:id                     Single-row purge
POST   /api/dlq/bulk-retry              { kind?, queue?, tag?, all? }
POST   /api/dlq/bulk-purge              { kind?, queue?, tag?, all? }
POST   /api/dlq/bulk-move               { kind?, queue?, reason? }

GET  /api/capabilities                  Feature flags / read-only detection
```

## Pages

### Dashboard (`/`)

At-a-glance health check. Shows state counter cards (available, running, failed, scheduled, waiting_external, completed/hr), a runtime summary, a queue table, and recent failures. Counter cards link to the filtered jobs view. Skeleton loading states prevent false-zero flashes.

### Jobs (`/jobs`)

Filterable, paginated job list with state filter pills (showing counts), a search bar supporting `kind:`, `queue:`, and `tag:` prefix filters, and checkbox-based bulk selection for retry/cancel operations. Queue and kind cells show declared display names when a descriptor is present; raw names otherwise.

Desktop uses a table layout; mobile switches to a card layout. Both show the job kind with an inline `#ID` suffix for easy reference. Cursor-based pagination using `before_id` on the primary key.

### Job Detail (`/jobs/:id`)

Full inspection of a single job: state badge, queue (linked), priority, attempt count, creation time, a timeline reconstructed from timestamps and error history, syntax-highlighted JSON arguments, and retry/cancel actions. Declared queue and job-kind descriptors render alongside the raw fields so operators can jump from a job instance to its owning team or runbook without leaving the page.

### Queues (`/queues`)

Per-queue stats table showing depth by state, completion rate, lag, concurrency mode, capacity, rate limits, runtime health, and status. Each queue has Pause and Drain action buttons. Declared-but-empty queues appear alongside active ones because the descriptor catalog is authoritative — before descriptors shipped, listings were driven by `queue_state_counts` so idle declared queues would silently disappear.

Descriptor display name, owner, tags, and docs URL (when declared) appear inline; a stale or drifted descriptor shows a subdued status badge so operators can tell when the catalog diverged from any live runtime.

### Queue Detail (`/queues/:name`)

Reuses the Jobs list with a pre-applied queue filter, plus a summary bar showing the queue's status and counts with Pause/Resume/Drain controls. The declared descriptor (display name, description, owner, docs URL, tags, extra JSON) renders above the filter; stale/drift status is surfaced the same way as on the Queues listing.

### Job Kinds (`/kinds`)

Catalog of every declared job kind. Shows display name, owner, tags, descriptor last-seen time, and job count. Declared-but-never-enqueued kinds still appear here because the descriptor catalog is authoritative, which lets operators see the full vocabulary of jobs the fleet could emit even during a quiet period. Each row links to the jobs view pre-filtered by `kind:<kind>`. Stale/drift status is surfaced the same way as on queue surfaces.

### Runtime (`/runtime`)

Cluster overview: instance count, liveness, leader status, and an attention panel for unexpected states (e.g. missing leader). Shows per-instance health and queue runtime configuration when workers are running.

### Cron (`/cron`)

Accordion list of registered periodic job schedules. Each entry shows the cron expression, job kind, target queue, priority, next fire time (countdown), and last run. Expandable to see full details including arguments and tags. "Trigger now" fires the job immediately without affecting the next scheduled run.

### Dead Letter Queue (`/dlq`, `/dlq/:id`)

Operator surface for `awa.jobs_dlq`. See [ADR-019](adr/019-dead-letter-queue.md) for the design and [troubleshooting.md](troubleshooting.md#somethings-in-the-dlq) for diagnostic recipes.

- **List (`/dlq`)** — filterable table (ID, kind, queue, DLQ reason, attempts, age). Same `kind:`/`queue:`/`tag:` prefix-search DSL as `/jobs`. Empty-state cell spans all six columns so the "DLQ is empty" message reads cleanly.
- **Bulk actions** — filter-scoped bulk retry and bulk purge act on every row matching the current filter (no checkbox selection on this screen). Destructive actions require a confirmation dialog, and the API layer rejects empty filters unless the caller opts in with `all=true`. Bulk move-from-failed lives on the CLI (`awa dlq move`) and the JSON API (`POST /api/dlq/bulk-move`), not the DLQ list page.
- **Detail (`/dlq/:id`)** — mirrors the `/jobs/:id` layout: state badge (always `dlq`), queue link, DLQ reason, `dlq_at`, original run lease, attempts, syntax-highlighted args, timeline reconstructed from `errors` + `progress`. Retry and Purge actions are first-class — DLQ rows surface these instead of the `/jobs` Retry/Cancel pair.
- **Queue depth banner** — the `/queues` page shows a `+N DLQ` link next to any queue with non-zero DLQ depth. The link navigates to `/dlq?q=queue:<name>` so operators jump directly from the queue overview into the relevant slice.

The shared job detail surface (`GET /api/jobs/:id`) transparently falls back to `jobs_dlq` so a direct link (e.g. from an alert) keeps working after a terminal failure has moved the row into DLQ. `can_retry` and `can_cancel` report `false` on DLQ rows so the frontend routes actions through the DLQ endpoints rather than the non-applicable `/jobs` ones.

## Interaction Patterns

**Data freshness:** Polling via TanStack Query `refetchInterval` (2s for jobs, 5s for dashboard/queues/cron). Pauses when a selection is active or the browser tab is backgrounded. Manual refresh button in the nav.

**URL-driven state:** All filters live in URL search params (`/jobs?state=failed&kind=send_email`). Back/forward and bookmarks work naturally.

**Bulk operations:** Select jobs via checkboxes → action toolbar appears with count and Retry/Cancel buttons. Destructive operations (cancel, drain) require a confirmation dialog.

**Dark mode:** Three modes — Light, Dark, System (follows OS). Persisted in localStorage. Theme toggle cycles through all three.

**Responsive:** Desktop table layouts, mobile card layouts. Nav collapses to a hamburger menu on small screens.

**Read-only detection:** When connected to a read replica, write actions are automatically hidden.

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
