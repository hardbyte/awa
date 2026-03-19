# AWA Web UI — Design Document

*Draft — March 2026*

---

## 1. Goals

Provide a standalone web UI for AWA that gives operators visibility into job processing
and the ability to take administrative actions, without querying Postgres directly or
using the CLI. The PRD positions this as a v0.4 deliverable in a separate `awa-ui` crate.

**Target users:** Developers and operators running AWA in production. Not end-users.

**Design principles:**
- **Operational, not analytical.** Show what's happening now and let users act on it.
  Deep historical analytics belong in Grafana via the existing OTel metrics.
- **Fast and lightweight.** Embedded in the worker binary or run standalone. No separate
  build step, no Node.js dependency at runtime. Static assets compiled into the binary.
- **Read-mostly, write-rarely.** Most visits are "check if things are healthy." Writes
  (retry, cancel, pause) are infrequent but critical.

---

## 2. Architecture

### 2.1 Crate: `awa-ui`

```
awa-ui/
  src/
    lib.rs          # Public API: AwaUi::router() -> axum::Router
    handlers/       # Axum handlers (JSON API)
      jobs.rs
      queues.rs
      cron.rs
      stats.rs
    api.rs          # Route definitions
  frontend/         # TypeScript SPA (built at release time, embedded via rust-embed)
    src/
      routes/
      components/
      services/
      stores/
    package.json
    vite.config.ts
```

**Integration pattern:** `awa-ui` exposes an `axum::Router` that can be mounted:

```rust
// Standalone (awa-ui binary or awa-cli serve)
let app = awa_ui::router(pool.clone());
axum::serve(listener, app).await?;

// Embedded in user's existing axum app
let app = my_app_router
    .nest("/awa", awa_ui::router(pool.clone()));
```

The frontend SPA is compiled with Vite, then embedded into the Rust binary via
`rust-embed`. The axum handler serves index.html for all non-API routes (SPA fallback).

### 2.2 Frontend stack

| Choice | Rationale |
|--------|-----------|
| **React 19 + TypeScript** | Industry standard. Same stack as RiverUI. Brian knows React. |
| **TanStack Router** | File-based routing with type-safe params. URL-driven state. |
| **TanStack Query** | Server state with auto-refetch, stale-while-revalidate, mutations. |
| **Tailwind CSS v4** | Utility-first. Dark mode via `dark:` prefix. Small bundle. |
| **Headless UI** | Accessible dropdowns, dialogs, transitions without style opinions. |
| **Heroicons** | Clean icon set. Same as RiverUI. |
| **date-fns** | Lightweight date formatting. |
| **Vite** | Fast builds, HMR for development. |

No heavy charting library initially. Use CSS-based sparklines or a lightweight library
(e.g., `uplot`) if charts are needed for the dashboard.

### 2.3 API layer

JSON REST API under `/api/`. All endpoints accept/return JSON.

```
GET  /api/stats                          # Dashboard summary
GET  /api/stats/timeseries?minutes=60    # Job counts over time (for sparklines)

GET  /api/jobs?state=&kind=&queue=&limit=&before_id=  # Paginated job list
GET  /api/jobs/:id                       # Single job detail

POST /api/jobs/:id/retry                 # Retry a job
POST /api/jobs/:id/cancel                # Cancel a job
POST /api/jobs/bulk-retry                # { ids: [...] }
POST /api/jobs/bulk-cancel               # { ids: [...] }

POST /api/jobs/retry-failed              # { kind: "..." } or { queue: "..." }
POST /api/jobs/discard-failed            # { kind: "..." }

GET  /api/queues                         # Queue list with stats
POST /api/queues/:name/pause             # Pause queue
POST /api/queues/:name/resume            # Resume queue
POST /api/queues/:name/drain             # Drain queue

GET  /api/cron                           # Cron job schedules
DELETE /api/cron/:name                   # Remove a cron schedule

GET  /api/kinds                          # Distinct job kinds (for autocomplete)
```

**Backend queries:** The API handlers call existing `awa_model::admin::*` functions.
Some new queries are needed:
- `stats` summary (aggregate counts by state, total throughput)
- `timeseries` (bucketed counts by state over the last N minutes)
- `distinct kinds` (for filter autocomplete)
- Cursor-based pagination (`WHERE id < $before_id ORDER BY id DESC LIMIT $n`)

---

## 3. Navigation & Layout

### 3.1 Shell

```
+-------+--------------------------------------------------+
|       | [AWA]  Dashboard  Jobs  Queues  Cron    [theme] |
|       +--------------------------------------------------+
|       |                                                  |
|       |  (page content)                                  |
|       |                                                  |
+-------+--------------------------------------------------+
```

**Top navigation bar** (horizontal tabs, like Sidekiq). Simpler than a sidebar for a
tool with 4-5 pages. Responsive: on mobile, tabs collapse into a hamburger menu.

- **Logo/title:** "AWA" (small, left-aligned). Links to dashboard.
- **Tabs:** Dashboard, Jobs, Queues, Cron
- **Right side:** Theme toggle (light/dark/system), auto-refresh indicator

### 3.2 Pages

| Page | Path | Purpose |
|------|------|---------|
| Dashboard | `/` | Overview: health check at a glance |
| Jobs | `/jobs` | Job list with filtering by state, kind, queue |
| Job Detail | `/jobs/:id` | Full job inspection |
| Queues | `/queues` | Queue list with stats and pause/resume |
| Queue Detail | `/queues/:name` | Filtered job list for one queue |
| Cron | `/cron` | Periodic job schedules |

---

## 4. Page Designs

### 4.1 Dashboard (`/`)

The landing page answers: "Is my system healthy right now?"

```
+------------------------------------------------------------------+
|  DASHBOARD                                                        |
|                                                                   |
|  +----------+  +----------+  +----------+  +----------+          |
|  | Available|  | Running  |  | Failed   |  | Completed|          |
|  |   142    |  |    23    |  |     3    |  |  12,847  |          |
|  +----------+  +----------+  +----------+  +----------+          |
|                                                                   |
|  Queues                                                           |
|  +--------------------------------------------------------------+|
|  | Queue     | Available | Running | Failed | Lag    | Status   ||
|  |-----------|-----------|---------|--------|--------|----------||
|  | default   | 89        | 15      | 2      | 1.2s   | Active   ||
|  | email     | 42        | 8       | 1      | 0.3s   | Active   ||
|  | billing   | 11        | 0       | 0      | 45.1s  | Paused   ||
|  +--------------------------------------------------------------+|
|                                                                   |
|  Recent failures                                                  |
|  +--------------------------------------------------------------+|
|  | #4821  send_email     email    3/25  "SMTP timeout"   2m ago ||
|  | #4819  process_order  default  25/25 "DB deadlock"    5m ago ||
|  | #4812  send_email     email    5/25  "Rate limited"   8m ago ||
|  +--------------------------------------------------------------+|
+------------------------------------------------------------------+
```

**Components:**
1. **State counters** — four cards showing current counts for active states. Each card
   is clickable, linking to `/jobs?state=<state>`. The "Failed" card uses red/warning
   styling when count > 0. "Completed" shows last-hour count rather than total
   (since completed jobs are cleaned up).
2. **Queue summary table** — from `queue_stats()`. Columns: name, available, running,
   failed, lag (humanized), status (Active/Paused). Clicking a row goes to
   `/queues/:name`. Lag > 30s gets amber warning color; lag > 5m gets red.
3. **Recent failures** — last 5-10 failed jobs. Shows ID, kind, queue, attempt/max,
   first line of latest error, relative time. Clicking goes to `/jobs/:id`.

**Auto-refresh:** Every 5 seconds via TanStack Query `refetchInterval`. Pauses when
the browser tab is backgrounded (pattern from Oban Web).

### 4.2 Jobs (`/jobs`)

The core page. Shows a filterable, paginated list of jobs.

```
+------------------------------------------------------------------+
|  JOBS                                                             |
|                                                                   |
|  [State filter tabs]                                              |
|  All(12,847)  Available(142)  Running(23)  Scheduled(7)          |
|  Retryable(4)  Failed(3)  Completed(12,668)  Cancelled(0)       |
|                                                                   |
|  [Search: kind, queue, tags...]           [Refresh: 2s ⏸]       |
|                                                                   |
|  [ ] | State | Kind            | Queue   | Attempt | Time       |
|  ----|-------|-----------------|---------|---------|------------|
|  [ ] | 🟢   | send_email      | email   | 1/25    | 2s ago     |
|  [ ] | 🟢   | process_order   | default | 1/5     | 5s ago     |
|  [ ] | 🔴   | send_email      | email   | 25/25   | 1m ago     |
|  [ ] | 🟡   | generate_report | default | 3/10    | 30s ago    |
|                                                                   |
|  Selected: 2  [Retry] [Cancel]           < 1 2 3 ... 42 >       |
+------------------------------------------------------------------+
```

**State filter tabs:** Horizontal pill-style tabs showing each state with a count badge.
Clicking a tab sets `?state=<state>` in the URL. "All" shows everything.
Inspired by RiverUI's state sidebar but rendered as horizontal tabs to save horizontal
space. Counts update on each refetch.

**Search/filter bar:**
- Text input with autocomplete suggestions (like RiverUI's `kind:`, `queue:` colon syntax)
- Selected filters appear as removable badge chips
- Supported filters: `kind:<value>`, `queue:<value>`, `tag:<value>`
- Autocomplete values fetched from `/api/kinds` and queue list

**Job table columns:**
| Column | Content | Notes |
|--------|---------|-------|
| Checkbox | Multi-select | For bulk operations |
| State | Colored dot | Green=running/completed, Red=failed, Amber=retryable/scheduled, Gray=cancelled, Blue=available |
| Kind | Job type | Monospace, linked to job detail |
| Queue | Queue name | Badge style |
| Attempt | `n/max` | e.g., "3/25" |
| Time | Relative | Context-dependent: created_at for available, attempted_at for running, finalized_at for completed/failed |
| Priority | 1-4 | Only shown if non-default (!=2). Small badge. |

**Bulk actions toolbar:** Appears when checkboxes are selected. Shows count of selected
items and action buttons: [Retry] (disabled for running/completed), [Cancel] (disabled
for terminal states). Selection pauses auto-refresh (RiverUI pattern).

**Pagination:** Cursor-based (`before_id`) with page size selector (20/50/100).
Forward/back buttons. Shows "Showing 1-20 of ~142" with approximate count.

### 4.3 Job Detail (`/jobs/:id`)

Full inspection of a single job.

```
+------------------------------------------------------------------+
|  < Back to Jobs                                                   |
|                                                                   |
|  Job #4821                          [Retry] [Cancel]             |
|  send_email                                                       |
|                                                                   |
|  +--Properties-----------+  +--Timeline-------------------+      |
|  | State:    Failed      |  |  ● Created     2m ago       |      |
|  | Queue:    email       |  |  ● Available   2m ago       |      |
|  | Priority: 2           |  |  ● Running     2m ago       |      |
|  | Attempt:  3/25        |  |  ● Retryable   1m ago       |      |
|  | Created:  14:32:01    |  |     "SMTP timeout"          |      |
|  | Tags:     [urgent]    |  |  ● Running     45s ago      |      |
|  +-----------------------+  |  ● Retryable   30s ago      |      |
|                              |     "SMTP timeout"          |      |
|                              |  ● Running     15s ago      |      |
|                              |  ● Failed      5s ago       |      |
|                              |     "SMTP timeout"          |      |
|                              +-----------------------------+      |
|                                                                   |
|  Arguments                                                        |
|  +--------------------------------------------------------------+|
|  | {                                                             ||
|  |   "to": "user@example.com",                                  ||
|  |   "subject": "Order confirmation #1234",                     ||
|  |   "template": "order_confirm"                                ||
|  | }                                                             ||
|  +--------------------------------------------------------------+|
|                                                                   |
|  Metadata                                                         |
|  +--------------------------------------------------------------+|
|  | { "trace_id": "abc123", "source": "api" }                    ||
|  +--------------------------------------------------------------+|
|                                                                   |
|  Errors (3 attempts)                                              |
|  +--------------------------------------------------------------+|
|  | Attempt 3 — 5s ago                                            ||
|  | SMTP timeout connecting to mx.example.com:587                 ||
|  |                                                               ||
|  | Attempt 2 — 30s ago                                           ||
|  | SMTP timeout connecting to mx.example.com:587                 ||
|  |                                                               ||
|  | Attempt 1 — 1m ago                                            ||
|  | SMTP timeout connecting to mx.example.com:587                 ||
|  +--------------------------------------------------------------+|
+------------------------------------------------------------------+
```

**Layout:** Two-column at desktop (properties + timeline side by side), stacking to
single column on mobile.

**Sections:**
1. **Header** — Job ID, kind, action buttons (Retry/Cancel based on state)
2. **Properties panel** — Key-value pairs: state (with colored badge), queue, priority,
   attempt count, created_at, attempted_at, finalized_at, tags (as badge chips)
3. **Timeline** — Visual state progression (inspired by RiverUI's `JobTimeline`).
   Vertical line with colored dots for each state transition. Errors shown inline
   after retryable/failed states. Color-coded: green for forward progress, red for
   errors, amber for retries.
4. **Arguments** — Collapsible JSON viewer with syntax highlighting. Pretty-printed.
5. **Metadata** — Same JSON viewer. Collapsed by default if empty `{}`.
6. **Errors** — Reverse-chronological list of attempt errors from the `errors[]` array.
   Each shows attempt number, relative time, and error message. Expandable for full
   stack traces if present.

### 4.4 Queues (`/queues`)

```
+------------------------------------------------------------------+
|  QUEUES                                                           |
|                                                                   |
|  +--------------------------------------------------------------+|
|  | Queue     | Available | Running | Failed | Lag    | Actions  ||
|  |-----------|-----------|---------|--------|--------|----------||
|  | default   | 89        | 15      | 2      | 1.2s   | [⏸ Pause]||
|  | email     | 42        | 8       | 1      | 0.3s   | [⏸ Pause]||
|  | billing   | 0         | 0       | 0      | —      | [▶ Resume]|
|  +--------------------------------------------------------------+|
+------------------------------------------------------------------+
```

**Columns:** Queue name (linked to detail view), available count, running count, failed
count, lag (humanized), status/actions.

**Actions per queue:**
- Pause/Resume toggle button
- Drain button (with confirmation dialog — destructive action)

**Status indicator:** "Paused" badge on paused queues. Lag uses color coding:
- Green: < 10s
- Amber: 10s–60s
- Red: > 60s

Clicking a queue name navigates to `/queues/:name`, which shows the jobs list filtered
to that queue (reuses the Jobs page component with a queue filter pre-applied).

### 4.5 Queue Detail (`/queues/:name`)

Reuses the Jobs list component with:
- Queue name as page title
- Queue filter pre-applied (non-removable)
- Queue stats summary at top (available/running/failed/lag)
- Pause/Resume/Drain actions in the header

### 4.6 Cron (`/cron`)

```
+------------------------------------------------------------------+
|  CRON SCHEDULES                                                   |
|                                                                   |
|  +--------------------------------------------------------------+|
|  | Name            | Schedule    | Kind          | Queue  | Last ||
|  |-----------------|-------------|---------------|--------|------||
|  | daily_report    | 0 9 * * *   | gen_report    | default| 3h  ||
|  | hourly_cleanup  | 0 * * * *   | cleanup       | maint  | 12m ||
|  | weekly_digest   | 0 8 * * MON | send_digest   | email  | 4d  ||
|  +--------------------------------------------------------------+|
+------------------------------------------------------------------+
```

**Columns:** Name, cron expression (with human-readable tooltip e.g., "Every day at
9:00 AM"), timezone, kind, queue, priority, last enqueued (relative time), next fire
(computed from cron expression).

**Read-only in v1.** Cron schedules are defined in application code and synced by the
leader. The UI shows what's registered but doesn't allow editing (schedules should be
managed as code). Future: "Run now" button to manually trigger a cron job.

---

## 5. Interaction Patterns

### 5.1 Auto-refresh

- TanStack Query `refetchInterval` on all list views
- Default: 2s for jobs (matches RiverUI), 5s for dashboard/queues
- **Pause on selection:** When any checkbox is selected, refetching pauses. A small
  amber badge "Updates paused" appears. Clears when selection is emptied.
- **Pause on background:** Stop refetching when `document.hidden === true`. Resume on
  focus. (Oban Web pattern — saves server load.)
- **Manual refresh button** in the nav bar

### 5.2 URL-driven state

All filter state lives in URL search params:
- `/jobs?state=failed&kind=send_email&limit=50`
- `/queues/email`
- Back/forward buttons work naturally
- Views are bookmarkable and shareable

### 5.3 Bulk operations

1. Select jobs via checkboxes (shift+click for range select)
2. Action toolbar appears with count and available operations
3. Click action → confirmation for destructive ops (cancel, but not retry)
4. Mutation fires, list refetches, selection clears
5. Toast notification shows result ("Retried 3 jobs" / "Cancelled 5 jobs")

### 5.4 Confirmations

Destructive operations require confirmation dialogs:
- **Drain queue** — "This will cancel N available/scheduled/retryable jobs in queue
  'billing'. This cannot be undone."
- **Bulk cancel** — "Cancel N selected jobs?"
- **Discard failed** — "Permanently delete all failed jobs of kind 'send_email'?"

Non-destructive operations (retry, pause, resume) execute immediately with a toast.

### 5.5 Dark mode

Support three modes: Light, Dark, System (follows OS preference). Persisted in
localStorage. Uses Tailwind's `dark:` class strategy.

### 5.6 Responsive design

- Desktop: Full table layouts with all columns
- Tablet: Reduce less-important columns (priority, tags)
- Mobile: Card-based layout for job list, stacked sections for job detail

---

## 6. State Color System

Consistent color mapping across all views:

| State | Color | Dot | Rationale |
|-------|-------|-----|-----------|
| Available | Blue | 🔵 | Ready, waiting — informational |
| Scheduled | Slate/Gray | ⚪ | Future — not yet active |
| Running | Green | 🟢 | Active — system is working |
| Completed | Green (muted) | ✅ | Success |
| Retryable | Amber | 🟡 | Warning — will retry |
| Failed | Red | 🔴 | Error — needs attention |
| Cancelled | Gray | ⚫ | Inactive — intentional stop |

Tailwind classes:
```
available:  bg-blue-100 text-blue-800   dark:bg-blue-900 dark:text-blue-200
scheduled:  bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300
running:    bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200
completed:  bg-green-50 text-green-600  dark:bg-green-950 dark:text-green-300
retryable:  bg-amber-100 text-amber-800 dark:bg-amber-900 dark:text-amber-200
failed:     bg-red-100 text-red-800     dark:bg-red-900 dark:text-red-200
cancelled:  bg-gray-100 text-gray-600   dark:bg-gray-800 dark:text-gray-300
```

---

## 7. What's Deferred (v2+)

Features explicitly not in v1, but worth noting for future:

| Feature | Rationale for deferring |
|---------|------------------------|
| **Time-series charts** | Dashboard sparklines would be nice but require a timeseries query and charting library. Grafana covers this today. Add in v2 if operators want it without Grafana. |
| **Job search by args** | Oban Web's `args.address.city:Edinburgh` is powerful but requires JSONB path queries. Expensive without a GIN index. Defer until demand is clear. |
| **Cron editing** | Schedules should be managed as code. A "Run now" button is the only write action that makes sense. |
| **Worker/process list** | Flower-style worker management. AWA workers are stateless Kubernetes pods — `kubectl` is the right tool. Revisit if health_check data is enriched with worker metadata. |
| **Authentication** | Start unauthenticated (assume network isolation or reverse proxy auth). Add optional basic auth or JWT middleware as a configuration option. |
| **Webhook jobs** | The `waiting_external` state (webhook completion, same v0.4 milestone) will need UI representation — an additional state color and filter tab. Design when the state is implemented. |
| **Workflow visualization** | RiverUI has Dagre-based workflow diagrams. AWA doesn't have workflows (explicit non-goal in PRD). Skip entirely. |
| **Batch operations across pages** | Oban Web lets you operate on ALL matching jobs, not just the current page. v1 operates on visible selection only. |
| **Rate limit controls** | Per-queue rate limit configuration from the UI. Defer — these are code-level settings. |

---

## 8. API Additions Required in `awa-model`

The existing `admin.rs` covers most operations. New queries needed:

```rust
// New in admin.rs or a new stats.rs module:

/// Aggregate counts by state (for dashboard counters and state filter badges).
pub async fn state_counts(executor) -> Result<HashMap<JobState, i64>, AwaError>

/// Job counts bucketed by minute and state (for dashboard sparklines, v2).
pub async fn state_timeseries(executor, minutes: i32)
    -> Result<Vec<(DateTime<Utc>, JobState, i64)>, AwaError>

/// Distinct job kinds (for search autocomplete).
pub async fn distinct_kinds(executor) -> Result<Vec<String>, AwaError>

/// Distinct queues (for search autocomplete — supplements queue_stats).
pub async fn distinct_queues(executor) -> Result<Vec<String>, AwaError>

// Modify existing:
/// list_jobs needs cursor-based pagination (before_id) and tag filtering.
pub struct ListJobsFilter {
    pub state: Option<JobState>,
    pub kind: Option<String>,
    pub queue: Option<String>,
    pub tag: Option<String>,       // NEW
    pub before_id: Option<i64>,    // NEW: cursor pagination
    pub limit: Option<i64>,
}

/// Bulk retry/cancel by ID list.
pub async fn bulk_retry(executor, ids: &[i64]) -> Result<Vec<JobRow>, AwaError>
pub async fn bulk_cancel(executor, ids: &[i64]) -> Result<Vec<JobRow>, AwaError>
```

---

## 9. Open Questions

1. **Standalone binary vs embedded-only?** The PRD says "separate `awa-ui` crate." Should
   `awa-cli serve` launch the UI, or should it be a separate binary? Recommendation:
   both. `awa-ui` is a library crate exposing a router. `awa-cli` gets a `serve` subcommand.
   Users can also embed the router in their own axum app.

2. **Auth story for v1?** Start with no auth (rely on network isolation / reverse proxy).
   Add optional `AwaUi::router_with_auth(pool, config)` in v2. Basic auth is the minimum
   viable auth for an admin tool.

3. **Embed frontend in binary vs serve from filesystem?** Recommendation: embed via
   `rust-embed` for zero-config deployment. Support `--ui-dir` override for development.

4. **How much should the UI duplicate CLI functionality?** The UI should cover the 80%
   case: view jobs, retry/cancel, pause/resume queues, view cron. The CLI remains the
   power-user tool for batch operations, migrations, and scripting.

5. **Real-time updates via WebSocket or polling?** Polling via TanStack Query is simpler,
   proven (RiverUI uses it), and sufficient for a 2-second refresh interval. WebSocket
   would reduce server load at scale but adds complexity. Start with polling.
