# awa-ui

Embedded web admin UI and REST API for the [Awa](https://crates.io/crates/awa) job queue.

`awa-ui` is shipped as an axum router and a static React/TypeScript bundle (built with [IntentUI](https://intentui.com) components and embedded via `rust-embed`). The CLI's `awa serve` mounts this router at `/` and `/api/*`; you can also embed it inside your own axum application.

## Embedding under a path

Mount the router with axum's `Router::nest`; no frontend rebuild or base-path
configuration is needed:

```rust,no_run
# async fn example(pool: sqlx::PgPool) -> Result<(), sqlx::Error> {
let ui = awa_ui::router(pool, std::time::Duration::from_secs(5)).await?;
let app = axum::Router::new().nest("/awa", ui);
# Ok(())
# }
```

The dashboard is available at `/awa`, API endpoints at `/awa/api/*`,
and assets at `/awa/assets/*`. Navigation, opening links in a new tab, and direct
requests to deep links such as `/awa/jobs/123` stay under the mount. Outer nested
routers work too, for example `/admin/awa`. The host application's `/api/*` and
`/assets/*` routes remain available for its own use. Serving the router directly
preserves the standalone behavior. A reverse proxy must preserve the prefix when forwarding
requests so that axum can infer it from its nesting.

Axum distinguishes trailing slashes at the mount root: `nest("/awa", ui)` serves
`/awa`, while `nest("/awa/", ui)` serves `/awa/`. The UI generates the matching
dashboard links automatically. To accept both root forms, use
`Router::new().nest_service("/awa", ui)` instead; assets, API and deep links work
the same way.

## Pages

The sidebar groups pages as Overview; Investigate (Jobs, Dead letters); Configure (Queues, Job kinds, Schedules); Operate (Fleet, Bulk operations).

- **Overview** (`/`) — a health strip (ready, retrying, failed, dead letters, stuck running, bulk operations in flight) and a ranked list of what needs attention, each item linking to a scoped view.
- **Jobs** (`/jobs`) — search, filter (state, kind, queue, tag, id, created time), act on selected jobs or on all matching live jobs as a previewed bulk operation, and a detail view with a state summary line, errors, timeline, progress and metadata.
- **Dead letters** (`/dlq`) — dead-letter browser with kind / queue / tag filters, single retry and purge, and retry or purge of all matching rows as a bulk operation.
- **Queues** (`/queues`, `/queues/:name`) — per-queue stats and pause / resume / drain; each queue's page has Overview, Jobs, Dead letters and Config tabs.
- **Job kinds** (`/kinds`, `/kinds/:kind`) — registered job kinds with descriptor metadata (display name, description, owner, tags, docs URL), stale / drift indicators and recent failures.
- **Schedules** (`/cron`, `/cron/:name`) — periodic schedules with next-run preview, pause / resume / trigger controls and a target-queue paused indicator.
- **Fleet** (`/runtime`) — live worker instances and their capabilities. Hosts the **storage-transition card** when the cluster is mid-migration: prepared / mixed / finalized state, drain progress, and the gates blocking finalization.
- **Bulk operations** (`/batch-ops`) — history, each operation's page, and a form for new operations with a preview.

A pill in the header says how current the page is and whether reads are failing.

## Read-only mode

When the backend is read-only — either Postgres reports `transaction_read_only = on` (e.g. a read replica) or the operator passes `--read-only` to `awa serve` — every mutation endpoint returns 503 and `/api/capabilities` reports `read_only: true`. The frontend disables admin actions in that mode and says why.

## REST surface

The router exposes JSON endpoints under `/api/*` covering jobs, queues, kinds, runtime instances, cron, DLQ, storage transition, and capabilities. See [the UI design doc](../docs/ui-design.md) for the endpoint catalogue and response shapes.

## Frontend stack

- React 18 with TanStack Router and TanStack Query
- TypeScript
- IntentUI / react-aria-components
- Vite build, output embedded via `rust-embed` so `awa serve` ships a single binary

## License

MIT OR Apache-2.0
