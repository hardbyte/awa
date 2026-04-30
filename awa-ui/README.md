# awa-ui

Embedded web admin UI and REST API for the [Awa](https://crates.io/crates/awa)
job queue.

`awa-ui` is shipped as an axum router and a static React/TypeScript
bundle (built with [IntentUI](https://intentui.com) components and
embedded via `rust-embed`). The CLI's `awa serve` mounts this router
at `/` and `/api/*`; you can also embed it inside your own axum
application.

## Tabs

- **Dashboard** — job state counts, throughput timeseries, runtime
  capability summary.
- **Jobs** — search, filter (state, kind, queue, has-error, tag),
  bulk retry / cancel, and a detail view with progress, attempt
  history, lifecycle hooks, and structured metadata.
- **Kinds** — registered job kinds with descriptor metadata
  (display name, description, owner, tags, docs URL) and stale /
  drift indicators.
- **Queues** — per-queue stats, descriptor metadata, pause / resume /
  drain.
- **Runtime** — live worker instances and their capabilities. Hosts
  the **storage-transition card** when the cluster is mid-migration:
  prepared / mixed / finalized state, drain progress, and the gates
  blocking finalization.
- **Cron** — registered periodic schedules with next-run preview.
- **DLQ** (`/dlq`) — Dead Letter Queue browser with kind / queue /
  tag filters, single and bulk retry, move, and purge actions. The
  DLQ tab is reachable from the Jobs tab via the row links on
  DLQ-state jobs.

## Read-only mode

When the backend is read-only — either Postgres reports
`transaction_read_only = on` (e.g. a read replica) or the operator
passes `--read-only` to `awa serve` — every mutation endpoint returns
503 and `/api/capabilities` reports `read_only: true`. The frontend
hides destructive actions in that mode.

## REST surface

The router exposes JSON endpoints under `/api/*` covering jobs,
queues, kinds, runtime instances, cron, DLQ, storage transition, and
capabilities. See [the UI design doc](../docs/ui-design.md) for the
endpoint catalogue and response shapes.

## Frontend stack

- React 18 with TanStack Router and TanStack Query
- TypeScript
- IntentUI / react-aria-components
- Vite build, output embedded via `rust-embed` so `awa serve` ships a
  single binary

## License

MIT OR Apache-2.0
