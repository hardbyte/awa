# awa-ui

Web UI and REST API for the [Awa](https://crates.io/crates/awa) job queue.

Provides an axum router with:
- **Dashboard** — job state counts, throughput timeseries
- **Job inspector** — search, filter, bulk retry/cancel, detail view
- **Queue management** — stats, pause/resume, drain
- **Cron schedules** — list, trigger, details

The frontend is React/TypeScript with [IntentUI](https://intentui.com) components, compiled to static assets and embedded via `rust-embed`.

Used by `awa-cli` to serve the UI via `awa serve`. See the [UI design doc](../docs/ui-design.md) for API endpoints and component details.

## License

MIT OR Apache-2.0
