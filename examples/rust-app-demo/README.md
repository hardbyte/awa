# Rust App Demo

Tiny Axum example showing how Awa fits into a real Rust application:

- `src/bin/app.rs` exposes a checkout endpoint
- `src/shared.rs` contains the transactional enqueue logic and shared job types
- `src/bin/workers.rs` runs the workers
- `src/bin/seed_demo.rs` populates the admin UI with realistic domain data

## Quick Start

```bash
# from the repo root
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_rust_video_demo

# Build the embedded frontend once per checkout, or whenever UI assets change.
(cd awa-ui/frontend && npm install && npm run build)

cd examples/rust-app-demo
cargo run --bin seed_demo -- --scale medium

# in another terminal, from the repo root:
cargo run -p awa-cli -- --database-url $DATABASE_URL serve
```

Then open `http://127.0.0.1:3000`.

## Optional: Run The App And Workers

Terminal 1:

```bash
cd examples/rust-app-demo
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_rust_video_demo
cargo run --bin workers
```

Terminal 2:

```bash
cd examples/rust-app-demo
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_rust_video_demo
cargo run --bin app
```

Create a new checkout:

```bash
curl -X POST http://127.0.0.1:8000/orders/checkout \
  -H 'content-type: application/json' \
  -d '{"checkout_id":"chk_rust_demo_001","customer_email":"alice@example.com","total_cents":1299}'
```

## Notes

- This demo is intentionally shaped like the Python app demo so the operational story is comparable across languages.
- Reusing the same `checkout_id` is idempotent and won't enqueue another order-confirmation email.
- The seed script deliberately adds waiting callbacks, failed inventory syncs, deferred reports, and backlog cache jobs so the UI has something worth inspecting.
