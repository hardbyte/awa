# Python App Demo

Tiny FastAPI example showing how Awa fits into a real application:

- `demo_app/app.py` exposes a checkout endpoint
- `demo_app/shared.py` contains the transactional enqueue logic
- `demo_app/workers.py` runs the workers
- `seed_demo.py` populates the admin UI with realistic domain data

## Quick Start

```bash
cd examples/python-app-demo
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test

uv sync
uv run awa --database-url $DATABASE_URL migrate
uv run python seed_demo.py --scale medium
uv run awa --database-url $DATABASE_URL serve
```

Then open `http://127.0.0.1:3000`.

## Optional: Run The App And Workers

Terminal 1:

```bash
cd examples/python-app-demo
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
uv run python -m demo_app.workers
```

Terminal 2:

```bash
cd examples/python-app-demo
export DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test
uv run uvicorn demo_app.app:app --reload
```

Create a new checkout:

```bash
curl -X POST http://127.0.0.1:8000/orders/checkout \
  -H 'content-type: application/json' \
  -d '{"checkout_id":"chk_demo_001","customer_email":"alice@example.com","total_cents":1299}'
```

## Notes

- This project uses local `tool.uv.sources` overrides so it works directly inside the Awa repo.
- If you copy this example into another repo, replace the local sources with published `awa-pg` and `awa-cli` dependencies.
- Reusing the same `checkout_id` is idempotent and won't enqueue another order-confirmation email.
- The seed script deliberately adds extra operational scenarios like waiting payment callbacks and failed inventory syncs so the UI has something worth inspecting.
