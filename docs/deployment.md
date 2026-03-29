# Deployment Guide

This guide covers how to deploy Awa workers and the web UI with Docker or Kubernetes.

## Deployment Model

Awa workers are stateless. Queue state lives in Postgres. A worker process typically contains:

- one dispatcher per configured queue
- one heartbeat loop
- one maintenance loop
- one elected leader per database for rescue, promotion, cleanup, and cron evaluation

That means you scale by running more worker processes, not by adding local state.

## Process Roles

In production, treat these as separate concerns:

- application worker: your Rust or Python service embedding Awa and calling `Client::start()` or `client.start()`
- migration/admin/UI process: `awa` CLI for `migrate`, `job`, `queue`, `cron`, and `serve`
- PostgreSQL: the only required external dependency

`awa serve` is an operator UI and admin API. It is not the worker runtime.

## Connection Pool Sizing

Practical starting point, based on the current runtime internals:

- each configured queue keeps one `LISTEN` connection open for wakeups
- the elected leader holds one advisory-lock connection while it remains leader
- claim, heartbeat, completion, cleanup, and handler SQL all share the same pool

Conservative starting heuristic, not a hard requirement:

```text
max_connections >= queue_count + 4
```

Then add headroom for:

- handler code that also talks to Postgres
- producer traffic sharing the same pool
- health checks and admin queries

Examples:

- one queue, light worker-only process: `10` is a reasonable start
- three queues plus handler SQL: start around `12-20`
- combined API + worker process: either use separate pools or size for both workloads explicitly

The Python client defaults to `max_connections=10`. `awa serve` defaults to a pool of `10` connections (configurable via `--pool-max` / `AWA_POOL_MAX`). Other CLI subcommands use a single connection.

## Docker

### CLI / UI Container

The repository already includes Dockerfiles for the `awa` CLI:

- [`docker/Dockerfile`](../docker/Dockerfile)
- [`docker/Dockerfile.runtime`](../docker/Dockerfile.runtime)

Build the CLI image locally:

```bash
docker build -f docker/Dockerfile -t awa-cli .
```

Run migrations:

```bash
docker run --rm \
  -e DATABASE_URL="$DATABASE_URL" \
  awa-cli \
  --database-url "$DATABASE_URL" migrate
```

Run the web UI:

```bash
docker run --rm -p 3000:3000 \
  -e DATABASE_URL="$DATABASE_URL" \
  awa-cli \
  --database-url "$DATABASE_URL" serve --host 0.0.0.0 --port 3000
```

### Worker Containers

Your worker image is your application image. Build and run it the same way you deploy the rest of your Rust or Python service, with Awa embedded in-process.

Recommended pattern:

- one image for your worker application
- one CLI/UI image for migrations and `awa serve`
- one Postgres service

## Kubernetes

Deploy workers as a `Deployment`. Awa does not require sticky sessions, local disks, or a sidecar.

Guidelines:

- scale replicas horizontally; `SKIP LOCKED` handles concurrent claiming
- split queues across deployments when you want isolation
- remember only one replica will be the maintenance leader at a time
- set `terminationGracePeriodSeconds` above your shutdown drain timeout

Example worker deployment settings:

```yaml
spec:
  replicas: 3
  template:
    spec:
      terminationGracePeriodSeconds: 40
```

If your code calls:

```rust
client.shutdown(Duration::from_secs(30)).await;
```

or:

```python
await client.shutdown(timeout_ms=30000)
```

then `40` seconds is a reasonable Kubernetes grace period.

## Health Checks

Awa provides a runtime health API in-process:

- Rust: `client.health_check().await`
- Python: `await client.health_check()`

It does not provide worker HTTP endpoints by itself. Your application should expose `/healthz` and `/readyz` if your platform expects HTTP probes.

Awa's current `health_check().healthy` result requires:

- Postgres reachable
- all configured dispatchers alive
- heartbeat loop alive
- maintenance loop alive
- not currently shutting down

Recommended worker probes:

- liveness: same conditions as `health_check().healthy`
- readiness: not shutting down

`awa serve` is separate; its HTTP server is for the web UI/admin API, not for worker liveness.

## Rolling Deployments

For smooth rollouts:

1. Apply additive migrations first.
2. Roll out new worker code.
3. Let old pods drain with `shutdown(...)`.
4. Keep `terminationGracePeriodSeconds` slightly above that drain timeout.

Because the schema migrations are additive-only, rolling upgrades are the normal path.

## Queue Isolation Patterns

Common patterns:

- critical vs bulk queues in separate deployments
- Python workers for I/O-heavy integration queues, Rust workers for high-throughput internal queues
- a dedicated deployment for cron-heavy workloads

Use:

- Rust: `ClientBuilder::queue("name", QueueConfig { ... })`
- Python: `client.start([("name", 10)])` or dict configs in weighted mode

## Web UI Deployment

`awa serve` binds to `127.0.0.1:3000` by default. In containers, set:

```bash
awa --database-url "$DATABASE_URL" serve --host 0.0.0.0 --port 3000
```

The UI is read/write admin surface. Put it behind your normal authentication, network policy, and ingress controls.

If you expose the callback receiver endpoints for `HttpWorker`, also configure
`AWA_CALLBACK_HMAC_SECRET` (or `--callback-hmac-secret`) so `awa-ui` verifies
`X-Awa-Signature` on callback requests.

## Next

- [PostgreSQL roles and privileges](security.md)
- [Migration guide](migrations.md)
- [Configuration](configuration.md)
- [Security notes](security.md)
- [Troubleshooting](troubleshooting.md)
