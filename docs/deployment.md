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
- migration/admin/UI process: `awa` CLI for `migrate`, `storage`, `job`, `queue`, `cron`, and `serve`
- PostgreSQL: the only required external dependency

`awa serve` is an operator UI and admin API. It is not the worker runtime.

## Queue Storage Cutover

Fresh 0.6 installs use queue storage as the worker engine. Existing 0.5.x
clusters must use the staged storage-transition flow documented in
[migrations.md](migrations.md) and the short operator checklist in
[upgrade-0.5-to-0.6.md](upgrade-0.5-to-0.6.md).

Operationally:

- queue storage is the 0.6 worker engine
- canonical tables remain in the schema for migration, rollback boundaries,
  and compatibility SQL surfaces
- while state is `canonical` or `prepared`, 0.5.x and 0.6 pods may coexist and
  all writes/execution remain canonical
- after `enter-mixed-transition`, new writes route to queue storage while 0.6
  drain workers finish the canonical backlog
- once queue-storage rows exist, rollback to a 0.5.x-only fleet is not
  supported; finish the transition or restore from backup

For a 0.5.x cluster, do not stop canonical workers and start queue-storage
workers as a separate manual cutover. Use the storage commands so producer
routing, canonical drain, queue-storage executor readiness, and rollback
interlocks move together.

Current Rust and Python worker starts default to the correct effective engine
for the storage-transition state. Only set `queue_storage_schema` /
`ClientBuilder::queue_storage(...)` when you need to override the default
schema name or segment sizing.

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

## Primary Database Hygiene

Queue storage keeps the main ready path append-only, but Awa is still a
high-churn Postgres workload. The main operational pitfall is no longer one
giant mutable queue heap; it is long-lived readers or stale transactions that
block lease or segment prune while the maintenance leader keeps rotating
forward.

Recommended practice:

- keep analytical reads and admin transactions short on the primary
- run long-lived reporting queries against a replica when possible
- avoid leaving sessions `idle in transaction`
- monitor `pg_stat_activity` for long transactions
- monitor `pg_stat_user_tables` on the active queue-storage schema; `ready`
  segments should stay near zero dead tuples, lease segments may spike within
  the rotation window but should collapse after prune, and `attempt_state`
  should roughly track live long-running attempts
- tune autovacuum for the database if the lease tables churn heavily

The nightly MVCC benchmark exists to catch changes that make this failure mode
worse, but it is not a substitute for keeping the primary free of stale
snapshots.

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

Code-only releases can roll normally because the schema migrations are
additive-only.

Storage-engine cutovers are different from normal code-only releases. Follow
the staged `prepare -> enter-mixed-transition -> finalize` flow in
[upgrade-0.5-to-0.6.md](upgrade-0.5-to-0.6.md) so the database state, producer
routing, and runtime roles remain aligned.

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

For a UI pinned to a less-trusted network or shared with stakeholders who shouldn't trigger retries, cancels, or pauses, run with `--read-only` (or set `AWA_READ_ONLY=1`):

```bash
awa --database-url "$DATABASE_URL" serve --read-only
```

This forces read-only even when the Postgres connection is fully writable — the UI hides mutation buttons and every mutation endpoint returns 503. See [configuration.md#read-only-mode](configuration.md#read-only-mode) for the tradeoff versus pointing at a read replica.

If you expose the callback receiver endpoints for `HttpWorker`, also configure
`AWA_CALLBACK_HMAC_SECRET` (or `--callback-hmac-secret`) so `awa-ui` verifies
`X-Awa-Signature` on callback requests.

## Next

- [PostgreSQL roles and privileges](security.md)
- [Migration guide](migrations.md)
- [Configuration](configuration.md)
- [Security notes](security.md)
- [Troubleshooting](troubleshooting.md)
