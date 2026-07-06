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

Fresh 0.6 installs use queue storage as the worker engine. Existing 0.5.x clusters must use the staged storage-transition flow documented in [migrations.md](migrations.md) and the short operator checklist in [upgrade-0.5-to-0.6.md](upgrade-0.5-to-0.6.md).

Operationally:

- queue storage is the 0.6 worker engine
- canonical tables remain in the schema for migration, rollback boundaries, and compatibility SQL surfaces
- while state is `canonical` or `prepared`, 0.5.x and 0.6 pods may coexist and all writes/execution remain canonical
- after `enter-mixed-transition`, new writes route to queue storage while 0.6 drain workers finish the canonical backlog
- once queue-storage rows exist, rollback to a 0.5.x-only fleet is not supported; finish the transition or restore from backup

For a 0.5.x cluster, do not stop canonical workers and start queue-storage workers as a separate manual cutover. Use the storage commands so producer routing, canonical drain, queue-storage executor readiness, and rollback interlocks move together.

Current Rust and Python worker starts default to the correct effective engine for the storage-transition state. Only set `queue_storage_schema` / `ClientBuilder::queue_storage(...)` when you need to override the default schema name or segment sizing.

## Connection Pool Sizing

Practical starting point, based on the current runtime internals:

- each active queue claimer keeps one `LISTEN` connection open for wakeups (`sum(queue.claimers)`, which equals `queue_count` with default claimers)
- one cancel listener keeps a `LISTEN` connection open for admin-cancel notifications
- the elected leader holds one advisory-lock connection while it remains leader
- claim, heartbeat, completion, cleanup, and handler SQL all share the same pool

Conservative starting heuristic, not a hard requirement:

```text
max_connections >= sum(queue.claimers) + 5
```

With the default `claimers = 1`, this is the old shortcut: `max_connections >= queue_count + 5`.

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

Queue storage keeps the main ready path append-only, but Awa is still a high-churn Postgres workload. The main operational pitfall is no longer one giant mutable queue heap; it is long-lived readers or stale transactions that block lease or segment prune while the maintenance leader keeps rotating forward.

Recommended practice:

- keep analytical reads and admin transactions short on the primary
- run long-lived reporting queries against a replica when possible
- avoid leaving sessions `idle in transaction`
- monitor `pg_stat_activity` for long transactions
- monitor `pg_stat_user_tables` on the active queue-storage schema; ready, tombstone, done, and terminal-delta segments should stay near zero dead tuples, lease segments may spike within the rotation window but should collapse after prune, and `attempt_state` should roughly track live long-running attempts
- tune autovacuum for the database if the lease tables churn heavily

Queue-storage depth metrics are designed for high-cadence monitoring. The worker records `available` from lane-head cursor differences and probes queue lag from the next claimable row per lane, instead of running the exact `queue_counts` ready-row scan on every metrics tick. Use the admin API or CLI when you need exact queue counts for an operator action.

The nightly MVCC benchmark exists to catch changes that make this failure mode worse, but it is not a substitute for keeping the primary free of stale snapshots.

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

Workers ship an opt-in HTTP health listener ([#368](https://github.com/hardbyte/awa/issues/368)).
Enable it with the `AWA_HEALTH_ADDR` environment variable or the builder:

```rust
let client = Client::builder(pool)
    .queue("email", QueueConfig::default())
    .health_addr("0.0.0.0:8321".parse()?)   // or AWA_HEALTH_ADDR=0.0.0.0:8321
    .build()?;
```

Two endpoints:

- `GET /healthz` — process liveness. `200` for as long as the runtime is up,
  **including during graceful drain** — a draining worker must not be killed
  by its liveness probe.
- `GET /readyz` — readiness. `200` only when the worker can do useful work:
  Postgres reachable, schema at least the binary's expected version, every
  queue's claim loop ticking, heartbeat + maintenance services alive, and not
  shutting down. Otherwise `503` with a JSON body naming each failing check
  (`postgres_connected`, `schema_version`, `expected_schema_version`,
  `schema_compatible`, `poll_loop_alive`, `heartbeat_alive`,
  `maintenance_alive`, `shutting_down`, `leader`).

Kubernetes probes against the listener:

```yaml
spec:
  template:
    spec:
      containers:
        - name: worker
          env:
            - name: AWA_HEALTH_ADDR
              value: "0.0.0.0:8321"
          ports:
            - name: health
              containerPort: 8321
          livenessProbe:
            httpGet: { path: /healthz, port: health }
            periodSeconds: 10
          readinessProbe:
            httpGet: { path: /readyz, port: health }
            periodSeconds: 5
```

The listener is deliberately minimal (no TLS, no auth): bind it to the pod IP
or loopback and keep it off public ingress.

The in-process API remains available without the listener:

- Rust: `client.health_check().await`
- Python: `await client.health_check()`

### `awa health` — probe-less environments

For cron drivers, systemd `ExecStartPre`, or CI smoke checks, `awa health`
answers the cluster-level question from the database alone — reachable,
schema migrated for this binary, and fleet heartbeat counts:

```console
$ awa health --database-url "$DATABASE_URL"
ready:              yes
postgres:           connected
schema version:     39 (binary expects >= 39)
storage:            active (queue_storage)
live runtimes:      3 (0 unhealthy)
```

`--json` emits the same report as a machine-readable object. Exit code `0`
means ready; `1` means unreachable or unmigrated. Fleet numbers are
informational — an idle cluster with zero workers is still "ready" for
producers and migrations.

`awa serve` is separate; its HTTP server is for the web UI/admin API, not for worker liveness.

## Rolling Deployments

For smooth rollouts:

1. Apply additive migrations first.
2. Roll out new worker code.
3. Let old pods drain with `shutdown(...)`.
4. Keep `terminationGracePeriodSeconds` slightly above that drain timeout.

Code-only releases can roll normally because the schema migrations are additive-only.

Storage-engine cutovers are different from normal code-only releases. Follow the staged `prepare -> enter-mixed-transition -> finalize` flow in [upgrade-0.5-to-0.6.md](upgrade-0.5-to-0.6.md) so the database state, producer routing, and runtime roles remain aligned.

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

If you expose the callback receiver endpoints for `HttpWorker`, also configure `AWA_CALLBACK_HMAC_SECRET` (or `--callback-hmac-secret`) so `awa-ui` verifies `X-Awa-Signature` on callback requests. See [HTTP workers and callback signatures](http-callbacks.md) for the worker, function, and receiver contract.

## Next

- [Deploying on managed Postgres](deploying-on-managed-postgres.md) — Cloud SQL / AlloyDB specifics, sizing data, IAM grants, auth-proxy sidecar
- [PostgreSQL roles and privileges](security.md)
- [Migration guide](migrations.md)
- [Configuration](configuration.md)
- [Security notes](security.md)
- [Troubleshooting](troubleshooting.md)
