# Deploying awa on managed Postgres

This page collects the operational gotchas and sizing data we learned running awa workers against Google Cloud SQL and AlloyDB in staging. Most of it applies to any managed Postgres (Amazon RDS / Aurora, Azure Database for PostgreSQL, etc.); GCP-specific advice is called out.

The reference data here was captured on a single 4-pod consumer × 4-pod producer fleet against a dedicated benchmarking database on each engine, with `enqueue_shards = 16` and the queue-storage direct COPY producer path. See [`benchmarking.md`](benchmarking.md) for methodology and the `awa-bench-driver` reports for raw numbers and EXPLAIN traces.

## Pick a Postgres version

**Use Postgres 18 if it's available.** On AlloyDB specifically we measured a hard cap at 4 vCPU on PG17 (~2 k jobs/s sustained, with `AccessShareLock` waiters queueing behind ring-rotation `ACCESS EXCLUSIVE` operations from the maintenance loop) that disappeared after an in-place upgrade to PG18 — the same 4 vCPU instance jumped to ~5 k jobs/s sustained, matching Cloud SQL PG18 at the same shape exactly.

Cloud SQL on PG17 is fine in our measurements; we did not see the same cap there. If you're on AlloyDB PG17 today and considering an upgrade for other reasons, awa throughput is one more reason to do it.

## Pick a vCPU size

The numbers below are _steady-state job completion_ and _burst enqueue_ on the queue-storage direct COPY producer path with `bench.noop` handlers (no per-job work). Real handlers reduce the consumer side proportionally; the burst-enqueue side is producer-bound and largely indifferent to handler cost.

| Engine | vCPU | PG | Sustained completed | 1 M spike enqueue | 1 M backlog drain |
| --- | --: | --: | --: | --: | --: |
| Cloud SQL | 1 | 17 | ~500 jobs/s | ~40,000 inserts/s | — |
| Cloud SQL | 4 | 18 | ~5,000 jobs/s | ~70,000 inserts/s | — |
| Cloud SQL | 16 | 18 | ~14,000 jobs/s | ~77,000 inserts/s | ~14,000 jobs/s |
| AlloyDB | 4 | 18 | ~5,200 jobs/s | ~52,000 inserts/s | ~9,000 jobs/s |

Reading the table:

- **Sustained completed** scales roughly linearly with vCPU on Cloud SQL PG18: 4 → 5 k/s, 16 → 14 k/s. AlloyDB PG18 4 vCPU matches Cloud SQL PG18 4 vCPU end-to-end at this scale.
- **Burst enqueue rate** saturates near 70–80 k/s on any 4 vCPU or larger instance — the bottleneck is the producer-side COPY pipeline and connection fan-out, not the DB CPU. Even a 1 vCPU Cloud SQL absorbed 1 M jobs at 40 k/s.
- **Backlog drain** matches the steady-state completion rate after awa-model v021 (PR #263) — heavy depth no longer collapses the dispatcher to ~10× slower than equilibrium.

Sizing rule of thumb: pick the vCPU count for your steady-state completion target, not the burst. Burst-only workloads can run on much smaller instances than their peak insertion rate would suggest.

## IAM and Cloud SQL connectivity

Migrations and custom queue-storage schema preparation need DDL-capable credentials. Ordinary workers can run with the runtime grants in [`security.md`](security.md) once `awa migrate` has materialized the default `awa` substrate, or once `awa storage prepare-queue-storage-schema` has materialized a custom queue-storage schema. If you rely on fresh-install auto-prepare from the first worker startup instead, that worker connection also needs the DDL privileges required by `prepare_schema()`.

### Cloud SQL with IAM authentication

If your runtime SA authenticates via Cloud SQL IAM (recommended over password-based auth in GCP), the SA needs `cloudsqlsuperuser` to call `CREATE SEQUENCE`, `CREATE TABLE`, and a handful of other DDL the queue-storage substrate uses. Granting the role is a one-shot manual step — Cloud SQL doesn't expose it through the IAM grant — so it typically lives next to your Terraform that creates the IAM user:

```sql
-- Run once per instance as a built-in superuser (e.g. `postgres`).
GRANT cloudsqlsuperuser TO "operator@PROJECT.iam";
```

If you skip this for the role that runs migrations / schema preparation, the setup job can fail with ownership or DDL permission errors before workers ever reach the queue-storage engine.

### AlloyDB

AlloyDB grants `alloydbsuperuser` to IAM SAs automatically — no manual GRANT step needed.

### Cloud SQL Auth Proxy / AlloyDB Auth Proxy as a native sidecar

Both Cloud SQL Auth Proxy 2.x and AlloyDB Auth Proxy 1.x work well as Kubernetes native sidecars (initContainer with `restartPolicy: Always`) — preferable to a regular container because the proxy starts before the main container and the lifecycle is tied to the pod. Example consumer-pod fragment:

```yaml
spec:
  serviceAccountName: awa-runtime
  initContainers:
    - name: auth-proxy
      image: gcr.io/cloud-sql-connectors/cloud-sql-proxy:2.13.0
      imagePullPolicy: IfNotPresent
      restartPolicy: Always # makes it a native sidecar
      args:
        - "--auto-iam-authn"
        - "--port=5432"
        - "PROJECT:REGION:INSTANCE"
      resources:
        requests: { cpu: 100m, memory: 128Mi }
        limits: { cpu: 500m, memory: 256Mi }
  containers:
    - name: worker
      image: ghcr.io/your-org/your-worker:tag
      env:
        - name: DATABASE_URL
          value: "postgres://operator%40PROJECT.iam@127.0.0.1:5432/your_db?sslmode=disable"
```

The same shape works for AlloyDB; swap the image to `gcr.io/alloydb-connectors/alloydb-auth-proxy:1.13.0` and the connection arg to `projects/PROJECT/locations/REGION/clusters/CLUSTER/instances/INSTANCE`.

## `enqueue_shards`: set this explicitly

The default `awa.queue_meta.enqueue_shards = 1` means every producer contends on a single enqueue-head row per `(queue, priority)`. With multiple concurrent producers this serialises the entire enqueue path.

**Recommendation: insert a `queue_meta` row for every high-volume queue and set `enqueue_shards` to at least 4 only when that queue can accept partitioned FIFO.** Keep `enqueue_shards = 1` for queues that require strict FIFO across the whole `(queue, priority)` lane. A 16-producer same-queue sweep in awa's own benchmark measured 1.0× / 1.60× / 2.75× / 3.69× at S=1/2/4/8 — that's about a 2.75× lift from S=1 to S=4 on a contended queue. The staging benchmark sweep at S=4/8/16/32 (a different, 4-producer setup with the producer not the bottleneck) showed no material difference between those values at 10 k offered.

```sql
INSERT INTO awa.queue_meta (queue, enqueue_shards)
VALUES ('your_queue', 4)
ON CONFLICT (queue)
DO UPDATE SET enqueue_shards = EXCLUDED.enqueue_shards;
```

Use an upsert — first-enqueue may create lane rows before any operator inserts a `queue_meta` row, so a plain `UPDATE` can quietly affect zero rows.

Going higher than 4 only helps when producer-side head-row contention is your bottleneck; raise it after measuring. Lowering it later is safe (see [`upgrade-0.5-to-0.6.md`](upgrade-0.5-to-0.6.md#lowering-enqueue_shards)) but the FIFO contract changes — see [`configuration.md`](configuration.md#sharding-the-enqueue-head-per-queue) and [ADR-025](adr/025-sharded-enqueue-heads.md).

## Producer path: use the direct COPY entry point

For any high-volume producer running against managed Postgres (rather than a Docker-localhost benchmark) prefer the queue-storage native COPY path:

- **Rust:** `QueueStorage::enqueue_params_copy(pool, &jobs)`.
- **Python:** `client.enqueue_many_copy(jobs)`.

The compat-friendly `insert_many_copy_from_pool` / `client.insert_many_copy` path routes each row through the `awa.insert_job_compat()` SQL function once per row. On a real DB the per-row function-call cost (lane head update, NOTIFY, admin metadata) sums to ~100–150 ms per row on AlloyDB through the auth-proxy — fine when the goal is "one writer, strict compatibility", catastrophic when the goal is "burst 10⁶ jobs in seconds." The direct path runs at the inserts-per-second numbers in the sizing table above.

See [`configuration.md`](configuration.md#producer-path-choice) for the full surface comparison.

## Things that broke for us in staging — worth pre-empting

These all eventually have fixes or workarounds, but each one cost hours the first time:

- **Concurrent worker startup on a fresh DB used to expose `prepare_schema` races.** The #264 fix serializes schema preparation under a transaction-scoped advisory lock and keeps the install body on that same connection. Still prefer running `awa migrate` as the explicit rollout step for the default `awa` substrate, or `awa storage prepare-queue-storage-schema` for custom queue-storage schemas, in production: it gives you one clear DDL owner, avoids first-request startup surprises, and lets workers use runtime-only grants.

- **PG18-on-AlloyDB upgrade preserves the schema and v021 indexes across the cut-over.** No re-bootstrap needed; just verify the upgrade completed cleanly with `gcloud alloydb clusters describe` showing `databaseVersion: POSTGRES_18` and run a smoke enqueue/claim.

- **Cloud SQL `must be owner of database` errors when cleaning up.** Test databases created by `partly_staging` (or whatever non-IAM user ran your migration job) can't be dropped by the IAM SA. Use the built-in `postgres` user via password from the `*-postgres-password` Secret Manager secret, or grant ownership over before dropping.

## Next

- [Configuration knobs](configuration.md) — `enqueue_shards`, claimers, retention, etc.
- [PostgreSQL roles and privileges](security.md) — the SQL grants the runtime expects.
- [Migration guide](migrations.md) — schema version table and upgrade ordering, including v021's shard-aware lane indexes.
- [Deployment guide](deployment.md) — generic deployment patterns (Docker, K8s, rolling deploys) not specific to managed Postgres.
- [Benchmarking notes](benchmarking.md) — the in-repo benchmark harness and reference numbers.
