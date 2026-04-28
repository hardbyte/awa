# PGMQ portable adapter

Long-horizon adapter for `pgmq` using the official SQL extension API.

- Runtime: Python 3.12 + psycopg3
- Extension API: `CREATE EXTENSION pgmq`, `pgmq.create`, `pgmq.send_batch`,
  `pgmq.read`, `pgmq.archive`
- Expected Postgres image:
  `ghcr.io/pgmq/pg18-pgmq:v1.10.0`

This adapter is opt-in rather than part of the default long-horizon matrix
because it requires a pgmq-enabled Postgres image. Example:

```bash
uv run python benchmarks/portable/long_horizon.py run \
  --systems pgmq \
  --pg-image ghcr.io/pgmq/pg18-pgmq:v1.10.0 \
  --fast
```
