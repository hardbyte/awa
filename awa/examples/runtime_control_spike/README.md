# Runtime control spike

Throwaway experiments for this question: can startup configuration and a live feature permission contain a running awa runtime, and which persistence and cancellation guarantees need a library control module?

Run from the awa repository:

```bash
cargo run -p awa --example runtime_control_spike
cargo run -p awa --example runtime_control_spike -- --interactive
```

The interactive model combines a startup ceiling, live feature permission, freshness and a generation-guarded halt. It distinguishes standalone local halt tokens from local halts associated with a durable generation: fleet resume releases associated latches, while standalone latches need explicit local resume. Feature re-enablement cannot clear a halt; expired permission denies admission. The model does not control an awa client or implement a distributed protocol.

For PostgreSQL experiments, start an isolated scratch database, then run:

```bash
docker run --rm -d --name awa-runtime-control-spike-pg -e POSTGRES_PASSWORD=spike -e POSTGRES_DB=awa_runtime_control_spike -p 127.0.0.1:15527:5432 postgres:18-alpine
cargo run -p awa --example runtime_control_spike -- --database
docker stop awa-runtime-control-spike-pg
```

The database command connects only to this fixed loopback endpoint. It installs awa's schema and creates `public.awa_spike_control`. It runs an actual awa client to inspect queue-pause and shutdown behavior, then independently exercises a proposed control row with pool starvation, reconnect without notifications, stale resume and preservation of queue pause. It also checks whether aborting a SQLx query future immediately stops its PostgreSQL query. Rerunning resets the scratch control row.

The prototype does not wire the proposed row into awa's claim or maintenance paths. It does not prove mixed-version behavior, handler preemption, fleet acknowledgements or a production stop deadline.
