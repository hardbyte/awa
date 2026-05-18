-- v021: Shard-aware lane indexes on ready_entries / done_entries / leases.
--
-- Background:
--   v017 added `enqueue_shard` to ready_entries (and the claim/enqueue head
--   rows) and threaded it into `claim_ready_runtime`'s WHERE clause. The
--   narrow lane indexes on each child partition —
--   `idx_{schema}_{ready|done|leases}_<slot>_lane` on
--   `(queue, priority, lane_seq)` — predate that change and do NOT include
--   `enqueue_shard`. Under `enqueue_shards > 1` and deep backlog the planner
--   scans the lane index forward and post-filters shard, discarding roughly
--   `(shards-1)/shards` of rows per partition per claim probe.
--
--   Staging measurement (Cloud SQL PG18 4 vCPU, 16 partitions,
--   `enqueue_shards = 16`, ~4.1M-row backlog):
--     * Single claim probe (EXPLAIN ANALYZE):
--         - before: 11.4 ms, scanning all 16 partitions, `Rows Removed by
--           Filter: 2,800 – 7,300` per partition, ~30k buffer hits.
--         - after:   0.81 ms, every partition uses an Index Only Scan with
--           2 – 5 buffer hits.
--     * End-to-end drain rate against a 3.5M-row backlog:
--         - before: ~1,300 jobs/s (vs. ~10,000 jobs/s equilibrium ceiling).
--         - after:  ~8,800 – 10,600 jobs/s — back to the equilibrium rate.
--
-- Fix:
--   Create `idx_{schema}_{kind}_<slot>_lane_shard` on
--   `(queue, priority, enqueue_shard, lane_seq)` on every partition of
--   `ready_entries`, `done_entries`, and `leases` in every queue-storage
--   schema, and drop the old narrow `_lane` indexes. The old indexes are
--   a strict prefix of the new ones and so become redundant. We drop them
--   in the same migration because, even after `ANALYZE`, the planner
--   sometimes prefers the old narrower index on smaller-startup-cost
--   grounds — keeping both in place leaves the regression latent.
--
-- Safety:
--   Index DDL only — no data rewrite. `CREATE INDEX IF NOT EXISTS` is
--   idempotent; `DROP INDEX IF EXISTS` is a no-op when the old index is
--   already gone. The migration discovers partitions dynamically via
--   `pg_inherits`, so it copes with any `queue_slot_count` /
--   `lease_slot_count` the runtime has provisioned.

DO $$
DECLARE
    v_schema      TEXT;
    v_kind        TEXT;
    v_parent_oid  OID;
    v_child_oid   OID;
    v_child_name  TEXT;
    v_slot        TEXT;
    v_new_index   TEXT;
    v_old_index   TEXT;
BEGIN
    FOR v_schema IN
        SELECT schema_name
        FROM awa.runtime_storage_backends
        WHERE schema_name IS NOT NULL
    LOOP
        FOREACH v_kind IN ARRAY ARRAY['ready_entries', 'done_entries', 'leases']
        LOOP
            v_parent_oid := to_regclass(format('%I.%I', v_schema, v_kind));
            IF v_parent_oid IS NULL THEN
                CONTINUE;
            END IF;

            FOR v_child_oid, v_child_name IN
                SELECT c.oid, c.relname
                FROM pg_inherits AS i
                JOIN pg_class    AS c ON c.oid = i.inhrelid
                WHERE i.inhparent = v_parent_oid
            LOOP
                -- child relname is `<kind>_<slot>`; pull the trailing
                -- slot off so the index names line up with what
                -- `prepare_schema` produces for fresh deployments.
                v_slot := substring(v_child_name FROM length(v_kind) + 2);

                -- Index-name short kind: leases -> "leases", others drop
                -- the `_entries` suffix to match prepare_schema naming.
                v_new_index := format(
                    'idx_%I_%s_%s_lane_shard',
                    v_schema,
                    CASE v_kind
                        WHEN 'ready_entries' THEN 'ready'
                        WHEN 'done_entries'  THEN 'done'
                        ELSE 'leases'
                    END,
                    v_slot
                );
                v_old_index := format(
                    'idx_%I_%s_%s_lane',
                    v_schema,
                    CASE v_kind
                        WHEN 'ready_entries' THEN 'ready'
                        WHEN 'done_entries'  THEN 'done'
                        ELSE 'leases'
                    END,
                    v_slot
                );

                EXECUTE format(
                    'CREATE INDEX IF NOT EXISTS %I ON %I.%I (queue, priority, enqueue_shard, lane_seq)',
                    v_new_index, v_schema, v_child_name
                );

                EXECUTE format(
                    'DROP INDEX IF EXISTS %I.%I',
                    v_schema, v_old_index
                );
            END LOOP;
        END LOOP;
    END LOOP;
END
$$ LANGUAGE plpgsql;

INSERT INTO awa.schema_version (version, description)
VALUES (21, 'Shard-aware lane indexes on ready_entries/done_entries/leases')
ON CONFLICT (version) DO NOTHING;
