-- #169 B1: drop the `idx_<schema>_leases_<slot>_state_hb` index from
-- every existing AWA queue-storage substrate. In receipts mode (the
-- only supported shape for the default `awa` schema)
-- `leases.heartbeat_at` is never written — heartbeats land in
-- `attempt_state.heartbeat_at` and the rescue path reads from there.
-- The `(state, heartbeat_at)` index then becomes pure overhead: every
-- non-HOT UPDATE on `leases` (state transition, callback register,
-- callback timeout, deadline write) still pays 2 dead index entries
-- per write per partition.
--
-- Legacy non-receipts custom schemas (those that set
-- `lease_claim_receipts=FALSE` when calling
-- `awa.install_queue_storage_substrate`) fall back to a seq-scan of
-- `WHERE state='running'` rows during stale-heartbeat rescue. That
-- query runs at 30s cadence with `LIMIT 500` against the live-running
-- row count (bounded by worker concurrency, typically thousands not
-- millions), so the seq scan is cheap enough that dropping the index
-- is the right global trade-off.
--
-- Discovery uses the same AWA sentinel as v024
-- (`apply_receipt_plane_fillfactor`): a schema is an AWA substrate
-- iff `<schema>.claim_ready_runtime(text, bigint, double precision,
-- double precision)` exists. This stays inside AWA's ownership
-- boundary; unrelated app-owned schemas with `leases` tables are not
-- touched.
--
-- v023's install helper has been updated to not create the index on
-- fresh installs, so this migration plus future `prepare_schema`
-- calls keep the database state coherent.

DO $$
DECLARE
    v_schema TEXT;
    v_partition_name TEXT;
    v_slot TEXT;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            n.nspname
        )) IS NOT NULL
    LOOP
        IF to_regclass(format('%I.leases', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

        FOR v_partition_name IN
            SELECT c.relname
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
            WHERE n.nspname = v_schema
              AND inh.inhparent = format('%I.leases', v_schema)::regclass
              AND c.relkind = 'r'
        LOOP
            v_slot := substring(v_partition_name FROM '^leases_(\d+)$');
            IF v_slot IS NULL THEN
                CONTINUE;
            END IF;
            EXECUTE format(
                'DROP INDEX IF EXISTS %I.%I',
                v_schema,
                format('idx_%s_leases_%s_state_hb', v_schema, v_slot)
            );
        END LOOP;
    END LOOP;
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (25, 'Drop idx_<schema>_leases_<slot>_state_hb on all AWA substrates (#169 B1)')
ON CONFLICT (version) DO NOTHING;
