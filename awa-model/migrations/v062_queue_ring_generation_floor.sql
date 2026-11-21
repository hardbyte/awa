-- v062: keep a lane's ring generations in order under REPEATABLE READ and
-- SERIALIZABLE producers.
-- Tracking: https://github.com/hardbyte/awa/issues/847
--
-- A producer reserves its lane positions under the lane's reservation lock and
-- then reads the queue-ring cursor. Under REPEATABLE READ or SERIALIZABLE the
-- cursor comes from a snapshot taken before the lock wait, so a job could take
-- a later lane position than a committed job in a newer generation and still
-- land in the older generation. Claims scan one (slot, generation) segment at a
-- time, so the lane's claim cursor moved past it and queue prune later deleted
-- it. A snapshot older than a whole ring cycle could also write into a slot the
-- ring had reused.
--
-- This adds, to the default and every discovered custom queue-storage schema:
--
-- * <schema>.queue_ring_generation_floor, a sequence holding the newest
--   queue-ring generation some transaction has read as committed. Reading a
--   sequence ignores the reader's snapshot.
-- * <schema>.enqueue_ring_cursor(), which raises the floor to the generation
--   its caller can see and returns the newer of the two. It is SECURITY
--   DEFINER, owned by the owner of queue_ring_state, and PUBLIC may execute
--   it, so runtime roles need no new grant. When the migration role cannot
--   hand it to that owner and cannot read the ring itself, the migration
--   warns and leaves the function out, so producers there keep reading the
--   cursor directly, as before.
--
-- The floor is seeded from the committed cursor. awa.insert_job_compat and
-- awa.insert_jobs_compat_batch are rewritten in place, from their own
-- definitions, to read enqueue_ring_cursor() when the active schema has it and
-- ring_cursor('queue') otherwise. A function that already reads
-- enqueue_ring_cursor() (a fresh install or a replay) is left alone. Binaries
-- from this release use enqueue_ring_cursor() for every ready and terminal row
-- they write, and the rotator calls it after each rotation commits.
--
-- N-1 binaries keep reading ring_cursor('queue') directly and behave as before
-- (their REPEATABLE READ producers can still misplace a job); rolling back
-- binaries needs no schema change.
--
-- Live load: one sequence and one function per schema, and two function
-- replacements. It takes no lock on any awa or queue-storage table and its work
-- scales with the number of schemas, not with job count. An enqueue in flight
-- finishes on the old body; the next call uses the new one.
--
-- External runners apply the complete `awa migrate --sql --from 61 --to 62`
-- bundle, which refreshes trusted queue-storage discovery and
-- awa.refresh_ring_generation_floor_helper before this block. Run it as the
-- owner of the queue-storage tables, or as a migrator that can SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_function REGPROCEDURE;
    v_definition TEXT;
    v_updated TEXT;
BEGIN
    FOR v_schema IN
        SELECT discovered.schema_name FROM awa.queue_storage_schemas() AS discovered
    LOOP
        IF to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL
           OR to_regprocedure(format('%I.ring_cursor(text)', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: enqueues and claims do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        PERFORM awa.refresh_ring_generation_floor_helper(v_schema);
    END LOOP;

    FOREACH v_function IN ARRAY ARRAY[
        to_regprocedure('awa.insert_job_compat(text,text,jsonb,awa.job_state,smallint,smallint,timestamp with time zone,jsonb,text[],bytea,bit,bytea)'),
        to_regprocedure('awa.insert_jobs_compat_batch(text[],text[],jsonb[],text[],smallint[],smallint[],timestamp with time zone[],jsonb[],jsonb[],bytea[],text[],text[],boolean)')
    ]
    LOOP
        IF v_function IS NULL THEN
            CONTINUE;
        END IF;
        v_definition := pg_get_functiondef(v_function::oid);
        IF strpos(v_definition, 'enqueue_ring_cursor()') > 0 THEN
            CONTINUE;
        END IF;
        v_updated := regexp_replace(
            v_definition,
            '([[:space:]]+)SELECT[[:space:]]+rc[.]slot,[[:space:]]+rc[.]generation[[:space:]]+INTO[[:space:]]+v_ready_slot,[[:space:]]+v_ready_generation[[:space:]]+FROM[[:space:]]+ring_cursor[(]''queue''[)][[:space:]]+AS[[:space:]]+rc;',
            '\1-- #847: enqueue_ring_cursor() keeps a lane''s generations in order
        -- under REPEATABLE READ and SERIALIZABLE. A schema prepared by an
        -- older installer lacks it.
        IF COALESCE(has_function_privilege(
            to_regprocedure(format(''%I.enqueue_ring_cursor()'', v_schema)),
            ''EXECUTE''
        ), false) THEN
            EXECUTE format(''SELECT slot, generation FROM %I.enqueue_ring_cursor()'', v_schema)
            INTO v_ready_slot, v_ready_generation;
        ELSE
            SELECT rc.slot, rc.generation
            INTO v_ready_slot, v_ready_generation
            FROM ring_cursor(''queue'') AS rc;
        END IF;',
            'g'
        );
        IF v_updated = v_definition THEN
            RAISE EXCEPTION 'v062: % does not read the queue-ring cursor in the expected form', v_function
                USING HINT = 'The function was changed outside awa migrations; restore it before migrating.';
        END IF;
        EXECUTE v_updated;
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (62, 'Keep a lane''s ring generations in order under REPEATABLE READ producers')
ON CONFLICT (version) DO NOTHING;
