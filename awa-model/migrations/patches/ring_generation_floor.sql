-- Schema patch ring_generation_floor: keep a lane's ring generations in order
-- under REPEATABLE READ and SERIALIZABLE producers (the generation floor of
-- awa 0.7 migration v062). Tracking: https://github.com/hardbyte/awa/issues/847
--
-- A producer reserves its lane positions under the lane's reservation lock and
-- then reads <schema>.queue_ring_state to choose the (ready_slot,
-- ready_generation) its rows go into. Under REPEATABLE READ or SERIALIZABLE
-- that read uses a snapshot taken before the lock wait, so a job could take a
-- later lane position than a committed job in a newer generation and still
-- land in the older generation. Claims scan one (slot, generation) segment at
-- a time, so the lane's claim cursor moved past it and queue prune later
-- deleted it. A snapshot older than a whole ring cycle could also write into a
-- slot the ring had reused.
--
-- This adds, to every queue-storage schema:
--
-- * <schema>.queue_ring_generation_floor, a sequence holding the newest
--   queue-ring generation some transaction has read as committed. Reading a
--   sequence ignores the reader's snapshot.
-- * <schema>.enqueue_ring_cursor(), which raises the floor to the generation
--   its caller can see and returns the newer of the two, with that
--   generation's slot (generation modulo slot_count). It is SECURITY DEFINER,
--   owned by the owner of queue_ring_state, and PUBLIC may execute it, so
--   runtime roles need no new grant. When the migration role cannot hand it
--   to that owner and cannot read the ring itself, it warns and leaves the
--   function out, so producers there keep reading the ring directly, as
--   before.
--
-- Producers call enqueue_ring_cursor() while they hold their lane locks, so
-- every earlier enqueue in a lane raised the floor to its own generation
-- before it committed, and generations never go backwards in a lane. Binaries
-- from this release use it for every ready and terminal row they write, and
-- their rotator calls it after each rotation commits. awa.insert_job_compat
-- and awa.insert_jobs_compat_batch are rewritten in place, from their own
-- definitions, to call it when the active schema has it and to read the ring
-- directly otherwise; signature, owner, privileges and settings are unchanged,
-- and a function that already calls it is left alone.
--
-- The versioned v023 installer is unchanged, so checksum-validating runners
-- see the same versioned files as 0.6.18. This patch adds
-- awa.refresh_ring_generation_floor_helper, which installs the floor in one
-- schema, and rewrites awa.install_queue_storage_substrate in place to call it
-- last, so a schema prepared later gets the floor too. The other schema
-- patches that embed the v023 installer put the released installer back, so
-- this patch runs after all of them: `awa migrate` re-applies it after any of
-- them, and its R__ name sorts after theirs. It also runs after
-- insert_jobs_compat_batch, which puts the released batch producer back.
--
-- Live load: one sequence and one function per schema, and three function
-- replacements. Its only table lock is ACCESS SHARE on each schema's
-- queue_ring_state, read to seed the floor, which blocks no claim, enqueue or
-- rotation. The exception is a floor ahead of the ring, as a logical restore
-- can leave it: seeding then locks the ring row FOR SHARE to repair it, and a
-- rotation in that schema waits until the migration commits. Its work scales
-- with the number of schemas, not with job count. An enqueue in flight
-- finishes on the old body; the next call uses the new one. It does not
-- wait for a schema's install lock, which prepare_schema() holds while it runs
-- the installer: when the lock is taken the patch fails with 55P03, changes
-- nothing, and can be run again.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Released 0.6 binaries keep reading queue_ring_state directly and
-- behave as before, so binaries can roll in either order and rolling back
-- needs no schema change. While they still enqueue or rotate, the floor does
-- not fully protect a REPEATABLE READ or SERIALIZABLE producer on this
-- release. Run it as the owner of the queue-storage tables, or as a migrator
-- that can SET ROLE to it.

-- Installs or refreshes the generation floor of p_schema: the sequence, and
-- enqueue_ring_cursor(), which it seeds the floor through. Seeding reads
-- queue_ring_state (ACCESS SHARE). The installer calls this too, once this
-- patch has rewritten it.
CREATE OR REPLACE FUNCTION awa.refresh_ring_generation_floor_helper(p_schema TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $refresh$
DECLARE
    v_namespace OID;
    v_state OID;
    v_owner OID;
    v_sequence OID;
    v_function OID;
    v_object_owner OID;
    v_previous_role TEXT;
    v_set_mode TEXT := CASE WHEN current_setting('server_version_num')::int >= 160000
                            THEN 'SET' ELSE 'MEMBER' END;
BEGIN
    IF p_schema IS NULL OR p_schema !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'refresh_ring_generation_floor_helper: schema name must match ^[a-z_][a-z0-9_]*$; got %',
            p_schema
            USING ERRCODE = '22023';
    END IF;

    -- Catalog lookups only until the role switch below: a migration role that
    -- may only SET ROLE to the ring's owner can lack USAGE on the schema.
    SELECT namespace.oid, state.oid, state.relowner
    INTO v_namespace, v_state, v_owner
    FROM pg_namespace AS namespace
    JOIN pg_class AS state
      ON state.relnamespace = namespace.oid
     AND state.relname = 'queue_ring_state'
     AND state.relkind IN ('r', 'p')
    WHERE namespace.nspname = p_schema
      -- The ring awa installs, not another table of the same name.
      AND (SELECT count(*)
           FROM pg_attribute AS col
           WHERE col.attrelid = state.oid
             AND col.attnum > 0
             AND NOT col.attisdropped
             AND (col.attname::text, col.atttypid) IN (
                     ('singleton', 'boolean'::regtype::oid),
                     ('current_slot', 'integer'::regtype::oid),
                     ('generation', 'bigint'::regtype::oid),
                     ('slot_count', 'integer'::regtype::oid))) = 4;
    IF v_owner IS NULL THEN
        RAISE EXCEPTION 'refresh_ring_generation_floor_helper: schema % has no queue ring',
            p_schema
            USING ERRCODE = '22023';
    END IF;

    -- A floor left from an earlier run that could not hand it over (the
    -- function was then left out) goes to the ring's owner first, while this
    -- role still holds the privileges of the sequence's owner.
    SELECT seq.oid, seq.relowner INTO v_sequence, v_object_owner
    FROM pg_class AS seq
    WHERE seq.relnamespace = v_namespace
      AND seq.relname = 'queue_ring_generation_floor'
      AND seq.relkind = 'S';
    IF v_sequence IS NOT NULL
       AND v_object_owner <> v_owner
       AND has_schema_privilege(current_user, v_namespace, 'USAGE')
       AND pg_has_role(current_user, v_object_owner, 'USAGE')
       AND pg_has_role(current_user, v_owner, v_set_mode)
       AND has_schema_privilege(v_owner, v_namespace, 'CREATE') THEN
        EXECUTE format('ALTER SEQUENCE %I.queue_ring_generation_floor OWNER TO %I',
            p_schema, pg_get_userbyid(v_owner));
    END IF;

    -- Create, replace and grant as the ring's owner when the migration role
    -- may SET ROLE to it, so a migrator that does not inherit the owner's
    -- privileges (PostgreSQL 16+ INHERIT FALSE membership) can create in the
    -- schema and replace the function on a replay, and the objects belong to
    -- the owner from the start.
    IF v_owner <> (SELECT role.oid FROM pg_roles AS role WHERE role.rolname = current_user)
       AND pg_has_role(current_user, v_owner, v_set_mode)
       AND has_schema_privilege(v_owner, v_namespace, 'CREATE')
       AND COALESCE(
           (SELECT routine.proowner FROM pg_proc AS routine
            WHERE routine.pronamespace = v_namespace
              AND routine.proname = 'enqueue_ring_cursor'
              AND routine.pronargs = 0),
           v_owner
       ) = v_owner THEN
        v_previous_role := current_setting('role');
        PERFORM set_config('role', pg_get_userbyid(v_owner), true);
    END IF;

    EXECUTE format(
        'CREATE SEQUENCE IF NOT EXISTS %I.queue_ring_generation_floor AS BIGINT MINVALUE 0 START WITH 0',
        p_schema
    );
    SELECT seq.oid INTO v_sequence
    FROM pg_class AS seq
    WHERE seq.relnamespace = v_namespace
      AND seq.relname = 'queue_ring_generation_floor'
      AND seq.relkind = 'S';

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.enqueue_ring_cursor()
        RETURNS TABLE(slot INT, generation BIGINT)
        LANGUAGE plpgsql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, pg_temp
        AS $func$
        DECLARE
            v_slot INT;
            v_generation BIGINT;
            v_slot_count INT;
            v_floor BIGINT;
            v_latest_slot INT;
            v_latest_generation BIGINT;
            v_lowered BOOLEAN := FALSE;
            v_lock_key BIGINT := hashtextextended(%3$L, 0);
        BEGIN
            SELECT state.current_slot, state.generation, state.slot_count
            INTO v_slot, v_generation, v_slot_count
            FROM %1$I.queue_ring_state AS state
            WHERE state.singleton;

            -- NULL until the first raise after creation or a reset.
            v_floor := pg_sequence_last_value(%2$L::regclass);
            IF v_floor > v_generation THEN
                -- The floor is ahead of the ring this snapshot sees. Usually
                -- the snapshot predates the latest rotation (REPEATABLE READ or
                -- SERIALIZABLE). But a logical restore can bring back a floor
                -- ahead of the ring, because pg_dump reads sequences outside
                -- its snapshot, and a floor nothing can reach would wedge the
                -- ring: enqueues would fill a slot rotation has not reached.
                -- Locking the ring row tells the two apart. FOR SHARE conflicts
                -- with a rotation's update (FOR KEY SHARE would not), so at
                -- REPEATABLE READ or SERIALIZABLE it fails with 40001 when a
                -- rotation committed after the snapshot, and at READ COMMITTED
                -- it reads the latest committed ring, waiting only for a
                -- rotation that is committing. Only an enqueue that sees the
                -- floor ahead of its snapshot gets here. The floor is read
                -- first, so a floor still ahead of the latest ring cannot have
                -- come from a producer or rotation.
                BEGIN
                    SELECT state.current_slot, state.generation
                    INTO v_latest_slot, v_latest_generation
                    FROM %1$I.queue_ring_state AS state
                    WHERE state.singleton
                    FOR SHARE;
                EXCEPTION WHEN serialization_failure THEN
                    v_latest_generation := NULL;
                END;

                IF v_latest_generation IS NOT NULL AND v_floor > v_latest_generation THEN
                    -- Lower it under the raise's lock, checking the ring
                    -- again, so a rotation and its raise cannot slip in
                    -- between. Every way out releases the lock, as above.
                    BEGIN
                        PERFORM pg_advisory_lock(v_lock_key);
                        BEGIN
                            SELECT state.current_slot, state.generation
                            INTO v_latest_slot, v_latest_generation
                            FROM %1$I.queue_ring_state AS state
                            WHERE state.singleton
                            FOR SHARE;
                            IF pg_sequence_last_value(%2$L::regclass) > v_latest_generation THEN
                                PERFORM setval(%2$L::regclass, v_latest_generation, true);
                            END IF;
                            v_lowered := TRUE;
                        EXCEPTION WHEN serialization_failure THEN
                            -- A rotation committed meanwhile; the floor is
                            -- not ahead of the ring any more.
                            NULL;
                        END;
                        PERFORM pg_advisory_unlock(v_lock_key);
                    EXCEPTION WHEN OTHERS OR query_canceled THEN
                        IF EXISTS (
                            SELECT 1
                            FROM pg_catalog.pg_locks AS held
                            WHERE held.locktype = 'advisory'
                              AND held.pid = pg_backend_pid()
                              AND held.granted
                              AND held.objsubid = 1
                              AND ((held.classid::int8 << 32) | held.objid::int8) = v_lock_key
                        ) THEN
                            PERFORM pg_advisory_unlock(v_lock_key);
                        END IF;
                        RAISE;
                    END;
                END IF;

                IF v_lowered OR (v_latest_generation IS NOT NULL AND v_latest_generation >= v_floor) THEN
                    -- The latest committed ring: after lowering a restored
                    -- floor, or at READ COMMITTED once a rotation committed
                    -- after this statement began.
                    v_slot := v_latest_slot;
                    v_generation := v_latest_generation;
                ELSE
                    -- A snapshot older than the latest rotation. Rotation
                    -- advances slot and generation together from (0, 0) and
                    -- slot_count changes only when reset restarts the ring,
                    -- so the floor's slot is its generation modulo slot_count.
                    v_slot := (v_floor %% v_slot_count)::int;
                    v_generation := v_floor;
                END IF;
            END IF;

            IF v_floor IS NULL OR v_floor < v_generation THEN
                -- Every generation returned is at or below the floor, so a
                -- later producer in the lane never writes into an older one.
                -- setval is not a compare-and-set, so raise under a short
                -- session lock, released before returning rather than at the
                -- end of the caller's transaction. Usually only the first
                -- producer to see each rotation gets here. A pooled connection would keep a
                -- leaked session lock, so every way out releases it, including
                -- a cancel or statement timeout, which OTHERS does not catch;
                -- the handler checks pg_locks because the error may have come
                -- before the lock was granted.
                BEGIN
                    PERFORM pg_advisory_lock(v_lock_key);
                    v_floor := pg_sequence_last_value(%2$L::regclass);
                    IF v_floor IS NULL OR v_floor < v_generation THEN
                        PERFORM setval(%2$L::regclass, v_generation, true);
                    END IF;
                    PERFORM pg_advisory_unlock(v_lock_key);
                EXCEPTION WHEN OTHERS OR query_canceled THEN
                    IF EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_locks AS held
                        WHERE held.locktype = 'advisory'
                          AND held.pid = pg_backend_pid()
                          AND held.granted
                          AND held.objsubid = 1
                          -- pg_locks shows a bigint key's high half in
                          -- classid and its low half in objid.
                          AND ((held.classid::int8 << 32) | held.objid::int8) = v_lock_key
                    ) THEN
                        PERFORM pg_advisory_unlock(v_lock_key);
                    END IF;
                    RAISE;
                END;
            END IF;

            slot := v_slot;
            generation := v_generation;
            RETURN NEXT;
        END;
        $func$
        $ddl$,
        p_schema,
        format('%I.queue_ring_generation_floor', p_schema),
        format('awa.queue_storage.ring_generation_floor:%s', p_schema)
    );
    SELECT routine.oid, routine.proowner INTO v_function, v_object_owner
    FROM pg_proc AS routine
    WHERE routine.pronamespace = v_namespace
      AND routine.proname = 'enqueue_ring_cursor'
      AND routine.pronargs = 0;

    -- The function reads and sets the floor with its owner's privileges, so
    -- the floor and the function belong to the queue ring's owner.
    IF (SELECT seq.relowner FROM pg_class AS seq WHERE seq.oid = v_sequence) <> v_owner THEN
        IF pg_has_role(current_user,
                       (SELECT seq.relowner FROM pg_class AS seq WHERE seq.oid = v_sequence),
                       'USAGE')
           AND pg_has_role(current_user, v_owner, v_set_mode)
           AND has_schema_privilege(v_owner, v_namespace, 'CREATE') THEN
            EXECUTE format('ALTER SEQUENCE %I.queue_ring_generation_floor OWNER TO %I',
                p_schema, pg_get_userbyid(v_owner));
        ELSE
            RAISE WARNING '%.queue_ring_generation_floor is not owned by %, the owner of %.queue_ring_state',
                p_schema, pg_get_userbyid(v_owner), p_schema
                USING HINT = format(
                    'Run awa migrate as a role that can SET ROLE to %s, or as %s itself.',
                    quote_ident(pg_get_userbyid(v_owner)),
                    quote_ident(pg_get_userbyid(v_owner))
                );
        END IF;
    END IF;

    IF v_object_owner <> v_owner THEN
        IF pg_has_role(current_user, v_object_owner, 'USAGE')
           AND pg_has_role(current_user, v_owner, v_set_mode)
           AND has_schema_privilege(v_owner, v_namespace, 'CREATE') THEN
            EXECUTE format('ALTER FUNCTION %I.enqueue_ring_cursor() OWNER TO %I',
                p_schema, pg_get_userbyid(v_owner));
            v_object_owner := v_owner;
        ELSE
            RAISE WARNING '%.enqueue_ring_cursor() is owned by %, not %, the owner of %.queue_ring_state',
                p_schema, pg_get_userbyid(v_object_owner), pg_get_userbyid(v_owner), p_schema
                USING HINT = format(
                    'Run awa migrate as a role that can SET ROLE to %s, or as %s itself.',
                    quote_ident(pg_get_userbyid(v_owner)),
                    quote_ident(pg_get_userbyid(v_owner))
                );
        END IF;
    END IF;

    -- The function runs as its owner, so its owner must be able to read and
    -- lock the ring and read and set the floor. If it cannot (the transfer
    -- above was not possible), the function is dropped: producers then read the ring as they did
    -- before the floor existed, instead of failing.
    IF has_schema_privilege(v_object_owner, v_namespace, 'USAGE')
       AND has_table_privilege(v_object_owner, v_state, 'SELECT')
       -- FOR SHARE, which a floor ahead of the snapshot reads the ring with,
       -- also needs UPDATE on some column.
       AND has_any_column_privilege(v_object_owner, v_state, 'UPDATE')
       AND has_sequence_privilege(v_object_owner, v_sequence, 'SELECT')
       AND has_sequence_privilege(v_object_owner, v_sequence, 'UPDATE') THEN
        -- New functions are executable by PUBLIC already; this restores it
        -- should anything have revoked it.
        EXECUTE format('GRANT EXECUTE ON FUNCTION %I.enqueue_ring_cursor() TO PUBLIC', p_schema);
        -- Seed the floor from the committed cursor.
        EXECUTE format('SELECT FROM %I.enqueue_ring_cursor()', p_schema);
    ELSE
        EXECUTE format('DROP FUNCTION %I.enqueue_ring_cursor()', p_schema);
        -- Drop the floor with it when this role owns it, so the ring's owner
        -- can install both later instead of meeting a sequence it cannot use.
        IF pg_has_role((SELECT relowner FROM pg_class WHERE oid = v_sequence), 'USAGE') THEN
            EXECUTE format('DROP SEQUENCE %I.queue_ring_generation_floor', p_schema);
        END IF;
        RAISE WARNING '% cannot read the queue ring of schema %, so enqueues there keep writing without the generation floor (#847)',
            pg_get_userbyid(v_object_owner), p_schema
            USING HINT = format(
                'Run awa migrate as a role that can SET ROLE to %s, or as %s itself.',
                quote_ident(pg_get_userbyid(v_owner)),
                quote_ident(pg_get_userbyid(v_owner))
            );
    END IF;

    IF v_previous_role IS NOT NULL THEN
        PERFORM set_config('role', v_previous_role, true);
    END IF;
END;
$refresh$;

REVOKE EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(TEXT) FROM PUBLIC;

DO $$
DECLARE
    v_schema TEXT;
    v_grantee OID;
    v_function REGPROCEDURE;
    v_definition TEXT;
    v_updated TEXT;
BEGIN
    -- The installer installs the floor last, so a schema prepared later gets
    -- it too.
    v_function := to_regprocedure(
        'awa.install_queue_storage_substrate(text,integer,integer,integer,boolean)'
    );
    IF v_function IS NOT NULL THEN
        v_definition := pg_get_functiondef(v_function::oid);
        IF strpos(v_definition, 'refresh_ring_generation_floor_helper') = 0 THEN
            v_updated := regexp_replace(
                v_definition,
                '\nEND;([[:space:]]*)[$]function[$]',
                '

    -- #847: the queue-ring generation floor (schema patch ring_generation_floor).
    -- A role that may run the installer but not the floor helper still
    -- installs the schema; enqueues there read the ring as before.
    IF has_function_privilege(''awa.refresh_ring_generation_floor_helper(text)'', ''EXECUTE'') THEN
        PERFORM awa.refresh_ring_generation_floor_helper(p_schema);
    ELSE
        RAISE WARNING ''schema % was installed without the queue-ring generation floor: % may not execute awa.refresh_ring_generation_floor_helper(text)'', p_schema, current_user
            USING HINT = ''Grant EXECUTE on that function to the roles that run awa.install_queue_storage_substrate, then run awa migrate.'';
    END IF;
END;\1$function$'
            );
            IF v_updated = v_definition THEN
                RAISE EXCEPTION 'ring_generation_floor: % does not end in the expected form', v_function
                    USING HINT = 'The function was changed outside awa migrations; restore it before migrating.';
            END IF;
            EXECUTE v_updated;
        END IF;
        -- Every role that may run the installer may run the floor helper it
        -- now calls.
        FOR v_grantee IN
            SELECT DISTINCT acl.grantee
            FROM pg_proc AS p
            CROSS JOIN LATERAL aclexplode(p.proacl) AS acl
            WHERE p.oid = v_function
              AND acl.privilege_type = 'EXECUTE'
              AND acl.grantee <> 0
        LOOP
            EXECUTE format(
                'GRANT EXECUTE ON FUNCTION awa.refresh_ring_generation_floor_helper(text) TO %I',
                pg_get_userbyid(v_grantee)
            );
        END LOOP;
    END IF;

    -- The compat producers read the ring through enqueue_ring_cursor() when
    -- the active schema has it.
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
            '(\n[ ]*)SELECT[[:space:]]+ring[.]current_slot,[[:space:]]+ring[.]generation[[:space:]]+INTO[[:space:]]+v_ready_slot,[[:space:]]+v_ready_generation[[:space:]]+FROM[[:space:]]+queue_ring_state[[:space:]]+AS[[:space:]]+ring[[:space:]]+WHERE[[:space:]]+ring[.]singleton[[:space:]]+=[[:space:]]+TRUE;',
            '\1-- #847: enqueue_ring_cursor() keeps a lane''s generations in order\1-- under REPEATABLE READ and SERIALIZABLE. A schema without schema\1-- patch ring_generation_floor lacks it.\1IF COALESCE(has_function_privilege(\1    to_regprocedure(format(''%I.enqueue_ring_cursor()'', v_schema)),\1    ''EXECUTE''\1), false) THEN\1    EXECUTE format(''SELECT slot, generation FROM %I.enqueue_ring_cursor()'', v_schema)\1    INTO v_ready_slot, v_ready_generation;\1ELSE\1    SELECT ring.current_slot, ring.generation\1    INTO v_ready_slot, v_ready_generation\1    FROM queue_ring_state AS ring\1    WHERE ring.singleton = TRUE;\1END IF;',
            'g'
        );
        IF v_updated = v_definition THEN
            RAISE EXCEPTION 'ring_generation_floor: % does not read the queue-ring cursor in the expected form', v_function
                USING HINT = 'The function was changed outside awa migrations; restore it before migrating.';
        END IF;
        EXECUTE v_updated;
    END LOOP;

    -- Substrates are found from the catalogs alone, without USAGE on their
    -- schemas: a migration role that may only SET ROLE to a schema's owner
    -- has no USAGE of its own there, and refresh_ring_generation_floor_helper
    -- is what switches to the owner. Only names awa can install a substrate
    -- under are considered; the helper refuses any other.
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE n.nspname ~ '^[a-z_][a-z0-9_]*$'
          AND EXISTS (
                  SELECT 1 FROM pg_proc AS p
                  WHERE p.pronamespace = n.oid
                    AND p.proname = 'claim_ready_runtime'
                    AND oidvectortypes(p.proargtypes)
                        = 'text, bigint, double precision, double precision')
          -- A ring with awa's columns: another schema with a table of the
          -- same name would otherwise fail the seeding below.
          AND EXISTS (SELECT 1 FROM pg_class AS c
                      WHERE c.relnamespace = n.oid
                        AND c.relname = 'queue_ring_state'
                        AND c.relkind IN ('r', 'p')
                        AND (SELECT count(*)
                             FROM pg_attribute AS col
                             WHERE col.attrelid = c.oid
                               AND col.attnum > 0
                               AND NOT col.attisdropped
                               AND (col.attname::text, col.atttypid) IN (
                                       ('singleton', 'boolean'::regtype::oid),
                                       ('current_slot', 'integer'::regtype::oid),
                                       ('generation', 'bigint'::regtype::oid),
                                       ('slot_count', 'integer'::regtype::oid))) = 4)
        ORDER BY n.nspname
    LOOP
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: enqueues and claims do not
        -- take it. The patch does not wait for it: in an `awa migrate --sql`
        -- bundle the earlier replica_identity patch still holds table locks the
        -- installer takes, so waiting could deadlock with that prepare_schema().
        IF NOT pg_try_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        ) THEN
            RAISE EXCEPTION 'ring_generation_floor: schema % is being prepared', v_schema
                USING ERRCODE = 'lock_not_available',
                      HINT = 'A prepare_schema() of that schema holds its install lock. This patch changed nothing and is safe to run again.';
        END IF;
        -- A role that can neither create in the schema nor SET ROLE to its
        -- owner leaves that schema without the floor; the patch probe names
        -- it until the patch runs as a role that can.
        BEGIN
            PERFORM awa.refresh_ring_generation_floor_helper(v_schema);
        EXCEPTION WHEN insufficient_privilege THEN
            RAISE WARNING 'could not install the queue-ring generation floor in schema %: %', v_schema, SQLERRM
                USING HINT = format(
                    'Run SELECT awa.refresh_ring_generation_floor_helper(%L) as the owner of %I.queue_ring_state.',
                    v_schema, v_schema
                );
        END;
    END LOOP;
END;
$$;
