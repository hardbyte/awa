-- Schema patch sequence_lock_cancel_release: release lane-sequence session locks
-- on cancel (awa 0.7 migration v062, minus the schema_version row and the
-- ring generation floor).
--
-- set_sequence_next, which moves a lane's claim or enqueue cursor, and the
-- enqueue-head trigger's block allocation hold a session advisory lock on the
-- lane's sequence for a moment. They released it in a WHEN OTHERS handler,
-- which does not catch a cancel or statement timeout, so a cancel that landed
-- just after the lock was granted left it held on the pooled connection, and
-- every later claim cursor move or reservation on that lane waited until the
-- connection closed. Both now take the lock inside the block and release it in
-- a handler that also catches query_canceled, checking pg_locks first because
-- the error can come before the lock is granted. set_sequence_next is now
-- defined by awa.refresh_lane_sequence_helpers, as in 0.7, so refreshing the
-- helpers refreshes it too.
--
-- The released v023 file is checksum-locked, so this patch carries the new
-- definitions itself: it replaces awa.refresh_lane_sequence_helpers, which now
-- also defines set_sequence_next, and adds
-- awa.refresh_lane_sequence_helpers_as_migrator. The installer keeps its own
-- released set_sequence_next, but it calls the helper refresh afterwards, so a
-- schema prepared after the patch gets the new one. A patch that re-runs v023
-- restores the released refresh function, so this patch runs after every such
-- patch.
--
-- On each installed schema the block below refreshes the lane helpers through
-- awa.refresh_lane_sequence_helpers_as_migrator, which retries as the schema's
-- owner for a migration role that may only SET ROLE to it, and warns and
-- leaves the old helpers for a role that can do neither; the patch then stays
-- pending for that schema.
--
-- Live load: function replacement only. The patch takes no table locks and
-- reads no queue rows, so workers keep claiming and completing while it runs.
-- A call already inside a helper finishes on the old body.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Released 0.6 binaries call the same functions with the same
-- arguments and results. Run it as the schema owner or migrator.

-- The lane-sequence helpers that create a missing lane sequence on demand.
-- They live outside the installer so a schema patch can replace just these
-- three functions on a live database: CREATE OR REPLACE FUNCTION takes no
-- table locks and reads no rows, while re-running the installer locks hot
-- queue-storage tables and rebuilds triggers. The installer calls this too.
CREATE OR REPLACE FUNCTION awa.refresh_lane_sequence_helpers(p_schema TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $refresh$
DECLARE
    v_schema_owner OID;
    v_function REGPROCEDURE;
    v_function_owner OID;
    v_set_mode TEXT := CASE WHEN current_setting('server_version_num')::int >= 160000
                            THEN 'SET' ELSE 'MEMBER' END;
BEGIN
    IF p_schema IS NULL OR p_schema !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'refresh_lane_sequence_helpers: schema name must match ^[a-z_][a-z0-9_]*$; got %',
            p_schema
            USING ERRCODE = '22023';
    END IF;

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.ensure_lane_sequences(
            p_queue TEXT,
            p_priority SMALLINT,
            p_enqueue_shard SMALLINT
        )
        RETURNS VOID
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_enqueue_seq TEXT := %1$I.queue_lane_sequence_name(
                'queue_enqueue_seq',
                p_queue,
                p_priority,
                p_enqueue_shard
            );
            v_claim_seq TEXT := %1$I.queue_lane_sequence_name(
                'queue_claim_seq',
                p_queue,
                p_priority,
                p_enqueue_shard
            );
        BEGIN
            IF to_regclass(format('%%I.%%I', %1$L, v_enqueue_seq)) IS NULL THEN
                IF has_function_privilege(
                    to_regprocedure(format('%%I.provision_lane_sequences(text,smallint,smallint)', %1$L)),
                    'EXECUTE'
                ) THEN
                    PERFORM %1$I.provision_lane_sequences(p_queue, p_priority, p_enqueue_shard);
                ELSIF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                    RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_enqueue_seq
                        USING ERRCODE = '42501',
                              HINT = 'Run awa migrate, or awa storage prepare-queue as the migrator, before starting producers or workers.';
                ELSE
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_enqueue_seq
                    );
                END IF;
            END IF;
            IF to_regclass(format('%%I.%%I', %1$L, v_claim_seq)) IS NULL THEN
                IF has_function_privilege(
                    to_regprocedure(format('%%I.provision_lane_sequences(text,smallint,smallint)', %1$L)),
                    'EXECUTE'
                ) THEN
                    PERFORM %1$I.provision_lane_sequences(p_queue, p_priority, p_enqueue_shard);
                ELSIF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                    RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_claim_seq
                        USING ERRCODE = '42501',
                              HINT = 'Run awa migrate, or awa storage prepare-queue as the migrator, before starting producers or workers.';
                ELSE
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_claim_seq
                    );
                END IF;
            END IF;

            UPDATE %1$I.queue_enqueue_heads
            SET seq_name = v_enqueue_seq
            WHERE queue = p_queue
              AND priority = p_priority
              AND enqueue_shard = p_enqueue_shard
              AND seq_name IS DISTINCT FROM v_enqueue_seq;

            UPDATE %1$I.queue_claim_heads
            SET seq_name = v_claim_seq
            WHERE queue = p_queue
              AND priority = p_priority
              AND enqueue_shard = p_enqueue_shard
              AND seq_name IS DISTINCT FROM v_claim_seq;
        END;
        $func$
        $ddl$,
        p_schema
    );

    -- set_sequence_next is defined here, beside the triggers that call it,
    -- so a migration refreshing these helpers refreshes it too.
    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.set_sequence_next(p_seq_name TEXT, p_next BIGINT)
        RETURNS VOID
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_lock_key TEXT;
        BEGIN
            IF p_seq_name IS NULL THEN
                RAISE EXCEPTION 'lane sequence name is NULL'
                    USING ERRCODE = '55000';
            END IF;

            v_lock_key := format('%%I.%%I', %1$L, p_seq_name);

            -- `setval` is not a compare-and-swap. Serialize it against
            -- reservation-side block allocation so this helper's "move to at
            -- least p_next" contract cannot rewind a hot enqueue sequence.
            -- Every way out of the block releases the session lock,
            -- including a cancel or statement timeout, which OTHERS does not
            -- catch; see queue_enqueue_head_sequence_sync.
            BEGIN
                PERFORM pg_advisory_lock(hashtextextended(v_lock_key, 0));
                p_next := GREATEST(p_next, %1$I.sequence_next_value(p_seq_name));

                IF p_next <= 1 THEN
                    EXECUTE format(
                        'SELECT setval(%%L::regclass, 1, false)',
                        format('%%I.%%I', %1$L, p_seq_name)
                    );
                ELSE
                    EXECUTE format(
                        'SELECT setval(%%L::regclass, %%s, true)',
                        format('%%I.%%I', %1$L, p_seq_name),
                        p_next - 1
                    );
                END IF;
                PERFORM pg_advisory_unlock(hashtextextended(v_lock_key, 0));
            EXCEPTION WHEN OTHERS OR query_canceled THEN
                IF EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_locks AS held
                    WHERE held.locktype = 'advisory'
                      AND held.pid = pg_backend_pid()
                      AND held.granted
                      AND held.objsubid = 1
                      AND held.classid = ((hashtextextended(v_lock_key, 0) >> 32) & 4294967295)::oid
                      AND held.objid = (hashtextextended(v_lock_key, 0) & 4294967295)::oid
                ) THEN
                    PERFORM pg_advisory_unlock(hashtextextended(v_lock_key, 0));
                END IF;
                RAISE;
            END;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.queue_enqueue_head_sequence_sync()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_seq_name TEXT := %1$I.queue_lane_sequence_name(
                'queue_enqueue_seq',
                NEW.queue,
                NEW.priority,
                NEW.enqueue_shard
            );
            v_qualified_seq TEXT := format('%%I.%%I', %1$L, v_seq_name);
            v_count BIGINT;
            v_start BIGINT;
        BEGIN
            IF to_regclass(format('%%I.%%I', %1$L, v_seq_name)) IS NULL THEN
                IF has_function_privilege(
                    to_regprocedure(format('%%I.provision_lane_sequences(text,smallint,smallint)', %1$L)),
                    'EXECUTE'
                ) THEN
                    PERFORM %1$I.provision_lane_sequences(NEW.queue, NEW.priority, NEW.enqueue_shard);
                ELSIF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                    RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_seq_name
                        USING ERRCODE = '42501',
                              HINT = 'Run awa migrate, or awa storage prepare-queue as the migrator, before starting producers or workers.';
                ELSE
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_seq_name
                    );
                END IF;
            END IF;
            NEW.seq_name := v_seq_name;

            IF TG_OP = 'UPDATE'
               AND NEW.next_seq IS DISTINCT FROM OLD.next_seq
               AND NEW.next_seq > OLD.next_seq
            THEN
                v_count := NEW.next_seq - OLD.next_seq;
                -- A pooled connection keeps a session lock it leaks, which
                -- would stall this lane's reservations, so every way out of
                -- the block releases it, including a cancel or statement
                -- timeout, which OTHERS does not catch. The lock is taken
                -- inside the block and the handler checks pg_locks, because
                -- the error can also come before the lock is granted.
                BEGIN
                    PERFORM pg_advisory_lock(hashtextextended(v_qualified_seq, 0));
                    EXECUTE format(
                        'SELECT nextval(%%L::regclass)::bigint',
                        v_qualified_seq
                    )
                    INTO v_start;

                    IF v_count > 1 THEN
                        EXECUTE format(
                            'SELECT setval(%%L::regclass, %%s, true)',
                            v_qualified_seq,
                            v_start + v_count - 1
                        );
                    END IF;
                    PERFORM pg_advisory_unlock(hashtextextended(v_qualified_seq, 0));
                EXCEPTION WHEN OTHERS OR query_canceled THEN
                    IF EXISTS (
                        SELECT 1
                        FROM pg_catalog.pg_locks AS held
                        WHERE held.locktype = 'advisory'
                          AND held.pid = pg_backend_pid()
                          AND held.granted
                          AND held.objsubid = 1
                          AND held.classid = ((hashtextextended(v_qualified_seq, 0) >> 32) & 4294967295)::oid
                          AND held.objid = (hashtextextended(v_qualified_seq, 0) & 4294967295)::oid
                    ) THEN
                        PERFORM pg_advisory_unlock(hashtextextended(v_qualified_seq, 0));
                    END IF;
                    RAISE;
                END;
                NEW.next_seq := v_start + v_count;
            ELSE
                PERFORM %1$I.set_sequence_next(v_seq_name, NEW.next_seq);
            END IF;

            RETURN NEW;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.queue_claim_head_sequence_sync()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_seq_name TEXT := %1$I.queue_lane_sequence_name(
                'queue_claim_seq',
                NEW.queue,
                NEW.priority,
                NEW.enqueue_shard
            );
        BEGIN
            IF to_regclass(format('%%I.%%I', %1$L, v_seq_name)) IS NULL THEN
                IF has_function_privilege(
                    to_regprocedure(format('%%I.provision_lane_sequences(text,smallint,smallint)', %1$L)),
                    'EXECUTE'
                ) THEN
                    PERFORM %1$I.provision_lane_sequences(NEW.queue, NEW.priority, NEW.enqueue_shard);
                ELSIF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                    RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_seq_name
                        USING ERRCODE = '42501',
                              HINT = 'Run awa migrate, or awa storage prepare-queue as the migrator, before starting producers or workers.';
                ELSE
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_seq_name
                    );
                END IF;
            END IF;
            NEW.seq_name := v_seq_name;

            IF TG_OP = 'INSERT' THEN
                PERFORM %1$I.set_sequence_next(v_seq_name, NEW.claim_seq);
            END IF;

            RETURN NEW;
        END;
        $func$
        $ddl$,
        p_schema
    );

    -- Lane sequences are created by the schema owner through these two
    -- SECURITY DEFINER functions, whichever role first touches the lane, so
    -- every runtime role can use them. Their grants mirror the head table each
    -- sequence backs (see awa.lane_sequence_problems); they never depend on
    -- the creating role's default privileges.
    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.provision_lane_sequences(
            p_queue TEXT,
            p_priority SMALLINT,
            p_enqueue_shard SMALLINT
        )
        RETURNS VOID
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, pg_temp
        AS $func$
        DECLARE
            v_seq_name TEXT;
            v_grant TEXT;
        BEGIN
            IF p_queue IS NULL
               OR p_priority IS NULL
               OR p_enqueue_shard IS NULL OR p_enqueue_shard NOT BETWEEN 0 AND 63 THEN
                RAISE EXCEPTION 'invalid queue lane: queue %%, priority %%, enqueue shard %%',
                    quote_nullable(p_queue), p_priority, p_enqueue_shard
                    USING ERRCODE = '22023';
            END IF;

            -- The lane lock QueueStorage takes before it writes lane rows, so
            -- concurrent first touches of one lane create its sequences once.
            PERFORM pg_advisory_xact_lock(hashtextextended(
                format('%%s:%%s:%%s:%%s', %1$L, p_queue, p_priority, p_enqueue_shard),
                0
            ));

            FOREACH v_seq_name IN ARRAY ARRAY[
                %1$I.queue_lane_sequence_name('queue_enqueue_seq', p_queue, p_priority, p_enqueue_shard),
                %1$I.queue_lane_sequence_name('queue_claim_seq', p_queue, p_priority, p_enqueue_shard)
            ] LOOP
                IF to_regclass(format('%%I.%%I', %1$L, v_seq_name)) IS NULL THEN
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_seq_name
                    );
                    FOR v_grant IN
                        SELECT unnest(problem.missing_grants)
                        FROM awa.lane_sequence_problems(%1$L, v_seq_name) AS problem
                        WHERE problem.owner = current_user
                    LOOP
                        EXECUTE v_grant;
                    END LOOP;
                END IF;
            END LOOP;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.provision_lane(
            p_queue TEXT,
            p_priority SMALLINT,
            p_enqueue_shard SMALLINT
        )
        RETURNS VOID
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, pg_temp
        AS $func$
        BEGIN
            -- Validates the lane and takes its lock before any row is written.
            PERFORM %1$I.provision_lane_sequences(p_queue, p_priority, p_enqueue_shard);

            INSERT INTO %1$I.queue_lanes (queue, priority)
            VALUES (p_queue, p_priority)
            ON CONFLICT (queue, priority) DO NOTHING;

            -- The head triggers fire before ON CONFLICT and set the lane's
            -- sequence, so rows that already exist are skipped outright.
            INSERT INTO %1$I.queue_enqueue_heads (queue, priority, enqueue_shard)
            SELECT p_queue, p_priority, p_enqueue_shard
            WHERE NOT EXISTS (
                SELECT 1 FROM %1$I.queue_enqueue_heads AS heads
                WHERE heads.queue = p_queue
                  AND heads.priority = p_priority
                  AND heads.enqueue_shard = p_enqueue_shard
            )
            ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

            INSERT INTO %1$I.queue_claim_heads (queue, priority, enqueue_shard)
            SELECT p_queue, p_priority, p_enqueue_shard
            WHERE NOT EXISTS (
                SELECT 1 FROM %1$I.queue_claim_heads AS heads
                WHERE heads.queue = p_queue
                  AND heads.priority = p_priority
                  AND heads.enqueue_shard = p_enqueue_shard
            )
            ON CONFLICT (queue, priority, enqueue_shard) DO NOTHING;

            PERFORM %1$I.ensure_lane_sequences(p_queue, p_priority, p_enqueue_shard);
        END;
        $func$
        $ddl$,
        p_schema
    );

    SELECT namespace.nspowner INTO v_schema_owner
    FROM pg_namespace AS namespace
    WHERE namespace.nspname = p_schema;

    FOREACH v_function IN ARRAY ARRAY[
        format('%I.provision_lane_sequences(text,smallint,smallint)', p_schema)::regprocedure,
        format('%I.provision_lane(text,smallint,smallint)', p_schema)::regprocedure
    ] LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', v_function);

        SELECT routine.proowner INTO v_function_owner
        FROM pg_proc AS routine
        WHERE routine.oid = v_function;

        IF v_function_owner <> v_schema_owner THEN
            IF pg_has_role(current_user, v_function_owner, 'USAGE')
               AND pg_has_role(current_user, v_schema_owner, v_set_mode) THEN
                EXECUTE format(
                    'ALTER FUNCTION %s OWNER TO %I',
                    v_function,
                    pg_get_userbyid(v_schema_owner)
                );
            ELSE
                RAISE WARNING '% is owned by %, not the owner % of schema %; lane sequences it creates will be owned by % too',
                    v_function, pg_get_userbyid(v_function_owner), pg_get_userbyid(v_schema_owner),
                    p_schema, pg_get_userbyid(v_function_owner)
                    USING HINT = format(
                        'Run awa migrate as a role that can SET ROLE to %s, or as %s itself.',
                        quote_ident(pg_get_userbyid(v_schema_owner)),
                        quote_ident(pg_get_userbyid(v_schema_owner))
                    );
            END IF;
        END IF;
    END LOOP;

END;
$refresh$;

REVOKE EXECUTE ON FUNCTION awa.refresh_lane_sequence_helpers(TEXT) FROM PUBLIC;

-- Refreshes p_schema's lane helpers from a migration. They are replaced in
-- place, which needs their owner's privileges: a migration role that may
-- only SET ROLE to the schema's owner (PostgreSQL 16+ INHERIT FALSE) retries
-- as that owner, lending it EXECUTE on awa.refresh_lane_sequence_helpers and
-- USAGE on awa for the call when it lacks them and taking them back after. A
-- role that can do neither keeps the old helpers, with a warning, and the
-- function returns false rather than failing the migration.
CREATE OR REPLACE FUNCTION awa.refresh_lane_sequence_helpers_as_migrator(p_schema TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $refresh$
DECLARE
    v_owner OID;
    v_owner_name TEXT;
    v_previous_role TEXT;
    v_lent_execute BOOLEAN := false;
    v_lent_usage BOOLEAN := false;
    v_set_mode TEXT := CASE WHEN current_setting('server_version_num')::int >= 160000
                            THEN 'SET' ELSE 'MEMBER' END;
BEGIN
    BEGIN
        PERFORM awa.refresh_lane_sequence_helpers(p_schema);
        RETURN true;
    EXCEPTION WHEN insufficient_privilege THEN
        NULL;
    END;

    SELECT namespace.nspowner INTO v_owner
    FROM pg_namespace AS namespace
    WHERE namespace.nspname = p_schema;
    v_owner_name := pg_get_userbyid(v_owner);
    -- Everything here runs in one subtransaction, so a failure also undoes
    -- the lent privileges and the role switch.
    BEGIN
        IF v_owner IS NULL OR NOT pg_has_role(current_user, v_owner, v_set_mode) THEN
            RAISE insufficient_privilege;
        END IF;
        IF NOT has_function_privilege(v_owner, 'awa.refresh_lane_sequence_helpers(text)', 'EXECUTE') THEN
            EXECUTE format('GRANT EXECUTE ON FUNCTION awa.refresh_lane_sequence_helpers(text) TO %I', v_owner_name);
            v_lent_execute := true;
        END IF;
        IF NOT has_schema_privilege(v_owner, 'awa', 'USAGE') THEN
            EXECUTE format('GRANT USAGE ON SCHEMA awa TO %I', v_owner_name);
            v_lent_usage := true;
        END IF;
        v_previous_role := current_setting('role');
        PERFORM set_config('role', v_owner_name, true);
        PERFORM awa.refresh_lane_sequence_helpers(p_schema);
        PERFORM set_config('role', v_previous_role, true);
        IF v_lent_execute THEN
            EXECUTE format('REVOKE EXECUTE ON FUNCTION awa.refresh_lane_sequence_helpers(text) FROM %I', v_owner_name);
        END IF;
        IF v_lent_usage THEN
            EXECUTE format('REVOKE USAGE ON SCHEMA awa FROM %I', v_owner_name);
        END IF;
        RETURN true;
    EXCEPTION WHEN insufficient_privilege THEN
        RAISE WARNING 'could not refresh the lane-sequence helpers of schema %: %', p_schema, SQLERRM
            USING HINT = format(
                'Run SELECT awa.refresh_lane_sequence_helpers(%L) as the owner of its lane functions; '
                'that role needs USAGE on schema awa and EXECUTE on the function.',
                p_schema
            );
        RETURN false;
    END;
END;
$refresh$;

REVOKE EXECUTE ON FUNCTION awa.refresh_lane_sequence_helpers_as_migrator(TEXT) FROM PUBLIC;

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    -- Substrates are found from the catalogs alone, without USAGE on their
    -- schemas: a migration role that may only SET ROLE to a schema's owner
    -- has no USAGE of its own there, and refresh_lane_sequence_helpers_as_migrator
    -- is what switches to the owner.
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE EXISTS (
                  SELECT 1 FROM pg_proc AS p
                  WHERE p.pronamespace = n.oid
                    AND p.proname = 'claim_ready_runtime'
                    AND oidvectortypes(p.proargtypes)
                        = 'text, bigint, double precision, double precision')
          AND EXISTS (SELECT 1 FROM pg_class AS c
                      WHERE c.relnamespace = n.oid AND c.relname = 'queue_enqueue_heads')
          AND EXISTS (SELECT 1 FROM pg_class AS c
                      WHERE c.relnamespace = n.oid AND c.relname = 'queue_claim_heads')
          AND EXISTS (SELECT 1 FROM pg_proc AS p
                      WHERE p.pronamespace = n.oid
                        AND p.proname = 'queue_lane_sequence_name'
                        AND oidvectortypes(p.proargtypes) = 'text, text, smallint, smallint')
        ORDER BY n.nspname
    LOOP
        -- Serialize with a concurrent prepare_schema() of the same schema. This
        -- is an advisory lock, not a table lock: claims and completions do not
        -- take it.
        PERFORM pg_advisory_xact_lock(
            hashtextextended(format('awa.queue_storage.install:%I', v_schema), 0)
        );
        PERFORM awa.refresh_lane_sequence_helpers_as_migrator(v_schema);
    END LOOP;
END;
$$;
