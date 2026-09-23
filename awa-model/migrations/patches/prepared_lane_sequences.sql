-- Schema patch prepared_lane_sequences: lane helpers that use provisioned
-- sequences without DDL (#501).
-- awa 0.7 migration v047 installs the same helper bodies.
--
-- The queue-storage lane helpers ran CREATE SEQUENCE IF NOT EXISTS on every
-- call. PostgreSQL requires CREATE on the schema for that statement even when
-- the sequence already exists, so a runtime role without schema CREATE could
-- not enqueue into or claim from any lane. The helpers now skip the DDL when
-- the sequence exists, and a caller that cannot create a missing lane gets
-- 42501 with a provisioning hint. Schema owners keep lazy creation. Function
-- signatures, cursor semantics and every other substrate function, including
-- claim_ready_runtime, are unchanged.
--
-- 0.6 delivery: `awa migrate` applies this after V40 when the probe in
-- migrations.rs reports it missing, and `awa migrate --sql` prints it as a
-- repeatable R__ script. It covers every schema holding the three lane
-- helpers; setting awa.prepared_lane_sequences_schema limits it to one.
-- External runners apply it after V40 and again after installing a custom
-- queue-storage schema. Released 0.6 binaries call these helpers by name and
-- operate unchanged once it is applied. It replaces three functions per
-- schema, takes no lock that conflicts with job traffic, and completes in
-- milliseconds.

DO $patch$
DECLARE
    v_only TEXT := nullif(current_setting('awa.prepared_lane_sequences_schema', true), '');
    v_schema TEXT;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_catalog.pg_namespace AS n
        JOIN pg_catalog.pg_proc AS p ON p.pronamespace = n.oid
        WHERE (v_only IS NULL OR n.nspname = v_only)
          AND ((p.proname = 'ensure_lane_sequences'
                 AND pg_catalog.oidvectortypes(p.proargtypes) = 'text, smallint, smallint')
              OR (p.proname IN ('queue_enqueue_head_sequence_sync', 'queue_claim_head_sequence_sync')
                  AND p.pronargs = 0))
        GROUP BY n.nspname
        HAVING count(*) = 3
        ORDER BY n.nspname
    LOOP
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
                    IF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                        RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_enqueue_seq
                            USING ERRCODE = '42501',
                                  HINT = 'Run awa storage prepare-queue as the migrator before starting producers or workers.';
                    END IF;
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_enqueue_seq
                    );
                END IF;
                IF to_regclass(format('%%I.%%I', %1$L, v_claim_seq)) IS NULL THEN
                    IF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                        RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_claim_seq
                            USING ERRCODE = '42501',
                                  HINT = 'Run awa storage prepare-queue as the migrator before starting producers or workers.';
                    END IF;
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_claim_seq
                    );
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
            v_schema
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
                    IF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                        RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_seq_name
                            USING ERRCODE = '42501',
                                  HINT = 'Run awa storage prepare-queue as the migrator before starting producers or workers.';
                    END IF;
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_seq_name
                    );
                END IF;
                NEW.seq_name := v_seq_name;

                IF TG_OP = 'UPDATE'
                   AND NEW.next_seq IS DISTINCT FROM OLD.next_seq
                   AND NEW.next_seq > OLD.next_seq
                THEN
                    v_count := NEW.next_seq - OLD.next_seq;
                    PERFORM pg_advisory_lock(hashtextextended(v_qualified_seq, 0));
                    BEGIN
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
                    EXCEPTION WHEN OTHERS THEN
                        PERFORM pg_advisory_unlock(hashtextextended(v_qualified_seq, 0));
                        RAISE;
                    END;
                    PERFORM pg_advisory_unlock(hashtextextended(v_qualified_seq, 0));
                    NEW.next_seq := v_start + v_count;
                ELSE
                    PERFORM %1$I.set_sequence_next(v_seq_name, NEW.next_seq);
                END IF;

                RETURN NEW;
            END;
            $func$
            $ddl$,
            v_schema
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
                    IF NOT has_schema_privilege(current_user, %1$L, 'CREATE') THEN
                        RAISE EXCEPTION 'lane sequence %%.%% has not been provisioned', %1$L, v_seq_name
                            USING ERRCODE = '42501',
                                  HINT = 'Run awa storage prepare-queue as the migrator before starting producers or workers.';
                    END IF;
                    EXECUTE format(
                        'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                        %1$L,
                        v_seq_name
                    );
                END IF;
                NEW.seq_name := v_seq_name;

                IF TG_OP = 'INSERT' THEN
                    PERFORM %1$I.set_sequence_next(v_seq_name, NEW.claim_seq);
                END IF;

                RETURN NEW;
            END;
            $func$
            $ddl$,
            v_schema
        );
    END LOOP;
END;
$patch$;
