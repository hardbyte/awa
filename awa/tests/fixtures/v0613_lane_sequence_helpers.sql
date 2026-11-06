-- The lane-sequence helpers as awa 0.6.13 installs them, before lane
-- provisioning functions existed. Installed under a separate name so a test
-- can put a schema back in its 0.6.13 shape.
CREATE OR REPLACE FUNCTION awa.v0613_refresh_lane_sequence_helpers(p_schema TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog
AS $refresh$
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
        p_schema
    );
END;
$refresh$;
