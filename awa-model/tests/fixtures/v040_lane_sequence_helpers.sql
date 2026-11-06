-- The lane helpers every 0.6 release installs (v0.6.0 through v0.6.9).

CREATE OR REPLACE FUNCTION __schema__.ensure_lane_sequences(
    p_queue TEXT,
    p_priority SMALLINT,
    p_enqueue_shard SMALLINT
)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $func$
DECLARE
    v_enqueue_seq TEXT := __schema__.queue_lane_sequence_name(
        'queue_enqueue_seq',
        p_queue,
        p_priority,
        p_enqueue_shard
    );
    v_claim_seq TEXT := __schema__.queue_lane_sequence_name(
        'queue_claim_seq',
        p_queue,
        p_priority,
        p_enqueue_shard
    );
BEGIN
    EXECUTE format(
        'CREATE SEQUENCE IF NOT EXISTS %I.%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
        '__schema__',
        v_enqueue_seq
    );
    EXECUTE format(
        'CREATE SEQUENCE IF NOT EXISTS %I.%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
        '__schema__',
        v_claim_seq
    );

    UPDATE __schema__.queue_enqueue_heads
    SET seq_name = v_enqueue_seq
    WHERE queue = p_queue
      AND priority = p_priority
      AND enqueue_shard = p_enqueue_shard
      AND seq_name IS DISTINCT FROM v_enqueue_seq;

    UPDATE __schema__.queue_claim_heads
    SET seq_name = v_claim_seq
    WHERE queue = p_queue
      AND priority = p_priority
      AND enqueue_shard = p_enqueue_shard
      AND seq_name IS DISTINCT FROM v_claim_seq;
END;
$func$;

CREATE OR REPLACE FUNCTION __schema__.queue_enqueue_head_sequence_sync()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $func$
DECLARE
    v_seq_name TEXT := __schema__.queue_lane_sequence_name(
        'queue_enqueue_seq',
        NEW.queue,
        NEW.priority,
        NEW.enqueue_shard
    );
    v_qualified_seq TEXT := format('%I.%I', '__schema__', v_seq_name);
    v_count BIGINT;
    v_start BIGINT;
BEGIN
    EXECUTE format(
        'CREATE SEQUENCE IF NOT EXISTS %I.%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
        '__schema__',
        v_seq_name
    );
    NEW.seq_name := v_seq_name;

    IF TG_OP = 'UPDATE'
       AND NEW.next_seq IS DISTINCT FROM OLD.next_seq
       AND NEW.next_seq > OLD.next_seq
    THEN
        v_count := NEW.next_seq - OLD.next_seq;
        PERFORM pg_advisory_lock(hashtextextended(v_qualified_seq, 0));
        BEGIN
            EXECUTE format(
                'SELECT nextval(%L::regclass)::bigint',
                v_qualified_seq
            )
            INTO v_start;

            IF v_count > 1 THEN
                EXECUTE format(
                    'SELECT setval(%L::regclass, %s, true)',
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
        PERFORM __schema__.set_sequence_next(v_seq_name, NEW.next_seq);
    END IF;

    RETURN NEW;
END;
$func$;

CREATE OR REPLACE FUNCTION __schema__.queue_claim_head_sequence_sync()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $func$
DECLARE
    v_seq_name TEXT := __schema__.queue_lane_sequence_name(
        'queue_claim_seq',
        NEW.queue,
        NEW.priority,
        NEW.enqueue_shard
    );
BEGIN
    EXECUTE format(
        'CREATE SEQUENCE IF NOT EXISTS %I.%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
        '__schema__',
        v_seq_name
    );
    NEW.seq_name := v_seq_name;

    IF TG_OP = 'INSERT' THEN
        PERFORM __schema__.set_sequence_next(v_seq_name, NEW.claim_seq);
    END IF;

    RETURN NEW;
END;
$func$;
