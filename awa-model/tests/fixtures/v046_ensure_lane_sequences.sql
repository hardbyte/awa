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
