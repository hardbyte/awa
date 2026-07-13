-- v040: allow queue-storage finalization while canonical drain-only runtimes live.
--
-- A pre-flip auto runtime resolves its executor to canonical storage and reports
-- canonical_drain_only after routing enters mixed_transition. Once
-- canonical_live_backlog() reaches zero, that runtime has no supported source
-- of new canonical work: producers and cron enqueue through the active
-- queue-storage route, and all canonical non-terminal states are included in
-- the backlog count. Requiring those idle runtimes to exit added an unnecessary
-- rollout step and a minimum heartbeat-liveness delay before finalization.
--
-- Canonical-only runtimes still block. They were not admitted by the
-- mixed-transition entry gate and may not understand the queue-storage routing
-- contract if they return.

CREATE OR REPLACE FUNCTION awa.storage_finalize()
RETURNS TABLE (
    current_engine TEXT,
    active_engine TEXT,
    prepared_engine TEXT,
    state TEXT,
    transition_epoch BIGINT,
    details JSONB,
    entered_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    finalized_at TIMESTAMPTZ
)
LANGUAGE plpgsql
SET search_path = pg_catalog, awa, public
AS $$
DECLARE
    v_prepared_engine TEXT;
    v_state TEXT;
    v_backlog BIGINT;
    v_live_canonical_count BIGINT;
BEGIN
    SELECT sts.prepared_engine, sts.state
    INTO v_prepared_engine, v_state
    FROM awa.storage_transition_state AS sts
    WHERE sts.singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    IF v_state <> 'mixed_transition' THEN
        RAISE EXCEPTION 'storage finalize is only allowed from mixed_transition state'
            USING ERRCODE = '55000';
    END IF;

    IF v_prepared_engine IS DISTINCT FROM 'queue_storage' THEN
        RAISE EXCEPTION 'prepared engine "%" is not supported by this release', v_prepared_engine
            USING ERRCODE = '22023';
    END IF;

    SELECT awa.canonical_live_backlog()
    INTO v_backlog;

    IF v_backlog > 0 THEN
        RAISE EXCEPTION 'cannot finalize while canonical live backlog is %', v_backlog
            USING ERRCODE = '55000';
    END IF;

    SELECT count(*)::bigint
    INTO v_live_canonical_count
    FROM awa.runtime_instances AS runtime
    WHERE runtime.storage_capability = 'canonical'
      AND runtime.last_seen_at + make_interval(
            secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
          ) >= now();

    IF v_live_canonical_count > 0 THEN
        RAISE EXCEPTION 'cannot finalize while % canonical-only runtime(s) are still live', v_live_canonical_count
            USING ERRCODE = '55000';
    END IF;

    UPDATE awa.storage_transition_state AS sts
    SET
        current_engine = sts.prepared_engine,
        prepared_engine = NULL,
        state = 'active',
        transition_epoch = sts.transition_epoch + 1,
        entered_at = now(),
        updated_at = now(),
        finalized_at = now()
    WHERE sts.singleton
      AND sts.state = 'mixed_transition';

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (40, 'Allow queue-storage finalization with live canonical drain-only runtimes')
ON CONFLICT (version) DO NOTHING;
