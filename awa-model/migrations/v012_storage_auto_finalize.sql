-- v012: storage auto-finalize for fresh installs.
--
-- Closes the 0.6 release-readiness UX gap (issue #197): on a fresh
-- cluster with no canonical jobs and no historical workers, the
-- operator should not need to run prepare → enter-mixed-transition →
-- finalize manually. The Auto worker role's existing storage_status
-- check (awa-worker/src/client.rs:893) bails to canonical unless
-- state ∈ {mixed_transition, active}, so without this fast-path a
-- fresh DB sits on canonical until somebody runs the staged commands.
--
-- This migration adds awa.storage_auto_finalize_if_fresh(p_schema TEXT)
-- which atomically advances state from `canonical` directly to
-- `active` when the fresh-install conditions hold:
--
--   * state = 'canonical' (default after install)
--   * prepared_engine IS NULL (no operator commands have run)
--   * awa.jobs is empty (no canonical work has ever been enqueued)
--   * no recently-heartbeating runtime_instances (no live worker fleet)
--
-- It does NOT skip the staged transition for upgrades. Once any
-- canonical work exists or any runtime has heartbeated within the
-- liveness window, the function returns FALSE and the caller falls
-- back to the manual prepare/enter-mixed-transition/finalize path.
--
-- The schema-install side (prepare_schema in Rust) is the caller's
-- responsibility — auto-finalize assumes the queue-storage tables are
-- already present, mirroring the contract of storage_finalize().
-- Workers should call prepare_schema() before invoking this function;
-- prepare_schema is idempotent so concurrent workers are safe.
--
-- The FOR UPDATE on the singleton row serialises with concurrent
-- callers: first worker's call returns TRUE and flips state to
-- `active`; subsequent calls see state = 'active' and return FALSE.

CREATE OR REPLACE FUNCTION awa.storage_auto_finalize_if_fresh(p_schema TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql
SET search_path = pg_catalog, awa
AS $$
DECLARE
    v_state           TEXT;
    v_prepared_engine TEXT;
    v_canonical_count BIGINT;
    v_runtime_count   BIGINT;
    v_schema          TEXT := COALESCE(NULLIF(btrim(p_schema), ''), 'awa_exp');
BEGIN
    SELECT state, prepared_engine
    INTO v_state, v_prepared_engine
    FROM awa.storage_transition_state
    WHERE singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    -- Already past canonical: nothing to do.
    IF v_state IS DISTINCT FROM 'canonical' THEN
        RETURN FALSE;
    END IF;

    -- Operator has explicitly prepared a target engine; respect that
    -- and let the staged path run.
    IF v_prepared_engine IS NOT NULL THEN
        RETURN FALSE;
    END IF;

    -- Any canonical work means this isn't a fresh install. Including
    -- terminal-state rows is intentional: even one finalized job means
    -- canonical was the active engine at some point and the operator
    -- should opt into the staged transition.
    SELECT count(*) INTO v_canonical_count FROM awa.jobs;
    IF v_canonical_count > 0 THEN
        RETURN FALSE;
    END IF;

    -- Any recently-live runtime means a worker fleet is heartbeating
    -- already — we should not skip the staged transition under them.
    -- The 90-second window matches the heartbeat-staleness gate used
    -- by storage_enter_mixed_transition.
    SELECT count(*) INTO v_runtime_count
    FROM awa.runtime_instances
    WHERE last_seen_at + make_interval(secs => 90) >= now();
    IF v_runtime_count > 0 THEN
        RETURN FALSE;
    END IF;

    -- Conditions hold. Promote directly to active. Skipping the
    -- intermediate states is safe specifically because there's no
    -- canonical work to drain and no live workers to coordinate.
    --
    -- NOTE: `active_engine` is derived in `awa.storage_status()` /
    -- `awa.active_storage_engine()` — it's not a column. To land in
    -- `active_engine = 'queue_storage'` we set `current_engine =
    -- 'queue_storage'` plus `state = 'active'`; the derivation does
    -- the rest.
    UPDATE awa.storage_transition_state
    SET
        state            = 'active',
        current_engine   = 'queue_storage',
        prepared_engine  = NULL,
        details          = jsonb_build_object(
                              'schema', v_schema,
                              'auto_finalized', true
                           ),
        transition_epoch = transition_epoch + 1,
        entered_at       = now(),
        updated_at       = now(),
        finalized_at     = now()
    WHERE singleton
      AND state = 'canonical';

    INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
    VALUES ('queue_storage', v_schema, now())
    ON CONFLICT (backend) DO UPDATE
    SET schema_name = EXCLUDED.schema_name,
        updated_at  = EXCLUDED.updated_at;

    RETURN TRUE;
END;
$$;

GRANT EXECUTE ON FUNCTION awa.storage_auto_finalize_if_fresh(TEXT) TO PUBLIC;

INSERT INTO awa.schema_version (version, description)
VALUES (12, 'Storage auto-finalize for fresh installs')
ON CONFLICT (version) DO NOTHING;
