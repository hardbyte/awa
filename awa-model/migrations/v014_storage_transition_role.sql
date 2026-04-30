-- Tighten the mixed-transition gate to require a runtime that will
-- actually execute queue-storage work *after* routing flips, not just
-- one that currently reports queue-storage capability.
--
-- ADR-context: `correctness/storage/AwaStorageTransition.tla` invariant
-- `MixedHasQueueExecutor` fails under `AwaStorageTransitionCurrentGate.cfg`
-- because the previous gate only checked
-- `runtime_instances.storage_capability = 'queue_storage'`. An auto-role
-- 0.6 runtime that started while state was canonical/prepared resolves
-- its effective storage to canonical at startup, but reports
-- `storage_capability = 'queue_storage'` in its snapshot. After
-- `enter_mixed_transition` flips routing, that auto runtime
-- re-evaluates and downgrades to `canonical_drain_only`, leaving the
-- cluster in mixed_transition with zero queue-storage executors.
--
-- Fix: persist the operator-selected `transition_role` for each
-- runtime (auto / canonical_drain / queue_storage_target) and require
-- at least one live `queue_storage_target` before allowing the
-- transition to mixed_transition. Auto runtimes started after the
-- routing flip will already resolve to queue_storage at startup, so
-- the operator only needs one explicit target to cross the gate.

ALTER TABLE awa.runtime_instances
    ADD COLUMN IF NOT EXISTS transition_role TEXT NOT NULL DEFAULT 'auto';

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_awa_runtime_instances_transition_role'
    ) THEN
        ALTER TABLE awa.runtime_instances
        ADD CONSTRAINT chk_awa_runtime_instances_transition_role
        CHECK (transition_role IN ('auto', 'canonical_drain', 'queue_storage_target'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_awa_runtime_instances_transition_role
    ON awa.runtime_instances (transition_role, last_seen_at);

CREATE OR REPLACE FUNCTION awa.storage_enter_mixed_transition()
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
    v_details JSONB;
    v_schema TEXT;
    v_live_canonical_count BIGINT;
    v_live_queue_target_count BIGINT;
BEGIN
    SELECT sts.prepared_engine, sts.state, sts.details
    INTO v_prepared_engine, v_state, v_details
    FROM awa.storage_transition_state AS sts
    WHERE sts.singleton
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage transition state row is missing'
            USING ERRCODE = '55000';
    END IF;

    IF v_state <> 'prepared' THEN
        RAISE EXCEPTION 'storage enter-mixed-transition is only allowed from prepared state'
            USING ERRCODE = '55000';
    END IF;

    IF v_prepared_engine IS DISTINCT FROM 'queue_storage' THEN
        RAISE EXCEPTION 'prepared engine "%" is not supported by this release', v_prepared_engine
            USING ERRCODE = '22023';
    END IF;

    v_schema := COALESCE(NULLIF(v_details->>'schema', ''), 'awa');

    IF to_regclass(format('%I.%I', v_schema, 'queue_ring_state')) IS NULL
        OR to_regclass(format('%I.%I', v_schema, 'ready_entries')) IS NULL
        OR to_regclass(format('%I.%I', v_schema, 'leases')) IS NULL
    THEN
        RAISE EXCEPTION 'queue storage schema "%" is not prepared', v_schema
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
        RAISE EXCEPTION 'cannot enter mixed transition while % canonical-only runtime(s) are still live', v_live_canonical_count
            USING ERRCODE = '55000';
    END IF;

    -- A runtime that will actually execute queue-storage work after the
    -- routing flip. Pre-flip auto runtimes report
    -- storage_capability='queue_storage' but downgrade to drain-only on
    -- the next snapshot once routing flips, so storage_capability alone
    -- is not a witness for post-flip executor liveness.
    SELECT count(*)::bigint
    INTO v_live_queue_target_count
    FROM awa.runtime_instances AS runtime
    WHERE runtime.transition_role = 'queue_storage_target'
      AND runtime.storage_capability = 'queue_storage'
      AND runtime.last_seen_at + make_interval(
            secs => GREATEST(((GREATEST(runtime.snapshot_interval_ms, 1000) / 1000) * 3)::int, 30)
          ) >= now();

    IF v_live_queue_target_count = 0 THEN
        RAISE EXCEPTION 'cannot enter mixed transition without a live runtime running with transition_role=queue_storage_target; auto-role runtimes downgrade to drain-only after routing flips'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
    VALUES ('queue_storage', v_schema, now())
    ON CONFLICT (backend)
    DO UPDATE SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at;

    UPDATE awa.storage_transition_state AS sts
    SET
        state = 'mixed_transition',
        transition_epoch = sts.transition_epoch + 1,
        entered_at = now(),
        updated_at = now(),
        finalized_at = NULL
    WHERE sts.singleton
      AND sts.state = 'prepared';

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END;
$$;

INSERT INTO awa.schema_version (version, applied_at, description)
VALUES (14, now(), 'Storage transition role tracking and tightened mixed-transition gate')
ON CONFLICT (version) DO NOTHING;
