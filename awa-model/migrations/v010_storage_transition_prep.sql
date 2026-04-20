ALTER TABLE awa.runtime_instances
ADD COLUMN IF NOT EXISTS storage_capability TEXT NOT NULL DEFAULT 'canonical';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'chk_awa_runtime_instances_storage_capability'
          AND conrelid = 'awa.runtime_instances'::regclass
    ) THEN
        ALTER TABLE awa.runtime_instances
        ADD CONSTRAINT chk_awa_runtime_instances_storage_capability
        CHECK (storage_capability IN ('canonical', 'canonical_drain_only', 'queue_storage'));
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS awa.storage_transition_state (
    singleton        BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    current_engine   TEXT        NOT NULL CHECK (btrim(current_engine) <> ''),
    prepared_engine  TEXT,
    state            TEXT        NOT NULL CHECK (state IN ('canonical', 'prepared', 'mixed_transition', 'active')),
    transition_epoch BIGINT      NOT NULL DEFAULT 0,
    details          JSONB       NOT NULL DEFAULT '{}'::jsonb,
    entered_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    finalized_at     TIMESTAMPTZ,
    CHECK (state NOT IN ('prepared', 'mixed_transition') OR prepared_engine IS NOT NULL)
);

INSERT INTO awa.storage_transition_state (
    singleton,
    current_engine,
    prepared_engine,
    state,
    transition_epoch,
    details,
    entered_at,
    updated_at,
    finalized_at
)
VALUES (
    TRUE,
    'canonical',
    NULL,
    'canonical',
    0,
    '{}'::jsonb,
    now(),
    now(),
    NULL
)
ON CONFLICT (singleton) DO NOTHING;

CREATE OR REPLACE FUNCTION awa.active_storage_engine()
RETURNS TEXT
LANGUAGE sql
STABLE
SET search_path = pg_catalog, awa
AS $$
    SELECT CASE
        WHEN state IN ('mixed_transition', 'active') THEN COALESCE(prepared_engine, current_engine)
        ELSE current_engine
    END
    FROM awa.storage_transition_state
    WHERE singleton
$$;

CREATE OR REPLACE FUNCTION awa.storage_status()
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
LANGUAGE sql
STABLE
SET search_path = pg_catalog, awa
AS $$
    SELECT
        current_engine,
        awa.active_storage_engine() AS active_engine,
        prepared_engine,
        state,
        transition_epoch,
        details,
        entered_at,
        updated_at,
        finalized_at
    FROM awa.storage_transition_state
    WHERE singleton
$$;

CREATE OR REPLACE FUNCTION awa.storage_prepare(
    p_engine TEXT,
    p_details JSONB DEFAULT '{}'::jsonb
)
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
SET search_path = pg_catalog, awa
AS $$
DECLARE
    target_engine TEXT := NULLIF(btrim(p_engine), '');
BEGIN
    IF target_engine IS NULL THEN
        RAISE EXCEPTION 'storage engine must not be blank'
            USING ERRCODE = '22023';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM awa.storage_transition_state AS sts
        WHERE sts.singleton
          AND sts.current_engine = target_engine
    ) THEN
        RAISE EXCEPTION 'storage engine "%" is already active', target_engine
            USING ERRCODE = '22023';
    END IF;

    UPDATE awa.storage_transition_state AS sts
    SET
        prepared_engine = target_engine,
        state = 'prepared',
        transition_epoch = CASE
            WHEN sts.state = 'prepared'
             AND sts.prepared_engine = target_engine
             AND sts.details = COALESCE(p_details, '{}'::jsonb)
                THEN sts.transition_epoch
            ELSE sts.transition_epoch + 1
        END,
        details = COALESCE(p_details, '{}'::jsonb),
        entered_at = CASE
            WHEN sts.state = 'prepared'
             AND sts.prepared_engine = target_engine
             AND sts.details = COALESCE(p_details, '{}'::jsonb)
                THEN sts.entered_at
            ELSE now()
        END,
        updated_at = now(),
        finalized_at = NULL
    WHERE sts.singleton
      AND sts.state IN ('canonical', 'prepared');

    IF NOT FOUND THEN
        RAISE EXCEPTION 'storage prepare is only allowed from canonical or prepared state'
            USING ERRCODE = '55000';
    END IF;

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END;
$$;

CREATE OR REPLACE FUNCTION awa.storage_abort()
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
SET search_path = pg_catalog, awa
AS $$
BEGIN
    UPDATE awa.storage_transition_state AS sts
    SET
        prepared_engine = NULL,
        state = 'canonical',
        transition_epoch = CASE
            WHEN sts.state = 'prepared' THEN sts.transition_epoch + 1
            ELSE sts.transition_epoch
        END,
        details = '{}'::jsonb,
        entered_at = CASE
            WHEN sts.state = 'canonical' THEN sts.entered_at
            ELSE now()
        END,
        updated_at = now(),
        finalized_at = NULL
    WHERE sts.singleton
      AND sts.state IN ('canonical', 'prepared');

    RETURN QUERY
    SELECT * FROM awa.storage_status();
END;
$$;

CREATE OR REPLACE FUNCTION awa.assert_writable_canonical_storage()
RETURNS BOOLEAN
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, awa
AS $$
DECLARE
    active_engine TEXT;
BEGIN
    active_engine := awa.active_storage_engine();

    IF active_engine IS DISTINCT FROM 'canonical' THEN
        RAISE EXCEPTION 'storage engine "%" is not writable in this release', active_engine
            USING ERRCODE = '55000';
    END IF;

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION awa.insert_job_compat(
    p_kind TEXT,
    p_queue TEXT DEFAULT 'default',
    p_args JSONB DEFAULT '{}'::jsonb,
    p_state awa.job_state DEFAULT 'available',
    p_priority SMALLINT DEFAULT 2,
    p_max_attempts SMALLINT DEFAULT 25,
    p_run_at TIMESTAMPTZ DEFAULT NULL,
    p_metadata JSONB DEFAULT '{}'::jsonb,
    p_tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    p_unique_key BYTEA DEFAULT NULL,
    p_unique_states BIT(8) DEFAULT NULL
)
RETURNS awa.jobs
LANGUAGE plpgsql
SET search_path = pg_catalog, awa
AS $$
DECLARE
    inserted awa.jobs%ROWTYPE;
BEGIN
    PERFORM awa.assert_writable_canonical_storage();

    INSERT INTO awa.jobs (
        kind,
        queue,
        args,
        state,
        priority,
        max_attempts,
        run_at,
        metadata,
        tags,
        unique_key,
        unique_states
    )
    VALUES (
        p_kind,
        p_queue,
        p_args,
        p_state,
        p_priority,
        p_max_attempts,
        p_run_at,
        p_metadata,
        p_tags,
        p_unique_key,
        p_unique_states
    )
    RETURNING * INTO inserted;

    RETURN inserted;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (10, 'Storage transition metadata and canonical compat routing')
ON CONFLICT (version) DO NOTHING;
