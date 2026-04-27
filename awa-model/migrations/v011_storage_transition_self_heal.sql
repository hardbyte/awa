-- Self-heal v010's storage transition metadata.
--
-- v010 introduced awa.storage_transition_state (a singleton row) plus
-- awa.active_storage_engine() and awa.assert_writable_canonical_storage().
-- Two failure modes were observed in the wild on 0.5.5:
--
--   1. The singleton row could be missing (concurrent test fixtures that
--      truncate awa tables, partial v010 application that wrote the table
--      but not the seed row, logical dump/restore that omitted the table,
--      etc.). active_storage_engine() then returned NULL.
--   2. NULL leaked into assert_writable_canonical_storage() and the user
--      saw `storage engine "<NULL>" is not writable in this release`.
--      The multi-row CTE insert path silently inserted zero rows because
--      both `engine = 'canonical'` and `engine <> 'canonical'` are false
--      against NULL.
--
-- This migration is purely defensive:
--
--   * Re-seed the singleton row idempotently.
--   * Replace active_storage_engine() with a NULL-safe version that
--     coalesces a missing/blank engine to 'canonical' (the only writable
--     engine in 0.5.x).
--   * Harden assert_writable_canonical_storage() to treat NULL as
--     canonical (belt-and-braces; not strictly needed once #2 lands).

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
    SELECT COALESCE(
        (
            SELECT NULLIF(
                btrim(
                    CASE
                        WHEN state IN ('mixed_transition', 'active')
                            THEN COALESCE(prepared_engine, current_engine)
                        ELSE current_engine
                    END
                ),
                ''
            )
            FROM awa.storage_transition_state
            WHERE singleton
        ),
        'canonical'
    )
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
    active_engine := COALESCE(awa.active_storage_engine(), 'canonical');

    IF active_engine <> 'canonical' THEN
        RAISE EXCEPTION 'storage engine "%" is not writable in this release', active_engine
            USING ERRCODE = '55000';
    END IF;

    RETURN TRUE;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (11, 'Storage transition self-heal: NULL-safe engine resolution and singleton re-seed')
ON CONFLICT (version) DO NOTHING;
