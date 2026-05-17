-- v020: make active queue-storage schema resolution authoritative.
--
-- `runtime_storage_backends` is an auxiliary marker used by the staged
-- transition helpers, but the source of truth for the active engine is
-- `storage_transition_state`. If the marker row is missing while the
-- transition state is already active queue_storage, producers must still
-- route through queue storage instead of silently writing canonical rows
-- that queue-storage workers will never claim.

CREATE OR REPLACE FUNCTION awa.active_queue_storage_schema()
RETURNS TEXT AS $$
DECLARE
    v_schema TEXT;
BEGIN
    SELECT COALESCE(
        NULLIF(sts.details->>'schema', ''),
        (
            SELECT rsb.schema_name
            FROM awa.runtime_storage_backends AS rsb
            WHERE rsb.backend = 'queue_storage'
        ),
        'awa'
    )
    INTO v_schema
    FROM awa.storage_transition_state AS sts
    WHERE sts.singleton
      AND awa.active_storage_engine() = 'queue_storage';

    RETURN v_schema;
END;
$$ LANGUAGE plpgsql STABLE
SET search_path = pg_catalog, awa, public;

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    SELECT awa.active_queue_storage_schema()
    INTO v_schema;

    IF v_schema IS NOT NULL THEN
        INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
        VALUES ('queue_storage', v_schema, now())
        ON CONFLICT (backend) DO UPDATE
        SET schema_name = EXCLUDED.schema_name,
            updated_at = EXCLUDED.updated_at;
    END IF;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (20, 'Derive active queue-storage schema from transition state')
ON CONFLICT (version) DO NOTHING;
