-- v065: fleet rate limit overrides with audit and revision (ADR-054).
-- Tracking: https://github.com/hardbyte/awa/issues/718
--
-- Adds to awa.queue_meta, beside ADR-038's override columns:
--
-- * override_fleet_rate_limit and override_fleet_rate_burst, a fleet-wide
--   rate limit each runtime enforces its share of;
-- * override_rate_burst, the burst for ADR-038's per-runtime override_rate_limit;
-- * overrides_revision, incremented by every override write, so a runtime can
--   report which write it applied;
-- * overrides_updated_by and overrides_reason, recorded with every write.
--
-- N-1 binaries are unaffected: they read and write the ADR-038 columns by
-- name. Their override writes leave the new columns as they were and do not
-- advance overrides_revision. Binaries from this release read the new columns
-- when they exist and fall back to the ADR-038 columns when they do not, so
-- they can run before this migration; their override writes need it.
--
-- External SQL runners: no runtime preflight is needed.
--
-- Locks: adding the columns takes ACCESS EXCLUSIVE on awa.queue_meta, a small
-- control table, until the migration commits. overrides_revision is NOT NULL
-- with a constant default, which PostgreSQL adds without rewriting the table;
-- the CHECK constraints scan its rows. When no lock_timeout is set, the lock
-- waits at most 2 s. A timeout (SQLSTATE 55P03) rolls the whole migration back
-- and it can be re-run.
--
-- Wall time: proportional to the number of queues, milliseconds in practice.
--
-- Design: docs/adr/054-fleet-rate-limits.md

DO $$
DECLARE
    v_previous_lock_timeout TEXT := current_setting('lock_timeout');
BEGIN
    IF v_previous_lock_timeout = '0' THEN
        PERFORM set_config('lock_timeout', '2s', true);
    END IF;

    BEGIN
        ALTER TABLE awa.queue_meta
            ADD COLUMN IF NOT EXISTS override_fleet_rate_limit DOUBLE PRECISION
                CHECK (override_fleet_rate_limit IS NULL OR override_fleet_rate_limit > 0),
            ADD COLUMN IF NOT EXISTS override_fleet_rate_burst INTEGER
                CHECK (override_fleet_rate_burst IS NULL OR override_fleet_rate_burst > 0),
            ADD COLUMN IF NOT EXISTS override_rate_burst INTEGER
                CHECK (override_rate_burst IS NULL OR override_rate_burst > 0),
            ADD COLUMN IF NOT EXISTS overrides_revision BIGINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS overrides_updated_by TEXT,
            ADD COLUMN IF NOT EXISTS overrides_reason TEXT;
    EXCEPTION
        WHEN lock_not_available THEN
            RAISE EXCEPTION 'v065 could not lock awa.queue_meta within lock_timeout % to add the fleet rate limit override columns; a long transaction holds it. Nothing was migrated: re-run it',
                current_setting('lock_timeout')
                USING ERRCODE = '55P03';
    END;

    PERFORM set_config('lock_timeout', v_previous_lock_timeout, true);
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (65, 'Fleet rate limit overrides with audit and revision')
ON CONFLICT (version) DO NOTHING;
