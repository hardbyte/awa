-- v045: owner-scoped periodic reconciliation, #481
-- https://github.com/hardbyte/awa/issues/481
-- N-1 retains additive sync; owned definitions/tombstones survive its UPSERT.
-- Its atomic enqueue is fenced by BEFORE UPDATE returning NULL on retired rows.
-- No hot job/lease table is locked. Catalog-only expansion plus triggers on the
-- small cron/runtime control tables; expected sub-second absent DDL contention.
-- External runners apply the entire file transactionally under AWA_MIGR;
-- no compatibility patch prerequisite is needed for unowned/default operation.
-- Operator actions on owned schedules require current clients (ADR-007).

ALTER TABLE awa.cron_jobs ADD COLUMN IF NOT EXISTS owner_id TEXT;
ALTER TABLE awa.cron_jobs ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;
ALTER TABLE awa.cron_jobs ADD COLUMN IF NOT EXISTS retired_by TEXT;
ALTER TABLE awa.cron_jobs ADD COLUMN IF NOT EXISTS retired_revision TEXT;
ALTER TABLE awa.runtime_instances ADD COLUMN IF NOT EXISTS cron_protocol INTEGER;

CREATE TABLE IF NOT EXISTS awa.cron_owners (
    owner_id TEXT PRIMARY KEY CHECK (length(btrim(owner_id)) BETWEEN 1 AND 200),
    synced_hash TEXT,
    agreed_hash TEXT,
    agreed_since TIMESTAMPTZ,
    evidence_until TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS awa.cron_manifests (
    owner_id TEXT NOT NULL REFERENCES awa.cron_owners(owner_id),
    desired_hash TEXT NOT NULL,
    manifest JSONB NOT NULL,
    PRIMARY KEY(owner_id, desired_hash)
);
CREATE TABLE IF NOT EXISTS awa.cron_declarations (
    instance_id UUID PRIMARY KEY REFERENCES awa.runtime_instances(instance_id) ON DELETE CASCADE,
    owner_id TEXT NOT NULL REFERENCES awa.cron_owners(owner_id),
    revision TEXT NOT NULL,
    desired_hash TEXT NOT NULL,
    grace_ms BIGINT NOT NULL CHECK (grace_ms >= 0),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS cron_declarations_owner ON awa.cron_declarations(owner_id);
CREATE INDEX IF NOT EXISTS cron_jobs_owner ON awa.cron_jobs(owner_id);

CREATE OR REPLACE FUNCTION awa.cron_protocol_version() RETURNS INTEGER
LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$;

CREATE OR REPLACE FUNCTION awa.cron_protocol_lock() RETURNS VOID
LANGUAGE sql VOLATILE AS $$ SELECT pg_advisory_xact_lock(4708307076680609614) $$;

CREATE OR REPLACE FUNCTION awa.cron_lock_evidence() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM awa.cron_protocol_lock();
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS cron_lock_evidence ON awa.runtime_instances;
CREATE TRIGGER cron_lock_evidence BEFORE INSERT OR UPDATE OR DELETE
ON awa.runtime_instances FOR EACH STATEMENT EXECUTE FUNCTION awa.cron_lock_evidence();
DROP TRIGGER IF EXISTS cron_lock_evidence ON awa.cron_declarations;
CREATE TRIGGER cron_lock_evidence BEFORE INSERT OR UPDATE OR DELETE
ON awa.cron_declarations FOR EACH STATEMENT EXECUTE FUNCTION awa.cron_lock_evidence();

CREATE OR REPLACE FUNCTION awa.cron_unknown_runtime() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.cron_protocol IS DISTINCT FROM awa.cron_protocol_version() THEN
        UPDATE awa.cron_owners SET agreed_since = NULL, evidence_until = NULL
        WHERE agreed_since IS NOT NULL OR evidence_until IS NOT NULL;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS cron_unknown_runtime ON awa.runtime_instances;
CREATE TRIGGER cron_unknown_runtime BEFORE INSERT OR UPDATE
ON awa.runtime_instances FOR EACH ROW EXECUTE FUNCTION awa.cron_unknown_runtime();

CREATE OR REPLACE FUNCTION awa.cron_schedule_fence() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
    v_admin BOOLEAN := COALESCE(current_setting('awa.cron_admin', true), '') = 'on';
    v_owner TEXT := current_setting('awa.cron_owner', true);
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.retired_at IS NOT NULL THEN
            RAISE EXCEPTION 'cron schedule % is retired; its tombstone must be retained (restore it explicitly to reactivate)', OLD.name
                USING ERRCODE = '55000';
        END IF;
        IF OLD.owner_id IS NOT NULL THEN
            RAISE EXCEPTION 'cron schedule % has durable ownership; retire it instead of deleting', OLD.name
                USING ERRCODE = '55000';
        END IF;
        RETURN OLD;
    END IF;
    IF v_admin THEN RETURN NEW; END IF;
    IF NEW.owner_id IS DISTINCT FROM OLD.owner_id
       OR NEW.retired_at IS DISTINCT FROM OLD.retired_at
       OR NEW.retired_by IS DISTINCT FROM OLD.retired_by
       OR NEW.retired_revision IS DISTINCT FROM OLD.retired_revision THEN
        RAISE EXCEPTION 'cron lifecycle requires an explicit operator action' USING ERRCODE = '55000';
    END IF;
    IF OLD.retired_at IS NOT NULL THEN RETURN NULL; END IF;
    IF OLD.owner_id IS NOT NULL AND v_owner IS DISTINCT FROM OLD.owner_id
       AND (to_jsonb(NEW) - ARRAY['last_enqueued_at','updated_at','paused_at','paused_by'])
           IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['last_enqueued_at','updated_at','paused_at','paused_by']) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS cron_schedule_fence ON awa.cron_jobs;
CREATE TRIGGER cron_schedule_fence BEFORE UPDATE OR DELETE ON awa.cron_jobs
FOR EACH ROW EXECUTE FUNCTION awa.cron_schedule_fence();

INSERT INTO awa.schema_version(version,description) VALUES (45,'Owner-scoped periodic reconciliation') ON CONFLICT (version) DO NOTHING;
