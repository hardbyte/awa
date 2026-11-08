-- Repeatable prune_controls patch: operator maintenance.prune drain control.
-- ADR-051 / https://github.com/hardbyte/awa/pull/662, Phase 1.
-- Additive: N-1 binaries ignore the control. Upgrade or fence every possible
-- maintenance leader before relying on a fleet halt. No hot-table DDL or
-- substrate/claim-function refresh; installation cost is independent of job count.

CREATE TABLE IF NOT EXISTS awa.runtime_controls (
    scope text PRIMARY KEY CHECK (scope = 'maintenance.prune'),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    halt_id uuid,
    mode text NOT NULL DEFAULT 'drain' CHECK (mode = 'drain'),
    reason text NOT NULL DEFAULT '',
    actor text NOT NULL DEFAULT session_user,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO awa.runtime_controls(scope) VALUES ('maintenance.prune') ON CONFLICT DO NOTHING;

ALTER TABLE awa.runtime_instances ADD COLUMN IF NOT EXISTS runtime_control_observation jsonb;

-- Direct mutation is never part of the producer/runtime contract, including
-- installations whose owner has broad ALTER DEFAULT PRIVILEGES grants.
DO $permissions$
DECLARE grantee_name text; had_select boolean;
BEGIN
    REVOKE ALL ON awa.runtime_controls FROM PUBLIC;
    FOR grantee_name IN
        SELECT DISTINCT r.rolname FROM pg_catalog.pg_class c
        CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) a
        JOIN pg_catalog.pg_roles r ON r.oid = a.grantee
        WHERE c.oid = 'awa.runtime_controls'::regclass AND a.grantee <> c.relowner
    LOOP
        had_select := has_table_privilege(grantee_name, 'awa.runtime_controls', 'SELECT');
        EXECUTE format('REVOKE ALL ON awa.runtime_controls FROM %I', grantee_name);
        IF had_select THEN EXECUTE format('GRANT SELECT ON awa.runtime_controls TO %I', grantee_name); END IF;
    END LOOP;
END
$permissions$;

-- Preserve intentional operator grants on replay, but scrub inherited default
-- EXECUTE grants when each function is first installed. The migrator grants
-- EXECUTE to its chosen operator role explicitly; no CREATEROLE is required.
DO $installation$
DECLARE
    new_halt boolean := to_regprocedure('awa.halt_runtime_scope(text,text,text)') IS NULL;
    new_resume boolean := to_regprocedure('awa.resume_runtime_scope(text,uuid)') IS NULL;
    routine regprocedure;
    grantee_name text;
BEGIN
    EXECUTE $ddl$
    CREATE OR REPLACE FUNCTION awa.halt_runtime_scope(p_scope text, p_reason text, p_mode text DEFAULT 'drain')
    RETURNS SETOF awa.runtime_controls
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
    AS $function$
    BEGIN
        IF p_scope IS DISTINCT FROM 'maintenance.prune' OR p_mode IS DISTINCT FROM 'drain' THEN
            RAISE EXCEPTION 'only maintenance.prune with mode drain is installed' USING ERRCODE = '22023';
        END IF;
        IF p_reason IS NULL OR btrim(p_reason) = '' THEN
            RAISE EXCEPTION 'a halt reason is required' USING ERRCODE = '22023';
        END IF;
        RETURN QUERY UPDATE awa.runtime_controls c
        SET revision = c.revision + 1, halt_id = gen_random_uuid(), mode = 'drain',
            reason = p_reason, actor = session_user, updated_at = clock_timestamp()
        WHERE c.scope = p_scope RETURNING c.*;
        IF NOT FOUND THEN RAISE EXCEPTION 'missing runtime control row' USING ERRCODE = '55000'; END IF;
    END
    $function$;
    $ddl$;
    EXECUTE $ddl$
    CREATE OR REPLACE FUNCTION awa.resume_runtime_scope(p_scope text, p_halt_id uuid)
    RETURNS SETOF awa.runtime_controls
    LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
    AS $function$
    BEGIN
        IF p_scope IS DISTINCT FROM 'maintenance.prune' OR p_halt_id IS NULL THEN
            RAISE EXCEPTION 'maintenance.prune and its current halt ID are required' USING ERRCODE = '22023';
        END IF;
        RETURN QUERY UPDATE awa.runtime_controls c
        SET revision = c.revision + 1, halt_id = NULL, reason = '',
            actor = session_user, updated_at = clock_timestamp()
        WHERE c.scope = p_scope AND c.halt_id = p_halt_id RETURNING c.*;
        IF NOT FOUND THEN RAISE EXCEPTION 'halt ID is no longer current' USING ERRCODE = '55000'; END IF;
    END
    $function$;
    $ddl$;
    FOR routine IN
        SELECT p.oid::regprocedure FROM pg_catalog.pg_proc p
        WHERE (new_halt AND p.oid = 'awa.halt_runtime_scope(text,text,text)'::regprocedure)
           OR (new_resume AND p.oid = 'awa.resume_runtime_scope(text,uuid)'::regprocedure)
    LOOP
        EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', routine);
        FOR grantee_name IN
            SELECT DISTINCT r.rolname FROM pg_catalog.pg_proc p
            CROSS JOIN LATERAL pg_catalog.aclexplode(p.proacl) a
            JOIN pg_catalog.pg_roles r ON r.oid = a.grantee
            WHERE p.oid = routine AND a.grantee <> p.proowner
        LOOP
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM %I', routine, grantee_name);
        END LOOP;
    END LOOP;
END
$installation$;

-- Installed last, so partial external execution remains visibly pending.
CREATE OR REPLACE FUNCTION awa.prune_controls_protocol_version() RETURNS integer
LANGUAGE sql IMMUTABLE AS $protocol$ SELECT 1 $protocol$;
