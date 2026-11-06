-- v054: Planner statistics for the queue-storage tables autovacuum never analyses (#631)
--
-- Autovacuum does not analyse partitioned tables, so the ring parents
-- (ready_entries, done_entries, leases, ...) have no inherited statistics. It
-- also never reaches the autoanalyze threshold on the two small head tables
-- (queue_claim_heads, queue_enqueue_heads), so the planner prices lane joins
-- against them at its defaults (320 rows, n_distinct 200) while they drive
-- joins over hundreds of thousands of retained rows.
--
-- The maintenance leader refreshes both on an interval. Runtimes usually
-- connect as a role that does not own the tables, and ANALYZE skips with a
-- warning a table its caller may not analyse, so the refresh runs through
-- this SECURITY DEFINER function owned by the migration role, which owns the
-- tables.
--
-- The function analyses one relation per call and the caller runs each call as
-- its own transaction, so the locks ANALYZE takes on a partitioned table's
-- children are held for that table rather than for the whole refresh.
--
-- Locking on a partitioned table, per the ANALYZE documentation: SKIP_LOCKED
-- applies when ANALYZE starts work on a relation, so a child that ring prune
-- holds ACCESS EXCLUSIVE is skipped during the per-child analysis, but
-- sampling the children for the parent's statistics can still wait for it.
-- That wait is bounded by the function's lock_timeout and fails the call with
-- SQLSTATE 55P03, which the caller records as skipped_lock. SKIP_LOCKED also
-- skips every partition silently when the parent itself is locked, so the
-- function takes the parent's SHARE UPDATE EXCLUSIVE lock first and lets that
-- wait fail instead.
--
-- Definer boundary: only a head table or ring parent of a schema that
-- awa.queue_storage_schemas() reports is analysed, which requires the schema's
-- ownership, grants and triggers to be trusted, and SQL is built from nothing
-- but quoted identifiers. ANALYZE writes statistics and nothing else.
-- Only the fixed list of ring parents and the two head tables is accepted;
-- a migration that adds a partitioned table to the substrate must extend it.
--
-- Who may call it: the lock above lasts until the caller's transaction ends
-- and blocks ring prune of that table, so EXECUTE is not granted to PUBLIC. It
-- goes to the roles that already hold INSERT, UPDATE and DELETE on the head
-- tables of the awa schema (the runtime grantees), and the function itself
-- requires the calling session user to hold those privileges on the target
-- schema's queue_claim_heads. A role created later needs the documented
-- `GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA awa` (or default privileges).
--
-- Returns 'analyzed'; 'skipped_privilege' when the function owner may not
-- analyse the relation; or 'missing' when the relation does not exist.
--
-- N-1 binaries never call it. A binary newer than this schema probes for the
-- function, logs once that it is missing and skips the refresh. Creating or
-- replacing it takes no table lock and completes in milliseconds; calling it
-- takes SHARE UPDATE EXCLUSIVE on the relation until the caller's transaction
-- ends.

CREATE OR REPLACE FUNCTION awa.analyze_queue_storage_relation(
    p_schema   TEXT,
    p_relation TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '2s'
AS $$
DECLARE
    v_heads       REGCLASS;
    v_relid       OID;
    v_relkind     "char";
    v_relowner    OID;
    v_may_analyze BOOLEAN;
BEGIN
    -- awa_statistics_helper_v1: bump when the definition or grants change.
    IF NOT EXISTS (
        SELECT 1 FROM awa.queue_storage_schemas() AS substrate
        WHERE substrate.schema_name = p_schema
    ) THEN
        RAISE EXCEPTION '% is not a queue-storage schema', p_schema
            USING ERRCODE = '22023';
    END IF;

    IF p_relation <> ALL (ARRAY[
            'queue_claim_heads',
            'queue_enqueue_heads',
            'done_entries',
            'lease_claim_batches',
            'lease_claim_closure_batches',
            'lease_claim_closures',
            'lease_claims',
            'leases',
            'queue_terminal_count_deltas',
            'ready_claim_attempt_batches',
            'ready_entries',
            'ready_segments',
            'ready_tombstones',
            'receipt_completion_batches',
            'receipt_completion_tombstones'
        ]) THEN
        RAISE EXCEPTION '%.% is not a ring parent or head table of a queue-storage schema',
            p_schema, p_relation
            USING ERRCODE = '22023';
    END IF;

    v_heads := to_regclass(format('%I.queue_claim_heads', p_schema));
    IF v_heads IS NULL
       OR NOT has_table_privilege(session_user, v_heads, 'INSERT')
       OR NOT has_table_privilege(session_user, v_heads, 'UPDATE')
       OR NOT has_table_privilege(session_user, v_heads, 'DELETE') THEN
        RAISE EXCEPTION 'permission denied: % may not maintain queue storage in schema %',
            session_user, p_schema
            USING ERRCODE = '42501';
    END IF;

    v_relid := to_regclass(format('%I.%I', p_schema, p_relation));
    IF v_relid IS NULL THEN
        RETURN 'missing';
    END IF;

    SELECT class.relkind, class.relowner INTO v_relkind, v_relowner
    FROM pg_class AS class WHERE class.oid = v_relid;

    -- ANALYZE skips, with a warning rather than an error, a table the caller
    -- may not analyse: the owner, a MAINTAIN holder (PostgreSQL 17+), a
    -- member of pg_maintain, or the database owner may analyse it.
    SELECT
        pg_has_role(current_user, v_relowner, 'USAGE')
        OR pg_has_role(
            current_user,
            (SELECT database.datdba FROM pg_database AS database
             WHERE database.datname = current_database()),
            'USAGE'
        )
        OR CASE
            WHEN current_setting('server_version_num')::int >= 170000
                THEN has_table_privilege(v_relid, 'MAINTAIN')
            ELSE FALSE
        END
    INTO v_may_analyze;

    IF NOT v_may_analyze THEN
        RETURN 'skipped_privilege';
    END IF;

    IF v_relkind = 'p' THEN
        EXECUTE format(
            'LOCK TABLE ONLY %I.%I IN SHARE UPDATE EXCLUSIVE MODE', p_schema, p_relation
        );
        EXECUTE format('ANALYZE (SKIP_LOCKED) %I.%I', p_schema, p_relation);
    ELSE
        EXECUTE format('ANALYZE %I.%I', p_schema, p_relation);
    END IF;
    RETURN 'analyzed';
END
$$;

REVOKE EXECUTE ON FUNCTION awa.analyze_queue_storage_relation(TEXT, TEXT) FROM PUBLIC;

DO $grant$
DECLARE
    v_grantee TEXT;
BEGIN
    FOR v_grantee IN
        SELECT pg_get_userbyid(privilege.grantee)
        FROM pg_class AS heads
        CROSS JOIN LATERAL aclexplode(heads.relacl) AS privilege
        WHERE heads.oid = to_regclass('awa.queue_claim_heads')
          AND privilege.grantee <> 0
          AND privilege.privilege_type IN ('INSERT', 'UPDATE', 'DELETE')
        GROUP BY privilege.grantee
        HAVING count(DISTINCT privilege.privilege_type) = 3
    LOOP
        EXECUTE format(
            'GRANT EXECUTE ON FUNCTION awa.analyze_queue_storage_relation(text, text) TO %I',
            v_grantee
        );
    END LOOP;
END
$grant$;

INSERT INTO awa.schema_version (version, description)
VALUES (54, 'Planner statistics refresh for queue-storage tables autovacuum never analyses')
ON CONFLICT (version) DO NOTHING;
