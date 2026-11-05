-- v053: Planner statistics for the queue-storage tables autovacuum never analyses
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
-- SKIP_LOCKED skips a child that ring prune holds ACCESS EXCLUSIVE instead of
-- waiting for it and aborting the statistics gathered so far.
--
-- Definer boundary: the function only analyses a head table or a partitioned
-- table of a schema that awa.queue_storage_schemas() reports, which requires
-- the schema's ownership, grants and triggers to be trusted, and builds SQL
-- from nothing but quoted identifiers. ANALYZE writes statistics and nothing
-- else.
--
-- Returns 'analyzed'; 'skipped_privilege' when the function owner may not
-- analyse the relation; or 'missing' when the relation does not exist.
--
-- N-1 binaries never call it. A binary newer than this schema logs once that
-- the function is missing and skips the refresh. Completes in milliseconds and
-- takes no lock on any queue-storage table.

CREATE OR REPLACE FUNCTION awa.analyze_queue_storage_relation(
    p_schema   TEXT,
    p_relation TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    v_relid       OID;
    v_relkind     "char";
    v_may_analyze BOOLEAN;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM awa.queue_storage_schemas() AS substrate
        WHERE substrate.schema_name = p_schema
    ) THEN
        RAISE EXCEPTION '% is not a queue-storage schema', p_schema
            USING ERRCODE = '22023';
    END IF;

    v_relid := to_regclass(format('%I.%I', p_schema, p_relation));
    IF v_relid IS NULL THEN
        RETURN 'missing';
    END IF;

    SELECT class.relkind INTO v_relkind FROM pg_class AS class WHERE class.oid = v_relid;
    IF NOT (
        v_relkind = 'p'
        OR p_relation IN ('queue_claim_heads', 'queue_enqueue_heads')
    ) THEN
        RAISE EXCEPTION '%.% is not a partitioned or head table of a queue-storage schema',
            p_schema, p_relation
            USING ERRCODE = '22023';
    END IF;

    -- ANALYZE skips, with a warning rather than an error, a table the caller
    -- may not analyse: the owner, a MAINTAIN holder (PostgreSQL 17+), a
    -- member of pg_maintain, or the database owner may analyse it.
    SELECT
        pg_has_role(current_user, class.relowner, 'USAGE')
        OR pg_has_role(
            current_user,
            (SELECT database.datdba FROM pg_database AS database
             WHERE database.datname = current_database()),
            'USAGE'
        )
        OR CASE
            WHEN current_setting('server_version_num')::int >= 170000
                THEN has_table_privilege(class.oid, 'MAINTAIN')
            ELSE FALSE
        END
    INTO v_may_analyze
    FROM pg_class AS class
    WHERE class.oid = v_relid;

    IF NOT v_may_analyze THEN
        RETURN 'skipped_privilege';
    END IF;

    IF v_relkind = 'p' THEN
        EXECUTE format('ANALYZE (SKIP_LOCKED) %I.%I', p_schema, p_relation);
    ELSE
        EXECUTE format('ANALYZE %I.%I', p_schema, p_relation);
    END IF;
    RETURN 'analyzed';
END
$$;

GRANT EXECUTE ON FUNCTION awa.analyze_queue_storage_relation(TEXT, TEXT) TO PUBLIC;

INSERT INTO awa.schema_version (version, description)
VALUES (53, 'Planner statistics refresh for queue-storage tables autovacuum never analyses')
ON CONFLICT (version) DO NOTHING;
