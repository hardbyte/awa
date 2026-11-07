CREATE OR REPLACE FUNCTION awa.repair_lane_sequence(p_schema TEXT, p_sequence TEXT)
RETURNS TABLE (
    sequence_name TEXT,
    owner TEXT,
    expected_owner TEXT,
    outcome TEXT,
    missing_grants TEXT[],
    reassign_sql TEXT,
    repair_roles TEXT[]
)
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
SET lock_timeout = '300ms'
AS $repair$
DECLARE
    v_problem RECORD;
    v_remaining RECORD;
    v_busy BOOLEAN := false;
    v_set_mode TEXT := CASE WHEN current_setting('server_version_num')::int >= 160000
                            THEN 'SET' ELSE 'MEMBER' END;
    v_statement TEXT;
BEGIN
    SELECT * INTO v_problem
    FROM awa.lane_sequence_problems(p_schema, p_sequence) AS problem;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    BEGIN
        -- ALTER ... OWNER TO needs the current owner's privileges, SET ROLE to
        -- the new owner, and the new owner's CREATE on the schema (which a
        -- schema owner has). Check first so an unrelated owner is skipped
        -- without waiting for the lock.
        IF v_problem.reassign_sql IS NOT NULL
           AND pg_has_role(current_user, to_regrole(quote_ident(v_problem.owner)), 'USAGE')
           AND pg_has_role(current_user, to_regrole(quote_ident(v_problem.expected_owner)), v_set_mode) THEN
            BEGIN
                EXECUTE v_problem.reassign_sql;
            EXCEPTION WHEN insufficient_privilege THEN
                NULL;
            END;
        END IF;

        SELECT * INTO v_remaining
        FROM awa.lane_sequence_problems(p_schema, p_sequence) AS problem;
        IF FOUND
           AND cardinality(v_remaining.missing_grants) > 0
           AND pg_has_role(current_user, to_regrole(quote_ident(v_remaining.owner)), 'USAGE') THEN
            FOREACH v_statement IN ARRAY v_remaining.missing_grants LOOP
                BEGIN
                    EXECUTE v_statement;
                EXCEPTION WHEN insufficient_privilege THEN
                    NULL;
                END;
            END LOOP;
        END IF;
    EXCEPTION WHEN lock_not_available THEN
        v_busy := true;
    END;

    SELECT * INTO v_remaining
    FROM awa.lane_sequence_problems(p_schema, p_sequence) AS problem;
    sequence_name := v_problem.sequence_name;
    expected_owner := v_problem.expected_owner;
    IF FOUND THEN
        owner := v_remaining.owner;
        missing_grants := v_remaining.missing_grants;
        reassign_sql := v_remaining.reassign_sql;
        repair_roles := v_remaining.repair_roles;
        outcome := CASE WHEN v_busy THEN 'busy'
                        WHEN cardinality(v_remaining.missing_grants) = 0 THEN 'owner_unchanged'
                        ELSE 'unrepaired' END;
    ELSE
        owner := v_problem.expected_owner;
        missing_grants := ARRAY[]::TEXT[];
        reassign_sql := NULL;
        repair_roles := ARRAY[]::TEXT[];
        outcome := 'repaired';
    END IF;
    RETURN NEXT;
END
$repair$;
