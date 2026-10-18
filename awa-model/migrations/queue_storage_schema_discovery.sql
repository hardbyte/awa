CREATE OR REPLACE FUNCTION awa.queue_storage_schema_is_trusted(p_schema TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $trusted$
    WITH trusted_owners AS (
        SELECT nspowner AS owner FROM pg_namespace WHERE nspname = 'awa'
        UNION
        SELECT oid FROM pg_roles WHERE rolname = current_user
    )
    SELECT NOT EXISTS (
        SELECT 1 FROM pg_namespace AS namespace
        WHERE namespace.nspname = p_schema
          AND (
              namespace.nspname IN ('public', 'information_schema')
              OR namespace.nspname ~ '^pg_'
              -- Superusers pass every pg_has_role check; authority is identity here.
              OR namespace.nspowner NOT IN (SELECT owner FROM trusted_owners)
              OR EXISTS (
                  SELECT 1
                  FROM aclexplode(COALESCE(namespace.nspacl,
                                          acldefault('n', namespace.nspowner))) AS privilege
                  WHERE privilege.privilege_type = 'CREATE'
                    AND privilege.grantee NOT IN (SELECT owner FROM trusted_owners)
              )
              OR EXISTS (
                  SELECT 1 FROM pg_class AS relation
                  WHERE relation.relnamespace = namespace.oid
                    AND relation.relowner NOT IN (SELECT owner FROM trusted_owners)
              )
              OR EXISTS (
                  SELECT 1 FROM pg_proc AS routine
                  WHERE routine.pronamespace = namespace.oid
                    AND routine.proowner NOT IN (SELECT owner FROM trusted_owners)
              )
              OR EXISTS (
                  SELECT 1 FROM pg_type AS data_type
                  WHERE data_type.typnamespace = namespace.oid
                    AND data_type.typowner NOT IN (SELECT owner FROM trusted_owners)
              )
              OR EXISTS (
                  SELECT 1 FROM pg_class AS relation
                  CROSS JOIN LATERAL aclexplode(COALESCE(relation.relacl,
                      acldefault('r', relation.relowner))) AS privilege
                  WHERE relation.relnamespace = namespace.oid
                    AND privilege.privilege_type = 'TRIGGER'
                    AND privilege.grantee NOT IN (SELECT owner FROM trusted_owners)
              )
              OR EXISTS (
                  SELECT 1 FROM pg_trigger AS trigger
                  JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
                  JOIN pg_proc AS routine ON routine.oid = trigger.tgfoid
                  JOIN pg_namespace AS routine_namespace ON routine_namespace.oid = routine.pronamespace
                  WHERE relation.relnamespace = namespace.oid
                    AND NOT trigger.tgisinternal
                    AND routine_namespace.nspname <> 'pg_catalog'
                    AND routine.proowner NOT IN (SELECT owner FROM trusted_owners)
              )
              OR EXISTS (
                  SELECT 1
                  FROM pg_trigger AS trigger
                  JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
                  JOIN pg_depend AS dependency
                    ON dependency.classid = 'pg_trigger'::regclass
                   AND dependency.objid = trigger.oid
                  WHERE relation.relnamespace = namespace.oid
                    AND NOT trigger.tgisinternal
                    AND CASE dependency.refclassid
                        WHEN 'pg_proc'::regclass THEN EXISTS (
                            SELECT 1 FROM pg_proc AS routine
                            WHERE routine.oid = dependency.refobjid
                              AND routine.pronamespace <> 'pg_catalog'::regnamespace
                              AND routine.proowner NOT IN (SELECT owner FROM trusted_owners)
                        )
                        WHEN 'pg_operator'::regclass THEN EXISTS (
                            SELECT 1
                            FROM pg_operator AS operator
                            LEFT JOIN pg_proc AS routine ON routine.oid = operator.oprcode
                            WHERE operator.oid = dependency.refobjid
                              AND (
                                  (operator.oprnamespace <> 'pg_catalog'::regnamespace
                                   AND operator.oprowner NOT IN (SELECT owner FROM trusted_owners))
                                  OR (routine.pronamespace <> 'pg_catalog'::regnamespace
                                      AND routine.proowner NOT IN (SELECT owner FROM trusted_owners))
                              )
                        )
                        WHEN 'pg_type'::regclass THEN EXISTS (
                            SELECT 1 FROM pg_type AS data_type
                            WHERE data_type.oid = dependency.refobjid
                              AND data_type.typnamespace <> 'pg_catalog'::regnamespace
                              AND data_type.typowner NOT IN (SELECT owner FROM trusted_owners)
                        )
                        WHEN 'pg_cast'::regclass THEN EXISTS (
                            SELECT 1
                            FROM pg_cast AS cast_rule
                            JOIN pg_proc AS routine ON routine.oid = cast_rule.castfunc
                            WHERE cast_rule.oid = dependency.refobjid
                              AND routine.pronamespace <> 'pg_catalog'::regnamespace
                              AND routine.proowner NOT IN (SELECT owner FROM trusted_owners)
                        )
                        WHEN 'pg_class'::regclass THEN FALSE
                        WHEN 'pg_collation'::regclass THEN FALSE
                        WHEN 'pg_constraint'::regclass THEN FALSE
                        WHEN 'pg_trigger'::regclass THEN FALSE
                        ELSE TRUE
                    END
              )
          )
    )
$trusted$;

CREATE OR REPLACE FUNCTION awa.queue_storage_schemas()
RETURNS TABLE (schema_name TEXT)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $discover$
    SELECT namespace.nspname::text
    FROM pg_namespace AS namespace
    WHERE has_schema_privilege(current_user, namespace.oid, 'USAGE')
      AND awa.queue_storage_schema_is_trusted(namespace.nspname)
      AND EXISTS (
          SELECT 1 FROM pg_proc AS claim_fn
          WHERE claim_fn.pronamespace = namespace.oid
            AND claim_fn.proname = 'claim_ready_runtime'
            AND oidvectortypes(claim_fn.proargtypes)
                = 'text, bigint, double precision, double precision'
      )
      AND EXISTS (
          SELECT 1 FROM pg_class AS ring
          WHERE ring.relnamespace = namespace.oid AND ring.relname = 'queue_ring_state'
      )
      AND NOT EXISTS (
          SELECT 1 FROM pg_class AS ring
          WHERE ring.relnamespace = namespace.oid
            AND ring.relname IN ('queue_ring_state', 'lease_ring_state', 'claim_ring_state')
            AND ring.relkind <> 'r'
      )
    ORDER BY namespace.nspname
$discover$;

-- Default-substrate refreshes precede discovery in some migration bundles.
DO $preflight$
DECLARE
    configured_schema TEXT;
BEGIN
    IF NOT awa.queue_storage_schema_is_trusted('awa') THEN
        RAISE EXCEPTION 'unsafe ownership or privileges in the awa schema'
            USING ERRCODE = '42501',
                  HINT = 'Use the migration role or awa schema owner for schema objects; remove foreign-owned objects and untrusted CREATE or TRIGGER grants before migrating.';
    END IF;
    FOR configured_schema IN
        SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'
        UNION
        SELECT NULLIF(details->>'schema', '') FROM awa.storage_transition_state
        WHERE current_engine = 'queue_storage' OR prepared_engine = 'queue_storage'
        UNION
        -- Legacy SQL activation used awa_exp for an unnamed prepared backend.
        SELECT 'awa_exp' FROM awa.storage_transition_state
        WHERE prepared_engine = 'queue_storage' AND NULLIF(details->>'schema', '') IS NULL
    LOOP
        IF configured_schema IS NOT NULL
           AND NOT awa.queue_storage_schema_is_trusted(configured_schema) THEN
            RAISE EXCEPTION 'unsafe configured queue-storage schema: %', configured_schema
                USING ERRCODE = '42501',
                      HINT = 'Review the registered/prepared substrate ownership and grants before upgrading; the global schema version has not advanced.';
        END IF;
    END LOOP;
END
$preflight$;
