-- Every trigger definition awa has created in the awa schema or in a
-- queue-storage schema, as pg_get_triggerdef renders it under
-- search_path = pg_catalog. New triggers must be added here.
CREATE OR REPLACE FUNCTION awa.queue_storage_canonical_triggers(p_schema TEXT)
RETURNS SETOF TEXT
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
SET quote_all_identifiers = off
AS $canonical$
    SELECT format(template.definition, p_schema)
    FROM (VALUES
        ($def$CREATE TRIGGER queue_claim_heads_sequence_sync BEFORE INSERT OR UPDATE OF claim_seq ON %1$I.queue_claim_heads FOR EACH ROW EXECUTE FUNCTION %1$I.queue_claim_head_sequence_sync()$def$),
        ($def$CREATE TRIGGER queue_enqueue_heads_sequence_sync BEFORE INSERT OR UPDATE OF next_seq ON %1$I.queue_enqueue_heads FOR EACH ROW EXECUTE FUNCTION %1$I.queue_enqueue_head_sequence_sync()$def$),
        ($def$CREATE TRIGGER reject_compat_ring_cursor_update_after_flip BEFORE UPDATE OF current_slot, generation ON %1$I.claim_ring_state FOR EACH ROW EXECUTE FUNCTION %1$I.reject_compat_ring_cursor_update_after_flip()$def$),
        ($def$CREATE TRIGGER reject_compat_ring_cursor_update_after_flip BEFORE UPDATE OF current_slot, generation ON %1$I.lease_ring_state FOR EACH ROW EXECUTE FUNCTION %1$I.reject_compat_ring_cursor_update_after_flip()$def$),
        ($def$CREATE TRIGGER reject_compat_ring_cursor_update_after_flip BEFORE UPDATE OF current_slot, generation ON %1$I.queue_ring_state FOR EACH ROW EXECUTE FUNCTION %1$I.reject_compat_ring_cursor_update_after_flip()$def$)
    ) AS template(definition)
    UNION ALL
    SELECT canonical.definition
    FROM (VALUES
        ($def$CREATE TRIGGER cron_lock_evidence BEFORE INSERT OR DELETE OR UPDATE ON awa.cron_declarations FOR EACH STATEMENT EXECUTE FUNCTION awa.cron_lock_evidence()$def$),
        ($def$CREATE TRIGGER cron_lock_evidence BEFORE INSERT OR DELETE OR UPDATE ON awa.runtime_instances FOR EACH STATEMENT EXECUTE FUNCTION awa.cron_lock_evidence()$def$),
        ($def$CREATE TRIGGER cron_schedule_fence BEFORE DELETE OR UPDATE ON awa.cron_jobs FOR EACH ROW EXECUTE FUNCTION awa.cron_schedule_fence()$def$),
        ($def$CREATE TRIGGER cron_unknown_runtime BEFORE INSERT OR UPDATE ON awa.runtime_instances FOR EACH ROW EXECUTE FUNCTION awa.cron_unknown_runtime()$def$),
        ($def$CREATE TRIGGER trg_awa_jobs_view_delete INSTEAD OF DELETE ON awa.jobs FOR EACH ROW EXECUTE FUNCTION awa.write_jobs_view()$def$),
        ($def$CREATE TRIGGER trg_awa_jobs_view_insert INSTEAD OF INSERT ON awa.jobs FOR EACH ROW EXECUTE FUNCTION awa.write_jobs_view()$def$),
        ($def$CREATE TRIGGER trg_awa_jobs_view_update INSTEAD OF UPDATE ON awa.jobs FOR EACH ROW EXECUTE FUNCTION awa.write_jobs_view()$def$),
        ($def$CREATE TRIGGER trg_awa_notify AFTER INSERT ON awa.jobs_hot FOR EACH ROW WHEN (((new.state = 'available'::awa.job_state) AND (new.run_at <= now()))) EXECUTE FUNCTION awa.notify_new_job()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_admin_metadata AFTER INSERT OR DELETE OR UPDATE ON awa.jobs_hot FOR EACH ROW EXECUTE FUNCTION awa.sync_job_admin_metadata()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_admin_metadata_delete_stmt AFTER DELETE ON awa.jobs_hot REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_delete()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_admin_metadata_insert_stmt AFTER INSERT ON awa.jobs_hot REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_insert()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_admin_metadata_update_stmt AFTER UPDATE ON awa.jobs_hot REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_update()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_dirty_keys_delete AFTER DELETE ON awa.jobs_hot REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.mark_dirty_keys_delete()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_dirty_keys_insert AFTER INSERT ON awa.jobs_hot REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.mark_dirty_keys_insert()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_dirty_keys_update AFTER UPDATE ON awa.jobs_hot REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.mark_dirty_keys_update()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_unique_claims_delete AFTER DELETE ON awa.jobs_hot FOR EACH ROW WHEN (((old.unique_key IS NOT NULL) AND (old.unique_states IS NOT NULL))) EXECUTE FUNCTION awa.sync_job_unique_claims()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_unique_claims_insert AFTER INSERT ON awa.jobs_hot FOR EACH ROW WHEN (((new.unique_key IS NOT NULL) AND (new.unique_states IS NOT NULL))) EXECUTE FUNCTION awa.sync_job_unique_claims()$def$),
        ($def$CREATE TRIGGER trg_jobs_hot_unique_claims_update AFTER UPDATE ON awa.jobs_hot FOR EACH ROW WHEN ((((old.unique_key IS NOT NULL) AND (old.unique_states IS NOT NULL)) OR ((new.unique_key IS NOT NULL) AND (new.unique_states IS NOT NULL)))) EXECUTE FUNCTION awa.sync_job_unique_claims()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_admin_metadata AFTER INSERT OR DELETE OR UPDATE ON awa.scheduled_jobs FOR EACH ROW EXECUTE FUNCTION awa.sync_job_admin_metadata()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_admin_metadata_delete_stmt AFTER DELETE ON awa.scheduled_jobs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_delete()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_admin_metadata_insert_stmt AFTER INSERT ON awa.scheduled_jobs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_insert()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_admin_metadata_update_stmt AFTER UPDATE ON awa.scheduled_jobs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.sync_job_admin_metadata_stmt_update()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_dirty_keys_delete AFTER DELETE ON awa.scheduled_jobs REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.mark_dirty_keys_delete()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_dirty_keys_insert AFTER INSERT ON awa.scheduled_jobs REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.mark_dirty_keys_insert()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_dirty_keys_update AFTER UPDATE ON awa.scheduled_jobs REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION awa.mark_dirty_keys_update()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_unique_claims_delete AFTER DELETE ON awa.scheduled_jobs FOR EACH ROW WHEN (((old.unique_key IS NOT NULL) AND (old.unique_states IS NOT NULL))) EXECUTE FUNCTION awa.sync_job_unique_claims()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_unique_claims_insert AFTER INSERT ON awa.scheduled_jobs FOR EACH ROW WHEN (((new.unique_key IS NOT NULL) AND (new.unique_states IS NOT NULL))) EXECUTE FUNCTION awa.sync_job_unique_claims()$def$),
        ($def$CREATE TRIGGER trg_scheduled_jobs_unique_claims_update AFTER UPDATE ON awa.scheduled_jobs FOR EACH ROW WHEN ((((old.unique_key IS NOT NULL) AND (old.unique_states IS NOT NULL)) OR ((new.unique_key IS NOT NULL) AND (new.unique_states IS NOT NULL)))) EXECUTE FUNCTION awa.sync_job_unique_claims()$def$)
    ) AS canonical(definition)
    WHERE p_schema = 'awa'
$canonical$;

-- Why p_schema is not trusted for a migrator refresh, one row per rule and
-- object. Trusted owners are the awa schema owner and the current role;
-- superusers pass every pg_has_role check, so authority is identity here.
-- Sequences are exempt from the ownership rule: they run no code.
CREATE OR REPLACE FUNCTION awa.queue_storage_schema_trust_violations(p_schema TEXT)
RETURNS TABLE (rule TEXT, object TEXT)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
SET quote_all_identifiers = off
AS $violations$
    WITH trusted_owners AS (
        SELECT nspowner AS owner FROM pg_namespace WHERE nspname = 'awa'
        UNION
        SELECT oid FROM pg_roles WHERE rolname = current_user
    ),
    target AS (
        SELECT oid, nspname, nspowner, nspacl FROM pg_namespace WHERE nspname = p_schema
    )
    SELECT 'reserved schema', format('schema %I', target.nspname)
    FROM target
    WHERE target.nspname IN ('public', 'information_schema') OR target.nspname ~ '^pg_'
    UNION ALL
    SELECT format('owned by %s', pg_get_userbyid(target.nspowner)), format('schema %I', target.nspname)
    FROM target
    WHERE target.nspowner NOT IN (SELECT owner FROM trusted_owners)
    UNION ALL
    SELECT format('CREATE granted to %s',
                  CASE privilege.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_get_userbyid(privilege.grantee) END),
           format('schema %I', target.nspname)
    FROM target
    CROSS JOIN LATERAL aclexplode(COALESCE(target.nspacl, acldefault('n', target.nspowner))) AS privilege
    WHERE privilege.privilege_type = 'CREATE'
      AND privilege.grantee NOT IN (SELECT owner FROM trusted_owners)
    UNION ALL
    SELECT format('owned by %s', pg_get_userbyid(relation.relowner)),
           format('relation %I.%I', target.nspname, relation.relname)
    FROM target
    JOIN pg_class AS relation ON relation.relnamespace = target.oid
    WHERE relation.relkind <> 'S'
      AND relation.relowner NOT IN (SELECT owner FROM trusted_owners)
    UNION ALL
    SELECT format('owned by %s', pg_get_userbyid(routine.proowner)),
           format('function %s', routine.oid::regprocedure)
    FROM target
    JOIN pg_proc AS routine ON routine.pronamespace = target.oid
    WHERE routine.proowner NOT IN (SELECT owner FROM trusted_owners)
    UNION ALL
    SELECT format('owned by %s', pg_get_userbyid(data_type.typowner)),
           format('type %s', data_type.oid::regtype)
    FROM target
    JOIN pg_type AS data_type ON data_type.typnamespace = target.oid
    WHERE data_type.typowner NOT IN (SELECT owner FROM trusted_owners)
    UNION ALL
    SELECT format('TRIGGER granted to %s',
                  CASE privilege.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_get_userbyid(privilege.grantee) END),
           format('relation %I.%I', target.nspname, relation.relname)
    FROM target
    JOIN pg_class AS relation ON relation.relnamespace = target.oid
    CROSS JOIN LATERAL aclexplode(COALESCE(relation.relacl, acldefault('r', relation.relowner))) AS privilege
    WHERE relation.relkind <> 'S'
      AND privilege.privilege_type = 'TRIGGER'
      AND privilege.grantee NOT IN (SELECT owner FROM trusted_owners)
    UNION ALL
    SELECT 'not a trigger awa installs',
           format('trigger %I on %I.%I', trigger.tgname, target.nspname, relation.relname)
    FROM target
    JOIN pg_class AS relation ON relation.relnamespace = target.oid
    JOIN pg_trigger AS trigger ON trigger.tgrelid = relation.oid
    WHERE NOT trigger.tgisinternal
      AND pg_get_triggerdef(trigger.oid) NOT IN (
          SELECT awa.queue_storage_canonical_triggers(target.nspname)
      )
$violations$;

CREATE OR REPLACE FUNCTION awa.queue_storage_schema_is_trusted(p_schema TEXT)
RETURNS BOOLEAN
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $trusted$
    SELECT NOT EXISTS (SELECT 1 FROM awa.queue_storage_schema_trust_violations(p_schema))
$trusted$;

-- The first few violations for p_schema as one line, for error text.
CREATE OR REPLACE FUNCTION awa.queue_storage_schema_trust_summary(p_schema TEXT)
RETURNS TEXT
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $summary$
    WITH violation AS (
        SELECT rule, object, count(*) OVER () AS total,
               row_number() OVER (ORDER BY object, rule) AS position
        FROM awa.queue_storage_schema_trust_violations(p_schema)
    )
    SELECT string_agg(format('%s %s', object, rule), '; ' ORDER BY position)
           || CASE WHEN max(total) > 5 THEN format('; and %s more', max(total) - 5) ELSE '' END
    FROM violation
    WHERE position <= 5
$summary$;

CREATE OR REPLACE FUNCTION awa.queue_storage_schemas()
RETURNS TABLE (schema_name TEXT)
LANGUAGE sql
STABLE
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
SET quote_all_identifiers = off
AS $discover$
    WITH candidate AS MATERIALIZED (
    SELECT namespace.nspname::text AS schema_name
    FROM pg_namespace AS namespace
    WHERE has_schema_privilege(current_user, namespace.oid, 'USAGE')
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
    )
    SELECT candidate.schema_name
    FROM candidate
    WHERE awa.queue_storage_schema_is_trusted(candidate.schema_name)
    ORDER BY candidate.schema_name
$discover$;

-- Default-substrate refreshes precede discovery in some migration bundles.
DO $preflight$
DECLARE
    configured_schema TEXT;
    skipped_schema TEXT;
    configured_schemas TEXT[];
BEGIN
    IF NOT awa.queue_storage_schema_is_trusted('awa') THEN
        RAISE EXCEPTION 'unsafe ownership or privileges in the awa schema: %. Nothing was migrated; see "Custom-schema discovery during migration" in the 0.6 to 0.7 upgrade guide',
            awa.queue_storage_schema_trust_summary('awa')
            USING ERRCODE = '42501',
                  HINT = 'Use the migration role or awa schema owner for schema objects; remove foreign-owned objects, triggers awa did not install, and untrusted CREATE or TRIGGER grants before migrating.';
    END IF;
    SELECT array_agg(DISTINCT schema_name) INTO configured_schemas
    FROM (
        SELECT schema_name FROM awa.runtime_storage_backends WHERE backend = 'queue_storage'
        UNION
        SELECT NULLIF(details->>'schema', '') FROM awa.storage_transition_state
        WHERE current_engine = 'queue_storage' OR prepared_engine = 'queue_storage'
        UNION
        -- Legacy SQL activation used awa_exp for an unnamed prepared backend.
        SELECT 'awa_exp' FROM awa.storage_transition_state
        WHERE prepared_engine = 'queue_storage' AND NULLIF(details->>'schema', '') IS NULL
    ) AS configured
    WHERE schema_name IS NOT NULL;
    FOREACH configured_schema IN ARRAY COALESCE(configured_schemas, ARRAY[]::TEXT[]) LOOP
        IF NOT awa.queue_storage_schema_is_trusted(configured_schema) THEN
            RAISE EXCEPTION 'unsafe configured queue-storage schema: %: %. The global schema version has not advanced; see "Custom-schema discovery during migration" in the 0.6 to 0.7 upgrade guide',
                configured_schema, awa.queue_storage_schema_trust_summary(configured_schema)
                USING ERRCODE = '42501',
                      HINT = 'Review the registered/prepared substrate ownership and grants before upgrading; the global schema version has not advanced.';
        END IF;
    END LOOP;
    FOR skipped_schema IN
        SELECT namespace.nspname
        FROM pg_namespace AS namespace
        WHERE namespace.nspname <> 'awa'
          AND namespace.nspname <> ALL (COALESCE(configured_schemas, ARRAY[]::TEXT[]))
          AND has_schema_privilege(current_user, namespace.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS claim_fn
              WHERE claim_fn.pronamespace = namespace.oid
                AND claim_fn.proname = 'claim_ready_runtime'
          )
          AND NOT awa.queue_storage_schema_is_trusted(namespace.nspname)
        ORDER BY namespace.nspname
    LOOP
        RAISE WARNING 'skipping untrusted queue-storage schema %: %; it will not be upgraded',
            skipped_schema, awa.queue_storage_schema_trust_summary(skipped_schema);
    END LOOP;
END
$preflight$;
