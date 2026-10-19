-- Every trigger definition awa has created in the awa schema or in a
-- queue-storage schema, as pg_get_triggerdef renders it under
-- search_path = pg_catalog. New triggers must be added here.
CREATE OR REPLACE FUNCTION awa.queue_storage_canonical_triggers(p_schema TEXT)
RETURNS SETOF TEXT
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, pg_temp
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
                  SELECT 1
                  FROM pg_trigger AS trigger
                  JOIN pg_class AS relation ON relation.oid = trigger.tgrelid
                  WHERE relation.relnamespace = namespace.oid
                    AND NOT trigger.tgisinternal
                    AND pg_get_triggerdef(trigger.oid) NOT IN (
                        SELECT awa.queue_storage_canonical_triggers(namespace.nspname)
                    )
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
                  HINT = 'Use the migration role or awa schema owner for schema objects; remove foreign-owned objects, triggers awa did not install, and untrusted CREATE or TRIGGER grants before migrating.';
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
