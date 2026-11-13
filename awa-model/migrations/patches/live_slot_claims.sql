-- Schema patch live_slot_claims: claim only through live queue slots.
--
-- claim_ready_runtime ran as a cached generic plan, which locks every
-- partition of every ring parent it reads, and its lane probe over
-- ready_segments has no slot predicate at all. Every claim therefore held
-- ACCESS SHARE on the sealed slot queue prune must truncate, and under
-- overlapping claims prune never got its ACCESS EXCLUSIVE lock, so the queue
-- ring stopped rotating. Tracking: https://github.com/hardbyte/awa/issues/721
--
-- The refreshed function plans each call with the slot values it was given
-- (plan_cache_mode = force_custom_plan), restricts its ready_segments scans
-- to the live slots, and leaves out the slot prune retires next while that
-- slot's retire barrier (an advisory lock) is held or requested. Claims only
-- try the barrier in shared mode and never wait on it.
--
-- Applied after the v023 definitions, which add awa.refresh_claim_runtime and
-- make the installer write the new function into new substrates. The block
-- below refreshes claim_ready_runtime in each installed queue-storage schema,
-- keeping that schema's receipts mode. CREATE OR REPLACE keeps the owner and
-- privileges; a function that already carries the change is left alone.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table and its work scales with the number of schemas, not
-- with job count. A claim in flight finishes on the old body; the next call
-- uses the new one.
--
-- Mixed fleets: function names, arguments, results and the claim protocol are
-- unchanged, so released 0.6 binaries run the same against a patched or an
-- unpatched schema. A released binary's prune never takes the barrier, so
-- patched claims always get it in shared mode and route as before. A 0.6.18
-- prune against an unpatched function takes a barrier no claim reads; it then
-- behaves as 0.6.17 did.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships as
-- an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Run it as the owner of the claim functions, or as a migrator that
-- can SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
    v_receipts BOOLEAN;
BEGIN
    FOR v_schema, v_receipts IN
        SELECT n.nspname, p.prosrc LIKE '%claim_attempt_batches AS (%'
        FROM pg_namespace AS n
        JOIN pg_proc AS p
          ON p.pronamespace = n.oid
         AND p.proname = 'claim_ready_runtime'
         AND oidvectortypes(p.proargtypes)
             = 'text, bigint, double precision, double precision'
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_class AS awa_c
              WHERE awa_c.relnamespace = n.oid
                AND awa_c.relname = 'ready_segments'
                AND awa_c.relkind = 'p'
          )
          AND EXISTS (
              SELECT 1 FROM pg_class AS awa_c
              WHERE awa_c.relnamespace = n.oid
                AND awa_c.relname = 'queue_ring_slots'
          )
          AND NOT (
              'plan_cache_mode=force_custom_plan' = ANY (COALESCE(p.proconfig, '{}'))
              AND p.prosrc LIKE '%awa.queue_storage.retire:%'
          )
        ORDER BY n.nspname
    LOOP
        PERFORM awa.refresh_claim_runtime(v_schema, v_receipts);
    END LOOP;
END;
$$;
