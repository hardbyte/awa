-- Schema patch live_slot_claims: claim through per-slot functions that lock
-- only the slots they read, and skip the slot queue prune is retiring.
--
-- claim_ready_runtime ran as a cached generic plan over the ring parents,
-- which locks every partition, and its lane probe over ready_segments had no
-- slot predicate. Every claim therefore held ACCESS SHARE on the sealed slot
-- queue prune must truncate, and under overlapping claims prune never got its
-- ACCESS EXCLUSIVE lock, so the queue ring stopped rotating. Tracking:
-- https://github.com/hardbyte/awa/issues/721
--
-- Applied after the v023 definitions, which add awa.refresh_claim_functions
-- and make the installer call it. The block below refreshes each installed
-- queue-storage schema with lease claim receipts whose claim functions lack
-- the change: claim_ready_runtime is replaced in place (same signature,
-- result, owner and privileges) and claim_slot_<n> is created for each queue
-- slot, owned by the owner of claim_ready_runtime and with its EXECUTE
-- privileges. Schemas with lease_claim_receipts = FALSE keep their claim
-- function. A schema that already carries the change is left alone.
--
-- Live load: function replacement only. It takes no lock on any awa or
-- queue-storage table and its work scales with the number of schemas and
-- slots, not with job count. A claim in flight finishes on the old body; the
-- next call uses the new one.
--
-- Mixed fleets: function names, arguments, results and the claim protocol are
-- unchanged, so released 0.6 binaries run the same against a patched or an
-- unpatched schema. A released binary's prune never takes the retire barrier,
-- so patched claims always get it in shared mode and route as before, still
-- locking only the slots they read. A 0.6.18 prune against an unpatched
-- function takes a barrier no claim reads and then behaves as 0.6.17 did.
--
-- 0.6 delivery: the 0.6 series cannot add a migration version, so this ships
-- as an idempotent schema patch. `awa migrate` applies it when the schema is
-- exactly at v040 and the probe in migrations.rs reports it missing;
-- `awa migrate --sql` prints it as a repeatable R__ script for external
-- runners. Run it as the owner of the claim functions, or as a migrator that
-- can SET ROLE to it.

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
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
          AND (
              -- A released body with receipts, or a refreshed one.
              p.prosrc LIKE '%claim_attempt_batches AS (%'
              OR p.prosrc LIKE '%awa:claim-routing:v2%'
          )
          AND (
              p.prosrc NOT LIKE '%awa:claim-routing:v2%'
              OR EXISTS (
                  SELECT 1
                  FROM pg_inherits AS inh
                  JOIN pg_class AS child ON child.oid = inh.inhrelid
                  WHERE inh.inhparent = to_regclass(format('%I.ready_entries', n.nspname))
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_proc AS slot_fn
                        WHERE slot_fn.pronamespace = n.oid
                          AND slot_fn.proname = 'claim_slot_' || substring(child.relname FROM '^ready_entries_([0-9]+)$')
                          AND oidvectortypes(slot_fn.proargtypes)
                              = 'text, bigint, smallint, smallint, text, bigint, bigint, boolean, double precision, double precision'
                          AND slot_fn.prosrc LIKE '%awa:claim-routing:v2%'
                    )
              )
          )
        ORDER BY n.nspname
    LOOP
        PERFORM awa.refresh_claim_functions(v_schema, TRUE);
    END LOOP;
END;
$$;
