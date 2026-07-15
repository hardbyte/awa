-- #169: lower fillfactor on the two UPDATE-heavy receipt-plane partitioned
-- tables to keep dead-tuple density bounded under pinned-MVCC pressure.
--
-- Targets (verified UPDATE call sites, see queue_storage.rs):
--   * leases               — heartbeat, state transition, callback fields
--   * lease_claims         — SET materialized_at = clock_timestamp()
--
-- The other receipt-plane partitioned tables (ready_entries, done_entries,
-- lease_claim_closures) are INSERT+DELETE only — no UPDATE call sites —
-- so lowering fillfactor on them would waste pages without restoring
-- any HOT-update behaviour. They stay at fillfactor=100.
--
-- The fillfactor change pattern follows commits d21e5db and ab99a31:
-- 50% on-page slack restores HOT-update headroom on the sibling
-- Warm tables (queue_claim_heads / queue_enqueue_heads / queue_ring_state).
-- Several of the indexes on `leases` still key on UPDATEd columns
-- (state_hb, state_deadline, state_callback_timeout, callback) so this
-- fillfactor change alone does not make heartbeat/state updates HOT in
-- receipts mode — it caps page-spill so the dead-tuple chain stays
-- bounded. Removing the state_hb index in receipts mode is tracked
-- separately as the B1 follow-up.
--
-- v023's install function was updated in the same change to apply these
-- settings to fresh installs. This migration applies the ALTERs directly
-- to existing partitions of `awa.leases` and `awa.lease_claims`, since
-- the install function already in pg_proc on an existing deployment
-- still carries the old (pre-edit) body. Direct ALTER avoids having to
-- reapply the entire helper SQL.
--
-- ALTER TABLE SET (fillfactor=N) only changes the hint for future page
-- allocations; it does not rewrite existing pages, so this is
-- non-blocking and fast even on populated tables.
--
-- Covers the default `awa` schema and any custom queue-storage schemas
-- whose substrate physically exists in the database. The discovery
-- query scans pg_class for any schema with both a partitioned `leases`
-- and `lease_claims` parent — broader than the registration tables
-- (`runtime_storage_backends` only lists the active backend;
-- `storage_transition_state.details` only lists the prepared schema
-- during an in-flight transition), so it picks up custom schemas that
-- were materialized but never activated.
--
-- Rust `QueueStorage::prepare_schema()` also calls
-- `apply_receipt_plane_fillfactor` after `install_queue_storage_substrate`,
-- so any schema prepared after this migration (via the CLI or worker
-- boot path) lands at fillfactor=50 even if the install helper still
-- cached in pg_proc has the pre-v024 body. External tooling that
-- bypasses prepare_schema and calls the install helper directly should
-- also call `apply_receipt_plane_fillfactor` against the same schema.

CREATE OR REPLACE FUNCTION awa.apply_receipt_plane_fillfactor(p_schema TEXT)
RETURNS VOID
LANGUAGE plpgsql
SET search_path = pg_catalog, awa
AS $$
DECLARE
    v_partition_relid OID;
    v_leases_oid      OID;
    v_claims_oid      OID;
    v_is_awa_substrate BOOLEAN;
BEGIN
    v_leases_oid := to_regclass(format('%I.leases', p_schema));
    v_claims_oid := to_regclass(format('%I.lease_claims', p_schema));

    IF v_leases_oid IS NULL OR v_claims_oid IS NULL THEN
        RETURN;
    END IF;

    -- Ownership boundary: only touch AWA-owned substrates. Schemas
    -- that happen to share the `leases` / `lease_claims` table names
    -- but were not installed by `awa.install_queue_storage_substrate`
    -- (e.g. unrelated application tables) must be left alone. The
    -- gating sentinel is the AWA-specific `claim_ready_runtime`
    -- function with its full signature — the same shape
    -- `awa_model::storage::queue_storage_schema_ready` uses to
    -- identify an AWA substrate. The install helper creates that
    -- function in every substrate; no other code path does.
    v_is_awa_substrate := to_regprocedure(format(
        '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
        p_schema
    )) IS NOT NULL;

    IF NOT v_is_awa_substrate THEN
        RETURN;
    END IF;

    FOR v_partition_relid IN
        SELECT inhrelid
        FROM pg_inherits
        WHERE inhparent IN (v_leases_oid, v_claims_oid)
    LOOP
        EXECUTE format(
            $alter$
            ALTER TABLE %s SET (
                fillfactor = 50,
                autovacuum_vacuum_scale_factor = 0.0,
                autovacuum_vacuum_threshold = 200,
                autovacuum_vacuum_cost_limit = 2000,
                autovacuum_vacuum_cost_delay = 2
            )
            $alter$,
            v_partition_relid::regclass::text
        );
    END LOOP;
END
$$;

GRANT EXECUTE ON FUNCTION awa.apply_receipt_plane_fillfactor(TEXT) TO PUBLIC;

DO $$
DECLARE
    v_schema TEXT;
BEGIN
    -- Discover every schema that holds an AWA queue-storage substrate.
    -- Matching just "has a partitioned `leases` table" would step on
    -- unrelated app-owned schemas that happen to share the name, so
    -- the gating sentinel is `awa.claim_ready_runtime` with the
    -- AWA-specific signature — the same shape
    -- `awa_model::storage::queue_storage_schema_ready` uses to identify
    -- an AWA substrate. The install helper creates that function in
    -- every substrate; no other code path does. This is broader than
    -- reading `awa.runtime_storage_backends` (active backend only) or
    -- `awa.storage_transition_state.details->>'schema'` (prepared
    -- schema only during an in-flight transition), but stays inside
    -- AWA's ownership boundary.
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE has_schema_privilege(current_user, n.oid, 'USAGE')
          AND EXISTS (
              SELECT 1 FROM pg_proc AS awa_p
              WHERE awa_p.pronamespace = n.oid
                AND awa_p.proname = 'claim_ready_runtime'
                AND oidvectortypes(awa_p.proargtypes)
                    = 'text, bigint, double precision, double precision'
          )
    LOOP
        -- The helper itself re-checks the sentinel and verifies the
        -- partitioned `leases` / `lease_claims` exist before doing
        -- anything, so this loop is safe to be liberal.
        PERFORM awa.apply_receipt_plane_fillfactor(v_schema);
    END LOOP;
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (24, 'Lower fillfactor to 50 on leases and lease_claims partitions (#169)')
ON CONFLICT (version) DO NOTHING;
