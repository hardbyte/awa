-- v042 (#371): append-only ring-rotation ledgers and terminal-rollup deltas.
--
-- The mutable ring-state cursor singletons (`queue_ring_state`,
-- `lease_ring_state`, `claim_ring_state`) were UPDATEd on every rotation;
-- under a pinned MVCC horizon those dead row versions accumulate
-- (~3.6k/hour/ring at a 1s cadence) and every hot-path claim/enqueue reads
-- these rows. The cursor now lives in an append-only per-ring rotation
-- ledger (`{ring}_ring_rotations`); the current cursor is the
-- max-generation row (backward PK scan, O(1)). Rotation appends one row;
-- the maintenance leader trims the ledger to one full ring wrap when the
-- MVCC horizon is clear. Similarly, queue prune appends its pruned
-- terminal counts to `queue_terminal_rollup_deltas` instead of upserting
-- `queue_terminal_rollups` in the prune transaction; the maintenance
-- leader folds the deltas when the horizon is clear.
--
-- DELIBERATE COMPATIBILITY BREAK (exception to the additive-only
-- migration policy in migrations.rs): the `current_slot` / `generation`
-- columns are DROPPED from the ring-state singletons after the ledger is
-- seeded from them. A pre-v042 binary reading a stale frozen cursor would
-- silently misroute writes into sealed slots; a missing column is a loud,
-- safe failure. Binaries and this migration must move together — do not
-- run a mixed fleet of pre-/post-v042 binaries against one database. See
-- the 0.7 upgrade notes in CHANGELOG.md and ADR-040.
--
-- v023 owns the substrate helper; the migration runner reapplies the
-- amended v018 (insert_job_compat) and v023 (install helper + default
-- `awa` install) files before this one, so the default `awa` substrate is
-- already converted. This migration refreshes every OTHER already-installed
-- queue-storage schema (v039-style discovery loop) so custom schemas and
-- in-transition schemas get the ledger, the seeded cursor, and the
-- column drops too.

DO $$
DECLARE
    v_schema TEXT;
    v_queue_slots INT;
    v_lease_slots INT;
    v_claim_slots INT;
    v_claim_runtime REGPROCEDURE;
    v_claim_runtime_def TEXT;
    v_lease_claim_receipts BOOLEAN;
BEGIN
    FOR v_schema IN
        SELECT n.nspname
        FROM pg_namespace AS n
        WHERE to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            n.nspname
        )) IS NOT NULL
    LOOP
        IF to_regclass(format('%I.queue_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.lease_ring_state', v_schema)) IS NULL
           OR to_regclass(format('%I.claim_ring_state', v_schema)) IS NULL THEN
            CONTINUE;
        END IF;

        EXECUTE format(
            'SELECT slot_count FROM %I.queue_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_queue_slots;

        EXECUTE format(
            'SELECT slot_count FROM %I.lease_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_lease_slots;

        EXECUTE format(
            'SELECT slot_count FROM %I.claim_ring_state WHERE singleton = TRUE',
            v_schema
        )
        INTO v_claim_slots;

        IF v_queue_slots IS NULL OR v_lease_slots IS NULL OR v_claim_slots IS NULL THEN
            CONTINUE;
        END IF;

        v_claim_runtime := to_regprocedure(format(
            '%I.claim_ready_runtime(text,bigint,double precision,double precision)',
            v_schema
        ));
        v_claim_runtime_def := pg_get_functiondef(v_claim_runtime::oid);
        -- Receipts detection matches the v041 (#246) heuristic: after the
        -- compact-deadline refresh a receipts claim function only INSERTs
        -- into lease_claim_batches (the row-local lease_claims path is
        -- gone), so checking lease_claims alone would misclassify a custom
        -- receipts schema as non-receipts on this later refresh. Check both
        -- tables so the classification is stable across the v041→v042 chain.
        v_lease_claim_receipts := v_schema = 'awa'
            OR position(format('INSERT INTO %I.lease_claims', v_schema) IN v_claim_runtime_def) > 0
            OR position(format('INSERT INTO %I.lease_claim_batches', v_schema) IN v_claim_runtime_def) > 0;

        PERFORM awa.install_queue_storage_substrate(
            v_schema,
            v_queue_slots,
            v_lease_slots,
            v_claim_slots,
            v_lease_claim_receipts
        );
    END LOOP;
END;
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (42, 'Append-only ring-rotation ledgers and terminal-rollup deltas (#371)')
ON CONFLICT (version) DO NOTHING;
