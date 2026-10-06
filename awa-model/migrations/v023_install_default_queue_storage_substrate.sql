-- Install the default `awa` substrate as part of migrate. Unlike the
-- reusable helper, this default-schema path also performs the one-shot
-- legacy fixups that let `awa migrate` upgrade a database where the
-- default `awa` queue-storage substrate was previously prepared by Rust.
-- Keep the whole cleanup -> helper -> copy-back path inside one statement
-- so the per-schema advisory xact lock serializes it with prepare_schema().
DO $$
DECLARE
    v_open_receipt_claims_count BIGINT;
    v_lease_claims_relkind TEXT;
    v_closures_relkind TEXT;
    v_legacy_claim_slot INT;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended('awa.queue_storage.install:awa', 0)
    );

    IF to_regclass('awa.open_receipt_claims') IS NOT NULL THEN
        SELECT count(*)::bigint
        INTO v_open_receipt_claims_count
        FROM awa.open_receipt_claims;

        IF v_open_receipt_claims_count > 0 THEN
            RAISE EXCEPTION 'awa.open_receipt_claims has % rows but the runtime no longer reads or writes this table',
                v_open_receipt_claims_count
                USING ERRCODE = '22023',
                      HINT = 'Run the ADR-023 reverse migration (recreate from lease_claims minus durable closure evidence) to drain it, then re-run awa migrate.';
        END IF;

        DROP TABLE IF EXISTS awa.open_receipt_claims CASCADE;
    END IF;

    SELECT c.relkind::text
    INTO v_lease_claims_relkind
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'awa'
      AND c.relname = 'lease_claims';

    SELECT c.relkind::text
    INTO v_closures_relkind
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'awa'
      AND c.relname = 'lease_claim_closures';

    IF v_lease_claims_relkind = 'r' THEN
        ALTER TABLE awa.lease_claims RENAME TO lease_claims_legacy;
    END IF;
    IF v_closures_relkind = 'r' THEN
        ALTER TABLE awa.lease_claim_closures RENAME TO lease_claim_closures_legacy;
    END IF;

    DROP TABLE IF EXISTS awa.queue_count_snapshots;

    PERFORM awa.install_queue_storage_substrate('awa');

    IF to_regclass('awa.lease_claims_legacy') IS NOT NULL
       OR to_regclass('awa.lease_claim_closures_legacy') IS NOT NULL THEN
        SELECT current_slot
        INTO v_legacy_claim_slot
        FROM awa.claim_ring_state
        WHERE singleton;
    END IF;

    IF to_regclass('awa.lease_claims_legacy') IS NOT NULL THEN
        ALTER TABLE awa.lease_claims_legacy
            ADD COLUMN IF NOT EXISTS enqueue_shard SMALLINT NOT NULL DEFAULT 0;
        ALTER TABLE awa.lease_claims_legacy
            ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ;

        INSERT INTO awa.lease_claims (
            claim_slot, job_id, run_lease, ready_slot, ready_generation,
            queue, priority, attempt, max_attempts, lane_seq,
            enqueue_shard, claimed_at, materialized_at, deadline_at
        )
        SELECT
            v_legacy_claim_slot,
            job_id, run_lease, ready_slot, ready_generation,
            queue, priority, attempt, max_attempts, lane_seq,
            enqueue_shard, claimed_at, materialized_at, deadline_at
        FROM awa.lease_claims_legacy
        ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING;

        DROP TABLE awa.lease_claims_legacy;
    END IF;

    IF to_regclass('awa.lease_claim_closures_legacy') IS NOT NULL THEN
        INSERT INTO awa.lease_claim_closures (
            claim_slot, job_id, run_lease, outcome, closed_at
        )
        SELECT
            v_legacy_claim_slot,
            job_id, run_lease, outcome, closed_at
        FROM awa.lease_claim_closures_legacy
        ON CONFLICT (claim_slot, job_id, run_lease) DO NOTHING;

        DROP TABLE awa.lease_claim_closures_legacy;
    END IF;
END
$$;

INSERT INTO awa.schema_version (version, description)
VALUES (23, 'Install default awa queue-storage substrate via awa.install_queue_storage_substrate() helper (#308)')
ON CONFLICT (version) DO NOTHING;
