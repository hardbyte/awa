-- #308: move the default queue-storage substrate DDL out of
-- QueueStorage::prepare_schema() in Rust and into a SQL helper function
-- that `awa migrate` runs as part of the canonical migration set.
--
-- Contract
-- --------
-- After this migration runs, `awa.install_queue_storage_substrate(p_schema)`
-- is the single source of truth for the per-schema queue-storage substrate
-- objects (sequences, ring-state singletons, partitioned ready/done/lease
-- tables, lane indexes, claim_ready_runtime() function, etc). It is callable
-- from Rust (`QueueStorage::prepare_schema`), from `awa migrate --sql`
-- output, from the Python client, and from operators driving DDL directly.
--
-- The default `awa` substrate is migration-owned and default-shaped:
-- (queue_slot_count=16, lease_slot_count=8, claim_slot_count=8,
--  lease_claim_receipts=TRUE). Operators wanting non-default slot counts
-- or the legacy non-receipts claim path MUST use a custom queue-storage
-- schema; the helper rejects non-default configs against `p_schema='awa'`
-- with errcode 22023.
--
-- The helper is SECURITY INVOKER and explicitly revokes EXECUTE from
-- PUBLIC — runtime roles MUST NOT gain DDL through it. The helper takes a per-schema
-- advisory xact lock named `awa.queue_storage.install:{p_schema}` to
-- serialize concurrent installs (matches the lock pattern previously
-- held in Rust).
--
-- Deliberately not owned by this helper:
-- - The `awa.runtime_storage_backends` cross-schema table is owned by the
--   v012 migration. Activation/finalization paths seed or update the
--   `queue_storage` row.
-- - The non-additive legacy upgrade edge cases (open_receipt_claims drop,
--   lease_claims / lease_claim_closures rename-and-rebuild from a
--   non-partitioned shape, queue_count_snapshots drop) — these are
--   one-shot upgrade fixups that don't belong in a forward-only DDL
--   function. They remain in `prepare_schema()` for custom schemas; the
--   default `awa` migration path runs the `awa`-specific copy below.

CREATE OR REPLACE FUNCTION awa.install_queue_storage_substrate(
    p_schema               TEXT,
    p_queue_slot_count     INT     DEFAULT 16,
    p_lease_slot_count     INT     DEFAULT 8,
    p_claim_slot_count     INT     DEFAULT 8,
    p_lease_claim_receipts BOOLEAN DEFAULT TRUE
)
RETURNS VOID
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, awa, public
AS $install$
DECLARE
    v_slot           INT;
    v_initial_gen    BIGINT;
    v_claimed_cte    TEXT;
    v_advance_claim_condition TEXT;
BEGIN
    IF p_schema IS NULL OR p_schema !~ '^[a-z_][a-z0-9_]*$' THEN
        RAISE EXCEPTION 'install_queue_storage_substrate: schema name must match ^[a-z_][a-z0-9_]*$; got %',
            p_schema
            USING ERRCODE = '22023';
    END IF;
    IF p_queue_slot_count IS NULL OR p_queue_slot_count < 4 THEN
        RAISE EXCEPTION 'install_queue_storage_substrate: queue_slot_count must be >= 4; got %',
            p_queue_slot_count
            USING ERRCODE = '22023';
    END IF;
    IF p_lease_slot_count IS NULL OR p_lease_slot_count < 2 THEN
        RAISE EXCEPTION 'install_queue_storage_substrate: lease_slot_count must be >= 2; got %',
            p_lease_slot_count
            USING ERRCODE = '22023';
    END IF;
    IF p_claim_slot_count IS NULL OR p_claim_slot_count < 2 THEN
        RAISE EXCEPTION 'install_queue_storage_substrate: claim_slot_count must be >= 2; got %',
            p_claim_slot_count
            USING ERRCODE = '22023';
    END IF;
    IF p_lease_claim_receipts IS NULL THEN
        RAISE EXCEPTION 'install_queue_storage_substrate: lease_claim_receipts must not be NULL'
            USING ERRCODE = '22023';
    END IF;

    -- Serialize concurrent installs for the same schema. The lock name
    -- matches the Rust `prepare_schema()` lock, so a CLI install racing
    -- with a Rust worker boot serializes on the same key.
    PERFORM pg_advisory_xact_lock(
        hashtextextended(format('awa.queue_storage.install:%I', p_schema), 0)
    );

    -- The default `awa` substrate is migration-owned. Reject non-default
    -- configuration with a clear hint directing the operator to a custom
    -- queue-storage schema.
    IF p_schema = 'awa' THEN
        IF p_lease_claim_receipts IS NOT TRUE THEN
            RAISE EXCEPTION 'install_queue_storage_substrate: the default ''awa'' substrate requires lease_claim_receipts=TRUE'
                USING ERRCODE = '22023',
                      HINT = 'Use a custom queue-storage schema if you need lease_claim_receipts=FALSE; the default awa schema is migration-owned and default-shaped.';
        END IF;
        IF p_queue_slot_count <> 16
           OR p_lease_slot_count <> 8
           OR p_claim_slot_count <> 8 THEN
            RAISE EXCEPTION 'install_queue_storage_substrate: the default ''awa'' substrate must use the default slot counts (queue=16, lease=8, claim=8); got (queue=%, lease=%, claim=%)',
                p_queue_slot_count, p_lease_slot_count, p_claim_slot_count
                USING ERRCODE = '22023',
                      HINT = 'Use a custom queue-storage schema if you need non-default slot counts; the default awa schema is migration-owned and default-shaped.';
        END IF;
    END IF;

    EXECUTE format('CREATE SCHEMA IF NOT EXISTS %I', p_schema);

    -- Job-id sequence.
    EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I.job_id_seq', p_schema);

    --------------------------------------------------------------------
    -- Ring-state singletons (queue / lease / claim) and slot tables.
    --------------------------------------------------------------------

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_ring_state (
            singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            current_slot INT NOT NULL,
            generation   BIGINT NOT NULL,
            slot_count   INT NOT NULL
        )
        $ddl$,
        p_schema
    );

    -- #290: live-terminal-counter trust marker. See queue_storage.rs for
    -- the rationale. Added via ALTER + IF NOT EXISTS so the column is
    -- present on both fresh installs and upgrades.
    EXECUTE format(
        'ALTER TABLE %I.queue_ring_state ADD COLUMN IF NOT EXISTS terminal_counter_trusted_at TIMESTAMPTZ',
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.queue_ring_state SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'INSERT INTO %I.queue_ring_state (singleton, current_slot, generation, slot_count) VALUES (TRUE, 0, 0, %s) ON CONFLICT (singleton) DO NOTHING',
        p_schema, p_queue_slot_count
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_ring_slots (
            slot       INT PRIMARY KEY,
            generation BIGINT NOT NULL
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.queue_ring_slots SET (
            fillfactor = 70,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.lease_ring_state (
            singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            current_slot INT NOT NULL,
            generation   BIGINT NOT NULL,
            slot_count   INT NOT NULL
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.lease_ring_state SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'INSERT INTO %I.lease_ring_state (singleton, current_slot, generation, slot_count) VALUES (TRUE, 0, 0, %s) ON CONFLICT (singleton) DO NOTHING',
        p_schema, p_lease_slot_count
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.lease_ring_slots (
            slot       INT PRIMARY KEY,
            generation BIGINT NOT NULL
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.lease_ring_slots SET (
            fillfactor = 70,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    -- ADR-023 claim-ring control plane.
    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.claim_ring_state (
            singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
            current_slot INT NOT NULL,
            generation   BIGINT NOT NULL,
            slot_count   INT NOT NULL
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.claim_ring_state SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'INSERT INTO %I.claim_ring_state (singleton, current_slot, generation, slot_count) VALUES (TRUE, 0, 0, %s) ON CONFLICT (singleton) DO NOTHING',
        p_schema, p_claim_slot_count
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.claim_ring_slots (
            slot       INT PRIMARY KEY,
            generation BIGINT NOT NULL
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.claim_ring_slots SET (
            fillfactor = 70,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 50,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    --------------------------------------------------------------------
    -- Lane / head tables.
    --------------------------------------------------------------------

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_lanes (
            queue                  TEXT NOT NULL,
            priority               SMALLINT NOT NULL,
            next_seq               BIGINT NOT NULL DEFAULT 1,
            claim_seq              BIGINT NOT NULL DEFAULT 1,
            pruned_completed_count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (queue, priority)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_enqueue_heads (
            queue         TEXT NOT NULL,
            priority      SMALLINT NOT NULL,
            enqueue_shard SMALLINT NOT NULL DEFAULT 0,
            next_seq      BIGINT NOT NULL DEFAULT 1,
            seq_name      TEXT,
            PRIMARY KEY (queue, priority, enqueue_shard)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.queue_enqueue_heads SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 200,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_claim_heads (
            queue         TEXT NOT NULL,
            priority      SMALLINT NOT NULL,
            enqueue_shard SMALLINT NOT NULL DEFAULT 0,
            claim_seq     BIGINT NOT NULL DEFAULT 1,
            seq_name      TEXT,
            PRIMARY KEY (queue, priority, enqueue_shard)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.queue_claim_heads SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 200,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'ALTER TABLE %I.queue_enqueue_heads ADD COLUMN IF NOT EXISTS seq_name TEXT',
        p_schema
    );

    EXECUTE format(
        'ALTER TABLE %I.queue_claim_heads ADD COLUMN IF NOT EXISTS seq_name TEXT',
        p_schema
    );

    --------------------------------------------------------------------
    -- Sequence-backed lane cursors (#295 pulled into v0.6).
    --
    -- queue_enqueue_heads / queue_claim_heads remain as cold lane
    -- registries and lock targets. The hot cursor movement happens via
    -- PostgreSQL sequences, which are not MVCC heap tuples. This removes
    -- the per-claim/per-enqueue UPDATE dead-tuple stream while preserving
    -- the existing lane identity and SKIP LOCKED fairness shape.
    --------------------------------------------------------------------

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.queue_lane_sequence_name(
            p_prefix TEXT,
            p_queue TEXT,
            p_priority SMALLINT,
            p_enqueue_shard SMALLINT
        )
        RETURNS TEXT
        LANGUAGE sql
        IMMUTABLE
        SET search_path = pg_catalog
        AS $func$
            SELECT p_prefix || '_' ||
                   substr(md5(p_queue || ':' || p_priority::text || ':' || p_enqueue_shard::text), 1, 32)
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.sequence_next_value(p_seq_name TEXT)
        RETURNS BIGINT
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_last BIGINT;
            v_is_called BOOLEAN;
        BEGIN
            IF p_seq_name IS NULL THEN
                RAISE EXCEPTION 'lane sequence name is NULL'
                    USING ERRCODE = '55000';
            END IF;

            EXECUTE format('SELECT last_value, is_called FROM %%I.%%I', %1$L, p_seq_name)
            INTO v_last, v_is_called;

            IF v_is_called THEN
                RETURN v_last + 1;
            END IF;
            RETURN v_last;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.set_sequence_next(p_seq_name TEXT, p_next BIGINT)
        RETURNS VOID
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        BEGIN
            IF p_seq_name IS NULL THEN
                RAISE EXCEPTION 'lane sequence name is NULL'
                    USING ERRCODE = '55000';
            END IF;

            p_next := GREATEST(p_next, %1$I.sequence_next_value(p_seq_name));

            IF p_next <= 1 THEN
                EXECUTE format(
                    'SELECT setval(%%L::regclass, 1, false)',
                    format('%%I.%%I', %1$L, p_seq_name)
                );
            ELSE
                EXECUTE format(
                    'SELECT setval(%%L::regclass, %%s, true)',
                    format('%%I.%%I', %1$L, p_seq_name),
                    p_next - 1
                );
            END IF;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.set_sequence_next_if(
            p_seq_name TEXT,
            p_next BIGINT,
            p_enabled BOOLEAN
        )
        RETURNS VOID
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        BEGIN
            IF p_enabled THEN
                PERFORM %1$I.set_sequence_next(p_seq_name, p_next);
            END IF;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.ensure_lane_sequences(
            p_queue TEXT,
            p_priority SMALLINT,
            p_enqueue_shard SMALLINT
        )
        RETURNS VOID
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_enqueue_seq TEXT := %1$I.queue_lane_sequence_name(
                'queue_enqueue_seq',
                p_queue,
                p_priority,
                p_enqueue_shard
            );
            v_claim_seq TEXT := %1$I.queue_lane_sequence_name(
                'queue_claim_seq',
                p_queue,
                p_priority,
                p_enqueue_shard
            );
        BEGIN
            EXECUTE format(
                'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                %1$L,
                v_enqueue_seq
            );
            EXECUTE format(
                'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                %1$L,
                v_claim_seq
            );

            UPDATE %1$I.queue_enqueue_heads
            SET seq_name = v_enqueue_seq
            WHERE queue = p_queue
              AND priority = p_priority
              AND enqueue_shard = p_enqueue_shard
              AND seq_name IS DISTINCT FROM v_enqueue_seq;

            UPDATE %1$I.queue_claim_heads
            SET seq_name = v_claim_seq
            WHERE queue = p_queue
              AND priority = p_priority
              AND enqueue_shard = p_enqueue_shard
              AND seq_name IS DISTINCT FROM v_claim_seq;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.reserve_enqueue_seq(
            p_queue TEXT,
            p_priority SMALLINT,
            p_enqueue_shard SMALLINT,
            p_count BIGINT
        )
        RETURNS BIGINT
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_seq_name TEXT;
            v_start BIGINT;
        BEGIN
            IF p_count <= 0 THEN
                RETURN %1$I.sequence_next_value((
                    SELECT seq_name
                    FROM %1$I.queue_enqueue_heads
                    WHERE queue = p_queue
                      AND priority = p_priority
                      AND enqueue_shard = p_enqueue_shard
                ));
            END IF;

            PERFORM %1$I.ensure_lane_sequences(p_queue, p_priority, p_enqueue_shard);

            SELECT seq_name
            INTO v_seq_name
            FROM %1$I.queue_enqueue_heads
            WHERE queue = p_queue
              AND priority = p_priority
              AND enqueue_shard = p_enqueue_shard;

            IF v_seq_name IS NULL THEN
                RAISE EXCEPTION 'missing enqueue lane sequence for queue %%, priority %%, shard %%',
                    p_queue, p_priority, p_enqueue_shard
                    USING ERRCODE = '55000';
            END IF;

            EXECUTE format(
                'SELECT min(nextval(%%L::regclass))::bigint FROM generate_series(1::bigint, $1)',
                format('%%I.%%I', %1$L, v_seq_name)
            )
            INTO v_start
            USING p_count;

            RETURN v_start;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.queue_enqueue_head_sequence_sync()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_seq_name TEXT := %1$I.queue_lane_sequence_name(
                'queue_enqueue_seq',
                NEW.queue,
                NEW.priority,
                NEW.enqueue_shard
            );
            v_qualified_seq TEXT := format('%%I.%%I', %1$L, v_seq_name);
            v_count BIGINT;
            v_start BIGINT;
        BEGIN
            EXECUTE format(
                'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                %1$L,
                v_seq_name
            );
            NEW.seq_name := v_seq_name;

            IF TG_OP = 'UPDATE'
               AND NEW.next_seq IS DISTINCT FROM OLD.next_seq
               AND NEW.next_seq > OLD.next_seq
            THEN
                v_count := NEW.next_seq - OLD.next_seq;
                EXECUTE format(
                    'SELECT min(nextval(%%L::regclass))::bigint FROM generate_series(1::bigint, $1)',
                    v_qualified_seq
                )
                INTO v_start
                USING v_count;
                NEW.next_seq := v_start + v_count;
            ELSE
                PERFORM %1$I.set_sequence_next(v_seq_name, NEW.next_seq);
            END IF;

            RETURN NEW;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE FUNCTION %1$I.queue_claim_head_sequence_sync()
        RETURNS TRIGGER
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $func$
        DECLARE
            v_seq_name TEXT := %1$I.queue_lane_sequence_name(
                'queue_claim_seq',
                NEW.queue,
                NEW.priority,
                NEW.enqueue_shard
            );
        BEGIN
            EXECUTE format(
                'CREATE SEQUENCE IF NOT EXISTS %%I.%%I AS bigint START WITH 1 MINVALUE 1 CACHE 1',
                %1$L,
                v_seq_name
            );
            NEW.seq_name := v_seq_name;

            IF TG_OP = 'INSERT' THEN
                PERFORM %1$I.set_sequence_next(v_seq_name, NEW.claim_seq);
            END IF;

            RETURN NEW;
        END;
        $func$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'DROP TRIGGER IF EXISTS queue_enqueue_heads_sequence_sync ON %I.queue_enqueue_heads',
        p_schema
    );

    EXECUTE format(
        'CREATE TRIGGER queue_enqueue_heads_sequence_sync BEFORE INSERT OR UPDATE OF next_seq ON %I.queue_enqueue_heads FOR EACH ROW EXECUTE FUNCTION %I.queue_enqueue_head_sequence_sync()',
        p_schema,
        p_schema
    );

    EXECUTE format(
        'DROP TRIGGER IF EXISTS queue_claim_heads_sequence_sync ON %I.queue_claim_heads',
        p_schema
    );

    EXECUTE format(
        'CREATE TRIGGER queue_claim_heads_sequence_sync BEFORE INSERT OR UPDATE OF claim_seq ON %I.queue_claim_heads FOR EACH ROW EXECUTE FUNCTION %I.queue_claim_head_sequence_sync()',
        p_schema,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_terminal_rollups (
            queue                  TEXT NOT NULL,
            priority               SMALLINT NOT NULL,
            pruned_completed_count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (queue, priority)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_claimer_leases (
            queue             TEXT NOT NULL,
            claimer_slot      SMALLINT NOT NULL,
            owner_instance_id UUID NOT NULL,
            lease_epoch       BIGINT NOT NULL DEFAULT 0,
            leased_at         TIMESTAMPTZ NOT NULL,
            last_claimed_at   TIMESTAMPTZ NOT NULL,
            expires_at        TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (queue, claimer_slot)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.queue_claimer_leases SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 200,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_claimer_state (
            queue           TEXT PRIMARY KEY,
            target_claimers SMALLINT NOT NULL,
            updated_at      TIMESTAMPTZ NOT NULL
        )
        $ddl$,
        p_schema
    );

    -- Drop the old expires_at-inclusive index shape (see queue_storage.rs
    -- for the HOT-update rationale) and replace with the narrower shape.
    EXECUTE format(
        'DROP INDEX IF EXISTS %I.idx_%s_queue_claimer_leases_owner',
        p_schema, p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_queue_claimer_leases_owner ON %I.queue_claimer_leases (queue, owner_instance_id)',
        p_schema, p_schema
    );

    -- Seed enqueue/claim heads + terminal rollups from any pre-existing
    -- queue_lanes rows. Idempotent under ON CONFLICT.
    EXECUTE format(
        $ddl$
        INSERT INTO %1$I.queue_enqueue_heads AS heads (queue, priority, enqueue_shard, next_seq)
        SELECT queue, priority, 0::smallint, next_seq
        FROM %1$I.queue_lanes
        ON CONFLICT (queue, priority, enqueue_shard) DO UPDATE
        SET next_seq = GREATEST(heads.next_seq, EXCLUDED.next_seq)
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        INSERT INTO %1$I.queue_claim_heads AS heads (queue, priority, enqueue_shard, claim_seq)
        SELECT queue, priority, 0::smallint, claim_seq
        FROM %1$I.queue_lanes
        ON CONFLICT (queue, priority, enqueue_shard) DO UPDATE
        SET claim_seq = GREATEST(heads.claim_seq, EXCLUDED.claim_seq)
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        SELECT %1$I.ensure_lane_sequences(lanes.queue, lanes.priority, lanes.enqueue_shard)
        FROM (
            SELECT queue, priority, enqueue_shard FROM %1$I.queue_enqueue_heads
            UNION
            SELECT queue, priority, enqueue_shard FROM %1$I.queue_claim_heads
        ) AS lanes
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        SELECT %1$I.set_sequence_next(seq_name, next_seq)
        FROM %1$I.queue_enqueue_heads
        WHERE seq_name IS NOT NULL
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        SELECT %1$I.set_sequence_next(seq_name, claim_seq)
        FROM %1$I.queue_claim_heads
        WHERE seq_name IS NOT NULL
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        INSERT INTO %1$I.queue_terminal_rollups AS rollups (queue, priority, pruned_completed_count)
        SELECT queue, priority, pruned_completed_count
        FROM %1$I.queue_lanes
        WHERE pruned_completed_count > 0
        ON CONFLICT (queue, priority) DO UPDATE
        SET pruned_completed_count = GREATEST(rollups.pruned_completed_count, EXCLUDED.pruned_completed_count)
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'UPDATE %I.queue_lanes SET pruned_completed_count = 0 WHERE pruned_completed_count > 0',
        p_schema
    );

    --------------------------------------------------------------------
    -- Partitioned leases / lease_claims / lease_claim_closures tables.
    --------------------------------------------------------------------

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.leases (
            lease_slot          INT NOT NULL,
            lease_generation    BIGINT NOT NULL,
            ready_slot          INT NOT NULL,
            ready_generation    BIGINT NOT NULL,
            job_id              BIGINT NOT NULL,
            queue               TEXT NOT NULL,
            state               awa.job_state NOT NULL DEFAULT 'running',
            priority            SMALLINT NOT NULL,
            attempt             SMALLINT NOT NULL DEFAULT 1,
            run_lease           BIGINT NOT NULL DEFAULT 1,
            max_attempts        SMALLINT NOT NULL DEFAULT 25,
            lane_seq            BIGINT NOT NULL,
            enqueue_shard       SMALLINT NOT NULL DEFAULT 0,
            heartbeat_at        TIMESTAMPTZ,
            deadline_at         TIMESTAMPTZ,
            attempted_at        TIMESTAMPTZ,
            callback_id         UUID,
            callback_timeout_at TIMESTAMPTZ,
            PRIMARY KEY (lease_slot, queue, priority, enqueue_shard, lane_seq)
        ) PARTITION BY LIST (lease_slot)
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.leases
            ADD COLUMN IF NOT EXISTS state awa.job_state NOT NULL DEFAULT 'running',
            ADD COLUMN IF NOT EXISTS enqueue_shard SMALLINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS attempted_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS callback_id UUID,
            ADD COLUMN IF NOT EXISTS callback_timeout_at TIMESTAMPTZ
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.lease_claims (
            claim_slot       INT NOT NULL,
            job_id           BIGINT NOT NULL,
            run_lease        BIGINT NOT NULL,
            ready_slot       INT NOT NULL,
            ready_generation BIGINT NOT NULL,
            queue            TEXT NOT NULL,
            priority         SMALLINT NOT NULL,
            attempt          SMALLINT NOT NULL,
            max_attempts     SMALLINT NOT NULL,
            lane_seq         BIGINT NOT NULL,
            enqueue_shard    SMALLINT NOT NULL DEFAULT 0,
            claimed_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            materialized_at  TIMESTAMPTZ,
            deadline_at      TIMESTAMPTZ,
            PRIMARY KEY (claim_slot, job_id, run_lease)
        ) PARTITION BY LIST (claim_slot)
        $ddl$,
        p_schema
    );

    -- Idempotent column-add for upgrades from before receipts carried
    -- shard/deadline metadata.
    EXECUTE format(
        $ddl$
        ALTER TABLE %I.lease_claims
            ADD COLUMN IF NOT EXISTS enqueue_shard SMALLINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ
        $ddl$,
        p_schema
    );

    FOR v_slot IN 0..(p_claim_slot_count - 1) LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.lease_claims FOR VALUES IN (%s)',
            p_schema, format('lease_claims_%s', v_slot), p_schema, v_slot
        );
    END LOOP;

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_lease_claims_stale ON %I.lease_claims (materialized_at, claimed_at, job_id)',
        p_schema, p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_lease_claims_deadline_brin ON %I.lease_claims USING BRIN (deadline_at) WITH (pages_per_range = 16)',
        p_schema, p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_lease_claims_job_run ON %I.lease_claims (job_id, run_lease)',
        p_schema, p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.lease_claim_closures (
            claim_slot INT NOT NULL,
            job_id     BIGINT NOT NULL,
            run_lease  BIGINT NOT NULL,
            outcome    TEXT NOT NULL,
            closed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (claim_slot, job_id, run_lease)
        ) PARTITION BY LIST (claim_slot)
        $ddl$,
        p_schema
    );

    FOR v_slot IN 0..(p_claim_slot_count - 1) LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.lease_claim_closures FOR VALUES IN (%s)',
            p_schema, format('lease_claim_closures_%s', v_slot), p_schema, v_slot
        );
    END LOOP;

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_lease_claim_closures_job_run ON %I.lease_claim_closures (job_id, run_lease)',
        p_schema, p_schema
    );

    --------------------------------------------------------------------
    -- Attempt state, ready_entries, done_entries, deferred_jobs, dlq_entries.
    --------------------------------------------------------------------

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.attempt_state (
            job_id               BIGINT NOT NULL,
            run_lease            BIGINT NOT NULL,
            heartbeat_at         TIMESTAMPTZ,
            progress             JSONB,
            callback_filter      TEXT,
            callback_on_complete TEXT,
            callback_on_fail     TEXT,
            callback_transform   TEXT,
            callback_result      JSONB,
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (job_id, run_lease)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'ALTER TABLE %I.attempt_state ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ',
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.attempt_state SET (
            fillfactor = 80,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 200,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.ready_entries (
            ready_slot       INT NOT NULL,
            ready_generation BIGINT NOT NULL,
            job_id           BIGINT NOT NULL,
            kind             TEXT NOT NULL,
            queue            TEXT NOT NULL,
            args             JSONB NOT NULL DEFAULT '{}'::jsonb,
            priority         SMALLINT NOT NULL,
            attempt          SMALLINT NOT NULL DEFAULT 0,
            run_lease        BIGINT NOT NULL DEFAULT 0,
            max_attempts     SMALLINT NOT NULL DEFAULT 25,
            lane_seq         BIGINT NOT NULL,
            enqueue_shard    SMALLINT NOT NULL DEFAULT 0,
            run_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            attempted_at     TIMESTAMPTZ,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            unique_key       BYTEA,
            unique_states    TEXT,
            payload          JSONB,
            PRIMARY KEY (ready_slot, queue, priority, enqueue_shard, lane_seq)
        ) PARTITION BY LIST (ready_slot)
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.done_entries (
            ready_slot       INT NOT NULL,
            ready_generation BIGINT NOT NULL,
            job_id           BIGINT NOT NULL,
            kind             TEXT NOT NULL,
            queue            TEXT NOT NULL,
            args             JSONB,
            state            awa.job_state NOT NULL DEFAULT 'completed',
            priority         SMALLINT NOT NULL,
            attempt          SMALLINT NOT NULL DEFAULT 1,
            run_lease        BIGINT NOT NULL DEFAULT 1,
            max_attempts     SMALLINT,
            lane_seq         BIGINT NOT NULL,
            enqueue_shard    SMALLINT NOT NULL DEFAULT 0,
            run_at           TIMESTAMPTZ,
            attempted_at     TIMESTAMPTZ,
            finalized_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            created_at       TIMESTAMPTZ,
            unique_key       BYTEA,
            unique_states    TEXT,
            payload          JSONB,
            PRIMARY KEY (ready_slot, queue, priority, enqueue_shard, lane_seq)
        ) PARTITION BY LIST (ready_slot)
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.done_entries
            ADD COLUMN IF NOT EXISTS args JSONB,
            ADD COLUMN IF NOT EXISTS state awa.job_state NOT NULL DEFAULT 'completed',
            ADD COLUMN IF NOT EXISTS max_attempts SMALLINT,
            ADD COLUMN IF NOT EXISTS enqueue_shard SMALLINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS run_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS attempted_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS unique_key BYTEA,
            ADD COLUMN IF NOT EXISTS unique_states TEXT,
            ADD COLUMN IF NOT EXISTS payload JSONB
        $ddl$,
        p_schema
    );

    -- Narrow terminal history: see queue_storage.rs comment.
    EXECUTE format(
        $ddl$
        ALTER TABLE %I.done_entries
            ALTER COLUMN args         DROP NOT NULL,
            ALTER COLUMN args         DROP DEFAULT,
            ALTER COLUMN max_attempts DROP NOT NULL,
            ALTER COLUMN max_attempts DROP DEFAULT,
            ALTER COLUMN run_at       DROP NOT NULL,
            ALTER COLUMN run_at       DROP DEFAULT,
            ALTER COLUMN created_at   DROP NOT NULL,
            ALTER COLUMN created_at   DROP DEFAULT
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE OR REPLACE VIEW %1$I.terminal_jobs AS
        SELECT
            done.ready_slot,
            done.ready_generation,
            done.job_id,
            done.kind,
            done.queue,
            COALESCE(done.args, ready.args, '{}'::jsonb) AS args,
            done.state,
            done.priority,
            done.attempt,
            done.run_lease,
            COALESCE(done.max_attempts, ready.max_attempts, 25::smallint) AS max_attempts,
            done.lane_seq,
            done.enqueue_shard,
            COALESCE(done.run_at, ready.run_at, done.finalized_at) AS run_at,
            COALESCE(done.attempted_at, ready.attempted_at) AS attempted_at,
            done.finalized_at,
            COALESCE(done.created_at, ready.created_at, done.finalized_at) AS created_at,
            COALESCE(done.unique_key, ready.unique_key) AS unique_key,
            COALESCE(done.unique_states, ready.unique_states) AS unique_states,
            COALESCE(done.payload, ready.payload, '{}'::jsonb) AS payload
        FROM %1$I.done_entries AS done
        LEFT JOIN %1$I.ready_entries AS ready
          ON ready.ready_slot       = done.ready_slot
         AND ready.ready_generation = done.ready_generation
         AND ready.queue            = done.queue
         AND ready.priority         = done.priority
         AND ready.enqueue_shard    = done.enqueue_shard
         AND ready.lane_seq         = done.lane_seq
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.deferred_jobs (
            job_id        BIGINT PRIMARY KEY,
            kind          TEXT NOT NULL,
            queue         TEXT NOT NULL,
            args          JSONB NOT NULL DEFAULT '{}'::jsonb,
            state         awa.job_state NOT NULL,
            priority      SMALLINT NOT NULL,
            attempt       SMALLINT NOT NULL DEFAULT 0,
            run_lease     BIGINT NOT NULL DEFAULT 0,
            max_attempts  SMALLINT NOT NULL DEFAULT 25,
            run_at        TIMESTAMPTZ NOT NULL,
            attempted_at  TIMESTAMPTZ,
            finalized_at  TIMESTAMPTZ,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            unique_key    BYTEA,
            unique_states TEXT,
            payload       JSONB,
            CONSTRAINT deferred_jobs_state_check CHECK (state IN ('scheduled', 'retryable'))
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.deferred_jobs
            ADD COLUMN IF NOT EXISTS args JSONB NOT NULL DEFAULT '{}'::jsonb,
            ADD COLUMN IF NOT EXISTS state awa.job_state NOT NULL DEFAULT 'scheduled',
            ADD COLUMN IF NOT EXISTS attempt SMALLINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS run_lease BIGINT NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS max_attempts SMALLINT NOT NULL DEFAULT 25,
            ADD COLUMN IF NOT EXISTS attempted_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            ADD COLUMN IF NOT EXISTS unique_key BYTEA,
            ADD COLUMN IF NOT EXISTS unique_states TEXT,
            ADD COLUMN IF NOT EXISTS payload JSONB
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_deferred_due ON %I.deferred_jobs (state, run_at, queue, priority, job_id)',
        p_schema, p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_deferred_job_unique ON %I.deferred_jobs (unique_key)',
        p_schema, p_schema
    );

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.dlq_entries (
            job_id             BIGINT PRIMARY KEY,
            kind               TEXT NOT NULL,
            queue              TEXT NOT NULL,
            args               JSONB NOT NULL DEFAULT '{}'::jsonb,
            state              awa.job_state NOT NULL DEFAULT 'failed',
            priority           SMALLINT NOT NULL,
            attempt            SMALLINT NOT NULL DEFAULT 1,
            run_lease          BIGINT NOT NULL DEFAULT 1,
            max_attempts       SMALLINT NOT NULL DEFAULT 25,
            run_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            attempted_at       TIMESTAMPTZ,
            finalized_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            unique_key         BYTEA,
            unique_states      TEXT,
            payload            JSONB,
            dlq_reason         TEXT NOT NULL,
            dlq_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            original_run_lease BIGINT NOT NULL
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_dlq_queue_time ON %I.dlq_entries (queue, dlq_at DESC)',
        p_schema, p_schema
    );

    --------------------------------------------------------------------
    -- ready_entries / done_entries partitions + lane indexes.
    --------------------------------------------------------------------

    FOR v_slot IN 0..(p_queue_slot_count - 1) LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.ready_entries FOR VALUES IN (%s)',
            p_schema, format('ready_entries_%s', v_slot), p_schema, v_slot
        );

        -- Shard-aware lane index (v021): includes enqueue_shard so
        -- claim_ready_runtime's WHERE clause hits the index directly.
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_ready_%s_lane_shard ON %I.%I (queue, priority, enqueue_shard, lane_seq)',
            p_schema, v_slot, p_schema, format('ready_entries_%s', v_slot)
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_ready_%s_job ON %I.%I (job_id)',
            p_schema, v_slot, p_schema, format('ready_entries_%s', v_slot)
        );

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.done_entries FOR VALUES IN (%s)',
            p_schema, format('done_entries_%s', v_slot), p_schema, v_slot
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_done_%s_lane_shard ON %I.%I (queue, priority, enqueue_shard, lane_seq)',
            p_schema, v_slot, p_schema, format('done_entries_%s', v_slot)
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_done_%s_job ON %I.%I (job_id)',
            p_schema, v_slot, p_schema, format('done_entries_%s', v_slot)
        );
    END LOOP;

    --------------------------------------------------------------------
    -- queue_terminal_live_counts (#290).
    --------------------------------------------------------------------

    EXECUTE format(
        $ddl$
        CREATE TABLE IF NOT EXISTS %I.queue_terminal_live_counts (
            ready_slot          INT NOT NULL,
            queue               TEXT NOT NULL,
            priority            SMALLINT NOT NULL,
            enqueue_shard       SMALLINT NOT NULL,
            counter_bucket      SMALLINT NOT NULL DEFAULT 0,
            live_terminal_count BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (ready_slot, queue, priority, enqueue_shard, counter_bucket)
        )
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'ALTER TABLE %I.queue_terminal_live_counts ADD COLUMN IF NOT EXISTS counter_bucket SMALLINT NOT NULL DEFAULT 0',
        p_schema
    );

    EXECUTE format(
        $ddl$
        DO $inner$
        DECLARE
            v_has_bucket_key BOOLEAN;
        BEGIN
            SELECT EXISTS (
                SELECT 1
                FROM pg_constraint AS c
                JOIN pg_class AS t ON t.oid = c.conrelid
                JOIN pg_namespace AS n ON n.oid = t.relnamespace
                WHERE n.nspname = %1$L
                  AND t.relname = 'queue_terminal_live_counts'
                  AND c.contype = 'p'
                  AND EXISTS (
                      SELECT 1
                      FROM unnest(c.conkey) AS key(attnum)
                      JOIN pg_attribute AS a
                        ON a.attrelid = t.oid
                       AND a.attnum = key.attnum
                      WHERE a.attname = 'counter_bucket'
                  )
            )
            INTO v_has_bucket_key;

            IF NOT v_has_bucket_key THEN
                ALTER TABLE %1$I.queue_terminal_live_counts
                    DROP CONSTRAINT IF EXISTS queue_terminal_live_counts_pkey;
                ALTER TABLE %1$I.queue_terminal_live_counts
                    ADD CONSTRAINT queue_terminal_live_counts_pkey
                    PRIMARY KEY (ready_slot, queue, priority, enqueue_shard, counter_bucket);
            END IF;
        END
        $inner$
        $ddl$,
        p_schema
    );

    EXECUTE format(
        'CREATE INDEX IF NOT EXISTS idx_%s_queue_terminal_live_counts_queue ON %I.queue_terminal_live_counts (queue, priority)',
        p_schema, p_schema
    );

    EXECUTE format(
        $ddl$
        ALTER TABLE %I.queue_terminal_live_counts SET (
            fillfactor = 50,
            autovacuum_vacuum_scale_factor = 0.0,
            autovacuum_vacuum_threshold = 200,
            autovacuum_vacuum_cost_limit = 2000,
            autovacuum_vacuum_cost_delay = 2
        )
        $ddl$,
        p_schema
    );

    -- Backfill from existing done_entries (no-op on fresh installs).
    EXECUTE format(
        $ddl$
        INSERT INTO %1$I.queue_terminal_live_counts AS counts (
            ready_slot, queue, priority, enqueue_shard, counter_bucket, live_terminal_count
        )
        SELECT
            ready_slot,
            queue,
            priority,
            enqueue_shard,
            mod(mod(job_id, 256::bigint) + 256::bigint, 256::bigint)::smallint AS counter_bucket,
            count(*)::bigint
        FROM %1$I.done_entries
        WHERE NOT EXISTS (
            SELECT 1 FROM %1$I.queue_terminal_live_counts LIMIT 1
        )
        GROUP BY ready_slot, queue, priority, enqueue_shard, counter_bucket
        ON CONFLICT (ready_slot, queue, priority, enqueue_shard, counter_bucket) DO NOTHING
        $ddl$,
        p_schema
    );

    -- Auto-mark trusted for fresh installs where done_entries is empty.
    EXECUTE format(
        $ddl$
        UPDATE %1$I.queue_ring_state
        SET terminal_counter_trusted_at = now()
        WHERE singleton = TRUE
          AND terminal_counter_trusted_at IS NULL
          AND NOT EXISTS (SELECT 1 FROM %1$I.done_entries LIMIT 1)
        $ddl$,
        p_schema
    );

    --------------------------------------------------------------------
    -- leases partitions + indexes.
    --------------------------------------------------------------------

    FOR v_slot IN 0..(p_lease_slot_count - 1) LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.leases FOR VALUES IN (%s)',
            p_schema, format('leases_%s', v_slot), p_schema, v_slot
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_leases_%s_lane_shard ON %I.%I (queue, priority, enqueue_shard, lane_seq)',
            p_schema, v_slot, p_schema, format('leases_%s', v_slot)
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_leases_%s_ready_ref ON %I.%I (ready_slot, ready_generation)',
            p_schema, v_slot, p_schema, format('leases_%s', v_slot)
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_leases_%s_job ON %I.%I (job_id, run_lease)',
            p_schema, v_slot, p_schema, format('leases_%s', v_slot)
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_leases_%s_callback ON %I.%I (callback_id)',
            p_schema, v_slot, p_schema, format('leases_%s', v_slot)
        );

        -- #169 B1: idx_state_hb dropped. In receipts mode (the only
        -- supported shape for the default `awa` schema)
        -- `leases.heartbeat_at` is never written, so the index would
        -- be empty; the rescue path reads `attempt_state.heartbeat_at`
        -- instead. Legacy non-receipts custom schemas fall back to a
        -- seq-scan of `WHERE state='running'` rows (bounded by live
        -- worker count, called at 30s cadence) — acceptable. v025
        -- drops the index from existing 0.6 deployments.

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_leases_%s_state_deadline ON %I.%I (state, deadline_at)',
            p_schema, v_slot, p_schema, format('leases_%s', v_slot)
        );

        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%s_leases_%s_state_callback_timeout ON %I.%I (state, callback_timeout_at)',
            p_schema, v_slot, p_schema, format('leases_%s', v_slot)
        );
    END LOOP;

    --------------------------------------------------------------------
    -- claim_ready_runtime() function. The claim CTE branches on the
    -- receipts mode: receipts=TRUE writes into lease_claims (ADR-023),
    -- receipts=FALSE writes directly into the partitioned leases table.
    --------------------------------------------------------------------

    v_advance_claim_condition := 'FALSE';

    IF p_lease_claim_receipts THEN
        v_claimed_cte := format(
            $cte$
            claim_ring AS (
                SELECT current_slot AS claim_slot
                FROM %1$I.claim_ring_state
                WHERE singleton = TRUE
            ),
            claimed AS (
                INSERT INTO %1$I.lease_claims AS claim_rows (
                    claim_slot,
                    job_id,
                    run_lease,
                    ready_slot,
                    ready_generation,
                    queue,
                    priority,
                    attempt,
                    max_attempts,
                    lane_seq,
                    enqueue_shard,
                    deadline_at
                )
                SELECT
                    claim_ring.claim_slot,
                    selected.job_id,
                    selected.run_lease + 1,
                    selected.ready_slot,
                    selected.ready_generation,
                    selected.queue,
                    selected.effective_priority,
                    selected.attempt + 1,
                    selected.max_attempts,
                    selected.lane_seq,
                    v_lane_shard,
                    v_deadline_at
                FROM selected
                CROSS JOIN claim_ring
                RETURNING
                    claim_rows.claim_slot,
                    claim_rows.ready_slot,
                    claim_rows.ready_generation,
                    claim_rows.job_id,
                    claim_rows.queue,
                    claim_rows.priority,
                    claim_rows.lane_seq,
                    claim_rows.attempt,
                    claim_rows.run_lease,
                    claim_rows.max_attempts
            )
            $cte$,
            p_schema
        );
    ELSE
        v_claimed_cte := format(
            $cte$
            claimed AS (
                INSERT INTO %1$I.leases AS lease_rows (
                    lease_slot,
                    lease_generation,
                    ready_slot,
                    ready_generation,
                    job_id,
                    queue,
                    state,
                    priority,
                    attempt,
                    run_lease,
                    max_attempts,
                    lane_seq,
                    enqueue_shard,
                    heartbeat_at,
                    deadline_at,
                    attempted_at
                )
                SELECT
                    lease_ring.lease_slot,
                    lease_ring.lease_generation,
                    selected.ready_slot,
                    selected.ready_generation,
                    selected.job_id,
                    selected.queue,
                    'running'::awa.job_state,
                    selected.effective_priority,
                    selected.attempt + 1,
                    selected.run_lease + 1,
                    selected.max_attempts,
                    selected.lane_seq,
                    v_lane_shard,
                    v_claimed_at,
                    v_deadline_at,
                    v_claimed_at
                FROM selected
                CROSS JOIN lease_ring
                RETURNING
                    0::int AS claim_slot,
                    lease_rows.ready_slot,
                    lease_rows.ready_generation,
                    lease_rows.lease_slot,
                    lease_rows.lease_generation,
                    lease_rows.queue,
                    lease_rows.priority,
                    lease_rows.lane_seq,
                    lease_rows.attempt,
                    lease_rows.run_lease,
                    lease_rows.max_attempts,
                    lease_rows.heartbeat_at,
                    lease_rows.deadline_at,
                    lease_rows.attempted_at
            )
            $cte$,
            p_schema
        );
    END IF;

    EXECUTE format(
        $create_runtime$
        CREATE OR REPLACE FUNCTION %1$I.claim_ready_runtime(
            p_queue TEXT,
            p_max_batch BIGINT,
            p_deadline_secs DOUBLE PRECISION,
            p_aging_secs DOUBLE PRECISION
        )
        RETURNS TABLE(
            ready_slot INT,
            ready_generation BIGINT,
            lane_seq BIGINT,
            enqueue_shard SMALLINT,
            lease_slot INT,
            lease_generation BIGINT,
            claim_slot INT,
            job_id BIGINT,
            kind TEXT,
            queue TEXT,
            args JSONB,
            lane_priority SMALLINT,
            priority SMALLINT,
            attempt SMALLINT,
            run_lease BIGINT,
            max_attempts SMALLINT,
            run_at TIMESTAMPTZ,
            heartbeat_at TIMESTAMPTZ,
            deadline_at TIMESTAMPTZ,
            attempted_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ,
            unique_key BYTEA,
            unique_states TEXT,
            payload JSONB
        )
        LANGUAGE plpgsql
        SET search_path = pg_catalog, awa, public
        AS $func$
        DECLARE
            v_lane_priority SMALLINT;
            v_lane_shard SMALLINT;
            v_claim_seq_name TEXT;
            v_lane_claim_seq BIGINT;
            v_lane_next_seq BIGINT;
            v_claim_limit BIGINT;
            v_claimed_count BIGINT;
            v_target_slot INT;
            v_target_generation BIGINT;
            v_claimed_at TIMESTAMPTZ;
            v_deadline_at TIMESTAMPTZ;
        BEGIN
            SELECT
                claims.priority,
                claims.enqueue_shard,
                claims.seq_name,
                cursors.claim_seq,
                cursors.next_seq
            INTO v_lane_priority, v_lane_shard, v_claim_seq_name, v_lane_claim_seq, v_lane_next_seq
            FROM %1$I.queue_claim_heads AS claims
            JOIN %1$I.queue_enqueue_heads AS enqueues
              ON enqueues.queue = claims.queue
             AND enqueues.priority = claims.priority
             AND enqueues.enqueue_shard = claims.enqueue_shard
            CROSS JOIN LATERAL (
                SELECT
                    %1$I.sequence_next_value(claims.seq_name) AS claim_seq,
                    %1$I.sequence_next_value(enqueues.seq_name) AS next_seq
            ) AS cursors
            LEFT JOIN LATERAL (
                SELECT
                    ready.ready_slot,
                    ready.ready_generation,
                    ready.run_at,
                    CASE
                        WHEN p_aging_secs > 0 THEN GREATEST(
                            1,
                            claims.priority - FLOOR(
                                EXTRACT(EPOCH FROM (clock_timestamp() - ready.run_at)) / p_aging_secs
                            )::smallint
                        )::smallint
                        ELSE claims.priority
                    END AS effective_priority
                FROM %1$I.ready_entries AS ready
                WHERE ready.queue = p_queue
                  AND ready.priority = claims.priority
                  AND ready.enqueue_shard = claims.enqueue_shard
                  AND ready.lane_seq >= cursors.claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT 1
            ) AS candidate ON TRUE
            WHERE claims.queue = p_queue
              AND NOT EXISTS (
                  SELECT 1
                  FROM awa.queue_meta AS meta
                  WHERE meta.queue = p_queue
                    AND meta.paused = TRUE
              )
              AND cursors.claim_seq < cursors.next_seq
            ORDER BY
                candidate.effective_priority ASC NULLS LAST,
                candidate.run_at ASC NULLS LAST,
                claims.priority ASC
            LIMIT 1
            FOR UPDATE OF claims SKIP LOCKED;

            IF NOT FOUND THEN
                RETURN;
            END IF;

            SELECT ready.ready_slot, ready.ready_generation
            INTO v_target_slot, v_target_generation
            FROM %1$I.ready_entries AS ready
            WHERE ready.queue = p_queue
              AND ready.priority = v_lane_priority
              AND ready.enqueue_shard = v_lane_shard
              AND ready.lane_seq >= v_lane_claim_seq
            ORDER BY ready.lane_seq ASC
            LIMIT 1;

            IF NOT FOUND THEN
                -- The enqueue cursor is sequence-backed and can include
                -- uncommitted reservations. If no committed ready row is
                -- visible yet, do not advance the claim cursor to the enqueue
                -- cursor: that can skip a ready row when the enqueue
                -- transaction commits moments later. True gaps from admin
                -- deletes are harmless over-count drift and close naturally
                -- when a later committed row on the lane is claimed.
                RETURN;
            END IF;

            v_claim_limit := LEAST(GREATEST(v_lane_next_seq - v_lane_claim_seq, 0), p_max_batch);
            IF v_claim_limit <= 0 THEN
                RETURN;
            END IF;

            v_claimed_at := clock_timestamp();
            IF p_deadline_secs > 0 THEN
                v_deadline_at := v_claimed_at + make_interval(secs => p_deadline_secs);
            ELSE
                v_deadline_at := NULL::timestamptz;
            END IF;

            RETURN QUERY
            WITH lease_ring AS (
                SELECT current_slot AS lease_slot, generation AS lease_generation
                FROM %1$I.lease_ring_state
                WHERE singleton = TRUE
            ),
            selected AS (
                SELECT
                    ready.ready_slot,
                    ready.ready_generation,
                    ready.job_id,
                    ready.kind,
                    ready.queue,
                    ready.args,
                    ready.priority AS lane_priority,
                    CASE
                        WHEN p_aging_secs > 0 THEN GREATEST(
                            1,
                            ready.priority - FLOOR(
                                EXTRACT(EPOCH FROM (clock_timestamp() - ready.run_at)) / p_aging_secs
                            )::smallint
                        )::smallint
                        ELSE ready.priority
                    END AS effective_priority,
                    ready.attempt,
                    ready.run_lease,
                    ready.max_attempts,
                    ready.lane_seq,
                    ready.run_at,
                    ready.created_at,
                    ready.unique_key,
                    ready.unique_states,
                    COALESCE(ready.payload, '{}'::jsonb) AS payload
                FROM %1$I.ready_entries AS ready
                WHERE ready.queue = p_queue
                  AND ready.priority = v_lane_priority
                  AND ready.enqueue_shard = v_lane_shard
                  AND ready.ready_slot = v_target_slot
                  AND ready.ready_generation = v_target_generation
                  AND ready.lane_seq >= v_lane_claim_seq
                ORDER BY ready.lane_seq ASC
                LIMIT v_claim_limit
            ),
            advanced AS (
                SELECT %1$I.set_sequence_next_if(
                    v_claim_seq_name,
                    COALESCE(
                        (SELECT max(selected.lane_seq) + 1 FROM selected),
                        v_lane_claim_seq
                    ),
                    %3$s
                )
            ),
            %2$s
            SELECT
                claimed.ready_slot,
                claimed.ready_generation,
                claimed.lane_seq,
                v_lane_shard AS enqueue_shard,
                lease_ring.lease_slot,
                lease_ring.lease_generation,
                claimed.claim_slot,
                selected.job_id,
                selected.kind,
                selected.queue,
                selected.args,
                selected.lane_priority,
                selected.effective_priority,
                claimed.attempt,
                claimed.run_lease,
                claimed.max_attempts,
                selected.run_at,
                CASE
                    WHEN p_deadline_secs > 0 THEN v_claimed_at
                    ELSE NULL::timestamptz
                END AS heartbeat_at,
                v_deadline_at AS deadline_at,
                CASE
                    WHEN p_deadline_secs > 0 THEN v_claimed_at
                    ELSE NULL::timestamptz
                END AS attempted_at,
                selected.created_at,
                selected.unique_key,
                selected.unique_states,
                selected.payload
            FROM claimed
            CROSS JOIN lease_ring
            CROSS JOIN advanced
            JOIN selected
             ON selected.ready_slot = claimed.ready_slot
             AND selected.ready_generation = claimed.ready_generation
             AND selected.queue = claimed.queue
             AND selected.effective_priority = claimed.priority
             AND selected.lane_seq = claimed.lane_seq
            ORDER BY selected.lane_seq ASC;

            GET DIAGNOSTICS v_claimed_count = ROW_COUNT;

            -- If a target ready row existed but the claim CTE produced no
            -- rows, leave the sequence cursor untouched. The earlier
            -- NOT FOUND branch handles true gaps. Advancing here can skip a
            -- live retry/rescue row because sequence movement is
            -- non-transactional and cannot be recovered by rollback.
            IF v_claimed_count = 0 THEN
                RETURN;
            END IF;
        END;
        $func$
        $create_runtime$,
        p_schema, v_claimed_cte, v_advance_claim_condition
    );

    --------------------------------------------------------------------
    -- Seed ring-slot generation rows. Slot 0 starts at generation 0;
    -- the rest start at -1 (sentinel for "never rotated through").
    --------------------------------------------------------------------

    FOR v_slot IN 0..(p_queue_slot_count - 1) LOOP
        v_initial_gen := CASE WHEN v_slot = 0 THEN 0 ELSE -1 END;
        EXECUTE format(
            'INSERT INTO %I.queue_ring_slots (slot, generation) VALUES (%s, %s) ON CONFLICT (slot) DO NOTHING',
            p_schema, v_slot, v_initial_gen
        );
    END LOOP;

    FOR v_slot IN 0..(p_lease_slot_count - 1) LOOP
        v_initial_gen := CASE WHEN v_slot = 0 THEN 0 ELSE -1 END;
        EXECUTE format(
            'INSERT INTO %I.lease_ring_slots (slot, generation) VALUES (%s, %s) ON CONFLICT (slot) DO NOTHING',
            p_schema, v_slot, v_initial_gen
        );
    END LOOP;

    FOR v_slot IN 0..(p_claim_slot_count - 1) LOOP
        v_initial_gen := CASE WHEN v_slot = 0 THEN 0 ELSE -1 END;
        EXECUTE format(
            'INSERT INTO %I.claim_ring_slots (slot, generation) VALUES (%s, %s) ON CONFLICT (slot) DO NOTHING',
            p_schema, v_slot, v_initial_gen
        );
    END LOOP;
END;
$install$;

COMMENT ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) IS
    'Installs the queue-storage substrate (sequences, ring-state singletons, partitioned ready/done/lease tables, lane indexes, claim_ready_runtime()) into the named schema. The default ''awa'' substrate is migration-owned and default-shaped: lease_claim_receipts=TRUE and (queue=16, lease=8, claim=8). Operators wanting non-default slot counts or lease_claim_receipts=FALSE must use a custom queue-storage schema. SECURITY INVOKER, takes a per-schema advisory xact lock. See #308.';

REVOKE EXECUTE ON FUNCTION awa.install_queue_storage_substrate(TEXT, INT, INT, INT, BOOLEAN) FROM PUBLIC;

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
                      HINT = 'Run the ADR-023 reverse migration (recreate from lease_claims minus lease_claim_closures) to drain it, then re-run awa migrate.';
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
