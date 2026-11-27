-- v063: let batch operations retry, cancel, discard and dead-letter jobs.
--
-- ADR-030's batch_operations table limited op_kind to set_priority and
-- move_queue. This widens the CHECK constraint to the operator bulk actions
-- the runner now applies a chunk at a time: retry, cancel, discard,
-- move_to_dlq, retry_from_dlq and purge_dlq.
--
-- Dropping and re-adding the constraint takes an ACCESS EXCLUSIVE lock on
-- awa.batch_operations only, a small control table, while the new constraint
-- checks its rows. No job or queue-storage table is touched. Binaries before
-- v063 never write the new kinds, so they can run before or after it.

ALTER TABLE awa.batch_operations
    DROP CONSTRAINT IF EXISTS batch_operations_op_kind_check;

ALTER TABLE awa.batch_operations
    ADD CONSTRAINT batch_operations_op_kind_check CHECK (
        op_kind IN (
            'set_priority',
            'move_queue',
            'retry',
            'cancel',
            'discard',
            'move_to_dlq',
            'retry_from_dlq',
            'purge_dlq'
        )
    );

INSERT INTO awa.schema_version (version, description)
VALUES (63, 'Let batch operations retry, cancel, discard and dead-letter jobs')
ON CONFLICT (version) DO NOTHING;
