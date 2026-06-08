CREATE TABLE IF NOT EXISTS awa.batch_operations (
    id              UUID PRIMARY KEY,
    op_kind         TEXT NOT NULL,
    filter          JSONB NOT NULL,
    spec            JSONB NOT NULL,
    state           TEXT NOT NULL,
    submitted_by    TEXT,
    submitted_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at      TIMESTAMPTZ,
    finalized_at    TIMESTAMPTZ,
    cursor          JSONB,
    total_matched   BIGINT,
    processed       BIGINT NOT NULL DEFAULT 0,
    skipped         BIGINT NOT NULL DEFAULT 0,
    errored         BIGINT NOT NULL DEFAULT 0,
    last_error      TEXT,
    runner_instance UUID,
    retention_until TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT batch_operations_state_check CHECK (
        state IN ('pending', 'scanning', 'running', 'cancelling', 'completed', 'cancelled', 'failed')
    ),
    CONSTRAINT batch_operations_op_kind_check CHECK (
        op_kind IN ('set_priority', 'move_queue')
    )
);

CREATE INDEX IF NOT EXISTS idx_batch_operations_state_submitted
    ON awa.batch_operations (state, submitted_at);

CREATE INDEX IF NOT EXISTS idx_batch_operations_retention
    ON awa.batch_operations (retention_until)
    WHERE retention_until IS NOT NULL;

CREATE TABLE IF NOT EXISTS awa.batch_operation_items (
    operation_id UUID NOT NULL REFERENCES awa.batch_operations(id) ON DELETE CASCADE,
    job_id       BIGINT NOT NULL,
    state        TEXT NOT NULL DEFAULT 'pending',
    error        TEXT,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (operation_id, job_id),
    CONSTRAINT batch_operation_items_state_check CHECK (
        state IN ('pending', 'processed', 'skipped', 'errored')
    )
);

CREATE INDEX IF NOT EXISTS idx_batch_operation_items_pending
    ON awa.batch_operation_items (operation_id, job_id)
    WHERE state = 'pending';
