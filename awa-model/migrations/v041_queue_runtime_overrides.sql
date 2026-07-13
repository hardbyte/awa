-- v041: per-queue runtime overrides (ADR-038, #385/#396).
--
-- Nullable override columns on the existing control-plane row. Dispatchers
-- already poll queue_meta for `paused`; they additionally refresh these on
-- a slow cadence and apply them without a restart. NULL means "no
-- override" — the builder-configured value stays in effect.
ALTER TABLE awa.queue_meta
    ADD COLUMN IF NOT EXISTS override_poll_interval_ms INTEGER
        CHECK (override_poll_interval_ms IS NULL OR override_poll_interval_ms > 0),
    ADD COLUMN IF NOT EXISTS override_claim_batch_size INTEGER
        CHECK (override_claim_batch_size IS NULL OR override_claim_batch_size > 0),
    ADD COLUMN IF NOT EXISTS override_rate_limit DOUBLE PRECISION
        CHECK (override_rate_limit IS NULL OR override_rate_limit > 0),
    ADD COLUMN IF NOT EXISTS override_deadline_ms BIGINT
        CHECK (override_deadline_ms IS NULL OR override_deadline_ms >= 0),
    ADD COLUMN IF NOT EXISTS overrides_updated_at TIMESTAMPTZ;

INSERT INTO awa.schema_version (version, description)
VALUES (41, 'Per-queue runtime overrides on queue_meta')
ON CONFLICT (version) DO NOTHING;
