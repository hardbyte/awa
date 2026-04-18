ALTER TABLE awa.queue_descriptors
ADD COLUMN IF NOT EXISTS sync_interval_ms BIGINT NOT NULL DEFAULT 10000;

ALTER TABLE awa.job_kind_descriptors
ADD COLUMN IF NOT EXISTS sync_interval_ms BIGINT NOT NULL DEFAULT 10000;

INSERT INTO awa.schema_version (version, description)
VALUES (10, 'Descriptor liveness tracking')
ON CONFLICT (version) DO NOTHING;
