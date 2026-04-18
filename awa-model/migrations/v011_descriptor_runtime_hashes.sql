ALTER TABLE awa.runtime_instances
    ADD COLUMN IF NOT EXISTS queue_descriptor_hashes JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS job_kind_descriptor_hashes JSONB NOT NULL DEFAULT '{}'::jsonb;

INSERT INTO awa.schema_version (version, description)
VALUES (11, 'Descriptor runtime hash snapshots') ON CONFLICT (version) DO NOTHING;
