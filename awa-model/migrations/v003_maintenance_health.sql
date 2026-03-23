ALTER TABLE awa.runtime_instances
    ADD COLUMN IF NOT EXISTS maintenance_alive BOOLEAN NOT NULL DEFAULT FALSE;

INSERT INTO awa.schema_version (version, description)
VALUES (3, 'Maintenance loop health in runtime snapshots') ON CONFLICT (version) DO NOTHING;
