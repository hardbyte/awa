-- v048: Schema patch ledger (#492)
--
-- A maintenance line cannot add migration versions once its released binaries
-- interpret higher numbers through their forward-compatibility guards, so a fix
-- that must reach such a line ships as a named, idempotent schema patch
-- (ADR-046). This ledger records which patches a database has received, by
-- name, so the patch runner, `awa migrate --pending`, and operators agree on
-- what is applied without probing object shapes.
--
-- The table text below is SCHEMA_PATCH_LEDGER_DDL in awa-model/src/migrations.rs
-- (a unit test keeps them identical): the 0.6 patch runner and the exported
-- R__ scripts create the same table on databases that never ran this
-- migration. The seed rows record the patches v046 and v047 already carry, so a
-- database that arrives here from 0.6.x (patches applied, no ledger) and a
-- fresh 0.7 install look the same.
--
-- N-1 (0.6.x) binaries never read this table. Additive; completes in
-- milliseconds and takes no lock that conflicts with job traffic.

CREATE TABLE IF NOT EXISTS awa.schema_patches (
    name        TEXT        PRIMARY KEY,
    description TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_by  TEXT        NOT NULL
);

INSERT INTO awa.schema_patches (name, description, applied_by)
VALUES
    ('wait_free_dirty_marks', 'Wait-free admin dirty-key marks', 'migration v046'),
    ('prepared_lane_sequences', 'Use provisioned lane sequences without runtime DDL', 'migration v047')
ON CONFLICT (name) DO NOTHING;

INSERT INTO awa.schema_version (version, description)
VALUES (48, 'Schema patch ledger')
ON CONFLICT (version) DO NOTHING;
