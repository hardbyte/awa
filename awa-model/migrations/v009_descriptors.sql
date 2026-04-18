-- Queue and job-kind descriptor catalogs.
--
-- Descriptors are code-owned control-plane metadata (display name,
-- description, owner, docs URL, tags, extra JSON). They are intentionally
-- separate from awa.queue_meta, which holds mutable operator-owned runtime
-- state (pause flags, etc) and participates in the dispatcher claim path.
--
-- sync_interval_ms + last_seen_at give admin surfaces a way to mark a
-- descriptor stale when the declaring runtime goes away.
--
-- queue_descriptor_hashes / job_kind_descriptor_hashes on runtime_instances
-- let admin surfaces detect drift when two live runtimes disagree on a
-- descriptor (e.g. mid-rollout).

CREATE TABLE IF NOT EXISTS awa.queue_descriptors (
    queue            TEXT PRIMARY KEY,
    display_name     TEXT,
    description      TEXT,
    owner            TEXT,
    docs_url         TEXT,
    tags             TEXT[]      NOT NULL DEFAULT '{}',
    extra            JSONB       NOT NULL DEFAULT '{}',
    descriptor_hash  TEXT        NOT NULL,
    sync_interval_ms BIGINT      NOT NULL DEFAULT 10000,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS awa.job_kind_descriptors (
    kind             TEXT PRIMARY KEY,
    display_name     TEXT,
    description      TEXT,
    owner            TEXT,
    docs_url         TEXT,
    tags             TEXT[]      NOT NULL DEFAULT '{}',
    extra            JSONB       NOT NULL DEFAULT '{}',
    descriptor_hash  TEXT        NOT NULL,
    sync_interval_ms BIGINT      NOT NULL DEFAULT 10000,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE awa.runtime_instances
    ADD COLUMN IF NOT EXISTS queue_descriptor_hashes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS job_kind_descriptor_hashes JSONB NOT NULL DEFAULT '{}'::jsonb;

INSERT INTO awa.schema_version (version, description)
VALUES (9, 'Queue and job-kind descriptor catalogs')
ON CONFLICT (version) DO NOTHING;
