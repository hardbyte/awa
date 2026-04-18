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
--
-- Length and cardinality limits match jobs_hot / scheduled_jobs so the
-- catalog can never hold descriptors that couldn't correspond to a real
-- queue or kind (200 chars for names, 20 tags max). display_name /
-- description / owner / docs_url are operator-facing strings; the limits
-- are generous but bounded so UI layouts and admin queries stay predictable.

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
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT queue_descriptors_queue_nonempty   CHECK (length(queue) > 0),
    CONSTRAINT queue_descriptors_queue_length     CHECK (length(queue) <= 200),
    CONSTRAINT queue_descriptors_display_length   CHECK (display_name IS NULL OR length(display_name) <= 200),
    CONSTRAINT queue_descriptors_description_len  CHECK (description  IS NULL OR length(description)  <= 2000),
    CONSTRAINT queue_descriptors_owner_length     CHECK (owner        IS NULL OR length(owner)        <= 200),
    CONSTRAINT queue_descriptors_docs_url_length  CHECK (docs_url     IS NULL OR length(docs_url)     <= 2048),
    CONSTRAINT queue_descriptors_tags_count       CHECK (cardinality(tags) <= 20),
    CONSTRAINT queue_descriptors_hash_nonempty    CHECK (length(descriptor_hash) > 0),
    CONSTRAINT queue_descriptors_hash_length      CHECK (length(descriptor_hash) <= 128),
    CONSTRAINT queue_descriptors_sync_interval_ms CHECK (sync_interval_ms > 0)
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
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT job_kind_descriptors_kind_nonempty     CHECK (length(kind) > 0),
    CONSTRAINT job_kind_descriptors_kind_length       CHECK (length(kind) <= 200),
    CONSTRAINT job_kind_descriptors_display_length    CHECK (display_name IS NULL OR length(display_name) <= 200),
    CONSTRAINT job_kind_descriptors_description_len   CHECK (description  IS NULL OR length(description)  <= 2000),
    CONSTRAINT job_kind_descriptors_owner_length      CHECK (owner        IS NULL OR length(owner)        <= 200),
    CONSTRAINT job_kind_descriptors_docs_url_length   CHECK (docs_url     IS NULL OR length(docs_url)     <= 2048),
    CONSTRAINT job_kind_descriptors_tags_count        CHECK (cardinality(tags) <= 20),
    CONSTRAINT job_kind_descriptors_hash_nonempty     CHECK (length(descriptor_hash) > 0),
    CONSTRAINT job_kind_descriptors_hash_length       CHECK (length(descriptor_hash) <= 128),
    CONSTRAINT job_kind_descriptors_sync_interval_ms  CHECK (sync_interval_ms > 0)
);

-- Admin queries (queue_overviews / job_kind_overviews) LEFT JOIN descriptor
-- catalogs on their natural key. The PRIMARY KEY index covers those joins;
-- no extra indexes needed at this cardinality (descriptor counts scale with
-- declared queues/kinds, not job volume).

ALTER TABLE awa.runtime_instances
    ADD COLUMN IF NOT EXISTS queue_descriptor_hashes    JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS job_kind_descriptor_hashes JSONB NOT NULL DEFAULT '{}'::jsonb;

INSERT INTO awa.schema_version (version, description)
VALUES (9, 'Queue and job-kind descriptor catalogs')
ON CONFLICT (version) DO NOTHING;
