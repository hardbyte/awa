CREATE TABLE IF NOT EXISTS awa.queue_descriptors (
    queue           TEXT PRIMARY KEY,
    display_name    TEXT,
    description     TEXT,
    owner           TEXT,
    docs_url        TEXT,
    tags            TEXT[]      NOT NULL DEFAULT '{}',
    extra           JSONB       NOT NULL DEFAULT '{}',
    descriptor_hash TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO awa.schema_version (version, description)
VALUES (8, 'Queue descriptor catalog')
ON CONFLICT (version) DO NOTHING;
