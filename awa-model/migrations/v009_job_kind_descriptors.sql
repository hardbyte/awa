CREATE TABLE IF NOT EXISTS awa.job_kind_descriptors (
    kind            TEXT PRIMARY KEY,
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
VALUES (9, 'Job kind descriptor catalog')
ON CONFLICT (version) DO NOTHING;
