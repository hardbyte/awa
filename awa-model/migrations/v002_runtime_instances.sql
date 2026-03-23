CREATE TABLE IF NOT EXISTS awa.runtime_instances (
    instance_id          UUID PRIMARY KEY,
    hostname             TEXT,
    pid                  INTEGER     NOT NULL,
    version              TEXT        NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    snapshot_interval_ms BIGINT      NOT NULL,
    healthy              BOOLEAN     NOT NULL,
    postgres_connected   BOOLEAN     NOT NULL,
    poll_loop_alive      BOOLEAN     NOT NULL,
    heartbeat_alive      BOOLEAN     NOT NULL,
    shutting_down        BOOLEAN     NOT NULL,
    leader               BOOLEAN     NOT NULL,
    global_max_workers   INTEGER,
    queues               JSONB       NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_awa_runtime_instances_last_seen
    ON awa.runtime_instances (last_seen_at DESC);

INSERT INTO awa.schema_version (version, description)
VALUES (2, 'Runtime observability snapshots') ON CONFLICT (version) DO NOTHING;
