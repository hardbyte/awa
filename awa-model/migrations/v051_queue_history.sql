-- v051: per-minute queue and fleet history for the admin overview
--
-- Job tables record current state, not how a backlog moved, so the admin UI
-- could not tell a queue that is draining from one falling behind. The
-- maintenance leader now writes one row per queue per minute (depth by state,
-- oldest available wait, and claim/completion/failure deltas differenced from
-- the per-queue counters runtimes publish in their snapshots) plus one fleet
-- row per minute (instance health, leader, running versions). The UI reads
-- these instead of sampling in each `awa serve` process.
--
-- Both tables are additive and written only by the elected maintenance
-- leader, never by triggers or the claim/finalize path, so ADR-045 does not
-- apply and enqueue/claim cost is unchanged. Rows older than the configured
-- retention (default 48 hours) are deleted by the same leader.
--
-- N-1 binaries: never read or write these tables, and a 0.7.x leader simply
-- does not populate them; the UI then reports no history. Counter columns are
-- NULL for a minute when any live runtime serving the queue predates the
-- counters, so partial sums are never presented as totals.
--
-- Locks and time: two CREATE TABLE IF NOT EXISTS on new relations; no
-- existing table is touched. Effectively instant.
-- Runtime roles need SELECT, INSERT and DELETE on both tables; the default
-- privileges in docs/security/database-roles.md grant that.
-- Tracking: https://github.com/hardbyte/awa/pull/543

CREATE TABLE IF NOT EXISTS awa.queue_history (
    queue              TEXT        NOT NULL,
    bucket             TIMESTAMPTZ NOT NULL,
    available          BIGINT      NOT NULL,
    running            BIGINT      NOT NULL,
    scheduled          BIGINT      NOT NULL,
    retryable          BIGINT      NOT NULL,
    failed             BIGINT      NOT NULL,
    waiting_external   BIGINT      NOT NULL,
    lag_seconds        DOUBLE PRECISION,
    paused             BOOLEAN     NOT NULL DEFAULT FALSE,
    -- Deltas since the previous sample. NULL when unknown: the first sample
    -- after a leader change, or a live runtime without counters.
    claimed            BIGINT,
    completed          BIGINT,
    failed_attempts    BIGINT,
    retried            BIGINT,
    rate_limited       BIGINT,
    claim_wait_ms      BIGINT,
    -- Live runtimes serving the queue and their summed in-flight count.
    live_workers       INT         NOT NULL DEFAULT 0,
    capacity           BIGINT,
    PRIMARY KEY (queue, bucket)
);

CREATE INDEX IF NOT EXISTS idx_awa_queue_history_bucket
    ON awa.queue_history (bucket);

CREATE TABLE IF NOT EXISTS awa.fleet_history (
    bucket             TIMESTAMPTZ PRIMARY KEY,
    live_instances     INT         NOT NULL,
    healthy_instances  INT         NOT NULL,
    stale_instances    INT         NOT NULL,
    leader_instance_id UUID,
    versions           TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[]
);

INSERT INTO awa.schema_version (version, description)
VALUES (51, 'Per-minute queue and fleet history')
ON CONFLICT (version) DO NOTHING;
