-- Add pause state to awa.cron_jobs so individual cron schedules can be
-- paused without deleting them. Column shape mirrors awa.queue_meta
-- (paused_at / paused_by) so operators see one convention across the
-- two pause surfaces.
--
-- `paused_at IS NULL` means the schedule is active. When non-null, the
-- evaluator skips it and atomic_enqueue refuses to fire (belt-and-braces).
-- `last_enqueued_at` is left untouched while paused, so the existing
-- `missed_fire_policy` (coalesce | catch_up) decides catch-up behaviour
-- on resume.

ALTER TABLE awa.cron_jobs
    ADD COLUMN IF NOT EXISTS paused_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS paused_by TEXT;

INSERT INTO awa.schema_version (version, description)
VALUES (26, 'Add paused_at + paused_by to cron_jobs for per-schedule pause')
ON CONFLICT (version) DO NOTHING;
