-- Add explicit cron missed-fire policy.
--
-- Existing schedules keep the historical coalesced/latest-only behavior.
-- New schedules can opt into catch_up to enqueue missed fire times in order.

ALTER TABLE awa.cron_jobs
  ADD COLUMN IF NOT EXISTS missed_fire_policy TEXT NOT NULL DEFAULT 'coalesce';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'cron_jobs_missed_fire_policy_check'
      AND conrelid = 'awa.cron_jobs'::regclass
  ) THEN
    ALTER TABLE awa.cron_jobs
      ADD CONSTRAINT cron_jobs_missed_fire_policy_check
      CHECK (missed_fire_policy IN ('coalesce', 'catch_up'));
  END IF;
END$$;

INSERT INTO awa.schema_version (version, description)
VALUES (15, 'Cron missed-fire policy')
ON CONFLICT (version) DO NOTHING;
