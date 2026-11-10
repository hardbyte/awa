-- Schema patch runtime_maintenance_eligibility: records which runtimes have
-- opted out of maintenance leadership (ClientBuilder::maintenance_leader).
--
-- A runtime started with maintenance_leader(false) never takes the
-- maintenance advisory lock. It writes one row here, keyed by its
-- runtime_instances.instance_id, so `awa control status`, the admin API and
-- the other runtimes can tell it apart from runtimes that may lead. Only
-- opted-out runtimes write rows; a runtime with no row is eligible, which is
-- also how runtimes from before the option behave.
--
-- A side table rather than a runtime_instances column: adding a column would
-- take an ACCESS EXCLUSIVE lock on runtime_instances, which every runtime
-- upserts on each snapshot. This patch creates a new table and locks nothing
-- that already exists.
--
-- Binaries without the option ignore the table. Binaries with it treat a
-- missing or unreadable table as "every runtime is eligible": an opted-out
-- runtime still never leads, but is not reported as opted out until the patch
-- is applied. The maintenance leader deletes rows whose runtime_instances row
-- has gone, as it does for runtime_control_observations.
--
-- Runtime roles need SELECT, INSERT and DELETE on the table. The default
-- privileges in the security guide grant them; installations without default
-- privileges grant them explicitly.
CREATE TABLE IF NOT EXISTS awa.runtime_maintenance_eligibility (
    instance_id uuid PRIMARY KEY,
    maintenance_leader boolean NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
