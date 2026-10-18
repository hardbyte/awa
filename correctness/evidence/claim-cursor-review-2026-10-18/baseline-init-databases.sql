-- Create separate databases for each job queue system.
--
-- The long-horizon harness additionally installs pgstattuple per-database for
-- phase-boundary snapshots. Extensions specific to an individual adapter
-- (pgmq, pgq, …) are declared in the adapter's adapter.json manifest and
-- created by the harness before launching the adapter.

CREATE DATABASE awa_bench;
CREATE DATABASE awa_docker_bench;
CREATE DATABASE awa_python_bench;
CREATE DATABASE procrastinate_bench;
CREATE DATABASE river_bench;
CREATE DATABASE oban_bench;
CREATE DATABASE pgque_bench;
CREATE DATABASE pgmq_bench;
CREATE DATABASE pgboss_bench;
CREATE DATABASE absurd_bench;

\connect awa_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect awa_docker_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect awa_python_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect procrastinate_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect river_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect oban_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect pgque_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect pgmq_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect pgboss_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;

\connect absurd_bench
CREATE EXTENSION IF NOT EXISTS pgstattuple;
