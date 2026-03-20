/**
 * Seed test data via direct SQL through the awa CLI.
 * Called from playwright globalSetup so all tests have data to work with.
 */
import { execSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const awaBinary = path.resolve(__dirname, "../../../target/debug/awa");
const databaseUrl =
  process.env.DATABASE_URL ??
  "postgres://postgres:test@localhost:15432/awa_test";

export default async function globalSetup() {
  // Insert test jobs in various states so E2E tests have data
  // Use psql via the DATABASE_URL since awa CLI doesn't have a raw SQL command
  const pgUrl = new URL(databaseUrl);
  const host = pgUrl.hostname;
  const port = pgUrl.port || "5432";
  const db = pgUrl.pathname.slice(1);
  const user = pgUrl.username;

  const sql = `
    -- Clean up any previous E2E data
    DELETE FROM awa.jobs WHERE queue = 'e2e_test';
    DELETE FROM awa.queue_meta WHERE queue = 'e2e_test';

    -- Available jobs
    INSERT INTO awa.jobs (kind, queue, state, args, priority, max_attempts, tags)
    VALUES
      ('e2e_job', 'e2e_test', 'available', '{"test": true}', 2, 5, '{e2e}'),
      ('e2e_job', 'e2e_test', 'available', '{"test": true}', 1, 3, '{e2e,priority}');

    -- Running job with progress
    INSERT INTO awa.jobs (kind, queue, state, args, priority, attempt, max_attempts, tags, heartbeat_at, attempted_at, progress)
    VALUES ('e2e_job', 'e2e_test', 'running', '{"test": true}', 2, 1, 5, '{e2e}', now(), now(), '{"percent": 50, "message": "halfway"}');

    -- Failed job with errors
    INSERT INTO awa.jobs (kind, queue, state, args, priority, attempt, max_attempts, tags, finalized_at, attempted_at, errors)
    VALUES ('e2e_job', 'e2e_test', 'failed', '{"test": true}', 2, 3, 3, '{e2e}', now(), now(),
      ARRAY['{"error": "connection refused", "attempt": 1, "at": "2026-01-01T00:00:00Z"}'::jsonb,
            '{"error": "timeout", "attempt": 2, "at": "2026-01-01T00:01:00Z"}'::jsonb,
            '{"error": "max retries exceeded", "attempt": 3, "at": "2026-01-01T00:02:00Z"}'::jsonb]);

    -- Completed job
    INSERT INTO awa.jobs (kind, queue, state, args, priority, attempt, max_attempts, tags, finalized_at, attempted_at)
    VALUES ('e2e_job', 'e2e_test', 'completed', '{"test": true}', 2, 1, 5, '{e2e}', now(), now());

    -- Cancelled job
    INSERT INTO awa.jobs (kind, queue, state, args, priority, attempt, max_attempts, tags, finalized_at)
    VALUES ('e2e_job', 'e2e_test', 'cancelled', '{"test": true}', 2, 1, 5, '{e2e}', now());
  `;

  try {
    execSync(
      `PGPASSWORD=${pgUrl.password} psql -h ${host} -p ${port} -U ${user} -d ${db} -c "${sql.replace(/"/g, '\\"')}"`,
      { stdio: "pipe", timeout: 10_000 }
    );
    console.log("E2E seed data inserted");
  } catch (e) {
    // If psql isn't available, try docker exec (CI uses service containers)
    try {
      execSync(
        `docker exec -i $(docker ps -q --filter ancestor=postgres:17-alpine) psql -U ${user} -d ${db} -c "${sql.replace(/"/g, '\\"')}"`,
        { stdio: "pipe", timeout: 10_000 }
      );
      console.log("E2E seed data inserted (via docker)");
    } catch {
      console.warn("Could not seed E2E data — tests may skip data-dependent assertions");
    }
  }
}
