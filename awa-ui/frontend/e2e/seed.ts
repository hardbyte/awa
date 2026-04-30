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
const queueStorageSchema = "awa_e2e_qs";
const queueSlotCount = 16;
const leaseSlotCount = 8;

export default async function globalSetup() {
  try {
    execSync(`${awaBinary} --database-url ${databaseUrl} migrate`, {
      stdio: "pipe",
      timeout: 30_000,
    });
  } catch {
    console.warn("Could not run migrations before E2E seed");
  }

  // Build the queue-storage schema via the CLI (the same code path the
  // runtime takes) instead of hand-rolling DDL. The hand-rolled version
  // had drifted: `queue_claim_heads`, `queue_enqueue_heads`,
  // `lease_claims`, `lease_claim_closures`, `claim_ring_state`, and
  // `claim_ring_slots` all post-date it, and the dashboard's
  // `state_counts` query JOINs `queue_claim_heads` — without the
  // table, `/api/stats` returns 500 and every dashboard test waits
  // 30 s for an OK response that never comes. Running the CLI keeps
  // the test fixture in lock-step with whatever the runtime actually
  // expects.
  try {
    execSync(
      `${awaBinary} --database-url ${databaseUrl} storage prepare-queue-storage-schema ` +
        `--schema ${queueStorageSchema} ` +
        `--queue-slot-count ${queueSlotCount} ` +
        `--lease-slot-count ${leaseSlotCount} ` +
        `--reset`,
      {
        stdio: "pipe",
        timeout: 30_000,
      }
    );
  } catch (e) {
    console.warn(
      "Could not prepare queue-storage schema via CLI; falling back to seed errors:",
      e
    );
  }

  const pgUrl = new URL(databaseUrl);
  const host = pgUrl.hostname;
  const port = pgUrl.port || "5432";
  const db = pgUrl.pathname.slice(1);
  const user = pgUrl.username;

  const sql = `
    -- The CLI's prepare-queue-storage-schema (above) created the
    -- queue_storage tables. This block only seeds test data + the
    -- control-plane catalogs that drive the dashboard tests.
    INSERT INTO awa.runtime_storage_backends (backend, schema_name, updated_at)
    VALUES ('queue_storage', '${queueStorageSchema}', now())
    ON CONFLICT (backend)
    DO UPDATE SET schema_name = EXCLUDED.schema_name, updated_at = EXCLUDED.updated_at;

    DELETE FROM awa.queue_meta WHERE queue IN ('e2e_test', 'legacy_queue', 'e2e_dlq');
    DELETE FROM awa.queue_descriptors WHERE queue IN ('e2e_test', 'legacy_queue');
    DELETE FROM awa.job_kind_descriptors WHERE kind IN ('e2e_job', 'legacy_job');
    UPDATE awa.runtime_instances SET
      queue_descriptor_hashes    = queue_descriptor_hashes    - ARRAY['e2e_test', 'legacy_queue'],
      job_kind_descriptor_hashes = job_kind_descriptor_hashes - ARRAY['e2e_job', 'legacy_job']
    WHERE queue_descriptor_hashes    ?| ARRAY['e2e_test', 'legacy_queue']
       OR job_kind_descriptor_hashes ?| ARRAY['e2e_job', 'legacy_job'];

    INSERT INTO awa.queue_descriptors (
      queue, display_name, description, owner, docs_url, tags, extra,
      descriptor_hash, sync_interval_ms, created_at, updated_at, last_seen_at
    )
    VALUES (
      'e2e_test',
      'E2E Queue',
      'End-to-end queue used for UI coverage',
      'qa-platform',
      'https://example.test/queues/e2e',
      ARRAY['e2e', 'critical'],
      '{"source":"playwright"}'::jsonb,
      'seeded-e2e-queue',
      10000,
      now(),
      now(),
      now()
    );

    INSERT INTO awa.job_kind_descriptors (
      kind, display_name, description, owner, docs_url, tags, extra,
      descriptor_hash, sync_interval_ms, created_at, updated_at, last_seen_at
    )
    VALUES (
      'e2e_job',
      'E2E Job',
      'End-to-end job kind used for UI coverage',
      'qa-platform',
      'https://example.test/kinds/e2e-job',
      ARRAY['e2e'],
      '{"source":"playwright"}'::jsonb,
      'seeded-e2e-kind',
      10000,
      now(),
      now(),
      now()
    );

    INSERT INTO ${queueStorageSchema}.queue_lanes (queue, priority, next_seq, claim_seq, available_count)
    VALUES
      ('e2e_test', 2, 6, 2, 1),
      ('e2e_test', 1, 2, 1, 1),
      ('legacy_queue', 2, 2, 1, 1),
      ('e2e_dlq', 2, 1, 1, 0);

    -- The per-lane next_seq/claim_seq cursors live in their own
    -- tables, not in queue_lanes. The runtime's enqueue and claim
    -- paths read these heads, so seeding queue_lanes alone leaves
    -- the next-INSERT cursor at next_seq=1 — colliding with the
    -- seeded ready_entries rows that already occupy lane_seq=1 the
    -- first time a UI test triggers a retry/enqueue. Mirror the
    -- queue_lanes values into queue_enqueue_heads / queue_claim_heads.
    INSERT INTO ${queueStorageSchema}.queue_enqueue_heads (queue, priority, next_seq)
    VALUES
      ('e2e_test', 2, 6),
      ('e2e_test', 1, 2),
      ('legacy_queue', 2, 2),
      ('e2e_dlq', 2, 1);

    INSERT INTO ${queueStorageSchema}.queue_claim_heads (queue, priority, claim_seq)
    VALUES
      ('e2e_test', 2, 2),
      ('e2e_test', 1, 1),
      ('legacy_queue', 2, 1),
      ('e2e_dlq', 2, 1);

    INSERT INTO ${queueStorageSchema}.ready_entries (
      ready_slot, ready_generation, job_id, kind, queue, args, priority, attempt,
      run_lease, max_attempts, lane_seq, run_at, attempted_at, created_at, payload
    )
    VALUES
      (
        0, 0, 800006, 'e2e_job', 'e2e_test', '{"test": true}'::jsonb, 2, 0,
        0, 5, 2, now() - interval '15 seconds', NULL, now() - interval '5 minutes',
        '{"metadata":{"source":"playwright"},"tags":["e2e"],"errors":[],"progress":null}'::jsonb
      ),
      (
        0, 0, 800005, 'e2e_job', 'e2e_test', '{"test": true}'::jsonb, 1, 0,
        0, 3, 1, now() - interval '10 seconds', NULL, now() - interval '4 minutes',
        '{"metadata":{"source":"playwright"},"tags":["e2e","priority"],"errors":[],"progress":null}'::jsonb
      ),
      (
        0, 0, 800004, 'e2e_job', 'e2e_test', '{"test": true}'::jsonb, 2, 1,
        1, 5, 1, now() - interval '30 seconds', now() - interval '20 seconds', now() - interval '3 minutes',
        '{"metadata":{"source":"playwright"},"tags":["e2e"],"errors":[],"progress":null}'::jsonb
      ),
      (
        0, 0, 700001, 'legacy_job', 'legacy_queue', '{"legacy": true}'::jsonb, 2, 0,
        0, 3, 1, now() - interval '5 seconds', NULL, now() - interval '2 minutes',
        '{"metadata":{"source":"playwright"},"tags":["legacy"],"errors":[],"progress":null}'::jsonb
      );

    INSERT INTO ${queueStorageSchema}.leases (
      lease_slot, lease_generation, ready_slot, ready_generation, job_id, queue, state,
      priority, attempt, run_lease, max_attempts, lane_seq, heartbeat_at, deadline_at, attempted_at
    )
    VALUES (
      0, 0, 0, 0, 800004, 'e2e_test', 'running', 2, 1, 1, 5, 1,
      now(), now() + interval '30 seconds', now() - interval '20 seconds'
    );

    INSERT INTO ${queueStorageSchema}.attempt_state (
      job_id, run_lease, progress, updated_at
    )
    VALUES (
      800004, 1, '{"percent": 50, "message": "halfway"}'::jsonb, now()
    );

    INSERT INTO ${queueStorageSchema}.done_entries (
      ready_slot, ready_generation, job_id, kind, queue, args, state, priority, attempt,
      run_lease, max_attempts, lane_seq, run_at, attempted_at, finalized_at, created_at, payload
    )
    VALUES
      (
        0, 0, 800003, 'e2e_job', 'e2e_test', '{"test": true}'::jsonb, 'failed', 2, 3,
        3, 3, 3, now() - interval '3 minutes', now() - interval '2 minutes', now() - interval '1 minute', now() - interval '10 minutes',
        '{"metadata":{"source":"playwright"},"tags":["e2e"],"errors":[{"error":"connection refused","attempt":1,"at":"2026-01-01T00:00:00Z"},{"error":"timeout","attempt":2,"at":"2026-01-01T00:01:00Z"},{"error":"max retries exceeded","attempt":3,"at":"2026-01-01T00:02:00Z"}],"progress":null}'::jsonb
      ),
      (
        0, 0, 800002, 'e2e_job', 'e2e_test', '{"test": true}'::jsonb, 'completed', 2, 1,
        1, 5, 4, now() - interval '4 minutes', now() - interval '3 minutes', now() - interval '2 minutes', now() - interval '12 minutes',
        '{"metadata":{"source":"playwright"},"tags":["e2e"],"errors":[],"progress":null}'::jsonb
      ),
      (
        0, 0, 800001, 'e2e_job', 'e2e_test', '{"test": true}'::jsonb, 'cancelled', 2, 1,
        1, 5, 5, now() - interval '2 minutes', NULL, now() - interval '90 seconds', now() - interval '8 minutes',
        '{"metadata":{"source":"playwright"},"tags":["e2e"],"errors":[],"progress":null}'::jsonb
      );

    INSERT INTO ${queueStorageSchema}.dlq_entries (
      job_id, kind, queue, args, state, priority, attempt, run_lease, max_attempts,
      run_at, attempted_at, finalized_at, created_at, payload, dlq_reason, dlq_at, original_run_lease
    )
    VALUES (
      600001, 'dlq_job', 'e2e_dlq', '{"test": true}'::jsonb, 'failed', 2, 2, 2, 5,
      now() - interval '6 minutes', now() - interval '5 minutes', now() - interval '4 minutes', now() - interval '15 minutes',
      '{"metadata":{"source":"playwright"},"tags":["dlq"],"errors":[{"error":"dead lettered","attempt":2,"at":"2026-01-01T00:03:00Z","terminal":true}],"progress":null}'::jsonb,
      'manual_test', now() - interval '3 minutes', 2
    );
  `;

  try {
    execSync(
      `PGPASSWORD=${pgUrl.password} psql -h ${host} -p ${port} -U ${user} -d ${db} -c "${sql.replace(/"/g, '\\"')}"`,
      { stdio: "pipe", timeout: 15_000 }
    );
    console.log("E2E seed data inserted");
  } catch {
    try {
      const containerId = execSync(
        `docker ps --format '{{.ID}} {{.Ports}}' | awk '$0 ~ /:${port}->5432\\/tcp/ {print $1; exit}'`
      )
        .toString()
        .trim();
      if (!containerId) {
        throw new Error(`No postgres container published on host port ${port}`);
      }
      execSync(
        `docker exec -i ${containerId} psql -U ${user} -d ${db} -c "${sql.replace(/"/g, '\\"')}"`,
        { stdio: "pipe", timeout: 15_000 }
      );
      console.log("E2E seed data inserted (via docker)");
    } catch {
      console.warn("Could not seed E2E data — tests may skip data-dependent assertions");
    }
  }
}
