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

function ringSlotRows(count: number): string {
  return Array.from({ length: count }, (_, slot) => {
    const generation = slot === 0 ? 0 : -1;
    return `(${slot}, ${generation})`;
  }).join(",\n      ");
}

export default async function globalSetup() {
  try {
    execSync(`${awaBinary} --database-url ${databaseUrl} migrate`, {
      stdio: "pipe",
      timeout: 30_000,
    });
  } catch {
    console.warn("Could not run migrations before E2E seed");
  }

  const pgUrl = new URL(databaseUrl);
  const host = pgUrl.hostname;
  const port = pgUrl.port || "5432";
  const db = pgUrl.pathname.slice(1);
  const user = pgUrl.username;
  const queueRingSlotRows = ringSlotRows(queueSlotCount);
  const leaseRingSlotRows = ringSlotRows(leaseSlotCount);

  const sql = `
    DROP SCHEMA IF EXISTS ${queueStorageSchema} CASCADE;
    CREATE SCHEMA ${queueStorageSchema};

    CREATE SEQUENCE ${queueStorageSchema}.job_id_seq;

    CREATE TABLE ${queueStorageSchema}.queue_ring_state (
      singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
      current_slot INT NOT NULL,
      generation   BIGINT NOT NULL,
      slot_count   INT NOT NULL
    );

    INSERT INTO ${queueStorageSchema}.queue_ring_state (
      singleton, current_slot, generation, slot_count
    )
    VALUES (TRUE, 0, 0, ${queueSlotCount});

    CREATE TABLE ${queueStorageSchema}.queue_ring_slots (
      slot       INT PRIMARY KEY,
      generation BIGINT NOT NULL
    );

    INSERT INTO ${queueStorageSchema}.queue_ring_slots (slot, generation)
    VALUES
      ${queueRingSlotRows};

    CREATE TABLE ${queueStorageSchema}.lease_ring_state (
      singleton    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
      current_slot INT NOT NULL,
      generation   BIGINT NOT NULL,
      slot_count   INT NOT NULL
    );

    INSERT INTO ${queueStorageSchema}.lease_ring_state (
      singleton, current_slot, generation, slot_count
    )
    VALUES (TRUE, 0, 0, ${leaseSlotCount});

    CREATE TABLE ${queueStorageSchema}.lease_ring_slots (
      slot       INT PRIMARY KEY,
      generation BIGINT NOT NULL
    );

    INSERT INTO ${queueStorageSchema}.lease_ring_slots (slot, generation)
    VALUES
      ${leaseRingSlotRows};

    CREATE TABLE ${queueStorageSchema}.queue_lanes (
      queue           TEXT NOT NULL,
      priority        SMALLINT NOT NULL,
      next_seq        BIGINT NOT NULL DEFAULT 1,
      claim_seq       BIGINT NOT NULL DEFAULT 1,
      available_count BIGINT NOT NULL DEFAULT 0,
      running_count   BIGINT NOT NULL DEFAULT 0,
      pruned_completed_count BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (queue, priority)
    );

    CREATE TABLE ${queueStorageSchema}.ready_entries (
      ready_slot        INT NOT NULL,
      ready_generation  BIGINT NOT NULL,
      job_id            BIGINT NOT NULL,
      kind              TEXT NOT NULL,
      queue             TEXT NOT NULL,
      args              JSONB NOT NULL DEFAULT '{}'::jsonb,
      priority          SMALLINT NOT NULL,
      attempt           SMALLINT NOT NULL DEFAULT 0,
      run_lease         BIGINT NOT NULL DEFAULT 0,
      max_attempts      SMALLINT NOT NULL DEFAULT 25,
      lane_seq          BIGINT NOT NULL,
      run_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      attempted_at      TIMESTAMPTZ,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      unique_key        BYTEA,
      unique_states     TEXT,
      payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
      PRIMARY KEY (ready_slot, queue, priority, lane_seq)
    );

    CREATE TABLE ${queueStorageSchema}.leases (
      lease_slot          INT NOT NULL,
      lease_generation    BIGINT NOT NULL,
      ready_slot          INT NOT NULL,
      ready_generation    BIGINT NOT NULL,
      job_id              BIGINT NOT NULL,
      queue               TEXT NOT NULL,
      state               awa.job_state NOT NULL DEFAULT 'running',
      priority            SMALLINT NOT NULL,
      attempt             SMALLINT NOT NULL DEFAULT 1,
      run_lease           BIGINT NOT NULL DEFAULT 1,
      max_attempts        SMALLINT NOT NULL DEFAULT 25,
      lane_seq            BIGINT NOT NULL,
      heartbeat_at        TIMESTAMPTZ,
      deadline_at         TIMESTAMPTZ,
      attempted_at        TIMESTAMPTZ,
      callback_id         UUID,
      callback_timeout_at TIMESTAMPTZ,
      PRIMARY KEY (lease_slot, queue, priority, lane_seq)
    );

    CREATE TABLE ${queueStorageSchema}.attempt_state (
      job_id               BIGINT NOT NULL,
      run_lease            BIGINT NOT NULL,
      progress             JSONB,
      callback_filter      TEXT,
      callback_on_complete TEXT,
      callback_on_fail     TEXT,
      callback_transform   TEXT,
      callback_result      JSONB,
      updated_at           TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      PRIMARY KEY (job_id, run_lease)
    );

    CREATE TABLE ${queueStorageSchema}.done_entries (
      ready_slot        INT NOT NULL,
      ready_generation  BIGINT NOT NULL,
      job_id            BIGINT NOT NULL,
      kind              TEXT NOT NULL,
      queue             TEXT NOT NULL,
      args              JSONB NOT NULL DEFAULT '{}'::jsonb,
      state             awa.job_state NOT NULL DEFAULT 'completed',
      priority          SMALLINT NOT NULL,
      attempt           SMALLINT NOT NULL DEFAULT 1,
      run_lease         BIGINT NOT NULL DEFAULT 1,
      max_attempts      SMALLINT NOT NULL DEFAULT 25,
      lane_seq          BIGINT NOT NULL,
      run_at            TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      attempted_at      TIMESTAMPTZ,
      finalized_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      unique_key        BYTEA,
      unique_states     TEXT,
      payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
      PRIMARY KEY (ready_slot, queue, priority, lane_seq)
    );

    CREATE TABLE ${queueStorageSchema}.deferred_jobs (
      job_id            BIGINT PRIMARY KEY,
      kind              TEXT NOT NULL,
      queue             TEXT NOT NULL,
      args              JSONB NOT NULL DEFAULT '{}'::jsonb,
      state             awa.job_state NOT NULL,
      priority          SMALLINT NOT NULL,
      attempt           SMALLINT NOT NULL DEFAULT 0,
      run_lease         BIGINT NOT NULL DEFAULT 0,
      max_attempts      SMALLINT NOT NULL DEFAULT 25,
      run_at            TIMESTAMPTZ NOT NULL,
      attempted_at      TIMESTAMPTZ,
      finalized_at      TIMESTAMPTZ,
      created_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      unique_key        BYTEA,
      unique_states     TEXT,
      payload           JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE TABLE ${queueStorageSchema}.dlq_entries (
      job_id             BIGINT PRIMARY KEY,
      kind               TEXT NOT NULL,
      queue              TEXT NOT NULL,
      args               JSONB NOT NULL DEFAULT '{}'::jsonb,
      state              awa.job_state NOT NULL DEFAULT 'failed',
      priority           SMALLINT NOT NULL,
      attempt            SMALLINT NOT NULL DEFAULT 1,
      run_lease          BIGINT NOT NULL DEFAULT 1,
      max_attempts       SMALLINT NOT NULL DEFAULT 25,
      run_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      attempted_at       TIMESTAMPTZ,
      finalized_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      created_at         TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      unique_key         BYTEA,
      unique_states      TEXT,
      payload            JSONB NOT NULL DEFAULT '{}'::jsonb,
      dlq_reason         TEXT NOT NULL,
      dlq_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
      original_run_lease BIGINT NOT NULL
    );

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
