#!/usr/bin/env node

const { PgBoss } = require("pg-boss");

const QUEUE_NAME = "long_horizon_bench";
const DEFAULT_SAMPLE_WINDOW_S = 30;

function databaseUrl() {
  const url = process.env.DATABASE_URL;
  if (!url) {
    throw new Error("DATABASE_URL must be set");
  }
  return url;
}

function envInt(key, defaultValue) {
  const value = process.env[key];
  return value !== undefined ? Number.parseInt(value, 10) : defaultValue;
}

function envStr(key, defaultValue) {
  const value = process.env[key];
  return value !== undefined ? value : defaultValue;
}

function instanceId() {
  const value = Number.parseInt(process.env.BENCH_INSTANCE_ID || "0", 10);
  return Number.isFinite(value) ? value : 0;
}

// Mirror of awa-bench's `observer_enabled`. Only instance 0 emits
// cross-system observer metrics (queue depth, total backlog, producer
// target rate) so multi-replica runs report a single global observation
// instead of one per replica that the summary aggregator would have to
// de-duplicate later.
function observerEnabled() {
  return instanceId() === 0;
}

// Adapter metrics that describe a *global* observation (queue depth,
// total backlog) rather than this replica's per-instance behaviour.
// Only instance 0 emits these.
const OBSERVER_METRICS = new Set([
  "queue_depth",
  "running_depth",
  "retryable_depth",
  "scheduled_depth",
  "total_backlog",
  "producer_target_rate",
]);

function emit(record) {
  if (record.instance_id === undefined) {
    record.instance_id = instanceId();
  }
  process.stdout.write(`${JSON.stringify(record)}\n`);
}

function nowIso() {
  return new Date().toISOString();
}

function nowMonoMs() {
  return Number(process.hrtime.bigint() / 1000000n);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readProducerRate(defaultValue) {
  const path = process.env.PRODUCER_RATE_CONTROL_FILE;
  if (!path) {
    return defaultValue;
  }
  try {
    const value = Number.parseFloat(require("node:fs").readFileSync(path, "utf8").trim());
    return Number.isFinite(value) ? value : defaultValue;
  } catch {
    return defaultValue;
  }
}

class TimedWindow {
  constructor(maxlen = 32768) {
    this.maxlen = maxlen;
    this.items = [];
  }

  push(tsMs, value) {
    this.items.push([tsMs, value]);
    if (this.items.length > this.maxlen) {
      this.items.splice(0, this.items.length - this.maxlen);
    }
  }

  percentiles(windowMs, nowMs) {
    const cutoff = nowMs - windowMs;
    const values = [];
    for (let i = 0; i < this.items.length; i += 1) {
      const [ts, value] = this.items[i];
      if (ts >= cutoff) {
        values.push(value);
      }
    }
    if (!values.length) {
      return { p50: 0, p95: 0, p99: 0 };
    }
    values.sort((a, b) => a - b);
    const q = (p) => {
      const idx = Math.min(values.length - 1, Math.max(0, Math.round(p * (values.length - 1))));
      return values[idx];
    };
    return { p50: q(0.5), p95: q(0.95), p99: q(0.99) };
  }
}

async function waitForNextBoundary(sampleEveryS) {
  const now = Date.now();
  const periodMs = sampleEveryS * 1000;
  const sleepMs = periodMs - (now % periodMs);
  await sleep(sleepMs);
}

async function discoverEventTables(boss) {
  const queueInfo = await boss.getQueue(QUEUE_NAME);
  const queueTable = queueInfo && queueInfo.table ? `pgboss.${queueInfo.table}` : null;
  return ["pgboss.queue", ...(queueTable ? [queueTable] : [])];
}

async function scenarioLongHorizon() {
  const sampleEveryS = envInt("SAMPLE_EVERY_S", 5);
  const producerRate = envInt("PRODUCER_RATE", 800);
  const producerMode = envStr("PRODUCER_MODE", "fixed");
  const targetDepth = envInt("TARGET_DEPTH", 1000);
  const workerCount = envInt("WORKER_COUNT", 32);
  const payloadBytes = envInt("JOB_PAYLOAD_BYTES", 256);
  const workMs = envInt("JOB_WORK_MS", 1);
  const producerBatchMs = envInt("PRODUCER_BATCH_MS", 10);
  const producerBatchMax = envInt("PRODUCER_BATCH_MAX", 128);
  const subscriberBatchSize = envInt(
    "SUBSCRIBER_BATCH_SIZE",
    Math.max(1, Math.min(64, Math.ceil((producerRate * 1.25) / Math.max(workerCount * 2, 1))))
  );
  const dbName = databaseUrl().split("/").pop();

  const boss = new PgBoss({
    connectionString: databaseUrl(),
    noSupervisor: true,
    noScheduling: true,
    noMonitoring: true,
  });

  boss.on("error", (err) => {
    console.error("[pgboss] error", err);
  });

  await boss.start();

  if (!(await boss.getQueue(QUEUE_NAME))) {
    await boss.createQueue(QUEUE_NAME, { partition: true });
  }
  await boss.deleteAllJobs(QUEUE_NAME);

  const schemaVersion = await boss.schemaVersion();
  const eventTables = await discoverEventTables(boss);

  emit({
    kind: "descriptor",
    system: "pgboss",
    event_tables: eventTables,
    extensions: [],
    version: "pg-boss@12.15.0",
    schema_version: schemaVersion === null ? null : String(schemaVersion),
    db_name: dbName,
    started_at: nowIso(),
  });

  const payloadPadding = "x".repeat(Math.max(0, payloadBytes - 96));
  const producerLatencies = new TimedWindow();
  const subscriberLatencies = new TimedWindow();
  const endToEndLatencies = new TimedWindow();

  let enqueued = 0;
  let completed = 0;
  let queueDepth = 0;
  let currentProducerTargetRate = Number(producerRate);
  let seq = 0;
  let shuttingDown = false;
  let shutdownResolve;
  const shutdownPromise = new Promise((resolve) => {
    shutdownResolve = resolve;
  });

  function beginShutdown() {
    if (!shuttingDown) {
      shuttingDown = true;
      shutdownResolve();
    }
  }

  process.on("SIGINT", beginShutdown);
  process.on("SIGTERM", beginShutdown);

  const workId = await boss.work(
    QUEUE_NAME,
    {
      pollingIntervalSeconds: 0.5,
      localConcurrency: workerCount,
      batchSize: subscriberBatchSize,
    },
    async (jobs) => {
      const startedAtMs = Date.now();
      for (const job of jobs) {
        const data = job.data || {};
        if (typeof data.enqueued_at_ms === "number") {
          subscriberLatencies.push(nowMonoMs(), startedAtMs - data.enqueued_at_ms);
        }
      }
      if (workMs > 0) {
        await sleep(workMs * jobs.length);
      }
      const completedAtMs = Date.now();
      for (const job of jobs) {
        const data = job.data || {};
        if (typeof data.enqueued_at_ms === "number") {
          endToEndLatencies.push(nowMonoMs(), completedAtMs - data.enqueued_at_ms);
        }
      }
      completed += jobs.length;
    }
  );

  const producerTask = (async () => {
    let nextAt = nowMonoMs();
    while (!shuttingDown) {
      const targetRate = readProducerRate(producerRate);
      currentProducerTargetRate = targetRate;

      let batchCount = 0;
      if (producerMode === "depth-target") {
        const stats = await boss.getQueueStats(QUEUE_NAME);
        queueDepth = stats.queuedCount;
        batchCount = Math.max(0, Math.min(producerBatchMax, targetDepth - queueDepth));
        if (batchCount === 0) {
          await sleep(producerBatchMs);
          continue;
        }
      } else {
        const now = nowMonoMs();
        const credit = Math.max(0, ((now - nextAt) * targetRate) / 1000 + 1);
        batchCount = Math.max(1, Math.min(producerBatchMax, Math.floor(credit)));
      }

      const jobs = [];
      for (let i = 0; i < batchCount; i += 1) {
        seq += 1;
        jobs.push({
          data: {
            seq,
            enqueued_at_ms: Date.now(),
            payload_padding: payloadPadding,
          },
        });
      }

      const started = nowMonoMs();
      await boss.insert(QUEUE_NAME, jobs);
      const elapsed = nowMonoMs() - started;
      const perJobLatency = elapsed / Math.max(jobs.length, 1);
      const sampleTs = nowMonoMs();
      for (let i = 0; i < jobs.length; i += 1) {
        producerLatencies.push(sampleTs, perJobLatency);
      }
      enqueued += jobs.length;

      if (producerMode === "fixed") {
        nextAt += Math.round((jobs.length * 1000) / Math.max(targetRate, 1));
        const sleepFor = Math.max(0, nextAt - nowMonoMs());
        if (sleepFor > 0) {
          await sleep(Math.min(sleepFor, producerBatchMs));
        }
      }
    }
  })();

  const depthTask = (async () => {
    if (!observerEnabled()) {
      // Non-zero replicas don't emit observer metrics; idle this
      // task instead of polling. Saves N-1 connections of polling
      // work on a multi-replica run.
      while (!shuttingDown) {
        await sleep(250);
      }
      return;
    }
    while (!shuttingDown) {
      const stats = await boss.getQueueStats(QUEUE_NAME);
      queueDepth = stats.queuedCount;
      await sleep(250);
    }
  })();

  const samplerTask = (async () => {
    await waitForNextBoundary(sampleEveryS);
    let lastEnqueued = enqueued;
    let lastCompleted = completed;

    while (!shuttingDown) {
      const sampleTs = nowIso();
      const monoNow = nowMonoMs();
      const producer = producerLatencies.percentiles(DEFAULT_SAMPLE_WINDOW_S * 1000, monoNow);
      const subscriber = subscriberLatencies.percentiles(DEFAULT_SAMPLE_WINDOW_S * 1000, monoNow);
      const e2e = endToEndLatencies.percentiles(DEFAULT_SAMPLE_WINDOW_S * 1000, monoNow);

      const enqueueRate = (enqueued - lastEnqueued) / Math.max(sampleEveryS, 1);
      const completionRate = (completed - lastCompleted) / Math.max(sampleEveryS, 1);
      lastEnqueued = enqueued;
      lastCompleted = completed;

      const metrics = [
        ["producer_p50_ms", producer.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["producer_p95_ms", producer.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["producer_p99_ms", producer.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["subscriber_p50_ms", subscriber.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["subscriber_p95_ms", subscriber.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["subscriber_p99_ms", subscriber.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["claim_p50_ms", subscriber.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["claim_p95_ms", subscriber.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["claim_p99_ms", subscriber.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["end_to_end_p50_ms", e2e.p50, DEFAULT_SAMPLE_WINDOW_S],
        ["end_to_end_p95_ms", e2e.p95, DEFAULT_SAMPLE_WINDOW_S],
        ["end_to_end_p99_ms", e2e.p99, DEFAULT_SAMPLE_WINDOW_S],
        ["enqueue_rate", enqueueRate, sampleEveryS],
        ["completion_rate", completionRate, sampleEveryS],
        ["queue_depth", queueDepth, 0],
        ["producer_target_rate", currentProducerTargetRate, 0],
      ];

      for (const [metric, value, windowS] of metrics) {
        if (OBSERVER_METRICS.has(metric) && !observerEnabled()) {
          continue;
        }
        emit({
          t: sampleTs,
          system: "pgboss",
          kind: "adapter",
          subject_kind: "adapter",
          subject: "",
          metric,
          value,
          window_s: windowS,
        });
      }

      await sleep(sampleEveryS * 1000);
    }
  })();

  await shutdownPromise;
  await boss.offWork(QUEUE_NAME, { id: workId, wait: true }).catch(() => {});
  await Promise.allSettled([producerTask, depthTask, samplerTask]);
  await boss.stop({ graceful: true }).catch(() => {});
}

async function main() {
  const scenario = envStr("SCENARIO", "long_horizon");
  if (scenario !== "long_horizon") {
    throw new Error(`Unsupported scenario ${scenario} for pgboss-bench`);
  }
  await scenarioLongHorizon();
}

main().catch((err) => {
  console.error("[pgboss] fatal", err);
  process.exit(1);
});
