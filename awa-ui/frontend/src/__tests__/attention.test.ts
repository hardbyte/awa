import { describe, expect, it } from "vitest";
import {
  attentionItems,
  healthFigures,
  isStuck,
  queuesNeedingAttention,
  type AttentionInput,
} from "@/lib/attention";
import type {
  BatchOperation,
  CronJobRow,
  JobRow,
  QueueOverview,
  RuntimeInstance,
  RuntimeOverview,
} from "@/lib/api";

const NOW = Date.parse("2026-11-28T12:00:00Z");
const ago = (ms: number) => new Date(NOW - ms).toISOString();

function queue(overrides: Partial<QueueOverview> = {}): QueueOverview {
  return {
    queue: "default",
    display_name: null,
    description: null,
    owner: null,
    docs_url: null,
    tags: [],
    extra: null,
    descriptor_last_seen_at: null,
    descriptor_stale: false,
    descriptor_mismatch: false,
    total_queued: 0,
    scheduled: 0,
    available: 0,
    retryable: 0,
    running: 0,
    failed: 0,
    waiting_external: 0,
    completed_last_hour: 0,
    lag_seconds: null,
    paused: false,
    ...overrides,
  };
}

function instance(overrides: Partial<RuntimeInstance> = {}): RuntimeInstance {
  return {
    instance_id: "i-1",
    hostname: "worker-1",
    pid: 1,
    version: "0.7.0",
    started_at: ago(3600_000),
    last_seen_at: ago(1000),
    snapshot_interval_ms: 10_000,
    stale: false,
    healthy: true,
    postgres_connected: true,
    poll_loop_alive: true,
    heartbeat_alive: true,
    maintenance_alive: true,
    shutting_down: false,
    leader: true,
    global_max_workers: null,
    queues: [],
    ...overrides,
  };
}

function runtime(instances: RuntimeInstance[]): RuntimeOverview {
  const live = instances.filter((i) => !i.stale);
  return {
    total_instances: instances.length,
    live_instances: live.length,
    stale_instances: instances.length - live.length,
    healthy_instances: live.filter((i) => i.healthy).length,
    leader_instances: live.filter((i) => i.leader).length,
    instances,
  };
}

function job(overrides: Partial<JobRow> = {}): JobRow {
  return {
    id: 1,
    kind: "send_email",
    queue: "default",
    args: {},
    state: "failed",
    priority: 2,
    attempt: 1,
    max_attempts: 5,
    run_at: ago(0),
    heartbeat_at: null,
    deadline_at: null,
    attempted_at: null,
    finalized_at: null,
    created_at: ago(0),
    errors: null,
    metadata: {},
    tags: [],
    progress: null,
    callback_id: null,
    callback_timeout_at: null,
    callback_filter: null,
    callback_on_complete: null,
    callback_on_fail: null,
    callback_transform: null,
    original_priority: 2,
    queue_descriptor: null,
    kind_descriptor: null,
    ...overrides,
  };
}

function batchOp(overrides: Partial<BatchOperation> = {}): BatchOperation {
  return {
    id: "op-1",
    op_kind: "retry",
    filter: { queue: "default" },
    spec: {},
    state: "completed",
    submitted_by: "ui",
    submitted_at: ago(60_000),
    started_at: ago(60_000),
    finalized_at: ago(30_000),
    cursor: null,
    total_matched: 10,
    processed: 10,
    skipped: 0,
    errored: 0,
    last_error: null,
    runner_instance: null,
    retention_until: null,
    updated_at: ago(30_000),
    ...overrides,
  };
}

function schedule(overrides: Partial<CronJobRow> = {}): CronJobRow {
  return {
    owner_id: null,
    retired_at: null,
    retired_by: null,
    retired_revision: null,
    name: "nightly",
    cron_expr: "0 0 * * *",
    timezone: "UTC",
    kind: "report",
    queue: "default",
    args: {},
    priority: 2,
    max_attempts: 3,
    tags: [],
    metadata: {},
    last_enqueued_at: null,
    next_fire_at: new Date(NOW + 3600_000).toISOString(),
    created_at: ago(0),
    updated_at: ago(0),
    paused_at: null,
    paused_by: null,
    ...overrides,
  };
}

const ids = (input: AttentionInput) => attentionItems(input).map((item) => item.id);

describe("attentionItems", () => {
  it("is empty when nothing is wrong", () => {
    expect(
      attentionItems({
        now: NOW,
        queues: [queue({ available: 3, lag_seconds: 1 })],
        queueRuntime: [],
        runtime: runtime([instance()]),
        dlqDepth: { total: 0, by_queue: [] },
        batchOps: [batchOp()],
        cron: [schedule()],
        failedJobs: [],
        runningJobs: [job({ state: "running", heartbeat_at: ago(1000) })],
      })
    ).toEqual([]);
  });

  it("flags a paused queue only while it holds ready jobs", () => {
    const items = attentionItems({
      now: NOW,
      queues: [queue({ queue: "imports", paused: true, available: 140, lag_seconds: 540 }), queue({ queue: "idle", paused: true })],
    });
    expect(items.map((i) => i.id)).toEqual(["queue:paused:imports"]);
    expect(items[0]).toMatchObject({
      severity: "warning",
      title: "imports is paused with 140 ready jobs",
      detail: "Oldest has waited 9m",
      target: { to: "/queues/$name", params: { name: "imports" } },
    });
  });

  it("flags lag only on unpaused queues past the threshold", () => {
    expect(
      ids({
        now: NOW,
        queues: [queue({ queue: "slow", available: 5, lag_seconds: 120 }), queue({ queue: "fast", available: 5, lag_seconds: 59 })],
      })
    ).toEqual(["queue:lag:slow"]);
  });

  it("groups failures by kind and error class, biggest first, linking to the failed jobs of the kind", () => {
    const failed = [
      job({ id: 3, kind: "charge", errors: [{ error: "GatewayTimeout: slow", attempt: 4 }] }),
      job({ id: 2, kind: "charge", errors: [{ error: "GatewayTimeout: slower", attempt: 4 }] }),
      job({ id: 1, kind: "sync", errors: [{ error: "KeyError: 'x'", attempt: 5 }] }),
    ];
    const items = attentionItems({ now: NOW, failedJobs: failed });
    expect(items.map((i) => i.title)).toEqual(["2× GatewayTimeout in charge", "1× KeyError in sync"]);
    expect(items[0]).toMatchObject({
      severity: "action",
      detail: "GatewayTimeout: slow",
      target: { to: "/jobs", search: { state: "failed", q: "kind:charge" } },
    });
  });

  it("lists dead letters per queue, linking to the queue's tab", () => {
    const items = attentionItems({
      now: NOW,
      queues: [queue({ queue: "billing", display_name: "Billing" })],
      dlqDepth: { total: 5, by_queue: [{ queue: "billing", count: 4 }, { queue: "email", count: 1 }, { queue: "none", count: 0 }] },
    });
    expect(items.map((i) => i.title)).toEqual(["4 dead letters in Billing", "1 dead letter in email"]);
    expect(items[0]!.target).toEqual({ to: "/queues/$name/dlq", params: { name: "billing" } });
  });

  it("reports no live workers as an action when work is ready", () => {
    const items = attentionItems({
      now: NOW,
      runtime: runtime([instance({ stale: true })]),
      queues: [queue({ available: 2 })],
    });
    expect(items[0]).toMatchObject({ id: "fleet:none-live", severity: "action" });
    expect(attentionItems({ now: NOW, runtime: runtime([]), queues: [] })[0]).toMatchObject({ severity: "warning" });
  });

  it("reports unhealthy instances, leader drift and mixed versions, but not stale instances", () => {
    const items = attentionItems({
      now: NOW,
      runtime: runtime([
        instance({ instance_id: "a", leader: false, healthy: false, poll_loop_alive: false }),
        instance({ instance_id: "b", leader: false, version: "0.7.1" }),
        instance({ instance_id: "c", stale: true, version: "0.6.0" }),
      ]),
    });
    expect(items.map((i) => i.id)).toEqual(["fleet:degraded:a", "fleet:leader", "fleet:versions"]);
    expect(items[0]!.detail).toBe("poll loop down");
    expect(items[2]!.detail).toBe("0.7.0, 0.7.1");
  });

  it("counts stuck running jobs past their deadline or heartbeat", () => {
    const running = [
      job({ id: 1, state: "running", deadline_at: ago(1000) }),
      job({ id: 2, state: "running", heartbeat_at: ago(5 * 60_000) }),
      job({ id: 3, state: "running", heartbeat_at: ago(5000) }),
      job({ id: 4, state: "running", heartbeat_at: null }),
    ];
    const items = attentionItems({ now: NOW, runningJobs: running });
    expect(items).toHaveLength(1);
    expect(items[0]).toMatchObject({ id: "jobs:stuck", title: "2 running jobs past deadline or without a recent heartbeat" });
  });

  it("shows recent failed bulk operations as actions and running ones as notices", () => {
    const items = attentionItems({
      now: NOW,
      batchOps: [
        batchOp({ id: "old", state: "failed", finalized_at: ago(48 * 3600_000) }),
        batchOp({ id: "new", state: "failed", last_error: "boom" }),
        batchOp({ id: "run", state: "running", processed: 2, total_matched: 10 }),
      ],
    });
    expect(items.map((i) => [i.id, i.severity, i.detail])).toEqual([
      ["batch:failed:new", "action", "boom"],
      ["batch:active:run", "notice", "2 of 10 done"],
    ]);
  });

  it("flags overdue schedules as warnings and paused ones as notices, skipping retired ones", () => {
    expect(
      ids({
        now: NOW,
        cron: [
          schedule({ name: "late", next_fire_at: ago(5 * 60_000) }),
          schedule({ name: "held", paused_at: ago(0) }),
          schedule({ name: "gone", retired_at: ago(0), paused_at: ago(0) }),
        ],
      })
    ).toEqual(["cron:overdue:late", "cron:paused:held"]);
  });

  it("flags config mismatch and descriptor drift", () => {
    expect(
      ids({
        now: NOW,
        queues: [queue({ queue: "q", descriptor_mismatch: true })],
        queueRuntime: [
          { queue: "q", instance_count: 2, live_instances: 2, stale_instances: 0, healthy_instances: 2, total_in_flight: 0, overflow_held_total: null, config_mismatch: true, config: null },
        ],
      })
    ).toEqual(["queue:config:q", "queue:drift:q"]);
  });

  it("orders by severity, then by rule, then by size", () => {
    expect(
      ids({
        now: NOW,
        queues: [queue({ queue: "p", paused: true, available: 1 })],
        dlqDepth: { total: 1, by_queue: [{ queue: "a", count: 1 }, { queue: "b", count: 9 }] },
        failedJobs: [job({ errors: [{ error: "Boom: x" }] })],
        cron: [schedule({ paused_at: ago(0) })],
      })
    ).toEqual(["failures:send_email:Boom", "queue:paused:p", "dlq:b", "dlq:a", "cron:paused:nightly"]);
  });
});

describe("healthFigures", () => {
  it("counts ready work in unpaused queues and keeps paused queues apart", () => {
    const figures = healthFigures({
      now: NOW,
      queues: [
        queue({ queue: "a", available: 10, lag_seconds: 30, retryable: 2, completed_last_hour: 5 }),
        queue({ queue: "b", available: 5, lag_seconds: 90, retryable: 7, completed_last_hour: 1 }),
        queue({ queue: "p", available: 140, lag_seconds: 900, paused: true }),
        queue({ queue: "s", scheduled: 3000 }),
      ],
    });
    expect(figures.ready).toBe(15);
    expect(figures.oldestReady).toEqual({ seconds: 90, queue: "b" });
    expect(figures.pausedReady).toEqual({ jobs: 140, queues: 1 });
    expect(figures.topRetrying).toEqual({ queue: "b", count: 7 });
    expect(figures.completedLastHour).toBe(6);
  });

  it("is null wherever a read is missing", () => {
    const figures = healthFigures({ now: NOW });
    expect(figures.ready).toBeNull();
    expect(figures.pausedReady).toBeNull();
    expect(figures.completedLastHour).toBeNull();
    expect(figures.stuck).toBeNull();
    expect(figures.activeBatchOps).toBeNull();
  });

  it("finds the busiest dead-letter queue, active bulk operations and the newest failure", () => {
    const figures = healthFigures({
      now: NOW,
      dlqDepth: { total: 5, by_queue: [{ queue: "a", count: 1 }, { queue: "b", count: 4 }] },
      batchOps: [
        batchOp({ id: "1", state: "running" }),
        batchOp({ id: "2", state: "failed", finalized_at: ago(10_000), last_error: "older" }),
        batchOp({ id: "3", state: "failed", finalized_at: ago(1_000), last_error: "newer" }),
      ],
      runningJobs: [job({ state: "running", deadline_at: ago(1) }), job({ state: "running" })],
    });
    expect(figures.topDeadLetters).toEqual({ queue: "b", count: 4 });
    expect(figures.activeBatchOps).toBe(1);
    expect(figures.latestFailedBatchOp?.last_error).toBe("newer");
    expect(figures.stuck).toEqual({ count: 1, sampled: 2 });
  });
});

describe("isStuck", () => {
  it("ignores jobs that are not running", () => {
    expect(isStuck(job({ state: "failed", deadline_at: ago(1000) }), NOW)).toBe(false);
  });
});

describe("queuesNeedingAttention", () => {
  it("ranks queues with failures, dead letters, retries or held work and leaves quiet ones out", () => {
    const result = queuesNeedingAttention(
      [
        queue({ queue: "quiet", available: 3, lag_seconds: 1, scheduled: 3000 }),
        queue({ queue: "retry", retryable: 2 }),
        queue({ queue: "fail", failed: 4 }),
        queue({ queue: "held", paused: true, available: 1 }),
      ],
      { total: 1, by_queue: [{ queue: "retry", count: 1 }] },
      3
    );
    expect(result.map((q) => [q.queue, q.deadLetters])).toEqual([
      ["fail", 0],
      ["retry", 1],
      ["held", 0],
    ]);
  });
});
