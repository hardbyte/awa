import type { Page } from "@playwright/test";
import type { JobRow, QueueStats, RuntimeInstance } from "../src/lib/api";

export async function mockOperations(
  page: Page,
  scenario: "busy" | "empty" | "error" | "loading" = "busy",
  readOnly = false,
) {
  const now = Date.now();
  const ago = (seconds: number) => new Date(now - seconds * 1000).toISOString();
  const queues: QueueStats[] = Array.from({ length: 24 }, (_, index) => ({
    queue:
      [
        "email-delivery",
        "invoice-export",
        "webhook-dispatch",
        "image-processing",
        "search-index",
        "daily-reports",
      ][index] ?? `tenant-sync-${index}`,
    display_name: null,
    description: "Synthetic queue for local operator validation",
    owner: "Platform",
    docs_url: null,
    tags: [],
    extra: {},
    descriptor_last_seen_at: ago(8),
    descriptor_stale: false,
    descriptor_mismatch: false,
    total_queued: 0,
    // Story: email-delivery started falling behind 20 minutes ago,
    // image-processing has no worker claiming it, search-index is draining a
    // burst, and invoice-export is paused with work waiting.
    available: index === 0 ? 184200 : index === 4 ? 12168 : 2100 + index * 17,
    running: index === 3 ? 0 : 40 + index,
    retryable: index === 0 ? 1840 : 10,
    failed: index === 0 ? 236 : 0,
    scheduled: 850 + index,
    waiting_external: index === 2 ? 14400 : 0,
    completed_last_hour: 128400 + index * 211,
    lag_seconds: index === 0 ? 458 : index === 1 ? 135 : index === 3 ? 95 : 2.4,
    paused: index === 1,
  })).map((q) => ({
    ...q,
    total_queued:
      q.available + q.running + q.retryable + q.scheduled + q.waiting_external,
  }));
  const config = {
    mode: "hard_reserved",
    max_workers: 64,
    min_workers: null,
    weight: null,
    global_max_workers: null,
    poll_interval_ms: 200,
    deadline_duration_secs: 300,
    priority_aging_interval_secs: 60,
    dlq_enabled: true,
    rate_limit: null,
  };
  const instances: RuntimeInstance[] = Array.from(
    { length: 48 },
    (_, index) => ({
      instance_id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
      hostname: `worker-eu-${String(index + 1).padStart(2, "0")}`,
      pid: 1300 + index,
      version: "0.7.0",
      storage_capability: "queue_storage",
      transition_role: "auto",
      started_at: ago(86400),
      last_seen_at: ago(index === 0 ? 170 : 4),
      snapshot_interval_ms: 10000,
      stale: index === 0,
      healthy: index > 1,
      postgres_connected: true,
      poll_loop_alive: index !== 1,
      heartbeat_alive: true,
      maintenance_alive: true,
      shutting_down: false,
      leader: index === 2,
      global_max_workers: 64,
      queues: queues.slice(0, 6).map((q) => ({
        queue: q.queue,
        in_flight: 8,
        overflow_held: 0,
        config: config as RuntimeInstance["queues"][number]["config"],
      })),
    }),
  );
  // Per-minute history the way the leader records it: an hour of buckets,
  // a leader that started 50 minutes ago, and a three-minute gap where
  // sampling failed. A rollout 0.7.0 -> 0.7.1 happens 18 to 15 minutes ago.
  const BUCKETS = 60;
  const minuteStart = Math.floor(now / 60_000) * 60_000;
  const bucketAt = (bucket: number) =>
    new Date(minuteStart - (BUCKETS - 1 - bucket) * 60_000).toISOString();
  const depth = (index: number, minutesAgo: number): number => {
    const current = queues[index]!.available;
    if (index === 0)
      return minutesAgo <= 20
        ? current - minutesAgo * 2500
        : 134200 + (minutesAgo % 3) * 300;
    if (index === 1)
      return minutesAgo <= 12 ? current : 40 + (minutesAgo % 4) * 5;
    if (index === 3)
      return minutesAgo <= 2
        ? current - minutesAgo * 900
        : 30 + (minutesAgo % 2) * 10;
    if (index === 4)
      return minutesAgo <= 15 ? current + minutesAgo * 2000 : 4000;
    return current + ((minutesAgo * 7 + index) % 5) * 4;
  };
  const sampled = (bucket: number) =>
    bucket >= 10 && ![33, 34, 35].includes(bucket);
  const series = <T>(value: (bucket: number) => T) =>
    Array.from({ length: BUCKETS }, (_, bucket) =>
      sampled(bucket) ? value(bucket) : null,
    );
  const queueHistory = {
    installed: true,
    bucket_secs: 60,
    minutes: BUCKETS,
    observed_since: bucketAt(10),
    buckets: Array.from({ length: BUCKETS }, (_, bucket) => bucketAt(bucket)),
    queues: Object.fromEntries(
      queues.map((q, index) => [
        q.queue,
        {
          available: series((b) => depth(index, BUCKETS - 1 - b)),
          running: series(() => q.running),
          lag_seconds: series(() => q.lag_seconds),
          claimed: series(() => (index === 3 ? 0 : 2140)),
          completed: series(() => (index === 3 ? 0 : 2140)),
          failed_attempts: series(() => (index === 0 ? 4 : 0)),
          rate_limited: series(() => (index === 0 ? 30 : 0)),
        },
      ]),
    ),
    fleet: {
      live_instances: series((b) => (b >= 41 && b < 47 ? 48 : 47)),
      healthy_instances: series(() => 46),
    },
    events: [
      {
        at: bucketAt(41),
        kind: "version_change",
        detail: "Versions 0.7.0 → 0.7.0, 0.7.1",
      },
      {
        at: bucketAt(44),
        kind: "version_change",
        detail: "Versions 0.7.0, 0.7.1 → 0.7.1",
      },
      {
        at: bucketAt(47),
        kind: "instances_lost",
        detail: "1 instance stopped reporting (48 → 47 live)",
      },
    ],
  };
  // The server classifies queues (awa-ui/src/assessment.rs); the fixture
  // reproduces its verdicts for this story from the live queue state, so
  // pause, resume and drain change the report as they would for real.
  const flow = (index: number) => ({
    claimed_per_minute: index === 3 ? 0 : 2140,
    completed_per_minute: index === 3 ? 0 : 2140,
    failed_per_minute: index === 0 ? 4 : 0,
    retried_per_minute: 0,
    arrivals_per_minute: index === 0 ? 4640 : index === 4 ? 140 : 2140,
    mean_claim_wait_seconds: index === 0 ? 410 : 2,
    rate_limited: index === 0,
  });
  const assess = (q: QueueStats, index: number) => {
    const base = {
      queue: q.queue,
      flow: flow(index),
      live_workers: 47,
      in_flight: q.running,
      capacity: 64 * 47,
      clear_seconds: null as number | null,
      trend: { per_minute: 0, change: 0, minutes: 5 },
      limited_by: null as string | null,
    };
    if (q.paused)
      return {
        ...base,
        health: "paused",
        label: "Paused",
        reason: q.available
          ? `${q.available} waiting while paused`
          : "Nothing waiting",
        needs_attention: q.available > 0,
        rank: q.available > 0 ? 3 : 7,
        limited_by: q.available > 0 ? "paused" : null,
      };
    if (index === 3 && q.available > 0)
      return {
        ...base,
        health: "stalled",
        label: "Not being worked",
        reason: "Nothing running or claimed; oldest has waited 1m 35s",
        needs_attention: true,
        rank: 0,
        limited_by: "nothing",
        trend: { per_minute: 415, change: 830, minutes: 2 },
      };
    if (index === 0 && q.available > 0)
      return {
        ...base,
        health: "behind",
        label: "Falling behind",
        reason: "Backlog +2,500/min over 5m",
        needs_attention: true,
        rank: 1,
        limited_by: "rate_limit",
        trend: { per_minute: 2500, change: 12500, minutes: 5 },
      };
    if (index === 4 && q.available > 0)
      return {
        ...base,
        health: "draining",
        label: "Draining",
        reason: "Clears in about 6m 5s at this pace",
        needs_attention: false,
        rank: 4,
        trend: { per_minute: -2000, change: -10000, minutes: 5 },
        clear_seconds: 365,
      };
    return q.available > 0
      ? {
          ...base,
          health: "steady",
          label: "Steady",
          reason: "Backlog holding level",
          needs_attention: false,
          rank: 6,
        }
      : {
          ...base,
          health: "idle",
          label: "Idle",
          reason: "Nothing waiting",
          needs_attention: false,
          rank: 7,
        };
  };
  const healthReport = () => {
    const assessed = queues
      .map((q, index) => assess(q, index))
      .sort(
        (a, b) =>
          a.rank - b.rank ||
          queues.find((q) => q.queue === b.queue)!.available -
            queues.find((q) => q.queue === a.queue)!.available ||
          a.queue.localeCompare(b.queue),
      );
    const stalled = assessed.filter((a) => a.health === "stalled");
    const behind = assessed.filter((a) => a.health === "behind").length;
    const paused = assessed.filter(
      (a) => a.health === "paused" && a.needs_attention,
    ).length;
    const extra = [
      behind && `${behind} queue${behind === 1 ? "" : "s"} falling behind`,
      paused && `${paused} paused queue${paused === 1 ? "" : "s"} holding work`,
    ].filter(Boolean);
    return {
      status: stalled.length ? "critical" : behind ? "warning" : "ok",
      title: stalled.length
        ? `${stalled.length} queue${stalled.length === 1 ? "" : "s"} not being worked`
        : behind
          ? `${behind} queue${behind === 1 ? "" : "s"} falling behind`
          : "Keeping up",
      detail: stalled.length
        ? [
            `Waiting work nobody is claiming: ${stalled.map((a) => a.queue).join(", ")}.`,
            ...extra,
          ].join(" · ")
        : `${assessed.length} queues steady, draining, or idle.`,
      generated_at: new Date(now).toISOString(),
      history_available: true,
      history_since: bucketAt(10),
      queues: assessed,
    };
  };
  const jobs: JobRow[] = Array.from({ length: 60 }, (_, index) => ({
    id: 984213 - index,
    queue: index % 7 === 3 ? "image-processing" : "email-delivery",
    kind: index % 7 === 3 ? "ResizeImage" : "SendTransactionalEmail",
    args: { template: "receipt", synthetic: true },
    state: "failed",
    priority: 2,
    original_priority: 2,
    attempt: 5,
    max_attempts: 5,
    run_at: ago(900),
    heartbeat_at: null,
    deadline_at: null,
    attempted_at: ago(120),
    finalized_at: ago(index * 12 + 8),
    created_at: ago(1000),
    errors: [
      {
        error:
          index % 7 === 3
            ? `Out of memory decoding upload ${7_310_000 + index}`
            : "Provider request timed out after 30 seconds",
        attempt: 5,
        at: ago(8),
      },
    ],
    metadata: {},
    tags: ["synthetic"],
    progress: null,
    callback_id: null,
    callback_timeout_at: null,
    callback_filter: null,
    callback_on_complete: null,
    callback_on_fail: null,
    callback_transform: null,
    queue_descriptor: null,
    kind_descriptor: null,
  }));
  const mutations: string[] = [];
  let failReads = scenario === "error";
  await page.route("**/api/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.slice(4);
    if (request.method() !== "GET") {
      mutations.push(`${request.method()} ${path}`);
      const queue = queues.find((q) => path.includes(q.queue));
      if (queue && path.endsWith("/pause")) queue.paused = true;
      if (queue && path.endsWith("/resume")) queue.paused = false;
      if (queue && path.endsWith("/drain")) {
        queue.available = 0;
        queue.retryable = 0;
        queue.scheduled = 0;
        queue.waiting_external = 0;
        queue.total_queued = queue.running;
      }
      const job = jobs.find((entry) => path.startsWith(`/jobs/${entry.id}/`));
      if (job && path.endsWith("/retry")) {
        job.state = "available";
        job.attempt = 0;
      }
      if (job && path.endsWith("/cancel")) job.state = "cancelled";
      return route.fulfill({ json: job ?? { ok: true, drained: 184200 } });
    }
    if (path === "/capabilities")
      return route.fulfill({
        json: {
          read_only: readOnly,
          poll_interval_ms: 60000,
          instance_name: "Synthetic / local",
          instance_color: "#23745a",
          peers: [],
        },
      });
    if (scenario === "loading")
      await new Promise((resolve) => setTimeout(resolve, 2500));
    if (
      failReads &&
      [
        "/stats",
        "/queues",
        "/stats/queue-history",
        "/health/queues",
        "/runtime",
        "/jobs",
        "/dlq/depth",
      ].includes(path)
    )
      return route.fulfill({
        status: 503,
        json: { error: "Synthetic API unavailable" },
      });
    const empty = scenario === "empty";
    let data: unknown = [];
    if (path === "/stats")
      data = empty
        ? {}
        : {
            available: queues.reduce((s, q) => s + q.available, 0),
            running: 1236,
            retryable: 2070,
            failed: 236,
            scheduled: 20676,
            waiting_external: 14400,
            completed: 14820000,
          };
    else if (path === "/queues") data = empty ? [] : queues;
    else if (path === "/health/queues")
      data = empty
        ? { ...healthReport(), status: "ok", title: "Keeping up", queues: [] }
        : healthReport();
    else if (path === "/stats/queue-history")
      data = empty
        ? { ...queueHistory, observed_since: null, queues: {}, events: [] }
        : queueHistory;
    else if (path === "/queues/runtime")
      data = empty
        ? []
        : queues.map((q) => ({
            queue: q.queue,
            instance_count: 48,
            live_instances: 47,
            stale_instances: 1,
            healthy_instances: 46,
            total_in_flight: q.running,
            overflow_held_total: 0,
            config_mismatch: false,
            config,
          }));
    else if (path.startsWith("/queues/"))
      data = queues.find((q) => q.queue === decodeURIComponent(path.slice(8)));
    else if (path === "/runtime")
      data = {
        total_instances: empty ? 0 : 48,
        live_instances: empty ? 0 : 47,
        stale_instances: empty ? 0 : 1,
        healthy_instances: empty ? 0 : 46,
        leader_instances: empty ? 0 : 1,
        instances: empty ? [] : instances,
      };
    else if (path === "/dlq/depth")
      data = {
        total: empty ? 0 : 1240,
        by_queue: empty ? [] : [{ queue: "email-delivery", count: 1240 }],
      };
    else if (path === "/jobs")
      data = empty
        ? []
        : jobs
            .filter(
              (job) =>
                !url.searchParams.has("queue") ||
                job.queue === url.searchParams.get("queue"),
            )
            .map((job) => ({
              ...job,
              state: url.searchParams.get("state") ?? job.state,
            }))
            .slice(0, Number(url.searchParams.get("limit") ?? 50));
    else if (/^\/jobs\/\d+$/.test(path))
      data = jobs.find((job) => job.id === Number(path.slice(6)));
    else if (path === "/stats/kinds") data = ["SendTransactionalEmail"];
    else if (path === "/stats/queues") data = queues.map((q) => q.queue);
    else if (path === "/storage")
      return route.fulfill({ status: 404, json: {} });
    return route.fulfill({ json: data });
  });
  return {
    mutations,
    setReadFailure: (value: boolean) => {
      failReads = value;
    },
  };
}
