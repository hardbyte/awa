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
    available: index === 0 ? 184200 : 2100 + index * 17,
    running: 40 + index,
    retryable: index === 0 ? 1840 : 10,
    failed: index === 0 ? 236 : 0,
    scheduled: 850 + index,
    waiting_external: index === 2 ? 14400 : 0,
    completed_last_hour: 128400 + index * 211,
    lag_seconds: index === 0 ? 458 : index === 1 ? 135 : 2.4,
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
      queues: queues
        .slice(0, 6)
        .map((q) => ({
          queue: q.queue,
          in_flight: 8,
          overflow_held: 0,
          config: config as RuntimeInstance["queues"][number]["config"],
        })),
    }),
  );
  const jobs: JobRow[] = Array.from({ length: 60 }, (_, index) => ({
    id: 984213 - index,
    queue: "email-delivery",
    kind: "SendTransactionalEmail",
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
        error: "Provider request timed out after 30 seconds",
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
      ["/stats", "/queues", "/runtime", "/jobs", "/dlq/depth"].includes(path)
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
