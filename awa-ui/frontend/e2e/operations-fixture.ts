import type { Page } from "@playwright/test";
import type {
  JobRow,
  QueueAssessment,
  QueueHealthKind,
  QueueStats,
  RuntimeInstance,
} from "../src/lib/api";
import { formatDuration, formatRate } from "../src/lib/operations";

/**
 * Three moments in one synthetic fleet:
 *
 * - `normal`: every queue keeps up; nothing needs a human.
 * - `incident`: a 0.7.1 rollout left image-processing with no worker
 *   claiming it, email-delivery is falling behind its rate limit,
 *   invoice-export was paused, and search-index drains a burst.
 * - `recovery`: a 0.7.2 hotfix is rolled out, the email rate limit raised
 *   and invoice-export resumed; the backlogs drain.
 */
export type Phase = "normal" | "incident" | "recovery";
type Scenario = Phase | "empty" | "error" | "loading";

const NAMES = [
  "email-delivery",
  "invoice-export",
  "webhook-dispatch",
  "image-processing",
  "search-index",
  "daily-reports",
];
const EMAIL = 0;
const INVOICE = 1;
const WEBHOOK = 2;
const IMAGE = 3;
const SEARCH = 4;

/** What a queue looks like now, and how its waiting count got there. */
interface QueueStory {
  available: number;
  running: number;
  retryable: number;
  failed: number;
  lag: number;
  paused?: boolean;
  /** Waiting jobs `minutesAgo` minutes ago. */
  depth: (minutesAgo: number) => number;
  /** The server's verdict while work is waiting (otherwise steady/idle). */
  verdict?: {
    health: QueueHealthKind;
    perMinute: number;
    limitedBy?: QueueAssessment["limited_by"];
    reason?: string;
  };
  claimed: number;
  arrivals: number;
  claimWait: number;
  failing?: number;
  rateLimited?: boolean;
}

function story(phase: Phase, index: number): QueueStory {
  const quiet = 180 + index * 7;
  const steady = (available: number): QueueStory => ({
    available,
    running: 40 + index,
    retryable: phase === "normal" ? 2 : 10,
    failed: 0,
    lag: phase === "normal" ? 1.2 : 2.4,
    depth: (m) => available + ((m * 7 + index) % 5) * 4,
    claimed: 2140,
    arrivals: 2140,
    claimWait: 1.2,
  });
  if (phase === "normal") {
    if (index === SEARCH) return { ...steady(0), depth: () => 0 };
    if (index === EMAIL)
      return { ...steady(1840), retryable: 12, failed: 3, lag: 2.1 };
    return steady(quiet);
  }
  if (phase === "incident") {
    if (index === EMAIL)
      return {
        ...steady(184200),
        retryable: 1840,
        failed: 236,
        lag: 458,
        depth: (m) => (m <= 20 ? 184200 - m * 2500 : 134200 + (m % 3) * 300),
        verdict: {
          health: "behind",
          perMinute: 2500,
          limitedBy: "rate_limit",
        },
        arrivals: 4640,
        claimWait: 410,
        failing: 4,
        rateLimited: true,
      };
    if (index === INVOICE)
      return {
        ...steady(2117),
        lag: 135,
        paused: true,
        depth: (m) => (m <= 12 ? 2117 : 40 + (m % 4) * 5),
      };
    if (index === IMAGE)
      return {
        ...steady(2151),
        running: 0,
        lag: 95,
        depth: (m) => (m <= 2 ? 2151 - m * 900 : 30 + (m % 2) * 10),
        verdict: {
          health: "stalled",
          perMinute: 415,
          limitedBy: "nothing",
          reason: "Nothing running or claimed; oldest has waited 1m 35s",
        },
        claimed: 0,
        arrivals: 415,
      };
    if (index === SEARCH)
      return {
        ...steady(12168),
        depth: (m) => (m <= 15 ? 12168 + m * 2000 : 4000),
        verdict: { health: "draining", perMinute: -2000 },
        arrivals: 140,
      };
    return steady(2100 + index * 17);
  }
  // recovery
  if (index === EMAIL)
    return {
      ...steady(62000),
      retryable: 310,
      failed: 236,
      lag: 210,
      depth: (m) =>
        m <= 14
          ? 62000 + m * 8700
          : m <= 34
            ? 184200 - (m - 14) * 2500
            : 134200 + (m % 3) * 300,
      verdict: { health: "draining", perMinute: -8700 },
      claimed: 10840,
      arrivals: 2140,
      claimWait: 190,
    };
  if (index === INVOICE)
    return {
      ...steady(640),
      lag: 20,
      depth: (m) => (m <= 6 ? 640 + m * 246 : m <= 26 ? 2117 : 40),
      verdict: { health: "draining", perMinute: -246 },
    };
  if (index === IMAGE)
    return {
      ...steady(380),
      lag: 40,
      depth: (m) => (m <= 10 ? 380 + m * 177 : m <= 22 ? 2151 : 30),
      verdict: { health: "draining", perMinute: -177 },
    };
  if (index === SEARCH) return { ...steady(0), depth: () => 0 };
  return steady(2100 + index * 17);
}

const RANK: Record<QueueHealthKind, number> = {
  stalled: 0,
  behind: 1,
  slow: 2,
  paused: 3,
  draining: 4,
  unknown: 5,
  steady: 6,
  idle: 7,
};

const LABEL: Record<QueueHealthKind, string> = {
  stalled: "Not being worked",
  behind: "Falling behind",
  slow: "Long wait",
  paused: "Paused",
  draining: "Draining",
  unknown: "No trend yet",
  steady: "Steady",
  idle: "Idle",
};

function plural(count: number, noun: string): string {
  return `${count} ${noun}${count === 1 ? "" : "s"}`;
}

export async function mockOperations(
  page: Page,
  scenario: Scenario = "incident",
  readOnly = false,
) {
  const phase: Phase =
    scenario === "normal" || scenario === "recovery" ? scenario : "incident";
  const now = Date.now();
  const ago = (seconds: number) => new Date(now - seconds * 1000).toISOString();
  const stories = Array.from({ length: 24 }, (_, index) => story(phase, index));
  const queues: QueueStats[] = stories.map((s, index) => ({
    queue: NAMES[index] ?? `tenant-sync-${index}`,
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
    available: s.available,
    running: s.running,
    retryable: s.retryable,
    failed: s.failed,
    scheduled: 850 + index,
    waiting_external:
      index === WEBHOOK ? (phase === "normal" ? 1400 : 14400) : 0,
    completed_last_hour: s.claimed * 60,
    lag_seconds: s.lag,
    paused: s.paused ?? false,
  }));
  for (const q of queues)
    q.total_queued =
      q.available + q.running + q.retryable + q.scheduled + q.waiting_external;

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
  const version = { normal: "0.7.1", incident: "0.7.1", recovery: "0.7.2" }[
    phase
  ];
  const instances: RuntimeInstance[] = Array.from(
    { length: 48 },
    (_, index) => {
      const troubled = phase === "incident";
      return {
        instance_id: `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`,
        hostname: `worker-eu-${String(index + 1).padStart(2, "0")}`,
        pid: 1300 + index,
        version,
        storage_capability: "queue_storage",
        transition_role: "auto",
        started_at: ago(phase === "recovery" ? 600 : 86400),
        last_seen_at: ago(troubled && index === 0 ? 170 : 4),
        snapshot_interval_ms: 10000,
        stale: troubled && index === 0,
        healthy: !troubled || index > 1,
        postgres_connected: true,
        poll_loop_alive: !troubled || index !== 1,
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
      };
    },
  );
  const live = instances.filter((i) => !i.stale);

  // Per-minute history the way the leader records it: an hour of buckets,
  // a leader that started 50 minutes ago, and a three-minute gap where
  // sampling failed.
  const BUCKETS = 60;
  const minuteStart = Math.floor(now / 60_000) * 60_000;
  const bucketAt = (bucket: number) =>
    new Date(minuteStart - (BUCKETS - 1 - bucket) * 60_000).toISOString();
  const sampled = (bucket: number) =>
    bucket >= 10 && ![33, 34, 35].includes(bucket);
  const series = <T>(value: (bucket: number) => T) =>
    Array.from({ length: BUCKETS }, (_, bucket) =>
      sampled(bucket) ? value(bucket) : null,
    );
  const events = {
    normal: [],
    incident: [
      [41, "version_change", "Versions 0.7.0 → 0.7.0, 0.7.1"],
      [44, "version_change", "Versions 0.7.0, 0.7.1 → 0.7.1"],
      [47, "instances_lost", "1 instance stopped reporting (48 → 47 live)"],
    ],
    recovery: [
      [27, "version_change", "Versions 0.7.0 → 0.7.0, 0.7.1"],
      [30, "version_change", "Versions 0.7.0, 0.7.1 → 0.7.1"],
      [32, "instances_lost", "1 instance stopped reporting (48 → 47 live)"],
      [48, "version_change", "Versions 0.7.1 → 0.7.1, 0.7.2"],
      [50, "leader_change", "Leader 3f2a91c4 → 8d07b1e2"],
      [51, "version_change", "Versions 0.7.1, 0.7.2 → 0.7.2"],
    ],
  }[phase] as [number, string, string][];
  const queueHistory = {
    installed: true,
    bucket_secs: 60,
    minutes: BUCKETS,
    observed_since: bucketAt(10),
    buckets: Array.from({ length: BUCKETS }, (_, bucket) => bucketAt(bucket)),
    queues: Object.fromEntries(
      queues.map((q, index) => {
        const s = stories[index]!;
        return [
          q.queue,
          {
            available: series((b) => Math.max(0, s.depth(BUCKETS - 1 - b))),
            running: series(() => q.running),
            lag_seconds: series(() => q.lag_seconds),
            claimed: series(() => s.claimed),
            completed: series(() => s.claimed),
            failed_attempts: series(() => s.failing ?? 0),
            rate_limited: series(() => (s.rateLimited ? 30 : 0)),
          },
        ];
      }),
    ),
    fleet: {
      live_instances: series(() => live.length),
      healthy_instances: series(() => live.filter((i) => i.healthy).length),
    },
    events: events.map(([bucket, kind, detail]) => ({
      at: bucketAt(bucket),
      kind,
      detail,
    })),
  };

  // The server classifies queues (awa-ui/src/assessment.rs); the fixture
  // reproduces its verdicts for each story from the live queue state, so
  // pause, resume and drain change the report as they would for real.
  const assess = (q: QueueStats, index: number): QueueAssessment => {
    const s = stories[index]!;
    const base = {
      queue: q.queue,
      flow: {
        claimed_per_minute: s.claimed,
        completed_per_minute: s.claimed,
        failed_per_minute: s.failing ?? 0,
        retried_per_minute: 0,
        arrivals_per_minute: s.arrivals,
        mean_claim_wait_seconds: s.claimWait,
        rate_limited: s.rateLimited ?? false,
      },
      live_workers: live.length,
      in_flight: q.running,
      capacity: 64 * live.length,
      clear_seconds: null as number | null,
      trend: { per_minute: 0, change: 0, minutes: 5 },
      limited_by: null as QueueAssessment["limited_by"],
    };
    const make = (
      health: QueueHealthKind,
      reason: string,
      extra: Partial<QueueAssessment> = {},
    ): QueueAssessment => ({
      ...base,
      health,
      label: LABEL[health],
      reason,
      needs_attention:
        health === "stalled" ||
        health === "behind" ||
        health === "slow" ||
        (health === "paused" && q.available > 0),
      rank: health === "paused" && q.available === 0 ? 7 : RANK[health],
      ...extra,
    });
    if (q.paused)
      return q.available
        ? make("paused", `${q.available} waiting while paused`, {
            limited_by: "paused",
          })
        : make("paused", "Nothing waiting");
    if (q.available === 0) return make("idle", "Nothing waiting");
    const verdict = s.verdict;
    if (!verdict) return make("steady", "Backlog holding level");
    const trend = {
      per_minute: verdict.perMinute,
      change: verdict.perMinute * 5,
      minutes: 5,
    };
    if (verdict.health === "draining") {
      const clear = (q.available / -verdict.perMinute) * 60;
      return make(
        "draining",
        `Clears in about ${formatDuration(clear)} at this pace`,
        { trend, clear_seconds: clear },
      );
    }
    return make(
      verdict.health,
      verdict.reason ??
        `Backlog ${formatRate(verdict.perMinute, true)} over 5m`,
      { trend, limited_by: verdict.limitedBy ?? null },
    );
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
    const count = (h: QueueHealthKind) =>
      assessed.filter((a) => a.health === h);
    const stalled = count("stalled");
    const behind = count("behind").length;
    const paused = assessed.filter(
      (a) => a.health === "paused" && a.needs_attention,
    ).length;
    const recovering = count("draining").filter(
      (a) => (a.clear_seconds ?? 0) >= 120,
    );
    const others = [
      behind && `${plural(behind, "queue")} falling behind`,
      paused && `${plural(paused, "paused queue")} holding work`,
    ].filter(Boolean);
    const longest = Math.max(...recovering.map((a) => a.clear_seconds ?? 0));
    const [status, title, detail] = stalled.length
      ? [
          "critical",
          `${plural(stalled.length, "queue")} not being worked`,
          [
            `Waiting work nobody is claiming: ${stalled.map((a) => a.queue).join(", ")}.`,
            ...others,
          ].join(" · "),
        ]
      : behind
        ? ["warning", `${plural(behind, "queue")} falling behind`, ""]
        : paused
          ? [
              "ok",
              `${plural(paused, "paused queue")} holding work`,
              "Every running queue is keeping up. Resume paused queues when ready.",
            ]
          : recovering.length
            ? [
                "ok",
                "Recovering",
                `${plural(recovering.length, "queue")} draining a backlog (${recovering
                  .slice(0, 3)
                  .map((a) => a.queue)
                  .join(
                    ", ",
                  )}); all clear in about ${formatDuration(longest)} at this pace.`,
              ]
            : [
                "ok",
                "Keeping up",
                `${plural(assessed.length, "queue")} steady, draining, or idle.`,
              ];
    return {
      status,
      title,
      detail,
      generated_at: new Date(now).toISOString(),
      history_available: true,
      history_since: bucketAt(10),
      queues: assessed,
    };
  };

  const failureCount = phase === "normal" ? 3 : 60;
  const jobs: JobRow[] = Array.from({ length: failureCount }, (_, index) => {
    const image = phase !== "normal" && index % 7 === 3;
    // After recovery the failures stopped about 14 minutes ago.
    const finalized =
      phase === "normal"
        ? 1200 + index * 380
        : index * 12 + (phase === "recovery" ? 840 : 8);
    return {
      id: 984213 - index,
      queue: image ? "image-processing" : "email-delivery",
      kind: image ? "ResizeImage" : "SendTransactionalEmail",
      args: { template: "receipt", synthetic: true },
      state: "failed",
      priority: 2,
      original_priority: 2,
      attempt: 5,
      max_attempts: 5,
      run_at: ago(finalized + 900),
      heartbeat_at: null,
      deadline_at: null,
      attempted_at: ago(finalized + 100),
      finalized_at: ago(finalized),
      created_at: ago(finalized + 1000),
      errors: [
        {
          error:
            phase === "normal"
              ? "Mailbox unavailable (550 5.1.1)"
              : image
                ? `Out of memory decoding upload ${7_310_000 + index}`
                : "Provider request timed out after 30 seconds",
          attempt: 5,
          at: ago(finalized),
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
    };
  });

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
    const sum = (pick: (q: QueueStats) => number) =>
      queues.reduce((total, q) => total + pick(q), 0);
    let data: unknown = [];
    if (path === "/stats")
      data = empty
        ? {}
        : {
            available: sum((q) => q.available),
            running: sum((q) => q.running),
            retryable: sum((q) => q.retryable),
            failed: sum((q) => q.failed),
            scheduled: sum((q) => q.scheduled),
            waiting_external: sum((q) => q.waiting_external),
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
            instance_count: instances.length,
            live_instances: live.length,
            stale_instances: instances.length - live.length,
            healthy_instances: live.filter((i) => i.healthy).length,
            total_in_flight: q.running,
            overflow_held_total: 0,
            config_mismatch: false,
            config,
          }));
    else if (path.startsWith("/queues/"))
      data = queues.find((q) => q.queue === decodeURIComponent(path.slice(8)));
    else if (path === "/runtime")
      data = {
        total_instances: empty ? 0 : instances.length,
        live_instances: empty ? 0 : live.length,
        stale_instances: empty ? 0 : instances.length - live.length,
        healthy_instances: empty ? 0 : live.filter((i) => i.healthy).length,
        leader_instances: empty ? 0 : 1,
        instances: empty ? [] : instances,
      };
    else if (path === "/dlq/depth")
      data = {
        total: empty ? 0 : phase === "normal" ? 12 : 1240,
        by_queue: empty
          ? []
          : [
              {
                queue: "email-delivery",
                count: phase === "normal" ? 12 : 1240,
              },
            ],
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
