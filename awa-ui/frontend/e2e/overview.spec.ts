import { test, expect, type Page } from "@playwright/test";

const now = () => new Date().toISOString();

function queue(overrides: Record<string, unknown> = {}) {
  return {
    queue: "e2e_test",
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

function job(overrides: Record<string, unknown> = {}) {
  return {
    id: 990001,
    kind: "e2e_job",
    queue: "e2e_test",
    args: {},
    state: "failed",
    priority: 2,
    attempt: 5,
    max_attempts: 5,
    run_at: now(),
    heartbeat_at: null,
    deadline_at: null,
    attempted_at: now(),
    finalized_at: now(),
    created_at: now(),
    errors: [{ error: "GatewayTimeout: upstream timed out", attempt: 5, at: now() }],
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

interface Fixture {
  stats: Record<string, number>;
  queues: unknown[];
  dlq: { total: number; by_queue: { queue: string; count: number }[] };
  failed: unknown[];
  running: unknown[];
  /** `GET /api/stats/outcomes`; absent answers 404, as a server without the endpoint does. */
  outcomes?: unknown;
}

interface HourCounts {
  completed?: number;
  failed?: number;
  dead_lettered?: number;
}

/**
 * The Overview's outcome read: two hours of one-minute buckets ending with
 * the current minute, `previous` and `current` per minute in each hour. The
 * terminal series is covered from `coveredMinutesAgo` and dead letters from
 * `deadLetteredMinutesAgo` (default: the whole window); a series is null
 * before its coverage.
 */
function outcomes(previous: HourCounts, current: HourCounts, coveredMinutesAgo = 119, deadLetteredMinutesAgo = 119) {
  const minute = Math.floor(Date.now() / 60_000) * 60_000;
  const first = minute - 119 * 60_000;
  const terminal = minute - coveredMinutesAgo * 60_000;
  const dead = minute - deadLetteredMinutesAgo * 60_000;
  const buckets = Array.from({ length: 120 }, (_, n) => {
    const counts = n < 60 ? previous : current;
    const at = first + n * 60_000;
    const covered = at >= terminal;
    return {
      bucket: new Date(at).toISOString(),
      completed: covered ? counts.completed ?? 0 : null,
      failed: covered ? counts.failed ?? 0 : null,
      cancelled: covered ? 0 : null,
      dead_lettered: at >= dead ? counts.dead_lettered ?? 0 : null,
    };
  }).filter((bucket) => Date.parse(bucket.bucket) >= Math.min(terminal, dead));
  return {
    covered_since: new Date(terminal).toISOString(),
    dead_lettered_covered_since: new Date(dead).toISOString(),
    buckets,
  };
}

const HEALTHY_RUNTIME = {
  total_instances: 1,
  live_instances: 1,
  stale_instances: 0,
  healthy_instances: 1,
  leader_instances: 1,
  instances: [
    {
      instance_id: "11111111-1111-4111-8111-111111111111",
      hostname: "worker-a",
      pid: 7,
      version: "0.7.0",
      started_at: now(),
      last_seen_at: now(),
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
    },
  ],
};

/** Serve every read the Overview makes from a fixture. */
async function mockOverview(page: Page, fixture: Fixture) {
  const json = (body: unknown) => ({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  await page.route("**/api/**", (route) => {
    const request = route.request();
    if (request.method() !== "GET") return route.fallback();
    const url = new URL(request.url());
    switch (url.pathname) {
      case "/api/stats":
        return route.fulfill(json(fixture.stats));
      case "/api/queues":
        return route.fulfill(json(fixture.queues));
      case "/api/queues/runtime":
        return route.fulfill(json([]));
      case "/api/runtime":
        return route.fulfill(json(HEALTHY_RUNTIME));
      case "/api/dlq/depth":
        return route.fulfill(json(fixture.dlq));
      case "/api/batch-ops":
        return route.fulfill(json([]));
      case "/api/cron":
        return route.fulfill(json([]));
      case "/api/jobs":
        return route.fulfill(json(url.searchParams.get("state") === "running" ? fixture.running : fixture.failed));
      case "/api/stats/outcomes":
        return fixture.outcomes === undefined
          ? route.fulfill({ status: 404, contentType: "text/plain", body: "Not Found" })
          : route.fulfill(json(fixture.outcomes));
      default:
        return route.fallback();
    }
  });
}

const BUSY: Fixture = {
  // The server's failed counts include the dead letters.
  stats: { available: 140, running: 1, failed: 5, retryable: 7, scheduled: 3108, waiting_external: 35, completed: 10, cancelled: 0 },
  queues: [
    queue({ queue: "imports", display_name: "Contact imports", paused: true, available: 140, lag_seconds: 540 }),
    queue({ queue: "billing", available: 4, lag_seconds: 2, retryable: 7, failed: 5, completed_last_hour: 273 }),
    queue({ queue: "email", scheduled: 3108, completed_last_hour: 1200 }),
  ],
  dlq: { total: 2, by_queue: [{ queue: "billing", count: 2 }] },
  failed: [job({ id: 3 }), job({ id: 2 }), job({ id: 1, kind: "sync_ledger", errors: [{ error: "KeyError: 'region'" }] })],
  running: [job({ id: 9, state: "running", deadline_at: new Date(Date.now() - 60_000).toISOString() })],
};

test.describe("Overview", () => {
  test("health strip shows each figure and keeps future work out of the backlog", async ({ page }) => {
    await mockOverview(page, BUSY);
    await page.goto("/");
    await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();

    await expect(page.getByTestId("tile-ready").getByTestId("tile-value")).toHaveText("4");
    await expect(page.getByTestId("tile-ready")).toContainText("+140 held in 1 paused queue");
    await expect(page.getByTestId("tile-retrying").getByTestId("tile-value")).toHaveText("7");
    await expect(page.getByTestId("tile-retrying")).toContainText("most in billing (7)");
    await expect(page.getByTestId("tile-failed").getByTestId("tile-value")).toHaveText("3");
    await expect(page.getByTestId("tile-failed")).toContainText("1,473 completed in the last hour");
    await expect(page.getByTestId("tile-failed")).toHaveAttribute("href", /\/jobs\?state=failed&dead_lettered=false$/);
    await expect(page.getByTestId("tile-dead-letters").getByTestId("tile-value")).toHaveText("2");
    await expect(page.getByTestId("tile-stuck").getByTestId("tile-value")).toHaveText("1");
    await expect(page.getByTestId("tile-bulk").getByTestId("tile-value")).toHaveText("0");

    await expect(page.getByTestId("deferred-line")).toContainText("3,108 scheduled · 35 waiting for callback");
    await expect(page.getByTestId("fleet-line")).toContainText("1/1 healthy · leader worker-a");
    await expect(page.locator("section[aria-label='Health']")).not.toContainText("3,108");
  });

  test("an older server without outcomes keeps the strip as it was", async ({ page }) => {
    let outcomeReads = 0;
    page.on("request", (request) => {
      if (new URL(request.url()).pathname === "/api/stats/outcomes") outcomeReads++;
    });
    await mockOverview(page, BUSY);
    await page.goto("/");
    await expect(page.getByTestId("tile-failed")).toContainText("1,473 completed in the last hour");
    await expect(page.getByTestId("failure-rate")).toHaveCount(0);
    await expect(page.getByTestId("tile-dead-letters")).toContainText("most in billing (2)");
    await expect(page.getByTestId("dead-letter-trend")).toHaveCount(0);
    await expect(page.getByTestId("attention-failed-checks")).toHaveCount(0);
    // The 404 stops the read rather than polling it.
    await page.waitForTimeout(7_000);
    expect(outcomeReads).toBe(1);
  });

  test("outcomes give the failure rate of both hours and the dead letters added", async ({ page }) => {
    await mockOverview(page, {
      ...BUSY,
      outcomes: outcomes({ completed: 99, failed: 1 }, { completed: 8, failed: 2, dead_lettered: 1 }),
    });
    await page.goto("/");
    const failed = page.getByTestId("tile-failed");
    await expect(failed.getByTestId("tile-value")).toHaveText("3");
    await expect(failed.getByTestId("failure-rate")).toHaveText("20% failed in the last hour1% the hour before");
    await expect(failed).not.toContainText("completed in the last hour");
    const dead = page.getByTestId("tile-dead-letters");
    await expect(dead.getByTestId("dead-letter-trend")).toHaveText("+60 in the last hour");
    await expect(dead).toContainText("most in billing (2)");

    const item = page.locator("[data-testid='attention-item'][data-id='jobs:failure-rate']");
    await expect(item).toHaveAttribute("data-severity", "warning");
    await expect(item).toContainText("Failure rate rose to 20% in the last hour");
    await expect(item).toContainText("120 of 600 finished jobs failed, against 1% the hour before");
    await item.getByRole("link").click();
    await expect(page).toHaveURL(/\/jobs\?state=failed$/);
  });

  test("outcomes covering less than the window leave the hours they miss alone", async ({ page }) => {
    // Both series covered for the last 45 minutes only: neither hour is complete.
    await mockOverview(page, { ...BUSY, outcomes: outcomes({ failed: 5 }, { completed: 1, dead_lettered: 1 }, 45, 45) });
    await page.goto("/");
    await expect(page.getByTestId("tile-failed")).toContainText("1,473 completed in the last hour");
    await expect(page.getByTestId("failure-rate")).toHaveCount(0);
    await expect(page.getByTestId("dead-letter-trend")).toHaveCount(0);
    await expect(page.locator("[data-id='jobs:failure-rate']")).toHaveCount(0);

    // Covered for the last 90 minutes: the dead letters' hour is, the rate's two are not.
    await mockOverview(page, { ...BUSY, outcomes: outcomes({ failed: 5 }, { completed: 1 }, 90, 90) });
    await page.reload();
    await expect(page.getByTestId("dead-letter-trend")).toHaveText("none added in the last hour");
    await expect(page.getByTestId("failure-rate")).toHaveCount(0);
  });

  test("dead letters keep their own coverage when ring prune shortens the terminal one", async ({ page }) => {
    // Queue storage under load: terminal rows cover five minutes, dead letters the window.
    await mockOverview(page, { ...BUSY, outcomes: outcomes({ dead_lettered: 1 }, { completed: 9, failed: 3, dead_lettered: 2 }, 5) });
    await page.goto("/");
    await expect(page.getByTestId("dead-letter-trend")).toHaveText("+120 in the last hour");
    await expect(page.getByTestId("tile-failed")).toContainText("1,473 completed in the last hour");
    await expect(page.getByTestId("failure-rate")).toHaveCount(0);
  });

  test("a failed read shows a dash and the error, never zero", async ({ page }) => {
    await mockOverview(page, BUSY);
    await page.route("**/api/stats", (route) => route.fulfill({ status: 500, json: { error: "database unavailable" } }));
    await page.route("**/api/dlq/depth", (route) => route.fulfill({ status: 500, json: { error: "database unavailable" } }));
    await page.goto("/");
    for (const tile of ["tile-retrying", "tile-failed", "tile-dead-letters"]) {
      await expect(page.getByTestId(tile).getByTestId("tile-value")).toHaveText("—");
      await expect(page.getByTestId(tile)).toContainText("Failed to load");
    }
    await expect(page.getByTestId("attention-failed-checks")).toContainText("dead letters");
    await expect(page.getByTestId("deferred-line")).toContainText("failed to load");
  });

  test("the attention list ranks problems and links to scoped views", async ({ page }) => {
    await mockOverview(page, BUSY);
    await page.goto("/");
    const items = page.getByTestId("attention-item");
    await expect(items.first()).toBeVisible();
    await expect(items.nth(0)).toHaveAttribute("data-id", "jobs:stuck");
    await expect(items.nth(1)).toContainText("2× GatewayTimeout in e2e_job");
    await expect(items.nth(2)).toContainText("1× KeyError in sync_ledger");
    await expect(items.nth(3)).toContainText("Contact imports is paused with 140 ready jobs");
    await expect(items.nth(4)).toContainText("2 dead letters in billing");

    await expect(items.nth(3).getByRole("link")).toHaveAttribute("href", /\/queues\/imports$/);
    await expect(items.nth(4).getByRole("link")).toHaveAttribute("href", /\/queues\/billing\/dlq$/);
    await items.nth(1).getByRole("link").click();
    await expect(page).toHaveURL(/\/jobs\?.*state=failed/);
    expect(decodeURIComponent(page.url())).toContain("q=kind:e2e_job");
  });

  test("queues needing attention leave quiet queues out", async ({ page }) => {
    await mockOverview(page, BUSY);
    await page.goto("/");
    const table = page.getByRole("grid", { name: "Queues needing attention" });
    await expect(table.getByRole("row")).toHaveCount(3);
    await expect(table).toContainText("Contact imports");
    await expect(table).toContainText("Paused");
    await expect(table).not.toContainText("email");
    const billing = table.getByRole("row").filter({ hasText: "billing" });
    await expect(billing.getByRole("gridcell")).toHaveText(["4", "2s", "7", "3", "2"]);
    await page.getByRole("link", { name: "All queues →" }).click();
    await expect(page).toHaveURL(/\/queues$/);
  });

  test("says nothing needs attention when all is well", async ({ page }) => {
    await mockOverview(page, {
      stats: { available: 0, running: 0, failed: 0, retryable: 0, scheduled: 0, waiting_external: 0 },
      queues: [queue({ completed_last_hour: 12 })],
      dlq: { total: 0, by_queue: [] },
      failed: [],
      running: [],
    });
    await page.goto("/");
    await expect(page.getByTestId("attention-empty")).toContainText("Nothing needs attention · checked");
    await expect(page.getByTestId("queues-attention-empty")).toBeVisible();
    await expect(page.getByTestId("attention-item")).toHaveCount(0);
    await expect(page.getByTestId("attention-stuck-sample")).toHaveCount(0);
  });

  test("says stuck jobs were looked for among the newest running jobs only", async ({ page }) => {
    await mockOverview(page, {
      stats: { available: 0, running: 250, failed: 0, retryable: 0, scheduled: 0, waiting_external: 0 },
      queues: [queue({ running: 250 })],
      dlq: { total: 0, by_queue: [] },
      failed: [],
      running: Array.from({ length: 100 }, (_, i) => job({ id: 1000 + i, state: "running", heartbeat_at: now() })),
    });
    await page.goto("/");
    await expect(page.getByTestId("attention-stuck-sample")).toHaveText(
      "Stuck jobs were looked for among the 100 newest running jobs only."
    );
  });

  test("live data loads every check", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByTestId("attention-item").or(page.getByTestId("attention-empty")).first()).toBeVisible();
    await expect(page.getByTestId("attention-failed-checks")).toHaveCount(0);
    await expect(page.getByTestId("tile-failed").getByTestId("tile-value")).toHaveText(/^[\d,]+$/);
  });
});
