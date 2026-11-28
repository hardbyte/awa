import { test, expect, type Page } from "@playwright/test";

/** Fail every matching GET with a 500, as an unreachable database would. */
async function failApi(page: Page, pattern: string) {
  await page.route(pattern, (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ status: 500, json: { error: "database unavailable" } })
      : route.fallback()
  );
}

function job(overrides: Record<string, unknown> = {}) {
  const now = new Date().toISOString();
  return {
    id: 990001,
    kind: "e2e_job",
    queue: "e2e_test",
    args: {},
    state: "available",
    priority: 2,
    attempt: 1,
    max_attempts: 5,
    run_at: now,
    heartbeat_at: null,
    deadline_at: null,
    attempted_at: now,
    finalized_at: null,
    created_at: now,
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

function batchOperation(overrides: Record<string, unknown> = {}) {
  const now = new Date().toISOString();
  return {
    id: "22222222-2222-4222-8222-222222222222",
    op_kind: "retry",
    filter: { queue: "e2e_test" },
    spec: {},
    state: "completed",
    submitted_by: "ui",
    submitted_at: now,
    started_at: now,
    finalized_at: now,
    cursor: null,
    total_matched: 3000,
    processed: 0,
    skipped: 3000,
    errored: 0,
    last_error: null,
    runner_instance: null,
    retention_until: null,
    updated_at: now,
    ...overrides,
  };
}

test.describe("Phone navigation", () => {
  test("the drawer opens at 390px and navigates", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/jobs");

    await page.locator('button[data-slot="sidebar-trigger"]').first().click();
    const drawer = page.getByRole("dialog", { name: "Sidebar" });
    await expect(drawer).toBeVisible();
    // The drawer used to close itself in the same tick it opened.
    await page.waitForTimeout(500);
    await expect(drawer).toBeVisible();

    await drawer.getByRole("link", { name: "Queues" }).click();
    await expect(page).toHaveURL(/\/queues$/);
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
    await expect(drawer).toBeHidden();
  });

  test("pages do not scroll sideways at 720px (desktop at 200% zoom)", async ({ page }) => {
    await page.setViewportSize({ width: 720, height: 900 });
    for (const path of ["/", "/jobs", "/queues", "/kinds", "/runtime"]) {
      await page.goto(path);
      await expect(page.getByRole("heading").first()).toBeVisible();
      const overflow = await page.evaluate(
        () => document.documentElement.scrollWidth - document.documentElement.clientWidth
      );
      expect(overflow, path).toBeLessThanOrEqual(0);
    }
  });
});

test.describe("Navigation", () => {
  test("Dead letters is in the sidebar and opens the DLQ", async ({ page }) => {
    await page.goto("/");
    const nav = page.locator('[data-slot="sidebar-section"]').first();
    await nav.getByRole("link", { name: "Dead letters" }).click();
    await expect(page).toHaveURL(/\/dlq$/);
    await expect(page.getByRole("heading", { name: "Dead Letter Queue" })).toBeVisible();
    await expect(nav.getByRole("link", { name: "Dead letters" })).toHaveAttribute("aria-current", "page");
  });

  test("each route sets its own document title", async ({ page }) => {
    await page.goto("/jobs");
    await expect(page).toHaveTitle(/^Jobs · /);
    await page.goto("/dlq");
    await expect(page).toHaveTitle(/^Dead letters · /);
    await page.goto("/");
    await expect(page).toHaveTitle(/^Dashboard · /);
  });
});

test.describe("Failed reads are not shown as empty", () => {
  test("dashboard tiles show no count when stats fail", async ({ page }) => {
    await failApi(page, "**/api/stats");
    await page.goto("/");
    await expect(page.getByTestId("stats-error")).toContainText("Failed to load job counts");
    const tiles = page.locator('a[href*="/jobs?state="]');
    await expect(tiles.first()).toContainText("—");
    await expect(tiles.filter({ hasText: /^0/ })).toHaveCount(0);
  });

  test("batch operations show an error with retry, then the list", async ({ page }) => {
    let fail = true;
    await page.route("**/api/batch-ops*", (route) => {
      const url = new URL(route.request().url());
      if (route.request().method() !== "GET" || url.pathname !== "/api/batch-ops") return route.fallback();
      return fail
        ? route.fulfill({ status: 500, json: { error: "database unavailable" } })
        : route.fulfill({ json: [batchOperation({ skipped: 0, processed: 3000 })] });
    });
    await page.goto("/batch-ops");
    const alert = page.getByRole("alert").filter({ hasText: "Failed to load batch operations" });
    await expect(alert).toBeVisible();
    await expect(page.getByText("No data found")).toHaveCount(0);

    fail = false;
    await alert.getByRole("button", { name: "Try again" }).click();
    await expect(page.getByRole("gridcell", { name: "22222222" })).toBeVisible();
  });

  test("batch operations show loading, not empty, while the first read is in flight", async ({ page }) => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    await page.route("**/api/batch-ops*", async (route) => {
      const url = new URL(route.request().url());
      if (route.request().method() !== "GET" || url.pathname !== "/api/batch-ops") return route.fallback();
      await held;
      return route.fulfill({ json: [] });
    });
    await page.goto("/batch-ops");
    await expect(page.getByText("Loading batch operations…")).toBeVisible();
    await expect(page.getByText("No batch operations yet.")).toHaveCount(0);
    release();
    await expect(page.getByText("No batch operations yet.")).toBeVisible();
  });

  test("cron shows an error, not an empty list", async ({ page }) => {
    await failApi(page, "**/api/cron");
    await page.goto("/cron");
    await expect(
      page.getByRole("alert").filter({ hasText: "Failed to load cron schedules" })
    ).toBeVisible();
    await expect(page.getByText("No cron schedules found.")).toHaveCount(0);
  });
});

test.describe("Batch operation progress", () => {
  test("a fully skipped operation does not read as a success", async ({ page }) => {
    await page.route("**/api/batch-ops*", (route) => {
      const url = new URL(route.request().url());
      if (route.request().method() !== "GET" || url.pathname !== "/api/batch-ops") return route.fallback();
      return route.fulfill({
        json: [
          batchOperation(),
          batchOperation({
            id: "33333333-3333-4333-8333-333333333333",
            op_kind: "retry_from_dlq",
            state: "failed",
            total_matched: 1250,
            processed: 400,
            skipped: 12,
            last_error: "chunk 5 failed: canceling statement due to statement timeout",
          }),
        ],
      });
    });
    await page.goto("/batch-ops");
    const grid = page.getByRole("grid", { name: "Batch operations" });
    for (const header of ["Processed", "Skipped", "Errors"]) {
      await expect(grid.getByRole("columnheader", { name: header, exact: true })).toBeVisible();
    }
    const skippedRow = grid.getByRole("row").filter({ hasText: "22222222" });
    await expect(skippedRow.getByTestId("batch-op-changed-nothing")).toHaveText("No jobs changed");
    await expect(skippedRow.getByRole("gridcell").nth(4)).toHaveText("0");
    await expect(skippedRow.getByRole("gridcell").nth(5)).toHaveText("3,000");

    const failedRow = grid.getByRole("row").filter({ hasText: "33333333" });
    await expect(failedRow.getByTestId("batch-op-last-error")).toHaveText(
      "chunk 5 failed: canceling statement due to statement timeout"
    );
  });
});

test.describe("Not-found detail pages", () => {
  for (const { path, title, back } of [
    { path: "/jobs/999999999", title: "Job #999999999 not found", back: "Back to jobs" },
    { path: "/queues/no-such-queue", title: 'Queue "no-such-queue" not found', back: "Back to queues" },
    { path: "/dlq/999999999", title: "DLQ row #999999999 not found", back: "Back to DLQ" },
    { path: "/runtime/no-such-instance", title: "Runtime instance not found", back: "Back to runtime" },
  ]) {
    test(`${path} explains and links back`, async ({ page }) => {
      await page.goto(path);
      const notFound = page.getByTestId("resource-not-found");
      await expect(notFound.getByRole("heading", { name: title })).toBeVisible();
      await expect(page.getByText(/ApiError|Error:/)).toHaveCount(0);
      await expect(page.getByText(/retained data is stale/)).toHaveCount(0);
      await notFound.getByRole("link", { name: back }).click();
      await expect(page).toHaveURL(new RegExp(`${path.split("/")[1]}$`));
    });
  }
});

test.describe("Job retry safety", () => {
  test("a waiting job is restarted only after a confirmation naming the risk", async ({ page }) => {
    let retried = 0;
    await page.route("**/api/jobs/990001", (route) =>
      route.fulfill({
        json: job({ state: "waiting_external", callback_id: "11111111-1111-4111-8111-111111111111" }),
      })
    );
    await page.route("**/api/jobs/990001/retry", (route) => {
      retried++;
      return route.fulfill({ json: job() });
    });
    await page.goto("/jobs/990001");
    await expect(page.getByRole("button", { name: "Retry", exact: true })).toHaveCount(0);
    await page.getByRole("button", { name: "Restart…" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toContainText("may happen twice");
    await dialog.getByRole("button", { name: "Cancel" }).click();
    expect(retried).toBe(0);

    await page.getByRole("button", { name: "Restart…" }).click();
    await page.getByRole("alertdialog").getByRole("button", { name: "Restart job" }).click();
    await expect.poll(() => retried).toBe(1);
  });

  test("a retryable job says why it has no Retry button", async ({ page }) => {
    await page.route("**/api/jobs/990001", (route) =>
      route.fulfill({
        json: job({ state: "retryable", run_at: new Date(Date.now() + 5 * 60_000).toISOString() }),
      })
    );
    await page.goto("/jobs/990001");
    await expect(page.getByTestId("retryable-hint")).toContainText("Retries automatically in");
    await expect(page.getByRole("button", { name: "Retry", exact: true })).toHaveCount(0);
  });
});
