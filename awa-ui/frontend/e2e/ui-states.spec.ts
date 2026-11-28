import { test, expect, type Page } from "@playwright/test";

/** A failed read shows its error after the query client's three retries. */
const READ_RETRY_TIMEOUT = 15_000;

/** Fail every matching GET with a 500, as an unreachable database would. */
async function failApi(page: Page, pattern: string) {
  await page.route(pattern, (route) =>
    route.request().method() === "GET"
      ? route.fulfill({ status: 500, json: { error: "database unavailable" } })
      : route.fallback()
  );
}

function batchOperation(overrides: Record<string, unknown> = {}) {
  const now = new Date().toISOString();
  return {
    id: "22222222-2222-4222-8222-222222222222",
    op_kind: "set_priority",
    filter: { queue: "e2e_test" },
    spec: { priority: 1 },
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

test.describe("Failed reads are not shown as empty", () => {
  test("dashboard tiles show no count when stats fail", async ({ page }) => {
    await failApi(page, "**/api/stats");
    await page.goto("/");
    await expect(page.getByTestId("stats-error")).toContainText("Failed to load job counts", {
      timeout: READ_RETRY_TIMEOUT,
    });
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
    await expect(alert).toBeVisible({ timeout: READ_RETRY_TIMEOUT });
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
    ).toBeVisible({ timeout: READ_RETRY_TIMEOUT });
    await expect(page.getByText("No cron schedules found.")).toHaveCount(0);
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
      await notFound.getByRole("link", { name: back }).click();
      await expect(page).toHaveURL(new RegExp(`${path.split("/")[1]}$`));
    });
  }
});
