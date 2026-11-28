import { test, expect, type Page } from "@playwright/test";

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
