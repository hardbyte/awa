import { test, expect } from "@playwright/test";

const NAV: { testId: string; label: string; url: RegExp; heading: string }[] = [
  { testId: "nav-jobs", label: "Jobs", url: /\/jobs$/, heading: "Jobs" },
  { testId: "nav-dlq", label: "Dead letters", url: /\/dlq$/, heading: "Dead letters" },
  { testId: "nav-queues", label: "Queues", url: /\/queues$/, heading: "Queues" },
  { testId: "nav-kinds", label: "Job kinds", url: /\/kinds$/, heading: "Job kinds" },
  { testId: "nav-cron", label: "Schedules", url: /\/cron$/, heading: "Schedules" },
  { testId: "nav-runtime", label: "Fleet", url: /\/runtime$/, heading: "Fleet" },
  { testId: "nav-batch-ops", label: "Bulk operations", url: /\/batch-ops$/, heading: "Bulk operations" },
  { testId: "nav-overview", label: "Overview", url: /\/$/, heading: "Overview" },
];

test.describe("Shell navigation", () => {
  test("the sidebar groups pages under Investigate, Configure and Operate", async ({ page }) => {
    await page.goto("/");
    const nav = page.getByRole("navigation", { name: "Primary" });
    for (const group of ["Investigate", "Configure", "Operate"]) {
      await expect(nav.getByText(group, { exact: true })).toBeVisible();
    }
  });

  test("all nav links work", async ({ page }) => {
    await page.goto("/");
    const nav = page.getByRole("navigation", { name: "Primary" });
    for (const item of NAV) {
      const link = nav.getByTestId(item.testId);
      await expect(link).toContainText(item.label);
      await link.click();
      await expect(page).toHaveURL(item.url);
      await expect(page.getByRole("heading", { level: 1, name: item.heading, exact: true })).toBeVisible();
    }
  });

  test("logo links to the overview", async ({ page }) => {
    await page.goto("/jobs");

    const logoLink = page.locator('a[href="/"]').filter({ hasText: "AWA" }).first();
    await expect(logoLink).toBeVisible();

    await logoLink.click();
    await expect(page).toHaveURL(/\/$/);
    await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
  });

  test("current page is highlighted in nav", async ({ page }) => {
    await page.goto("/jobs");
    const nav = page.getByRole("navigation", { name: "Primary" });

    await expect(nav.getByTestId("nav-jobs")).toHaveAttribute("aria-current", "page");
    await expect(nav.getByTestId("nav-overview")).not.toHaveAttribute("aria-current", "page");

    await nav.getByTestId("nav-queues").click();
    await expect(nav.getByTestId("nav-queues")).toHaveAttribute("aria-current", "page");
  });

  test("Dead letters badge shows the depth and hides on error", async ({ page }) => {
    let fail = false;
    await page.route("**/api/dlq/depth", (route) =>
      fail
        ? route.fulfill({ status: 500, json: { error: "boom" } })
        : route.fulfill({ json: { total: 1234, by_queue: [{ queue: "e2e_dlq", count: 1234 }] } })
    );
    await page.goto("/jobs");
    const badge = page.getByTestId("nav-dlq").locator('[data-slot="sidebar-badge"]');
    await expect(badge).toHaveText((1234).toLocaleString("en-US"));

    fail = true;
    await page.reload();
    await expect(page.getByTestId("nav-dlq")).toBeVisible();
    await expect(page.getByTestId("nav-dlq").locator('[data-slot="sidebar-badge"]')).toHaveCount(0);
  });

  test("Bulk operations badge counts only active operations", async ({ page }) => {
    const op = (id: string, state: string) => ({
      id, op_kind: "retry", filter: { kind: "e2e_job" }, spec: {}, state, submitted_by: "ui",
      submitted_at: new Date().toISOString(), started_at: null, finalized_at: null, cursor: null,
      total_matched: 10, processed: 0, skipped: 0, errored: 0, last_error: null,
      runner_instance: null, retention_until: null, updated_at: new Date().toISOString(),
    });
    await page.route("**/api/batch-ops?*", (route) =>
      route.fulfill({
        json: [
          op("11111111-1111-4111-8111-111111111111", "running"),
          op("22222222-2222-4222-8222-222222222222", "pending"),
          op("33333333-3333-4333-8333-333333333333", "completed"),
        ],
      })
    );
    await page.goto("/jobs");
    await expect(page.getByTestId("nav-batch-ops").locator('[data-slot="sidebar-badge"]')).toHaveText("2");
  });
});

test.describe("Theme toggle", () => {
  test("cycles through system, light, dark themes", async ({ page }) => {
    await page.goto("/");

    // The theme toggle button should be visible with initial "system" label
    const themeBtn = page.getByRole("button", { name: /Theme:/ }).first();
    await expect(themeBtn).toBeVisible();

    // Initial state should be "system" (default)
    await expect(themeBtn).toHaveAttribute("aria-label", "Theme: system");

    // Click to cycle to "light"
    await themeBtn.click();
    await expect(themeBtn).toHaveAttribute("aria-label", "Theme: light");

    // Click to cycle to "dark"
    await themeBtn.click();
    await expect(themeBtn).toHaveAttribute("aria-label", "Theme: dark");

    // Verify dark class is applied to the document
    const isDark = await page.evaluate(() =>
      document.documentElement.classList.contains("dark")
    );
    expect(isDark).toBeTruthy();

    // Click to cycle back to "system"
    await themeBtn.click();
    await expect(themeBtn).toHaveAttribute("aria-label", "Theme: system");
  });

  test("theme persists across page reloads", async ({ page }) => {
    await page.goto("/");

    const themeBtn = page.getByRole("button", { name: /Theme:/ }).first();

    // Set to light
    await themeBtn.click();
    await expect(themeBtn).toHaveAttribute("aria-label", "Theme: light");

    // Verify localStorage was set
    const storedTheme = await page.evaluate(() =>
      localStorage.getItem("theme")
    );
    expect(storedTheme).toBe("light");

    // Reload and verify persistence
    await page.reload();
    const themeBtnAfterReload = page.getByRole("button", { name: /Theme:/ }).first();
    await expect(themeBtnAfterReload).toHaveAttribute("aria-label", "Theme: light");

    // Clean up - reset to system
    await themeBtnAfterReload.click(); // light -> dark
    await themeBtnAfterReload.click(); // dark -> system
  });
});

test.describe("Refresh button", () => {
  test("clicking refresh button triggers data refetch", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/"),
    ]);

    const refreshBtn = page.getByRole("button", { name: "Refresh data" }).first();
    await expect(refreshBtn).toBeVisible();

    // Click refresh and verify that API calls are made
    const [statsRefetch] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      refreshBtn.click(),
    ]);

    expect(statsRefetch.ok()).toBeTruthy();
  });
});
