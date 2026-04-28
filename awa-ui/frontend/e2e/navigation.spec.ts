import { test, expect } from "@playwright/test";

test.describe("Shell navigation", () => {
  test("all nav links work: Dashboard, Jobs, Kinds, Queues, Runtime, Cron", async ({
    page,
  }) => {
    await page.goto("/");

    // Scope link lookups to the sidebar so we don't match content links.
    const nav = page.locator('[data-slot="sidebar-section"]').first();

    // Click Jobs nav link
    await nav.getByRole("link", { name: "Jobs" }).click();
    await expect(page).toHaveURL(/\/jobs/);
    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();

    // Click Kinds nav link
    await nav.getByRole("link", { name: "Kinds" }).click();
    await expect(page).toHaveURL(/\/kinds/);
    await expect(page.getByRole("heading", { name: "Job Kinds" })).toBeVisible();

    // Click Queues nav link
    await nav.getByRole("link", { name: "Queues" }).click();
    await expect(page).toHaveURL(/\/queues/);
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();

    // Click Runtime nav link
    await nav.getByRole("link", { name: "Runtime" }).click();
    await expect(page).toHaveURL(/\/runtime/);
    await expect(page.getByRole("heading", { name: "Runtime" })).toBeVisible();

    // Click Cron nav link (use exact match to avoid matching queue names like "cron_...")
    await nav.getByRole("link", { name: "Cron", exact: true }).click();
    await expect(page).toHaveURL(/\/cron/);
    await expect(
      page.getByRole("heading", { name: "Cron Schedules" })
    ).toBeVisible();

    // Click Dashboard nav link
    await nav.getByRole("link", { name: "Dashboard" }).click();
    await expect(page).toHaveURL(/\/$/);
    await expect(
      page.getByRole("heading", { name: "Dashboard" })
    ).toBeVisible();
  });

  test("logo links to dashboard", async ({ page }) => {
    await page.goto("/jobs");

    // The logo is wrapped in an <a> with href="/". There are two (desktop + mobile),
    // so use .first() to target the desktop navbar version.
    const logoLink = page.locator('a[href="/"]').filter({ hasText: "AWA" }).first();
    await expect(logoLink).toBeVisible();

    await logoLink.click();
    await expect(page).toHaveURL(/\/$/);
    await expect(
      page.getByRole("heading", { name: "Dashboard" })
    ).toBeVisible();
  });

  test("current page is highlighted in nav", async ({ page }) => {
    // Go to Jobs page
    await page.goto("/jobs");

    // The Jobs nav item should be marked as current
    const jobsLink = page.getByRole("link", { name: "Jobs" }).first();
    await expect(jobsLink).toHaveAttribute("aria-current", "page");

    // Dashboard should NOT be marked as current
    const dashboardLink = page
      .getByRole("link", { name: "Dashboard" })
      .first();
    await expect(dashboardLink).not.toHaveAttribute("aria-current", "page");

    // Navigate to Queues and verify
    await page.getByRole("link", { name: "Queues" }).click();
    const queuesLink = page.getByRole("link", { name: "Queues" }).first();
    await expect(queuesLink).toHaveAttribute("aria-current", "page");
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
