import { test, expect } from "@playwright/test";

test.describe("Dashboard page", () => {
  test("page loads and shows Dashboard heading", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" })
    ).toBeVisible();
  });

  test("state counter cards are visible", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/"),
    ]);

    // Counter cards link to /jobs — should always render even with 0 counts
    const counterCards = page.locator('a[href*="/jobs"]');
    const count = await counterCards.count();
    expect(count).toBeGreaterThanOrEqual(3); // available, running, failed + completed/hr
  });

  test("queues section present", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/"),
    ]);

    // The Queues card header should always render
    await expect(
      page.locator('[data-slot="card-header"]', { hasText: "Queues" })
    ).toBeVisible();
  });

  test("recent failures section present", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("failed") &&
          r.ok()
      ),
      page.goto("/"),
    ]);

    // The Recent Failures card header should always render
    await expect(
      page.locator('[data-slot="card-header"]', {
        hasText: "Recent Failures",
      })
    ).toBeVisible();
  });
});
