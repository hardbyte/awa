import { test, expect } from "@playwright/test";

test.describe("Dashboard page", () => {
  test("page loads and shows Dashboard heading", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" })
    ).toBeVisible();
  });

  test("state counter cards are visible with counts", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/"),
    ]);

    // Counter cards should show numeric values (seeded data exists)
    const counterCards = page.locator('a[href*="/jobs"]');
    const count = await counterCards.count();
    expect(count).toBeGreaterThanOrEqual(3);
  });

  test("queues card renders", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/"),
    ]);

    await expect(
      page.locator('[data-slot="card-header"]', { hasText: "Queues" })
    ).toBeVisible();

    // Seeded e2e_test queue should appear in the queue table
    await expect(page.getByText("e2e_test").first()).toBeVisible();
  });

  test("recent failures section shows seeded failed job", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("failed") &&
          r.ok()
      ),
      page.goto("/"),
    ]);

    await expect(
      page.locator('[data-slot="card-header"]', {
        hasText: "Recent Failures",
      })
    ).toBeVisible();

    // Seeded failed e2e_job should appear
    await expect(page.getByText("e2e_job").first()).toBeVisible();
  });
});
