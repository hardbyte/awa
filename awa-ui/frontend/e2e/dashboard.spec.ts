import { test, expect } from "@playwright/test";

test.describe("Dashboard page", () => {
  test("page loads and shows Dashboard heading", async ({ page }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Dashboard" })
    ).toBeVisible();
  });

  test("state counter cards are visible", async ({ page }) => {
    // Wait for the stats API response before asserting
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/"),
    ]);

    // The dashboard renders counter cards that link to /jobs with state badges.
    // Each card has a StateBadge with the state name. Verify at least some cards exist.
    // The counter cards are links to /jobs with numeric values.
    const counterCards = page.locator('a[href*="/jobs"]').filter({ hasText: /\d+/ });
    const count = await counterCards.count();
    expect(count).toBeGreaterThanOrEqual(4);
  });

  test("queue summary table renders with column headers", async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queue summary" });
    await expect(queueTable).toBeVisible();

    // Check column headers
    for (const header of [
      "Queue",
      "Available",
      "Running",
      "Failed",
      "Completed/hr",
    ]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header })
      ).toBeVisible();
    }

    // Table should have at least one data row (test data exists)
    const rows = queueTable.getByRole("row");
    // Header row + at least one data row
    await expect(rows).not.toHaveCount(1);
  });

  test("recent failures section is present", async ({ page }) => {
    await page.goto("/");

    // Wait for the failed jobs API call to complete
    await page.waitForResponse(
      (r) => r.url().includes("/api/jobs") && r.url().includes("failed") && r.ok()
    );

    // The "Recent Failures" card header should be visible
    await expect(page.getByText("Recent Failures")).toBeVisible();
  });
});
