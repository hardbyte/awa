import { test, expect } from "@playwright/test";

test.describe("Queues page", () => {
  test("navigate to /queues, heading visible", async ({ page }) => {
    await page.goto("/queues");
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
  });

  test("queue table renders with seeded data", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    // Desktop table (visible at default viewport)
    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    for (const header of ["Queue", "Available", "Running", "Failed", "Status"]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header })
      ).toBeVisible();
    }

    // Seeded e2e_test queue should be in the table
    await expect(queueTable.getByText("e2e_test")).toBeVisible();
  });

  test("click queue navigates to jobs with queue filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const queueLink = queueTable.getByRole("link", { name: "e2e_test" });
    await expect(queueLink).toBeVisible();

    await queueLink.click();

    // Queue detail redirects to /jobs with queue filter
    await page.waitForURL(/\/jobs/);
    expect(page.url()).toContain("queue");
  });
});
