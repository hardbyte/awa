import { test, expect } from "@playwright/test";

test.describe("Queues page", () => {
  test("navigate to /queues, heading visible", async ({ page }) => {
    await page.goto("/queues");
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
  });

  test("queue table or empty state renders", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    // Either the table renders or the empty state shows
    const queueTable = page.getByRole("grid", { name: "Queues" });
    const emptyMsg = page.getByText("No queues found.");
    const tableVisible = await queueTable.isVisible().catch(() => false);
    const emptyVisible = await emptyMsg.isVisible().catch(() => false);
    expect(tableVisible || emptyVisible).toBeTruthy();

    if (tableVisible) {
      for (const header of ["Queue", "Available", "Running", "Failed", "Status"]) {
        await expect(
          queueTable.getByRole("columnheader", { name: header })
        ).toBeVisible();
      }
    }
  });

  test("click a queue name navigates to jobs with queue filter", async ({
    page,
  }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const tableVisible = await queueTable.isVisible().catch(() => false);
    if (!tableVisible) {
      test.skip(true, "No queue data in database");
      return;
    }

    const queueLink = queueTable.getByRole("link").first();
    const linkCount = await queueTable.getByRole("link").count();
    if (linkCount === 0) {
      test.skip(true, "No queue links found");
      return;
    }

    await queueLink.click();

    // Queue detail redirects to /jobs?q=queue:<name>
    await page.waitForURL(/\/jobs/);
    expect(page.url()).toContain("queue%3A");
  });
});
