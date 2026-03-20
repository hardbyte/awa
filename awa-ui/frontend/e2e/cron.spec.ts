import { test, expect } from "@playwright/test";

test.describe("Cron page", () => {
  test("navigate to /cron, heading visible", async ({ page }) => {
    await page.goto("/cron");
    await expect(
      page.getByRole("heading", { name: "Cron Schedules" })
    ).toBeVisible();
  });

  test("cron table renders or shows empty state", async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    // Either the cron table is present with data, or the empty state message shows
    const cronTable = page.getByRole("grid", { name: "Cron schedules" });
    const emptyMessage = page.getByText("No cron schedules found.");

    // One of these should be visible
    const tableVisible = await cronTable.isVisible().catch(() => false);
    const emptyVisible = await emptyMessage.isVisible().catch(() => false);
    expect(tableVisible || emptyVisible).toBeTruthy();

    if (tableVisible) {
      // Verify column headers if table has data
      for (const header of ["Name", "Cron", "Kind"]) {
        await expect(
          cronTable.getByRole("columnheader", { name: header, exact: true })
        ).toBeVisible();
      }
      // "Queue" needs exact matching to avoid partial match with "Last Enqueued"
      await expect(
        cronTable.getByRole("columnheader", { name: "Queue", exact: true })
      ).toBeVisible();
    }
  });

  test("trigger now button creates a job", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    const cronTable = page.getByRole("grid", { name: "Cron schedules" });
    const tableVisible = await cronTable.isVisible().catch(() => false);
    if (!tableVisible) {
      test.skip();
      return;
    }

    // Click the first "Trigger now" button and verify the API response
    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    await expect(triggerBtn).toBeVisible();

    const [triggerResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/api/cron/") && r.url().includes("/trigger") && r.ok()
      ),
      triggerBtn.click(),
    ]);

    // Verify the response contains a created job
    const job = await triggerResponse.json();
    expect(job.id).toBeTruthy();
    expect(job.state).toBe("available");
    expect(job.metadata.triggered_manually).toBe(true);
  });
});
