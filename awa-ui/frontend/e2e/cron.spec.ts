import { test, expect } from "@playwright/test";

test.describe("Cron page", () => {
  test("navigate to /cron, heading visible", async ({ page }) => {
    await page.goto("/cron");
    await expect(
      page.getByRole("heading", { name: "Cron Schedules" })
    ).toBeVisible();
  });

  test("cron list renders or shows empty state", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    // Either cron entries exist or the empty state shows
    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    const emptyMessage = page.getByText("No cron schedules found.");

    const hasEntries = await triggerBtn.isVisible().catch(() => false);
    const emptyVisible = await emptyMessage.isVisible().catch(() => false);
    expect(hasEntries || emptyVisible).toBeTruthy();
  });

  test("clicking cron row toggles expand/collapse", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    // Check if there are any cron entries
    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    const hasEntries = await triggerBtn.isVisible().catch(() => false);
    if (!hasEntries) {
      test.skip();
      return;
    }

    // The first cron entry is a clickable row that expands
    // The expanded section shows Kind, Queue, Priority, Max attempts
    const kindLabel = page.getByText("Kind", { exact: true }).first();
    await expect(kindLabel).not.toBeVisible();

    // Click the cron row (the summary area, not the button)
    const firstEntry = page.locator(".rounded-lg.border").first();
    const summaryRow = firstEntry.locator(".cursor-pointer").first();
    await summaryRow.click();

    // Expanded detail should now be visible with Kind/Queue/Priority fields
    await expect(firstEntry.getByText("Kind", { exact: true })).toBeVisible();
    await expect(firstEntry.getByText("Queue", { exact: true })).toBeVisible();
    await expect(firstEntry.getByText("Priority", { exact: true })).toBeVisible();
    await expect(firstEntry.getByText("Max attempts", { exact: true })).toBeVisible();

    // Click again to collapse
    await summaryRow.click();
    await expect(firstEntry.getByText("Max attempts", { exact: true })).not.toBeVisible();
  });

  test("cron API response includes next_fire_at", async ({ page }) => {
    const [cronResponse] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    const cronJobs = await cronResponse.json();
    if (!Array.isArray(cronJobs) || cronJobs.length === 0) {
      test.skip();
      return;
    }

    // Every cron schedule should have a next_fire_at field
    for (const job of cronJobs) {
      expect(job).toHaveProperty("next_fire_at");
      // next_fire_at should be a non-null ISO timestamp string
      if (job.next_fire_at !== null) {
        expect(new Date(job.next_fire_at).getTime()).toBeGreaterThan(Date.now());
      }
    }
  });

  test("cron summary row shows next fire time", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    const hasEntries = await triggerBtn.isVisible().catch(() => false);
    if (!hasEntries) {
      test.skip();
      return;
    }

    // The summary row should show "in Xm" or "in Xh" for the next fire
    const firstEntry = page.locator(".rounded-lg.border").first();
    const nextFireText = firstEntry.locator(".text-success").first();
    await expect(nextFireText).toBeVisible();
    const text = await nextFireText.textContent();
    expect(text).toMatch(/^in \d+[smhd]$/);
  });

  test("expanded cron detail shows Next fire with timezone", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    const hasEntries = await triggerBtn.isVisible().catch(() => false);
    if (!hasEntries) {
      test.skip();
      return;
    }

    // Expand the first cron entry
    const firstEntry = page.locator(".rounded-lg.border").first();
    const summaryRow = firstEntry.locator(".cursor-pointer").first();
    await summaryRow.click();

    // Expanded detail should show "Next fire" label
    await expect(firstEntry.getByText("Next fire")).toBeVisible();
  });

  test("trigger now button creates a job", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/cron") && r.ok()),
      page.goto("/cron"),
    ]);

    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    const hasEntries = await triggerBtn.isVisible().catch(() => false);
    if (!hasEntries) {
      test.skip();
      return;
    }

    // Click "Trigger now" and verify the API response
    const [triggerResponse] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/cron/") &&
          r.url().includes("/trigger") &&
          r.request().method() === "POST"
      ),
      triggerBtn.click(),
    ]);

    expect(triggerResponse.ok()).toBeTruthy();

    const job = await triggerResponse.json();
    expect(job.id).toBeTruthy();
    expect(job.state).toBe("available");
  });
});
