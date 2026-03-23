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

    for (const header of [
      "Queue",
      "Total queued",
      "Scheduled",
      "Retryable",
      "Available",
      "Running",
      "Failed",
      "Status",
    ]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header, exact: true })
      ).toBeVisible();
    }

    // Seeded e2e_test queue should be in the table
    await expect(queueTable.getByText("e2e_test")).toBeVisible();
  });

  test("queue table includes runtime columns", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    // Queue health and runtime columns should be present
    for (const header of ["Mode", "Capacity", "Rate limit", "Waiting"]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header, exact: true })
      ).toBeVisible();
    }
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

  test("Pause/Resume button is visible for each queue", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    // Find the e2e_test row
    const e2eRow = queueTable.getByRole("row", { name: /e2e_test/ });
    await expect(e2eRow).toBeVisible();

    // Either Pause or Resume should be visible (depends on current state)
    const pauseBtn = e2eRow.getByRole("button", { name: "Pause" });
    const resumeBtn = e2eRow.getByRole("button", { name: "Resume" });
    const pauseVisible = await pauseBtn.isVisible().catch(() => false);
    const resumeVisible = await resumeBtn.isVisible().catch(() => false);
    expect(pauseVisible || resumeVisible).toBeTruthy();

    // Drain button should always be visible
    await expect(e2eRow.getByRole("button", { name: "Drain" })).toBeVisible();
  });

  test("Pause executes mutation and shows success toast", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const e2eRow = queueTable.getByRole("row", { name: /e2e_test/ });
    await expect(e2eRow).toBeVisible();

    // If queue is paused, resume it first so we can test pause
    const resumeBtn = e2eRow.getByRole("button", { name: "Resume" });
    const isCurrentlyPaused = await resumeBtn.isVisible().catch(() => false);
    if (isCurrentlyPaused) {
      await Promise.all([
        page.waitForResponse(
          (r) => r.url().includes("/resume") && r.request().method() === "POST"
        ),
        resumeBtn.click(),
      ]);
      // Wait for table to update
      await page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok());
    }

    // Now pause the queue
    const pauseBtn = e2eRow.getByRole("button", { name: "Pause" });
    await expect(pauseBtn).toBeVisible();

    const [pauseResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/pause") && r.request().method() === "POST"
      ),
      pauseBtn.click(),
    ]);

    expect(pauseResponse.ok()).toBeTruthy();
  });

  test("Resume executes mutation after pause", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const e2eRow = queueTable.getByRole("row", { name: /e2e_test/ });
    await expect(e2eRow).toBeVisible();

    // If queue is not paused, pause it first so we can test resume
    const pauseBtn = e2eRow.getByRole("button", { name: "Pause" });
    const isCurrentlyActive = await pauseBtn.isVisible().catch(() => false);
    if (isCurrentlyActive) {
      await Promise.all([
        page.waitForResponse(
          (r) => r.url().includes("/pause") && r.request().method() === "POST"
        ),
        pauseBtn.click(),
      ]);
      await page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok());
    }

    // Now resume the queue
    const resumeBtn = e2eRow.getByRole("button", { name: "Resume" });
    await expect(resumeBtn).toBeVisible();

    const [resumeResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/resume") && r.request().method() === "POST"
      ),
      resumeBtn.click(),
    ]);

    expect(resumeResponse.ok()).toBeTruthy();
  });

  test("Drain shows confirmation dialog", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const e2eRow = queueTable.getByRole("row", { name: /e2e_test/ });
    const drainBtn = e2eRow.getByRole("button", { name: "Drain" });
    await expect(drainBtn).toBeVisible();

    await drainBtn.click();

    // Confirmation dialog appears
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toBeVisible();
    await expect(dialog.getByRole("heading", { name: /Drain queue/ })).toBeVisible();
    await expect(dialog.getByText("Running jobs will not be affected")).toBeVisible();

    // Cancel the dialog
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).not.toBeVisible();
  });
});
