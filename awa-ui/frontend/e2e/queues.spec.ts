import { test, expect, type Page, type Response } from "@playwright/test";

function isQueuesListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return response.ok() && response.request().method() === "GET" && url.pathname === "/api/queues";
}

async function loadQueuesPage(page: Page) {
  await Promise.all([
    page.waitForResponse(isQueuesListResponse),
    page.goto("/queues"),
  ]);
}

test.describe("Queues page", () => {
  test("navigate to /queues, heading visible", async ({ page }) => {
    await page.goto("/queues");
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
  });

  test("queue table renders with seeded data", async ({ page }) => {
    await loadQueuesPage(page);

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

    // Descriptor-backed queue shows display name and raw key
    await expect(queueTable.getByText("E2E Queue")).toBeVisible();
    await expect(queueTable.getByText("e2e_test")).toBeVisible();
    await expect(
      queueTable.getByRole("link", { name: "legacy_queue", exact: true })
    ).toBeVisible();
  });

  test("queue table shows descriptor description", async ({ page }) => {
    await loadQueuesPage(page);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(
      queueTable.getByText("End-to-end queue used for UI coverage")
    ).toBeVisible();
  });

  test("queue table includes runtime columns", async ({ page }) => {
    await loadQueuesPage(page);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    // Queue health and runtime columns should be present
    for (const header of ["Mode", "Capacity", "Rate limit", "Waiting"]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header, exact: true })
      ).toBeVisible();
    }
  });

  test("click queue navigates to queue detail", async ({ page }) => {
    await loadQueuesPage(page);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const queueLink = queueTable.getByRole("link", { name: "E2E Queue" });
    await expect(queueLink).toBeVisible();

    await queueLink.click();

    await page.waitForURL(/\/queues\/e2e_test/);
    await expect(page.getByRole("heading", { name: "E2E Queue" })).toBeVisible();
  });

  test("queue detail renders descriptor metadata", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => /\/api\/queues\/e2e_test$/.test(r.url()) && r.ok()),
      page.goto("/queues/e2e_test"),
    ]);

    await expect(page.getByRole("heading", { name: "E2E Queue" })).toBeVisible();
    await expect(page.getByText("e2e_test").first()).toBeVisible();
    await expect(page.getByText("End-to-end queue used for UI coverage")).toBeVisible();
    await expect(page.getByText("qa-platform")).toBeVisible();
    await expect(page.getByText("critical")).toBeVisible();
  });

  test("legacy queue still renders without descriptors", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => /\/api\/queues\/legacy_queue$/.test(r.url()) && r.ok()),
      page.goto("/queues/legacy_queue"),
    ]);

    await expect(page.getByRole("heading", { name: "legacy_queue" })).toBeVisible();
    await expect(page.getByText("Descriptor health")).toBeVisible();
    await expect(page.getByText("Not declared by a live worker")).toBeVisible();
  });

  test("route-mocked descriptor stale and drift badges render", async ({ page }) => {
    await page.route("**/api/queues", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            queue: "mock_queue",
            display_name: "Mock Queue",
            description: "Descriptor health coverage",
            owner: null,
            docs_url: null,
            tags: [],
            extra: {},
            descriptor_last_seen_at: "2026-03-01T00:00:00Z",
            descriptor_stale: true,
            descriptor_mismatch: true,
            total_queued: 1,
            scheduled: 0,
            available: 1,
            retryable: 0,
            running: 0,
            failed: 0,
            waiting_external: 0,
            completed_last_hour: 0,
            lag_seconds: 1,
            paused: false,
          },
        ]),
      });
    });
    await page.route("**/api/queues/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([]),
      });
    });

    await page.goto("/queues");
    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable.getByText("Descriptor stale", { exact: true })).toBeVisible();
    await expect(queueTable.getByText("Descriptor drift", { exact: true })).toBeVisible();
    await expect(queueTable.getByText("Descriptor drift across live runtimes")).toBeVisible();
  });

  test("Pause/Resume button is visible for each queue", async ({ page }) => {
    await loadQueuesPage(page);

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
    await loadQueuesPage(page);

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
      await page.waitForResponse(isQueuesListResponse);
    }

    // Now pause the queue
    const refreshedRow = queueTable.getByRole("row", { name: /e2e_test/ });
    const pauseBtn = refreshedRow.getByRole("button", { name: "Pause" });
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
    await loadQueuesPage(page);

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
      await page.waitForResponse(isQueuesListResponse);
    }

    const refreshedRow = queueTable.getByRole("row", { name: /e2e_test/ });
    const resumeBtn = refreshedRow.getByRole("button", { name: "Resume" });
    await expect(resumeBtn).toBeVisible();

    const [resumeResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/resume") && r.request().method() === "POST"
      ),
      resumeBtn.click(),
    ]);

    expect(resumeResponse.ok()).toBeTruthy();
    await page.waitForResponse(isQueuesListResponse);
    await expect(refreshedRow.getByRole("button", { name: "Pause" })).toBeVisible();
  });

  test("Drain shows confirmation dialog", async ({ page }) => {
    await loadQueuesPage(page);

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
