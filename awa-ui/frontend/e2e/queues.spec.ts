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

    // Columns were consolidated: Scheduled/Available fold into Queued;
    // Mode/RateLimit fold into Capacity; Waiting is a subline under Queue.
    for (const header of [
      "Queue",
      "Queued",
      "Running",
      "Retry",
      "Failed",
      "Rate/hr",
      "Capacity",
      "Status",
      "Actions",
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

  test("queue table surfaces consolidated capacity and actions columns", async ({ page }) => {
    await loadQueuesPage(page);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    // Mode / rate limit / waiting folded into the Capacity and Queue cells.
    for (const header of ["Capacity", "Status", "Actions"]) {
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

  async function openActionsMenu(page: Page, queueName: string) {
    const queueTable = page.getByRole("grid", { name: "Queues" });
    const row = queueTable.getByRole("row", { name: new RegExp(queueName) });
    await expect(row).toBeVisible();
    await row
      .getByRole("button", { name: new RegExp(`Actions for ${queueName}`) })
      .click();
  }

  test("Pause/Resume and Drain appear in the Actions menu", async ({ page }) => {
    await loadQueuesPage(page);
    await openActionsMenu(page, "e2e_test");

    // Menu renders either "Pause queue" or "Resume queue" depending on state,
    // plus Drain. We just need one of pause/resume visible.
    const pauseItem = page.getByRole("menuitem", { name: "Pause queue" });
    const resumeItem = page.getByRole("menuitem", { name: "Resume queue" });
    const pauseVisible = await pauseItem.isVisible().catch(() => false);
    const resumeVisible = await resumeItem.isVisible().catch(() => false);
    expect(pauseVisible || resumeVisible).toBeTruthy();

    await expect(page.getByRole("menuitem", { name: /^Drain queue/ })).toBeVisible();
  });

  test("Pause executes mutation from the Actions menu", async ({ page }) => {
    await loadQueuesPage(page);

    // Ensure the queue is active so "Pause" is the available verb.
    await openActionsMenu(page, "e2e_test");
    const resumeItem = page.getByRole("menuitem", { name: "Resume queue" });
    const isCurrentlyPaused = await resumeItem.isVisible().catch(() => false);
    if (isCurrentlyPaused) {
      await Promise.all([
        page.waitForResponse(
          (r) => r.url().includes("/resume") && r.request().method() === "POST"
        ),
        resumeItem.click(),
      ]);
      await page.waitForResponse(isQueuesListResponse);
      await openActionsMenu(page, "e2e_test");
    } else {
      await page.keyboard.press("Escape");
      await openActionsMenu(page, "e2e_test");
    }

    const pauseItem = page.getByRole("menuitem", { name: "Pause queue" });
    await expect(pauseItem).toBeVisible();
    const [pauseResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/pause") && r.request().method() === "POST"
      ),
      pauseItem.click(),
    ]);
    expect(pauseResponse.ok()).toBeTruthy();
  });

  test("Resume executes mutation from the Actions menu", async ({ page }) => {
    await loadQueuesPage(page);

    await openActionsMenu(page, "e2e_test");
    const pauseItem = page.getByRole("menuitem", { name: "Pause queue" });
    const isCurrentlyActive = await pauseItem.isVisible().catch(() => false);
    if (isCurrentlyActive) {
      await Promise.all([
        page.waitForResponse(
          (r) => r.url().includes("/pause") && r.request().method() === "POST"
        ),
        pauseItem.click(),
      ]);
      await page.waitForResponse(isQueuesListResponse);
      await openActionsMenu(page, "e2e_test");
    } else {
      await page.keyboard.press("Escape");
      await openActionsMenu(page, "e2e_test");
    }

    const resumeItem = page.getByRole("menuitem", { name: "Resume queue" });
    await expect(resumeItem).toBeVisible();
    const [resumeResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/resume") && r.request().method() === "POST"
      ),
      resumeItem.click(),
    ]);
    expect(resumeResponse.ok()).toBeTruthy();
    await page.waitForResponse(isQueuesListResponse);
  });

  test("Drain shows confirmation dialog from the Actions menu", async ({ page }) => {
    await loadQueuesPage(page);
    await openActionsMenu(page, "e2e_test");

    await page.getByRole("menuitem", { name: /^Drain queue/ }).click();

    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toBeVisible();
    await expect(dialog.getByRole("heading", { name: /Drain queue/ })).toBeVisible();
    await expect(dialog.getByText("Running jobs will not be affected")).toBeVisible();

    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).not.toBeVisible();
  });
});
