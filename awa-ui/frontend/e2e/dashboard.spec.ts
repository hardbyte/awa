import { test, expect, type Response } from "@playwright/test";

function isQueuesListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return response.ok() && response.request().method() === "GET" && url.pathname === "/api/queues";
}

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
      page.waitForResponse(isQueuesListResponse),
      page.goto("/"),
    ]);

    await expect(
      page.locator('[data-slot="card-header"]', { hasText: "Queues" })
    ).toBeVisible();

    const queueTable = page.getByRole("grid", { name: "Queue summary" });
    for (const header of ["Queue", "Total queued", "Scheduled", "Retryable"]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header, exact: true })
      ).toBeVisible();
    }

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

  test("counter card click navigates to /jobs with state filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/"),
    ]);

    // Click the "available" counter card — it links to /jobs?state=available
    const availableCard = page.locator('a[href*="state=available"]').first();
    await expect(availableCard).toBeVisible();
    await availableCard.click();

    await page.waitForURL(/\/jobs/);
    expect(page.url()).toContain("state=available");
    await expect(page.getByRole("heading", { name: /Jobs/ })).toBeVisible();
  });

  test("failed counter card navigates to /jobs?state=failed", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/"),
    ]);

    const failedCard = page.locator('a[href*="state=failed"]').first();
    await expect(failedCard).toBeVisible();
    await failedCard.click();

    await page.waitForURL(/\/jobs/);
    expect(page.url()).toContain("state=failed");
  });

  test("queue row click navigates to /jobs with queue filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(isQueuesListResponse),
      page.goto("/"),
    ]);

    // The dashboard queue table has clickable rows with onAction
    const queueTable = page.getByRole("grid", { name: "Queue summary" });
    await expect(queueTable).toBeVisible();

    // Click the e2e_test row (the row itself, not the link inside)
    const e2eRow = queueTable.getByRole("row", { name: /e2e_test/ });
    await expect(e2eRow).toBeVisible();
    await e2eRow.click();

    await page.waitForURL(/\/jobs/);
    expect(page.url()).toContain("queue");
    expect(page.url()).toContain("e2e_test");
  });

  test("runtime card renders with stat counters", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/api/runtime") && r.ok()
      ),
      page.goto("/"),
    ]);

    // Runtime card header should be visible
    await expect(
      page.locator('[data-slot="card-header"]', { hasText: "Runtime" })
    ).toBeVisible();

    // Stat counter labels should be present
    for (const label of ["Live", "Healthy", "Leader", "Stale"]) {
      await expect(page.getByText(label, { exact: true }).first()).toBeVisible();
    }
  });

  test("runtime card shows empty state when no workers running", async ({
    page,
  }) => {
    await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/api/runtime") && r.ok()
      ),
      page.goto("/"),
    ]);

    // When no workers are running, expect either the table or the empty state text
    const runtimeCard = page.locator('[data-slot="card-header"]', {
      hasText: "Runtime",
    });
    await expect(runtimeCard).toBeVisible();

    // Either instances are shown (table) or the empty/loading message appears
    const emptyText = page.getByText(
      "No runtime snapshots yet"
    );
    const table = page.getByRole("grid", { name: "Runtime instances" });
    const hasTable = await table.isVisible().catch(() => false);
    const hasEmptyText = await emptyText.isVisible().catch(() => false);
    // One or the other should be present (if a worker happens to be running, table shows)
    expect(hasTable || hasEmptyText).toBeTruthy();
  });

  test("recent failure row click navigates to job detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("failed") &&
          r.ok()
      ),
      page.goto("/"),
    ]);

    const failuresTable = page.getByRole("grid", { name: "Recent failures" });
    await expect(failuresTable).toBeVisible();

    // Click the first failure row
    const firstRow = failuresTable.getByRole("row").nth(1);
    await expect(firstRow).toBeVisible();
    await firstRow.click();

    await page.waitForURL(/\/jobs\/\d+/);
    await expect(
      page.getByRole("heading", { name: /Job #\d+/ })
    ).toBeVisible();
  });
});
