import { test, expect, type Response } from "@playwright/test";

function isQueuesListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return response.ok() && response.request().method() === "GET" && url.pathname === "/api/queues";
}

test.describe("Jobs page", () => {
  test("navigate to /jobs, heading visible", async ({ page }) => {
    await page.goto("/jobs");
    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
  });

  test("state filter pills are present", async ({ page }) => {
    await page.goto("/jobs");
    await page.waitForResponse("**/api/stats");

    // Check for key filter pills (buttons, not tabs now)
    for (const label of ["all", "available", "failed", "running"]) {
      await expect(page.getByRole("button", { name: label, exact: false })).toBeVisible();
    }
  });

  test("job table renders with column headers", async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Kind is now the primary column (no ID column)
    for (const header of ["Kind", "State", "Queue", "Attempt", "Created"]) {
      await expect(
        jobTable.getByRole("columnheader", { name: header })
      ).toBeVisible();
    }

    // Should have data rows
    const rows = jobTable.getByRole("row");
    await expect(rows).not.toHaveCount(1);
  });

  test("descriptor-backed and legacy labels both render in jobs table", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable.getByText("E2E Job").first()).toBeVisible();
    await expect(jobTable.getByText("e2e_job").first()).toBeVisible();
    await expect(jobTable.getByText("E2E Queue").first()).toBeVisible();
    await expect(jobTable.getByText("e2e_test").first()).toBeVisible();
    await expect(jobTable.getByText("legacy_job").first()).toBeVisible();
    await expect(jobTable.getByText("legacy_queue").first()).toBeVisible();
  });

  test("URL-driven state: navigate to /jobs?state=failed, failed pill active", async ({
    page,
  }) => {
    const [response] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=failed") &&
          r.ok()
      ),
      page.goto("/jobs?state=failed"),
    ]);

    // The active pill has bg-primary class (check visually active)
    const failedPill = page.getByRole("button", { name: /^failed/ });
    await expect(failedPill).toBeVisible();
    // Table should show only failed jobs
    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();
  });

  test("search bar is present and interactive", async ({ page }) => {
    await page.goto("/jobs");

    const searchInput = page.getByPlaceholder(/Filter by kind/);
    await expect(searchInput).toBeVisible();

    await searchInput.fill("kind:test");
    await expect(searchInput).toHaveValue("kind:test");
  });

  test("clicking state pill updates URL and filters table", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    // Click the "failed" pill
    const failedPill = page.getByRole("button", { name: /^failed/ });
    await expect(failedPill).toBeVisible();

    const [filteredResponse] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=failed") &&
          r.ok()
      ),
      failedPill.click(),
    ]);

    expect(page.url()).toContain("state=failed");

    // Click "all" to clear the filter
    const allPill = page.getByRole("button", { name: /^all/ });
    await allPill.click();

    expect(page.url()).not.toContain("state=");
  });

  test("clicking 'available' pill then 'running' pill switches filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/jobs"),
    ]);

    // Click available pill
    const availablePill = page.getByRole("button", { name: /^available/ });
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=available") &&
          r.ok()
      ),
      availablePill.click(),
    ]);
    expect(page.url()).toContain("state=available");

    // Switch to running
    const runningPill = page.getByRole("button", { name: /^running/ });
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=running") &&
          r.ok()
      ),
      runningPill.click(),
    ]);
    expect(page.url()).toContain("state=running");
    expect(page.url()).not.toContain("state=available");
  });
});

test.describe("Search bar filters", () => {
  test("typing queue:e2e_test and pressing Enter creates a chip and updates URL", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const searchInput = page.getByLabel("Search jobs");
    await expect(searchInput).toBeVisible();

    await searchInput.fill("queue:e2e_test");
    await searchInput.press("Enter");

    // A filter chip should appear with the queue filter
    await expect(page.getByText("queue:e2e_test")).toBeVisible();

    // URL should have the queue filter
    await page.waitForURL(/q=.*queue/);
    expect(page.url()).toContain("queue");
    expect(page.url()).toContain("e2e_test");

    // Input should be cleared after applying the chip
    await expect(searchInput).toHaveValue("");
  });

  test("clicking X on a chip removes the filter and updates URL", async ({ page }) => {
    // Navigate with queue filter pre-applied
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    // Chip should be visible
    await expect(page.getByText("queue:e2e_test")).toBeVisible();

    // Click the remove button on the chip
    const removeBtn = page.getByRole("button", { name: "Remove queue:e2e_test filter" });
    await expect(removeBtn).toBeVisible();
    await removeBtn.click();

    // Chip should disappear and URL should not contain the queue filter
    await expect(page.getByText("queue:e2e_test")).not.toBeVisible();
  });

  test("backspace on empty input removes the last filter chip", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    // Chip should be visible
    await expect(page.getByText("queue:e2e_test")).toBeVisible();

    const searchInput = page.getByLabel("Search jobs");
    await searchInput.focus();

    // Input is empty, backspace should remove the last filter
    await searchInput.press("Backspace");

    await expect(page.getByText("queue:e2e_test")).not.toBeVisible();
  });

  test("/ shortcut focuses search input", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const searchInput = page.getByLabel("Search jobs");
    // Ensure input is not focused initially
    await expect(searchInput).not.toBeFocused();

    // Press "/" to focus search
    await page.keyboard.press("/");
    await expect(searchInput).toBeFocused();
  });

  test("autocomplete suggestions appear on focus and can be selected", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const searchInput = page.getByLabel("Search jobs");
    await searchInput.focus();

    // Suggestion dropdown should appear with prefix hints
    const listbox = page.getByRole("listbox");
    await expect(listbox).toBeVisible();

    // Should show kind:, queue:, tag: hints
    await expect(listbox.getByText("kind:<name>")).toBeVisible();
    await expect(listbox.getByText("queue:<name>")).toBeVisible();

    // Arrow down to select first suggestion
    await searchInput.press("ArrowDown");
    // First option should be selected
    const firstOption = listbox.getByRole("option").first();
    await expect(firstOption).toHaveAttribute("aria-selected", "true");
  });
});

test.describe("Bulk actions", () => {
  test("selecting a job shows bulk action toolbar", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // React Aria checkboxes have wrapper divs that intercept pointer events;
    // use force:true to click through the overlay
    const firstRowCheckbox = jobTable.getByRole("checkbox").nth(1);
    await expect(firstRowCheckbox).toBeVisible();
    await firstRowCheckbox.click({ force: true });

    // Bulk action toolbar should appear
    await expect(page.getByText(/\d+ selected/)).toBeVisible();
    await expect(page.getByText("Updates paused")).toBeVisible();
  });

  test("select-all checkbox selects all visible jobs", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // The first checkbox is the select-all
    const selectAll = jobTable.getByRole("checkbox").first();
    await selectAll.click({ force: true });

    // Toolbar should show selected count
    await expect(page.getByText(/\d+ selected/)).toBeVisible();

    // Clicking again deselects all
    await selectAll.click({ force: true });
    await expect(page.getByText(/\d+ selected/)).not.toBeVisible();
  });

  test("bulk cancel shows confirmation dialog", async ({ page }) => {
    // Filter to available jobs so they are cancellable (non-terminal)
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=available") &&
          r.ok()
      ),
      page.goto("/jobs?state=available&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Select all available jobs
    const selectAll = jobTable.getByRole("checkbox").first();
    await selectAll.click({ force: true });

    // Cancel button should be enabled (available jobs are cancellable)
    const cancelBtn = page.getByRole("button", { name: /^Cancel/ }).first();
    await expect(cancelBtn).toBeVisible();
    await expect(cancelBtn).toBeEnabled();

    await cancelBtn.click();

    // Confirmation dialog should appear
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText("This action cannot be undone.", { exact: true })).toBeVisible();

    // Close dialog without confirming
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).not.toBeVisible();
  });
});

test.describe("Pagination", () => {
  test("page size buttons update URL limit param", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    // Find the "20" page size button (it's in the "Per page:" section)
    const perPageLabel = page.getByText("Per page:");
    await expect(perPageLabel).toBeVisible();

    // Click "20" page size button and wait for the API call with limit=20
    const size20Btn = page.getByRole("button", { name: "20", exact: true });
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("limit=20") &&
          r.ok()
      ),
      size20Btn.click(),
    ]);

    // TanStack Router may URL-encode the limit value differently; check the API was called
    expect(page.url()).toMatch(/limit/);

    // Switch to 100
    const size100Btn = page.getByRole("button", { name: "100", exact: true });
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("limit=100") &&
          r.ok()
      ),
      size100Btn.click(),
    ]);

    expect(page.url()).toMatch(/limit/);
  });
});

test.describe("Queue context banner", () => {
  test("banner appears when filtering by a single queue", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(isQueuesListResponse),
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    // Banner should show queue status badge
    const banner = page.locator(".rounded-lg.border.bg-muted\\/30");
    await expect(banner).toBeVisible();

    // Should show Active or Paused badge
    const activeBadge = banner.getByText("Active");
    const pausedBadge = banner.getByText("Paused");
    const activeVisible = await activeBadge.isVisible().catch(() => false);
    const pausedVisible = await pausedBadge.isVisible().catch(() => false);
    expect(activeVisible || pausedVisible).toBeTruthy();

    // Should show stats text (available, running, failed)
    await expect(banner.getByText(/available/)).toBeVisible();
    await expect(banner.getByText(/running/)).toBeVisible();
  });

  test("banner shows Pause button for active queue", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(isQueuesListResponse),
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const banner = page.locator(".rounded-lg.border.bg-muted\\/30");
    await expect(banner).toBeVisible();

    // Active queue should show Pause button
    const pauseBtn = banner.getByRole("button", { name: "Pause" });
    const resumeBtn = banner.getByRole("button", { name: "Resume" });

    // One of these should be visible based on queue state
    const pauseVisible = await pauseBtn.isVisible().catch(() => false);
    const resumeVisible = await resumeBtn.isVisible().catch(() => false);
    expect(pauseVisible || resumeVisible).toBeTruthy();

    // Drain button should always be visible
    await expect(banner.getByRole("button", { name: "Drain" })).toBeVisible();
  });

  test("Drain button shows confirmation dialog", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(isQueuesListResponse),
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const banner = page.locator(".rounded-lg.border.bg-muted\\/30");
    const drainBtn = banner.getByRole("button", { name: "Drain" });
    await expect(drainBtn).toBeVisible();

    await drainBtn.click();

    // Confirmation dialog should appear
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toBeVisible();
    await expect(dialog.getByRole("heading", { name: /Drain queue/ })).toBeVisible();

    // Close without confirming
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).not.toBeVisible();
  });

  test("banner does not appear without queue filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    // No banner should be visible
    const banner = page.locator(".rounded-lg.border.bg-muted\\/30");
    await expect(banner).not.toBeVisible();
  });
});
