import { test, expect } from "@playwright/test";

test.describe("Job detail page", () => {
  test("click job row navigates to detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Seeded data guarantees rows exist
    const rows = jobTable.getByRole("row");
    await expect(rows).not.toHaveCount(1); // more than just header

    await rows.nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(
      page.getByRole("heading", { name: /Job #\d+/ })
    ).toBeVisible();
  });

  test("job detail shows properties", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByText("Queue", { exact: true })).toBeVisible();
    await expect(page.getByText("Priority", { exact: true })).toBeVisible();
    await expect(page.getByText("Attempt", { exact: true })).toBeVisible();
    await expect(page.getByText("e2e_test")).toBeVisible();
  });

  test("job detail shows queue and kind descriptors when available", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByRole("heading", { name: /E2E Job/ })).toBeVisible();
    await expect(page.getByText("e2e_job").first()).toBeVisible();
    await expect(page.getByRole("link", { name: "E2E Queue" })).toBeVisible();
    await expect(page.getByText("End-to-end job kind used for UI coverage")).toBeVisible();
  });

  test("legacy job detail still renders raw names without descriptors", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=legacy_queue") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Alegacy_queue"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByRole("heading", { name: /legacy_job/ })).toBeVisible();
    await expect(page.getByText("legacy_queue")).toBeVisible();

    // Legacy jobs have no descriptor, so descriptor-derived affordances
    // must not render: no display-name heading for the descriptor-backed
    // E2E queue, no kind description paragraph.
    await expect(page.getByRole("heading", { name: /E2E Queue/ })).toHaveCount(0);
    await expect(
      page.getByText("End-to-end job kind used for UI coverage"),
    ).toHaveCount(0);
  });

  test("arguments section shows JSON", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(page.getByText("Arguments")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("pre").first()).toBeVisible();
  });

  test("retry button visible on failed job", async ({ page }) => {
    // Navigate to failed jobs in e2e_test queue
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?state=failed&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Seeded failed job should exist
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
  });

  test("timeline renders on job detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(
      page.locator('[data-slot="card-header"]', { hasText: "Timeline" })
    ).toBeVisible({ timeout: 10000 });
    await expect(page.getByText("Created").first()).toBeVisible();
  });

  test("cancel button visible on available job and executes mutation", async ({ page }) => {
    // Navigate to available jobs in e2e_test queue
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?state=available&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    // Available jobs can be cancelled
    const cancelBtn = page.getByRole("button", { name: "Cancel" });
    await expect(cancelBtn).toBeVisible();

    // Click cancel and verify mutation fires
    const [cancelResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/cancel") && r.request().method() === "POST"
      ),
      cancelBtn.click(),
    ]);

    expect(cancelResponse.ok()).toBeTruthy();
  });

  test("retry button on failed job executes mutation", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?state=failed&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    const retryBtn = page.getByRole("button", { name: "Retry" });
    await expect(retryBtn).toBeVisible();

    // Click retry and verify mutation fires
    const [retryResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/retry") && r.request().method() === "POST"
      ),
      retryBtn.click(),
    ]);

    expect(retryResponse.ok()).toBeTruthy();
  });

  test("back link navigates to /jobs", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);

    const backLink = page.getByRole("link", { name: /Back to jobs/ });
    await expect(backLink).toBeVisible();
    await backLink.click();

    await page.waitForURL(/\/jobs$/);
  });

  test("queue link navigates to queue detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    // Queue link in the description list uses the display name when a descriptor exists
    const queueLink = page.getByRole("link", { name: "E2E Queue" });
    await expect(queueLink).toBeVisible();
    await queueLink.click();

    await page.waitForURL(/\/queues\/e2e_test/);
    await expect(page.getByRole("heading", { name: "E2E Queue" })).toBeVisible();
  });

  test("copy arguments button is visible", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("row").nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);

    // The copy button for arguments should be present
    const copyBtn = page.getByRole("button", { name: "Copy arguments JSON" });
    await expect(copyBtn).toBeVisible({ timeout: 10000 });
  });
});
