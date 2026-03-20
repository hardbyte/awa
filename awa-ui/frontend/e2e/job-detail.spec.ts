import { test, expect } from "@playwright/test";

test.describe("Job detail page", () => {
  test("click first job row, navigate to /jobs/<id>", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Click the first data row
    const firstDataRow = jobTable.getByRole("row").nth(1);
    await firstDataRow.click();

    // Wait for navigation to job detail
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    // Verify heading shows "Job #<id>"
    await expect(
      page.getByRole("heading", { name: /Job #\d+/ })
    ).toBeVisible();
  });

  test("job detail shows properties: Queue, Priority, Attempt", async ({
    page,
  }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    const firstDataRow = jobTable.getByRole("row").nth(1);
    await firstDataRow.click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    // Check description list terms
    await expect(page.getByText("Queue", { exact: true })).toBeVisible();
    await expect(page.getByText("Priority", { exact: true })).toBeVisible();
    await expect(page.getByText("Attempt", { exact: true })).toBeVisible();
  });

  test("arguments section shows JSON", async ({ page }) => {
    // Navigate to jobs, click first row, verify args are shown
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Aui_demo"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    const firstDataRow = jobTable.getByRole("row").nth(1);
    await firstDataRow.click();
    await page.waitForURL(/\/jobs\/\d+/);

    // Wait for the detail page to render
    await expect(page.getByText("Arguments")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("pre").first()).toBeVisible();
  });

  test("retry/cancel buttons visible based on state", async ({ page }) => {
    // Navigate to a failed job to check for Retry button
    await page.goto("/jobs?state=failed&q=queue%3Aui_demo");
    await page.waitForResponse("**/api/jobs*");

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    const rows = jobTable.getByRole("row");
    const rowCount = await rows.count();

    if (rowCount > 1) {
      await rows.nth(1).click();
      await page.waitForURL(/\/jobs\/\d+/);
      await page.waitForResponse((r) =>
        /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
      );
      await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
    }
  });
});
