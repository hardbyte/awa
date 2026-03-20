import { test, expect } from "@playwright/test";

test.describe("Job detail page", () => {
  test("click first job row, navigate to /jobs/<id>", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    const rows = jobTable.getByRole("row");
    const rowCount = await rows.count();
    if (rowCount <= 1) {
      test.skip(true, "No job data in database");
      return;
    }

    await rows.nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

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
    const rows = jobTable.getByRole("row");
    const rowCount = await rows.count();
    if (rowCount <= 1) {
      test.skip(true, "No job data in database");
      return;
    }

    await rows.nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByText("Queue", { exact: true })).toBeVisible();
    await expect(page.getByText("Priority", { exact: true })).toBeVisible();
    await expect(page.getByText("Attempt", { exact: true })).toBeVisible();
  });

  test("arguments section shows JSON", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    const rows = jobTable.getByRole("row");
    const rowCount = await rows.count();
    if (rowCount <= 1) {
      test.skip(true, "No job data in database");
      return;
    }

    await rows.nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(page.getByText("Arguments")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("pre").first()).toBeVisible();
  });

  test("retry button visible on failed jobs", async ({ page }) => {
    await page.goto("/jobs?state=failed");
    await page.waitForResponse("**/api/jobs*");

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    const rows = jobTable.getByRole("row");
    const rowCount = await rows.count();
    if (rowCount <= 1) {
      test.skip(true, "No failed jobs in database");
      return;
    }

    await rows.nth(1).click();
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );
    await expect(page.getByRole("button", { name: "Retry" })).toBeVisible();
  });
});
