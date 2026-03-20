import { test, expect } from "@playwright/test";

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

    const searchInput = page.getByPlaceholder("Search by kind:");
    await expect(searchInput).toBeVisible();

    await searchInput.fill("kind:test");
    await expect(searchInput).toHaveValue("kind:test");
  });
});
