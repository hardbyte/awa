import { test, expect } from "@playwright/test";

test.describe("Kinds page", () => {
  test("navigate to /kinds, heading visible", async ({ page }) => {
    await page.goto("/kinds");
    await expect(page.getByRole("heading", { name: "Job Kinds" })).toBeVisible();
  });

  test("kinds table renders descriptor-backed and legacy kinds", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/kinds") && r.ok()),
      page.goto("/kinds"),
    ]);

    const kindsTable = page.getByRole("grid", { name: "Job kinds" });
    await expect(kindsTable).toBeVisible();

    await expect(kindsTable.getByText("E2E Job")).toBeVisible();
    await expect(kindsTable.getByText("e2e_job")).toBeVisible();
    await expect(
      kindsTable.getByText("End-to-end job kind used for UI coverage")
    ).toBeVisible();

  });

  test("clicking a kind navigates to jobs with kind filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/kinds") && r.ok()),
      page.goto("/kinds"),
    ]);

    await page.getByRole("link", { name: "E2E Job" }).click();
    await page.waitForURL(/\/jobs/);
    expect(page.url()).toContain("kind");
    expect(page.url()).toContain("e2e_job");
  });

  test("route-mocked stale and drift badges render", async ({ page }) => {
    await page.route("**/api/kinds", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            kind: "mock_kind",
            display_name: "Mock Kind",
            description: "Descriptor health coverage",
            owner: "qa-platform",
            docs_url: null,
            tags: [],
            extra: {},
            descriptor_last_seen_at: "2026-03-01T00:00:00Z",
            descriptor_stale: true,
            descriptor_mismatch: true,
            job_count: 4,
            queue_count: 2,
            completed_last_hour: 1,
          },
        ]),
      });
    });

    await page.goto("/kinds");
    const kindsTable = page.getByRole("grid", { name: "Job kinds" });
    await expect(kindsTable.getByText("Descriptor stale", { exact: true })).toBeVisible();
    await expect(kindsTable.getByText("Descriptor drift", { exact: true })).toBeVisible();
    await expect(kindsTable.getByText("Descriptor drift across live runtimes")).toBeVisible();
  });
});
