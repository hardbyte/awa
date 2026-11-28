import { test, expect } from "@playwright/test";

test.describe("Job kind page", () => {
  test("shows the descriptor and links to its failed jobs", async ({ page }) => {
    await page.goto("/kinds/e2e_job");
    await expect(page.getByRole("heading", { level: 1, name: "E2E Job" })).toBeVisible();
    await expect(page.getByText("End-to-end job kind used for UI coverage").first()).toBeVisible();
    await expect(page.getByText("qa-platform")).toBeVisible();
    await expect(page.getByText("E2eSummary", { exact: true })).toBeVisible();

    const failed = page.getByTestId("kind-failed-count").getByRole("link");
    await expect(failed).not.toHaveText("…");
    await failed.click();
    await page.waitForURL(/\/jobs\?/);
    const url = new URL(page.url());
    expect(url.searchParams.get("state")).toBe("failed");
    expect(url.searchParams.get("q")).toBe("kind:e2e_job");
  });

  test("lists recent failures and the queues they ran in", async ({ page }) => {
    // Other specs retry the seeded failed job, so serve a failure directly.
    await page.route("**/api/jobs?*", async (route) => {
      const url = new URL(route.request().url());
      if (url.searchParams.get("state") !== "failed" || url.searchParams.get("kind") !== "e2e_job") {
        return route.fallback();
      }
      const response = await route.fetch({ url: new URL("/api/jobs/800002", url).toString() });
      const job = await response.json();
      await route.fulfill({
        json: [{ ...job, id: 800003, state: "failed", errors: [{ error: "timeout", attempt: 1, at: new Date().toISOString() }] }],
      });
    });
    await page.goto("/kinds/e2e_job");
    await expect(page.getByTestId("kind-failures").getByRole("link", { name: "#800003" })).toBeVisible();
    await page.getByTestId("kind-queues").getByRole("link", { name: "e2e_test" }).click();
    await expect(page).toHaveURL(/\/queues\/e2e_test$/);
  });

  test("links the schedules that enqueue it", async ({ page }) => {
    await page.goto("/kinds/e2e_job");
    const schedule = page.getByTestId("kind-schedules").getByRole("link", { name: "e2e_cron_owner" });
    await expect(schedule).toBeVisible();
    await schedule.click();
    await expect(page).toHaveURL(/\/cron\/e2e_cron_owner$/);
  });

  test("a legacy kind without a descriptor still renders", async ({ page }) => {
    await page.goto("/kinds/legacy_job");
    await expect(page.getByRole("heading", { level: 1, name: "legacy_job" })).toBeVisible();
    await expect(page.getByText("Not declared").first()).toBeVisible();
  });

  test("an unknown kind shows not found", async ({ page }) => {
    await page.goto("/kinds/no_such_kind");
    const notFound = page.getByTestId("resource-not-found");
    await expect(notFound.getByRole("heading", { name: 'Job kind "no_such_kind" not found' })).toBeVisible();
    await notFound.getByRole("link", { name: "Back to job kinds" }).click();
    await expect(page).toHaveURL(/\/kinds$/);
  });

  test("a malformed kind in the URL shows a not-found page", async ({ page }) => {
    await page.goto("/kinds/%E0%A4%A");
    await expect(page.getByRole("heading", { level: 1, name: /not found/i })).toBeVisible();
    await expect(page.getByRole("navigation", { name: "Primary" })).toBeVisible();
  });
});
