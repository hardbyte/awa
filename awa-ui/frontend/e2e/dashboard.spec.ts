import { test, expect } from "@playwright/test";

test.describe("Operations overview", () => {
  test("overview separates current work and last-hour completions", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(
      page.getByRole("heading", { name: "Work in motion" }),
    ).toBeVisible();
    await expect(
      page.getByRole("heading", { name: "Current work" }),
    ).toBeVisible();
    await expect(
      page.getByText("Completed · last 60 minutes", { exact: true }),
    ).toBeVisible();
    await expect(page.locator(".work-counter")).toHaveCount(6);
  });

  test("queue pressure includes available wait and state drilldowns", async ({
    page,
  }) => {
    await page.goto("/");
    const table = page.getByRole("table", { name: "Queue pressure" });
    await expect(table).toBeVisible();
    for (const header of [
      "Queue",
      "Available",
      "Oldest available",
      "Running",
      "Retryable / failed",
    ]) {
      await expect(
        table.getByRole("columnheader", { name: header, exact: true }),
      ).toBeVisible();
    }
    const row = table.getByRole("row", { name: /e2e_test/ });
    await row.locator('a[href*="state=available"]').click();
    await expect(page).toHaveURL(/state=available/);
    await expect(page).toHaveURL(/e2e_test/);
  });

  for (const state of ["available", "retryable", "failed"]) {
    test(`${state} counter links to its job state`, async ({ page }) => {
      await page.goto("/");
      await page.locator(`.work-counter[href*="state=${state}"]`).click();
      await expect(page).toHaveURL(new RegExp(`state=${state}`));
      await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    });
  }

  test("worker summary reports health or explicit empty state", async ({
    page,
  }) => {
    await page.goto("/");
    const section = page.getByRole("region", { name: "Worker health" });
    await expect(
      section.getByRole("link", { name: "All workers" }),
    ).toBeVisible();
    await expect(section).toContainText(
      /healthy.*live.*stale.*leader|No runtime snapshots/,
    );
  });

  test("failed job drilldown opens the retained failure", async ({ page }) => {
    await page.goto("/");
    const section = page.getByRole("region", {
      name: "Failed jobs to inspect",
    });
    await expect(
      section.getByText(/Latest 5 retained failed jobs/),
    ).toBeVisible();
    await section.locator(".failure-preview-row").first().click();
    await expect(page).toHaveURL(/\/jobs\/\d+/);
    await expect(page.getByRole("heading", { name: /Job #\d+/ })).toBeVisible();
  });
});
