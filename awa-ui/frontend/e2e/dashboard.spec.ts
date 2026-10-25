import { test, expect, type Page } from "@playwright/test";

/** The overview lists only queues needing attention until expanded; seeded
 * queues may or may not qualify depending on their age, so expand first. */
async function showAllQueues(page: Page) {
  await expect(page.getByTestId("verdict")).not.toHaveText("Checking queues…");
  const expand = page.getByRole("button", {
    name: /^Show \d+ queues? keeping up$/,
  });
  if (await expand.isVisible()) await expand.click();
  return page.getByRole("table", {
    name: /^(All queues by risk|Queues needing attention)$/,
  });
}

test.describe("Operations overview", () => {
  test("answers whether the system is keeping up", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();
    await expect(page.getByText("Is it keeping up?")).toBeVisible();
    await expect(page.getByTestId("verdict")).not.toHaveText(
      "Checking queues…",
    );
    await expect(page.getByText("Waiting across all queues")).toBeVisible();
    await expect(
      page.getByText(
        /Trends from samples every|Queue history starts with the first sample/,
      ),
    ).toBeVisible();
  });

  test("queue table includes trend columns and state drilldowns", async ({
    page,
  }) => {
    await page.goto("/");
    const table = await showAllQueues(page);
    await expect(table).toBeVisible();
    for (const header of [
      "Queue",
      "Status",
      "Waiting",
      "Last hour",
      "Trend",
      "Oldest wait",
    ]) {
      await expect(
        table.getByRole("columnheader", { name: header, exact: true }),
      ).toBeVisible();
    }
    const row = table.getByRole("row", { name: /E2E Queue/ });
    await row.locator('a[href*="state=available"]').click();
    await expect(page).toHaveURL(/state=available/);
    await expect(page).toHaveURL(/e2e_test/);
  });

  for (const [label, state] of [
    ["Retrying", "retryable"],
    ["Failed", "failed"],
    ["Running", "running"],
  ] as const) {
    test(`${label} counter links to its job state`, async ({ page }) => {
      await page.goto("/");
      await page
        .getByRole("navigation", { name: "Job states" })
        .getByRole("link", { name: new RegExp(`^${label}`) })
        .click();
      await expect(page).toHaveURL(new RegExp(`state=${state}`));
      await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    });
  }

  test("worker summary reports problems or an explicit healthy state", async ({
    page,
  }) => {
    await page.goto("/");
    await expect(page.getByRole("link", { name: "All workers" })).toBeVisible();
    await expect(
      page
        .getByText(
          /Workers needing attention|reporting instances are healthy|No runtime snapshots/,
        )
        .or(page.getByRole("list", { name: "Workers needing attention" })),
    ).toBeVisible();
  });

  test("failure group opens the latest retained failure", async ({ page }) => {
    await page.goto("/");
    const groups = page.getByRole("list", { name: "Failure groups" });
    await expect(groups).toBeVisible();
    await groups.getByRole("link").first().click();
    await expect(page).toHaveURL(/\/jobs\/\d+/);
    await expect(page.getByRole("heading", { name: /Job #\d+/ })).toBeVisible();
  });
});
