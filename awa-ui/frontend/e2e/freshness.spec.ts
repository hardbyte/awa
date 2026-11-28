import { test, expect } from "@playwright/test";

test.describe("Freshness pill", () => {
  test("turns amber when some reads fail, red when all do, and green again once they recover", async ({ page }) => {
    await page.goto("/");
    const pill = page.getByTestId("freshness");
    await expect(pill).toHaveAttribute("data-tone", "live");
    await expect(pill).toHaveAccessibleName(/^Live · updated /);

    await page.route("**/api/stats", (route) => route.abort());
    await page.getByRole("button", { name: "Refresh data" }).click();
    await expect(pill).toHaveAttribute("data-tone", "stale", { timeout: 15_000 });
    await expect(pill).toHaveAccessibleName(/^Last updated .* — reconnecting$/);

    await page.route("**/api/**", (route) =>
      new URL(route.request().url()).pathname === "/api/capabilities" ? route.fallback() : route.abort()
    );
    await page.getByRole("button", { name: "Refresh data" }).click();
    await expect(pill).toHaveAttribute("data-tone", "down", { timeout: 20_000 });
    await expect(pill).toHaveAccessibleName("Server unreachable");
    await expect(page.locator("[data-stale]")).toHaveCount(1);

    await page.unrouteAll({ behavior: "ignoreErrors" });
    await page.getByRole("button", { name: "Refresh data" }).click();
    await expect(pill).toHaveAttribute("data-tone", "live", { timeout: 30_000 });
    await expect(page.locator("[data-stale]")).toHaveCount(0);
  });

  test("a refused request waits for a refresh without reading as unreachable", async ({ page }) => {
    await page.route("**/api/**", (route) =>
      new URL(route.request().url()).pathname === "/api/capabilities"
        ? route.fallback()
        : route.fulfill({ status: 400, json: { error: "unknown parameter" } })
    );
    await page.goto("/");
    const pill = page.getByTestId("freshness");
    await expect(pill).toHaveAttribute("data-tone", "paused", { timeout: 15_000 });
    await expect(pill).toHaveAccessibleName("A view's request was refused — refresh to retry");
    await expect(page.locator("[data-stale]")).toHaveCount(0);
  });

  test("the page no longer prints a polling or refresh status line", async ({ page }) => {
    await page.goto("/jobs");
    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
    await expect(page.getByText(/scheduled reconciliation maximum/)).toHaveCount(0);
    await expect(page.getByText(/awaiting authoritative refresh/)).toHaveCount(0);
  });

  test("a list paused for a selection says so", async ({ page }) => {
    await page.goto("/jobs?q=queue%3Ae2e_test");
    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable.getByRole("row").nth(1)).toBeVisible();
    await jobTable.getByRole("checkbox").nth(1).click({ force: true });
    await expect(page.getByTestId("freshness")).toHaveAttribute("data-tone", "paused");
  });
});
