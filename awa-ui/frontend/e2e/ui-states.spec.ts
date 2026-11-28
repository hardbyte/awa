import { test, expect, type Page } from "@playwright/test";

/** Fail every matching GET with a 500, as an unreachable database would. */
test.describe("Phone navigation", () => {
  test("the drawer opens at 390px and navigates", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/jobs");

    await page.locator('button[data-slot="sidebar-trigger"]').first().click();
    const drawer = page.getByRole("dialog", { name: "Sidebar" });
    await expect(drawer).toBeVisible();
    await page.waitForTimeout(500);
    await expect(drawer).toBeVisible();

    await drawer.getByRole("link", { name: "Queues" }).click();
    await expect(page).toHaveURL(/\/queues$/);
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
    await expect(drawer).toBeHidden();
  });

  test("pages do not scroll sideways at 720px (desktop at 200% zoom)", async ({ page }) => {
    await page.setViewportSize({ width: 720, height: 900 });
    for (const path of ["/", "/jobs", "/queues", "/kinds", "/runtime"]) {
      await page.goto(path);
      await expect(page.getByRole("heading").first()).toBeVisible();
      const overflow = await page.evaluate(
        () => document.documentElement.scrollWidth - document.documentElement.clientWidth
      );
      expect(overflow, path).toBeLessThanOrEqual(0);
    }
  });
});
