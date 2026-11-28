import { test, expect } from "@playwright/test";

test.describe("Older URLs", () => {
  test("/jobs?kind= becomes a kind search term instead of being ignored", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/api/jobs?") && r.url().includes("kind=e2e_job") && r.url().includes("state=failed")
      ),
      page.goto("/jobs?kind=e2e_job&state=failed"),
    ]);
    const url = new URL(page.url());
    expect(url.searchParams.get("kind")).toBeNull();
    expect(url.searchParams.get("q")).toBe("kind:e2e_job");
    expect(url.searchParams.get("state")).toBe("failed");
    await expect(page.getByRole("button", { name: "Remove kind:e2e_job filter" })).toBeVisible();
  });

  test("/dlq?queue= becomes a queue search term", async ({ page }) => {
    await page.goto("/dlq?queue=e2e_dlq");
    await expect(page).toHaveURL(/\/dlq\?q=queue%3Ae2e_dlq$/);
    await expect(page.getByRole("button", { name: "Remove queue:e2e_dlq filter" })).toBeVisible();
  });

  test("/batch-ops with a filter opens the new-operation form", async ({ page }) => {
    await page.goto("/batch-ops?queue=e2e_test");
    await expect(page).toHaveURL(/\/batch-ops\/new\?queue=e2e_test$/);
    await expect(page.getByLabel("Queue filter")).toHaveValue("e2e_test");
  });
});
