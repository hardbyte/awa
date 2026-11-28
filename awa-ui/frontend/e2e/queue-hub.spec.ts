import { test, expect } from "@playwright/test";

test.describe("Queue hub", () => {
  test("the overview shows the queue's counts and links into its jobs", async ({ page }) => {
    await page.goto("/queues/e2e_test");
    await expect(page.getByRole("heading", { level: 1, name: "E2E Queue" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Overview" })).toHaveAttribute("aria-selected", "true");
    await expect(page.getByTestId("queue-stat-ready")).toBeVisible();
    await page.getByTestId("queue-stat-failed").getByRole("link").click();
    await expect(page).toHaveURL(/\/queues\/e2e_test\/jobs\?state=failed$/);
    await expect(page.getByRole("tab", { name: "Jobs" })).toHaveAttribute("aria-selected", "true");
  });

  test("tabs are deep-linkable and the jobs tab keeps its own filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs?") &&
          r.url().includes("queue=e2e_test") &&
          r.url().includes("state=failed") &&
          r.ok()
      ),
      page.goto("/queues/e2e_test/jobs?state=failed"),
    ]);
    await expect(page.getByRole("tab", { name: "Jobs" })).toHaveAttribute("aria-selected", "true");
    await expect(page.locator('[data-testid="state-pill"][data-state="failed"]')).toHaveAttribute("aria-pressed", "true");
    await expect(page.getByRole("grid", { name: "Jobs" })).toBeVisible();
  });

  test("the queue filter is locked on the jobs tab", async ({ page }) => {
    await page.goto("/queues/e2e_test/jobs");
    const locked = page.getByTestId("locked-filter");
    await expect(locked).toHaveText("queue:e2e_test");
    await expect(page.getByRole("button", { name: "Remove queue:e2e_test filter" })).toHaveCount(0);

    await page.getByRole("textbox", { name: "Search jobs" }).fill("kind:e2e_job");
    await page.getByRole("textbox", { name: "Search jobs" }).press("Enter");
    await expect(page).toHaveURL(/\/queues\/e2e_test\/jobs\?q=kind/);
    await page.getByRole("button", { name: "Remove kind:e2e_job filter" }).click();
    await expect(locked).toHaveText("queue:e2e_test");
  });

  test("tabs navigate between views", async ({ page }) => {
    await page.goto("/queues/e2e_test");
    await page.getByRole("tab", { name: "Dead letters" }).click();
    await expect(page).toHaveURL(/\/queues\/e2e_test\/dlq$/);
    await page.getByRole("tab", { name: "Config" }).click();
    await expect(page).toHaveURL(/\/queues\/e2e_test\/config$/);
    await expect(page.getByText("Descriptor health")).toBeVisible();
    await page.getByRole("tab", { name: "Overview" }).click();
    await expect(page).toHaveURL(/\/queues\/e2e_test$/);
  });

  test("pause and resume round-trip from the header", async ({ page }) => {
    await page.goto("/queues/legacy_queue");
    const header = page.locator('[data-slot="page-header"]');
    await expect(header.getByRole("heading", { name: "legacy_queue" })).toBeVisible();

    if (await header.getByRole("button", { name: "Resume" }).isVisible()) {
      await header.getByRole("button", { name: "Resume" }).click();
    }
    await Promise.all([
      page.waitForResponse((r) => r.url().endsWith("/api/queues/legacy_queue/pause") && r.ok()),
      header.getByRole("button", { name: "Pause" }).click(),
    ]);
    await expect(header.getByTestId("queue-paused")).toBeVisible();
    await Promise.all([
      page.waitForResponse((r) => r.url().endsWith("/api/queues/legacy_queue/resume") && r.ok()),
      header.getByRole("button", { name: "Resume" }).click(),
    ]);
    await expect(header.getByTestId("queue-paused")).toHaveCount(0);
  });

  test("Drain asks for confirmation", async ({ page }) => {
    await page.goto("/queues/e2e_test");
    await page.locator('[data-slot="page-header"]').getByRole("button", { name: "Drain" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: /Drain queue/ })).toBeVisible();
    await dialog.getByRole("button", { name: "Keep", exact: true }).click();
    await expect(dialog).not.toBeVisible();
  });
});
