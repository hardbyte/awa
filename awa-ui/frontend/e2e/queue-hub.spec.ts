import { test, expect } from "@playwright/test";

test.describe("Queue hub", () => {
  test("the overview shows the queue's counts and links into its jobs", async ({ page }) => {
    await page.goto("/queues/e2e_test");
    await expect(page.getByRole("heading", { level: 1, name: "E2E Queue" })).toBeVisible();
    await expect(page.getByRole("tab", { name: "Overview" })).toHaveAttribute("aria-selected", "true");
    await expect(page.getByTestId("queue-stat-ready")).toBeVisible();
    await page.getByTestId("queue-stat-failed").getByRole("link").click();
    await expect(page).toHaveURL(/\/queues\/e2e_test\/jobs\?state=failed&dead_lettered=false$/);
    await expect(page.getByRole("tab", { name: "Jobs" })).toHaveAttribute("aria-selected", "true");
    await expect(page.getByTestId("dead-letters-hidden")).toBeVisible();
    await page.getByTestId("dead-letters-hidden").getByRole("button", { name: "Include them" }).click();
    await expect(page).toHaveURL(/\/queues\/e2e_test\/jobs\?state=failed$/);
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
    // The UI pauses as "ui"; the server reports when and by whom.
    await expect(header.getByTestId("queue-paused")).toHaveText(/^Paused (just now|\d+s ago) by ui$/);
    await Promise.all([
      page.waitForResponse((r) => r.url().endsWith("/api/queues/legacy_queue/resume") && r.ok()),
      header.getByRole("button", { name: "Resume" }).click(),
    ]);
    await expect(header.getByTestId("queue-paused")).toHaveCount(0);
  });

  test("Drain shows how many jobs it cancels and asks for the queue's name", async ({ page }) => {
    let drains = 0;
    await page.route("**/api/queues/e2e_test/drain", (route) => {
      drains++;
      return route.fulfill({ json: { drained: 3 } });
    });
    await page.goto("/queues/e2e_test");
    const drainButton = page.locator('[data-slot="page-header"]').getByRole("button", { name: "Drain" });
    await drainButton.click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: 'Drain queue "e2e_test"?' })).toBeVisible();
    await expect(dialog.getByTestId("drain-count")).toHaveText(/^\d[\d,]* jobs?$/);
    await expect(dialog).toContainText("whatever filter a list shows");
    const confirm = dialog.getByRole("button", { name: 'Drain "e2e_test"' });
    await expect(confirm).toBeDisabled();
    await dialog.getByRole("textbox", { name: /Type e2e_test/ }).fill("e2e_tes");
    await expect(confirm).toBeDisabled();
    await dialog.getByRole("button", { name: "Keep", exact: true }).click();
    await expect(dialog).not.toBeVisible();
    await expect(drainButton).toBeFocused();
    expect(drains).toBe(0);

    await drainButton.click();
    await dialog.getByRole("textbox", { name: /Type e2e_test/ }).fill("e2e_test");
    await confirm.click();
    await expect(dialog).toBeHidden();
    await expect(page.getByText('Drained queue "e2e_test": 3 job(s) cancelled')).toBeVisible();
    expect(drains).toBe(1);
  });

  test("the Failed figure is a dash with the error while the dead-letter count can't load", async ({ page }) => {
    await page.route("**/api/dlq/depth", (route) => route.fulfill({ status: 500, json: { error: "depth unavailable" } }));
    await page.goto("/queues/e2e_test");
    const failed = page.getByTestId("queue-stat-failed");
    await expect(failed.getByRole("link")).toHaveText("—");
    await expect(failed).toContainText("Failed to load dead letters: depth unavailable");
  });
});
