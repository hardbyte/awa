import { test, expect, type Locator, type Page } from "@playwright/test";

/** Click the first job once the match count, which loads after the list
 * and moves the rows, has rendered. */
async function openFirstJob(page: Page, jobTable: Locator) {
  await expect(page.getByTestId("match-count")).toBeVisible();
  await jobTable.getByRole("row").nth(1).click();
}

test.describe("Job detail page", () => {
  test("dlq job detail redirects to /dlq/:id", async ({ page }) => {
    const jobResponse = page.waitForResponse(
      (r) => /\/api\/jobs\/600001$/.test(r.url()) && r.ok()
    );
    const dlqResponse = page.waitForResponse(
      (r) => /\/api\/dlq\/600001$/.test(r.url()) && r.ok()
    );

    await page.goto("/jobs/600001");

    await jobResponse;
    await page.waitForURL(/\/dlq\/600001$/);
    await dlqResponse;
    await expect(page.getByRole("heading", { name: /#600001/ })).toBeVisible();
  });

  test("click job row navigates to detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Seeded data guarantees rows exist
    const rows = jobTable.getByRole("row");
    await expect(rows).not.toHaveCount(1); // more than just header

    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(
      page.getByRole("heading", { name: /Job #\d+/ })
    ).toBeVisible();
  });

  test("job detail shows properties", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByText("Queue", { exact: true })).toBeVisible();
    await expect(page.getByText("Priority", { exact: true })).toBeVisible();
    await expect(page.getByText("Attempt", { exact: true })).toBeVisible();
    await expect(page.getByText("e2e_test")).toBeVisible();
  });

  test("job detail shows queue and kind descriptors when available", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByRole("heading", { name: /E2E Job/ })).toBeVisible();
    await expect(page.getByText("e2e_job").first()).toBeVisible();
    await expect(page.getByRole("link", { name: "E2E Queue" })).toBeVisible();
    await expect(page.getByText("End-to-end job kind used for UI coverage")).toBeVisible();
  });

  test("legacy job detail still renders raw names without descriptors", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=legacy_queue") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Alegacy_queue"),
    ]);
    await expect(page.getByTestId("open-queue")).toContainText("legacy_queue");

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByRole("heading", { name: /legacy_job/ })).toBeVisible();
    await expect(page.getByText("legacy_queue")).toBeVisible();

    // Legacy jobs have no descriptor, so descriptor-derived affordances
    // must not render: no display-name heading for the descriptor-backed
    // E2E queue, no kind description paragraph.
    await expect(page.getByRole("heading", { name: /E2E Queue/ })).toHaveCount(0);
    await expect(
      page.getByText("End-to-end job kind used for UI coverage"),
    ).toHaveCount(0);
  });

  test("arguments section shows JSON", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(page.getByText("Arguments")).toBeVisible({ timeout: 10000 });
    await expect(page.locator("pre").first()).toBeVisible();
  });

  test("retry button visible on failed job", async ({ page }) => {
    // Navigate to failed jobs in e2e_test queue
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?state=failed&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Seeded failed job should exist
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    await expect(page.getByRole("button", { name: "Retry…" })).toBeVisible();
  });

  test("timeline renders on job detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);

    await expect(
      page.locator('[data-slot="card-header"]', { hasText: "Timeline" })
    ).toBeVisible({ timeout: 10000 });
    await expect(page.getByText("Created").first()).toBeVisible();
  });

  test("cancel button visible on available job and executes mutation", async ({ page }) => {
    // Navigate to available jobs in e2e_test queue
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?state=available&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    // Available jobs can be cancelled, after confirming
    const cancelBtn = page.getByRole("button", { name: "Cancel…" });
    await expect(cancelBtn).toBeVisible();
    await cancelBtn.click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: /^Cancel job #\d+\?$/ })).toBeVisible();
    await expect(dialog.getByText("It won't run. A cancelled job can be retried later.")).toBeVisible();

    const [cancelResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/cancel") && r.request().method() === "POST"
      ),
      dialog.getByRole("button", { name: /^Cancel job #\d+$/ }).click(),
    ]);

    expect(cancelResponse.ok()).toBeTruthy();
    await expect(dialog).toBeHidden();
  });

  test("retry button on failed job executes mutation", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?state=failed&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    const retryBtn = page.getByRole("button", { name: "Retry…" });
    await expect(retryBtn).toBeVisible();
    await retryBtn.click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByText(/happens again/)).toBeVisible();

    const [retryResponse] = await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/retry") && r.request().method() === "POST"
      ),
      dialog.getByRole("button", { name: /^Retry job #\d+$/ }).click(),
    ]);

    expect(retryResponse.ok()).toBeTruthy();
  });

  test("a refused cancel keeps the dialog open with the server's error", async ({ page }) => {
    await page.route("**/api/jobs/*/cancel", (route) =>
      route.fulfill({ status: 409, json: { error: "job is no longer cancellable" } })
    );
    await page.goto("/jobs/800004");
    const cancelBtn = page.getByRole("button", { name: "Cancel…" });
    await cancelBtn.click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: "Cancel job #800004?" })).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel job #800004" }).click();
    await expect(dialog.getByTestId("confirm-error")).toContainText("job is no longer cancellable");
    await expect(dialog).toBeVisible();
    await dialog.getByRole("button", { name: "Keep" }).click();
    await expect(dialog).toBeHidden();
    await expect(cancelBtn).toBeFocused();
  });

  test("on a read-only server the actions are disabled and say why", async ({ page }) => {
    await page.route("**/api/capabilities", async (route) => {
      const response = await route.fetch();
      await route.fulfill({ response, json: { ...(await response.json()), read_only: true } });
    });
    await page.goto("/jobs/800004");
    const cancelBtn = page.getByRole("button", { name: "Cancel…" });
    await expect(cancelBtn).toBeDisabled();
    await expect(cancelBtn).toHaveAccessibleDescription("Read-only server — admin actions are off");
  });

  test("breadcrumb navigates to /jobs", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);

    const crumbs = page.getByRole("navigation", { name: "Breadcrumbs" });
    await expect(crumbs.getByText(/^#\d+$/)).toBeVisible();
    await crumbs.getByRole("link", { name: "Jobs" }).click();

    await page.waitForURL(/\/jobs$/);
  });

  test("queue link navigates to queue detail", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);
    await page.waitForResponse((r) =>
      /\/api\/jobs\/\d+$/.test(r.url()) && r.ok()
    );

    // Queue link in the description list uses the display name when a descriptor exists
    const queueLink = page.getByRole("link", { name: "E2E Queue" });
    await expect(queueLink).toBeVisible();
    await queueLink.click();

    await page.waitForURL(/\/queues\/e2e_test/);
    await expect(page.getByRole("heading", { name: "E2E Queue" })).toBeVisible();
  });

  test("copy arguments button is visible", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await openFirstJob(page, jobTable);
    await page.waitForURL(/\/jobs\/\d+/);

    // The copy button for arguments should be present
    const copyBtn = page.getByRole("button", { name: "Copy arguments JSON" });
    await expect(copyBtn).toBeVisible({ timeout: 10000 });
  });
  test("completed job shows its output labelled with the declared type", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => /\/api\/jobs\/800002$/.test(r.url()) && r.ok()),
      page.goto("/jobs/800002"),
    ]);

    await expect(page.getByText("Output", { exact: true })).toBeVisible();
    await expect(page.getByLabel("Output type E2eSummary")).toBeVisible();
    await expect(page.getByText("seeded output")).toBeVisible();
    await expect(page.getByRole("button", { name: "Copy output JSON" })).toBeVisible();
  });

  test("failed job shows the latest failed attempt's output", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => /\/api\/jobs\/800003$/.test(r.url()) && r.ok()),
      page.goto("/jobs/800003"),
    ]);

    await expect(page.getByText("Output from failed attempt 3")).toBeVisible();
    await expect(page.getByLabel("Output type E2eSummary")).toBeVisible();
    await expect(page.getByText("partial").first()).toBeVisible();
  });

  test("job without an output shows no output card", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => /\/api\/jobs\/800001$/.test(r.url()) && r.ok()),
      page.goto("/jobs/800001"),
    ]);

    await expect(page.getByRole("button", { name: "Copy arguments JSON" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Copy output JSON" })).toHaveCount(0);
  });
  test("dlq job detail shows the dead-lettered attempt's output", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => /\/api\/dlq\/600001$/.test(r.url()) && r.ok()),
      page.goto("/dlq/600001"),
    ]);

    await expect(page.getByText("Output from failed attempt 2")).toBeVisible();
    await expect(page.getByText("dead-lettered").first()).toBeVisible();
    await expect(page.getByRole("button", { name: "Copy output JSON" })).toBeVisible();
  });

  test("the kind and tags link to their pages", async ({ page }) => {
    await page.goto("/jobs/800002");
    await page.getByRole("link", { name: "E2E Job", exact: true }).click();
    await expect(page).toHaveURL(/\/kinds\/e2e_job$/);
    await page.goBack();
    await page.getByTestId("tag-chip").filter({ hasText: "e2e" }).first().click();
    await page.waitForURL(/\/jobs\?/);
    expect(new URL(page.url()).searchParams.get("q")).toBe("tag:e2e");
  });

  test("a job a schedule created links to the schedule", async ({ page }) => {
    await page.route("**/api/jobs/800002", async (route) => {
      const response = await route.fetch();
      const job = await response.json();
      await route.fulfill({
        response,
        json: { ...job, metadata: { cron_name: "e2e_cron_owner", triggered_manually: true } },
      });
    });
    await page.goto("/jobs/800002");
    const origin = page.getByTestId("job-cron-origin");
    await expect(origin).toContainText("(manual trigger)");
    await origin.getByRole("link", { name: "e2e_cron_owner" }).click();
    await expect(page).toHaveURL(/\/cron\/e2e_cron_owner$/);
  });
});
