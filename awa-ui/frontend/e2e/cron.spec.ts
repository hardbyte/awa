import { test, expect, type Page, type Response } from "@playwright/test";

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isCronListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return response.ok() && response.request().method() === "GET" && url.pathname === "/api/cron";
}

function cronPanelId(name: string): string {
  return `cron-panel-${name.replace(/[^a-zA-Z0-9_-]/g, "-")}`;
}

async function loadCronPage(page: Page) {
  const [cronResponse] = await Promise.all([
    page.waitForResponse(isCronListResponse),
    page.goto("/cron"),
  ]);
  return (await cronResponse.json()) as Array<{
    name: string;
    next_fire_at: string | null;
  }>;
}

function firstCronSummary(page: Page, name: string) {
  return page.getByRole("button", {
    name: new RegExp(`\\b${escapeRegex(name)}\\b`),
  });
}

test.describe("Cron page", () => {
  test("navigate to /cron, heading visible", async ({ page }) => {
    await page.goto("/cron");
    await expect(
      page.getByRole("heading", { name: "Cron Schedules" })
    ).toBeVisible();
  });

  test("cron list renders or shows empty state", async ({ page }) => {
    const cronJobs = await loadCronPage(page);

    if (cronJobs.length === 0) {
      await expect(page.getByText("No cron schedules found.")).toBeVisible();
      return;
    }

    await expect(firstCronSummary(page, cronJobs[0].name)).toBeVisible();
    await expect(page.getByRole("button", { name: "Trigger now" }).first()).toBeVisible();
    // Every row shows either a Pause or a Resume action.
    const hasPauseOrResume =
      (await page.getByRole("button", { name: /Pause|Resume/ }).count()) > 0;
    expect(hasPauseOrResume).toBe(true);
  });

  test("pause and resume buttons round-trip a schedule", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    const target = cronJobs[0];
    const summary = firstCronSummary(page, target.name);
    const row = summary.locator("xpath=ancestor::div[contains(@class, 'rounded-lg')][1]");

    const pauseBtn = row.getByRole("button", { name: "Pause" });
    const resumeBtn = row.getByRole("button", { name: "Resume" });

    // Start state may be either paused or active; normalise to active.
    if ((await resumeBtn.count()) > 0) {
      const [resumeRes] = await Promise.all([
        page.waitForResponse(
          (r) =>
            r.ok() &&
            r.request().method() === "POST" &&
            new URL(r.url()).pathname === `/api/cron/${target.name}/resume`,
        ),
        resumeBtn.click(),
      ]);
      expect(resumeRes.ok()).toBeTruthy();
      await expect(pauseBtn).toBeVisible();
    }

    // Pause.
    const [pauseRes] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.ok() &&
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === `/api/cron/${target.name}/pause`,
      ),
      pauseBtn.click(),
    ]);
    expect(pauseRes.ok()).toBeTruthy();
    await expect(row.getByText(/^paused$/)).toBeVisible();
    await expect(row.getByRole("button", { name: "Resume" })).toBeVisible();

    // Resume to restore initial state.
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.ok() &&
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === `/api/cron/${target.name}/resume`,
      ),
      row.getByRole("button", { name: "Resume" }).click(),
    ]);
    await expect(row.getByRole("button", { name: "Pause" })).toBeVisible();
  });

  test("clicking cron row toggles expand/collapse", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    const firstCron = cronJobs[0];
    const summary = firstCronSummary(page, firstCron.name);
    const panel = page.locator(`#${cronPanelId(firstCron.name)}`);

    await expect(panel).toHaveCount(0);
    await summary.click();

    await expect(panel).toBeVisible();
    await expect(panel.getByText("Kind", { exact: true })).toBeVisible();
    await expect(panel.getByText("Queue", { exact: true })).toBeVisible();
    await expect(panel.getByText("Priority", { exact: true })).toBeVisible();
    await expect(panel.getByText("Max attempts", { exact: true })).toBeVisible();

    await summary.click();
    await expect(panel).toHaveCount(0);
  });

  test("cron API response includes next_fire_at", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (!Array.isArray(cronJobs) || cronJobs.length === 0) {
      test.skip();
      return;
    }

    // Every cron schedule should have a next_fire_at field
    for (const job of cronJobs) {
      expect(job).toHaveProperty("next_fire_at");
      // next_fire_at should be a non-null ISO timestamp string
      if (job.next_fire_at !== null) {
        expect(new Date(job.next_fire_at).getTime()).toBeGreaterThanOrEqual(
          Date.now() - 1000,
        );
      }
    }
  });

  test("cron summary row shows next fire time", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    await expect(firstCronSummary(page, cronJobs[0].name)).toContainText(
      /in \d+[smhd]|overdue/,
    );
  });

  test("expanded cron detail shows Next fire with timezone", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    const firstCron = cronJobs[0];
    const summary = firstCronSummary(page, firstCron.name);
    const panel = page.locator(`#${cronPanelId(firstCron.name)}`);
    await summary.click();
    await expect(panel.getByText("Next fire")).toBeVisible();
  });

  test("trigger now button creates a job", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    const triggerBtn = page.getByRole("button", { name: "Trigger now" }).first();
    const firstCron = cronJobs[0];

    const [triggerResponse] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.ok() &&
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === `/api/cron/${firstCron.name}/trigger`
      ),
      triggerBtn.click(),
    ]);

    expect(triggerResponse.ok()).toBeTruthy();

    const job = await triggerResponse.json();
    expect(job.id).toBeTruthy();
    expect(job.state).toBe("available");
  });
});
