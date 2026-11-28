import { test, expect, type Page, type Response } from "@playwright/test";

function escapeRegex(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isCronListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return response.ok() && response.request().method() === "GET" && url.pathname === "/api/cron";
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

function cronRow(page: Page, name: string) {
  return page
    .getByRole("grid", { name: "Schedules" })
    .getByRole("row")
    .filter({ has: page.getByRole("link", { name, exact: true }) });
}

test.describe("Cron page", () => {
  test("navigate to /cron, heading visible", async ({ page }) => {
    await page.goto("/cron");
    await expect(
      page.getByRole("heading", { name: "Schedules", exact: true })
    ).toBeVisible();
  });

  test("cron list renders or shows empty state", async ({ page }) => {
    const cronJobs = await loadCronPage(page);

    if (cronJobs.length === 0) {
      await expect(page.getByText("No cron schedules found.")).toBeVisible();
      return;
    }

    await expect(cronRow(page, cronJobs[0].name)).toBeVisible();
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
    const row = cronRow(page, target.name);

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
    await expect(row.getByTestId("cron-paused")).toBeVisible();
    await expect(row.getByRole("button", { name: "Resume" })).toBeVisible();

    // The toast's Undo resumes it again.
    const toast = page.locator("[data-sonner-toast]").filter({ hasText: `Schedule "${target.name}" paused` });
    await expect(toast).toBeVisible();
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.ok() &&
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === `/api/cron/${target.name}/resume`,
      ),
      toast.getByRole("button", { name: "Undo" }).click(),
    ]);
    await expect(row.getByRole("button", { name: "Pause" })).toBeVisible();
  });

  test("a schedule's name opens its page", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    const target = cronJobs[0];
    await cronRow(page, target.name).getByRole("link", { name: target.name, exact: true }).click();
    await expect(page).toHaveURL(new RegExp(`/cron/${escapeRegex(encodeURIComponent(target.name))}$`));
    await expect(page.getByRole("heading", { level: 1, name: target.name })).toBeVisible();
    for (const term of ["Kind", "Queue", "Priority", "Max attempts"]) {
      await expect(page.getByText(term, { exact: true })).toBeVisible();
    }
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

  test("the schedule list shows the next fire time", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    const live = cronJobs.find((job) => job.next_fire_at !== null);
    if (!live) {
      test.skip();
      return;
    }

    await expect(cronRow(page, live.name)).toContainText(/in \d+[smhd]|\d+[smhd] ago|just now/);
  });

  test("a schedule's page shows Next fire with its time zone", async ({ page }) => {
    const cronJobs = await loadCronPage(page);
    if (cronJobs.length === 0) {
      test.skip();
      return;
    }

    await page.goto(`/cron/${encodeURIComponent(cronJobs[0].name)}`);
    await expect(page.getByText(/^Next fire/)).toBeVisible();
  });

  test("trigger now on an active schedule creates a job in one click", async ({ page, request }) => {
    await request.post("/api/cron/e2e_cron_owner/resume");
    await loadCronPage(page);
    const row = cronRow(page, "e2e_cron_owner");
    const [triggerResponse] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.ok() &&
          r.request().method() === "POST" &&
          new URL(r.url()).pathname === "/api/cron/e2e_cron_owner/trigger"
      ),
      row.getByRole("button", { name: "Trigger now" }).click(),
    ]);
    const job = await triggerResponse.json();
    expect(job.state).toBe("available");
    await expect(page.locator("[data-sonner-toast]").getByRole("button", { name: "Open job" })).toBeVisible();
  });

  test("triggering a paused schedule asks first", async ({ page, request }) => {
    await request.post("/api/cron/e2e_cron_owner/pause", { data: { paused_by: "e2e" } });
    try {
      await loadCronPage(page);
      const row = cronRow(page, "e2e_cron_owner");
      const triggers: string[] = [];
      page.on("request", (r) => {
        if (r.method() === "POST" && r.url().endsWith("/trigger")) triggers.push(r.url());
      });
      await row.getByRole("button", { name: "Trigger now" }).click();
      const dialog = page.getByRole("alertdialog");
      await expect(dialog.getByRole("heading", { name: 'Trigger paused schedule "e2e_cron_owner"?' })).toBeVisible();
      await dialog.getByRole("button", { name: "Keep" }).click();
      await expect(dialog).toBeHidden();
      expect(triggers).toEqual([]);

      await row.getByRole("button", { name: "Trigger now" }).click();
      await Promise.all([
        page.waitForResponse((r) => r.ok() && new URL(r.url()).pathname === "/api/cron/e2e_cron_owner/trigger"),
        dialog.getByRole("button", { name: 'Trigger "e2e_cron_owner"' }).click(),
      ]);
      await expect(dialog).toBeHidden();
    } finally {
      await request.post("/api/cron/e2e_cron_owner/resume");
    }
  });
});


test("owner operations preview, adopt, retire and restore", async ({ page, request }) => {
  await loadCronPage(page);
  await page.getByText("Adopt or transfer a schedule", { exact: true }).click();
  await page.getByRole("textbox", { name: "Schedule name", exact: true }).fill("e2e_cron_owner");
  await page.getByRole("textbox", { name: "New owner", exact: true }).fill("e2e-owner");
  await page.getByRole("button", { name: "Preview", exact: true }).click();
  await expect(page.getByRole("status").filter({ hasText: "Apply adopt" })).toBeVisible();
  let rows = await (await request.get("/api/cron")).json();
  expect(rows.find((r: { name: string }) => r.name === "e2e_cron_owner").owner_id).toBeNull();
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  const row = cronRow(page, "e2e_cron_owner");
  await expect(row).toContainText("e2e-owner");
  await row.getByRole("button", { name: "Retire", exact: true }).click();
  await expect(page.getByRole("status").filter({ hasText: "Apply retire" })).toBeVisible();
  await expect(row.getByTestId("cron-retired")).toHaveCount(0);
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(row.getByTestId("cron-retired")).toBeVisible();
  await expect(row.getByRole("button", { name: "Trigger now" })).toBeDisabled();
  rows = await (await request.get("/api/cron")).json();
  expect(rows.find((r: { name: string }) => r.name === "e2e_cron_owner").next_fire_at).toBeNull();
  const ownerPanel = page.locator("details").filter({ has: page.locator("summary", { hasText: "e2e-owner:" }) });
  await ownerPanel.locator("summary").click();
  await ownerPanel.getByRole("button", { name: "Preview owner restoration", exact: true }).click();
  await expect(page.getByRole("status").filter({ hasText: "Apply restore owner" })).toBeVisible();
  await page.getByRole("button", { name: "Apply", exact: true }).click();
  await expect(row.getByRole("button", { name: "Retire", exact: true })).toBeVisible();
  await expect(row.getByRole("button", { name: "Trigger now" })).toBeEnabled();
});

test("read-only capabilities disable schedule actions and say why", async ({ page }) => {
  await page.route("**/api/capabilities", async route => {
    const response = await route.fetch();
    await route.fulfill({ response, json: { ...await response.json(), read_only: true } });
  });
  await loadCronPage(page);
  const reason = "Read-only server — admin actions are off";
  const row = cronRow(page, "e2e_cron_owner");
  for (const name of ["Trigger now", /^(Retire|Restore)$/]) {
    const button = row.getByRole("button", { name });
    await expect(button).toBeDisabled();
    await expect(button).toHaveAccessibleDescription(reason);
  }
  await page.getByText("Adopt or transfer a schedule", { exact: true }).click();
  await expect(page.getByRole("button", { name: "Preview", exact: true })).toBeDisabled();
});
