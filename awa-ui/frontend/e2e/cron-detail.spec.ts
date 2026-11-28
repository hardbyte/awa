import { test, expect, type Page } from "@playwright/test";

function schedule(overrides = {}) {
  const now = Date.now();
  return {
    owner_id: null,
    retired_at: null,
    retired_by: null,
    retired_revision: null,
    name: "nightly/report run",
    cron_expr: "30 2 * * *",
    timezone: "Pacific/Auckland",
    kind: "e2e_job",
    queue: "e2e_test",
    args: { report: "daily" },
    priority: 2,
    max_attempts: 5,
    tags: ["reports"],
    metadata: {},
    last_enqueued_at: new Date(now - 3_600_000).toISOString(),
    next_fire_at: new Date(now + 3_600_000).toISOString(),
    created_at: new Date(now - 86_400_000).toISOString(),
    updated_at: new Date(now - 86_400_000).toISOString(),
    paused_at: null,
    paused_by: null,
    ...overrides,
  };
}

async function serveCron(page: Page, rows: unknown[]) {
  await page.route("**/api/cron", (route) =>
    route.request().method() === "GET" ? route.fulfill({ json: rows }) : route.fallback()
  );
}

test.describe("Schedule page", () => {
  test("shows the definition and links its kind and queue", async ({ page }) => {
    await serveCron(page, [schedule()]);
    await page.goto(`/cron/${encodeURIComponent("nightly/report run")}`);
    await expect(page.getByRole("heading", { level: 1, name: "nightly/report run" })).toBeVisible();
    await expect(page.getByText("Daily at 02:30")).toBeVisible();
    await expect(page.getByText("Pacific/Auckland").first()).toBeVisible();
    await expect(page.getByTestId("tag-chip").filter({ hasText: "reports" })).toBeVisible();
    await expect(page.getByTestId("cron-overdue")).toHaveCount(0);

    await page.getByRole("link", { name: "e2e_job", exact: true }).first().click();
    await expect(page).toHaveURL(/\/kinds\/e2e_job$/);
    await page.goBack();
    await page.getByRole("link", { name: "e2e_test", exact: true }).first().click();
    await expect(page).toHaveURL(/\/queues\/e2e_test$/);
  });

  test("the list opens a schedule whose name needs encoding", async ({ page }) => {
    await serveCron(page, [schedule()]);
    await page.goto("/cron");
    await page.getByRole("link", { name: "nightly/report run", exact: true }).click();
    await expect(page.getByRole("heading", { level: 1, name: "nightly/report run" })).toBeVisible();
  });

  test("a schedule that missed its fire reads as overdue", async ({ page }) => {
    await serveCron(page, [schedule({ next_fire_at: new Date(Date.now() - 600_000).toISOString() })]);
    await page.goto(`/cron/${encodeURIComponent("nightly/report run")}`);
    await expect(page.getByTestId("cron-overdue")).toBeVisible();
  });

  test("links the jobs of its kind in its queue", async ({ page }) => {
    await serveCron(page, [schedule()]);
    await page.goto(`/cron/${encodeURIComponent("nightly/report run")}`);
    await page.getByRole("link", { name: /Jobs of this kind in this queue/ }).click();
    await page.waitForURL(/\/jobs\?/);
    expect(new URL(page.url()).searchParams.get("q")).toBe("kind:e2e_job queue:e2e_test");
  });

  test("links the jobs it enqueued, quoting a name with spaces", async ({ page }) => {
    await serveCron(page, [schedule()]);
    await page.goto(`/cron/${encodeURIComponent("nightly/report run")}`);
    await page.getByTestId("cron-jobs-link").click();
    await page.waitForURL(/\/jobs\?/);
    expect(new URL(page.url()).searchParams.get("q")).toBe('kind:e2e_job cron:"nightly/report run"');
    await expect(page.getByText('cron:"nightly/report run"', { exact: true })).toBeVisible();
  });

  test("its jobs link lists only the jobs the schedule enqueued", async ({ page }) => {
    await page.goto("/cron/e2e_cron_owner");
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs?") && r.url().includes("cron=e2e_cron_owner") && r.ok()),
      page.getByRole("link", { name: "Jobs from this schedule" }).click(),
    ]);
    expect(new URL(page.url()).searchParams.get("q")).toBe("kind:e2e_job cron:e2e_cron_owner");
    await expect(page.getByText("#800005").filter({ visible: true }).first()).toBeVisible();
    await expect(page.getByText("#800006")).toHaveCount(0);
  });

  test("retirement is previewed and applied on the page", async ({ page }) => {
    let applied: unknown = null;
    await serveCron(page, [schedule()]);
    await page.route("**/api/capabilities", (route) =>
      route.fulfill({ json: { read_only: false, poll_interval_ms: 2000, instance_name: null, instance_color: null, peers: [] } })
    );
    await page.route("**/api/cron/owner-action", async (route) => {
      const body = route.request().postDataJSON();
      if (body.apply) applied = body;
      await route.fulfill({
        json: { action: { action: "retire", name: "nightly/report run" }, schedules: ["nightly/report run"], applied: !!body.apply },
      });
    });
    await page.goto(`/cron/${encodeURIComponent("nightly/report run")}`);
    await page.getByRole("button", { name: "Preview retirement" }).click();
    const notice = page.getByRole("status").filter({ hasText: "Apply retire" });
    await expect(notice).toContainText("nightly/report run");
    await notice.getByRole("button", { name: "Apply" }).click();
    await expect.poll(() => applied).toMatchObject({ action: "retire", name: "nightly/report run", apply: true });
  });

  test("an unknown schedule shows not found", async ({ page }) => {
    await page.goto("/cron/no-such-schedule");
    const notFound = page.getByTestId("resource-not-found");
    await expect(notFound.getByRole("heading", { name: 'Schedule "no-such-schedule" not found' })).toBeVisible();
    await notFound.getByRole("link", { name: "Back to schedules" }).click();
    await expect(page).toHaveURL(/\/cron$/);
  });
});
