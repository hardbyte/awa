import { test, expect } from "@playwright/test";

test.describe("Kinds page", () => {
  test("navigate to /kinds, heading visible", async ({ page }) => {
    await page.goto("/kinds");
    await expect(page.getByRole("heading", { name: "Job kinds" })).toBeVisible();
  });

  test("kinds table renders descriptor-backed and legacy kinds", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/kinds") && r.ok()),
      page.goto("/kinds"),
    ]);

    const kindsTable = page.getByRole("grid", { name: "Job kinds" });
    await expect(kindsTable).toBeVisible();

    // Descriptor-backed kind renders its display name + description.
    await expect(kindsTable.getByText("E2E Job")).toBeVisible();
    await expect(kindsTable.getByRole("link", { name: "E2E Job" })).toHaveAttribute("title", "e2e_job");
    await expect(
      kindsTable.getByText("End-to-end job kind used for UI coverage")
    ).toBeVisible();

    // Legacy kind has no descriptor, so its raw name is the display label
    // and the status column shows "Not declared", not "Live". Match the
    // badge exactly — "Not declared" also appears inside the
    // descriptor-sync subtitle below the kind name, which would otherwise
    // trip Playwright's strict-mode multi-match check.
    await expect(kindsTable.getByText("legacy_job")).toBeVisible();
    const legacyRow = kindsTable.getByRole("row", { name: /legacy_job/ });
    await expect(legacyRow.getByText("Not declared", { exact: true })).toBeVisible();
  });

  test("clicking a kind opens its page", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/kinds") && r.ok()),
      page.goto("/kinds"),
    ]);

    await page.getByRole("grid", { name: "Job kinds" }).getByRole("link", { name: "E2E Job" }).click();
    await expect(page).toHaveURL(/\/kinds\/e2e_job$/);
    await expect(page.getByRole("heading", { level: 1, name: "E2E Job" })).toBeVisible();
  });

  test("the jobs link beside a kind opens its jobs", async ({ page }) => {
    await page.goto("/kinds");
    await page.getByRole("grid", { name: "Job kinds" }).getByRole("link", { name: "Jobs of kind e2e_job" }).click();
    await page.waitForURL(/\/jobs\?/);
    expect(new URL(page.url()).searchParams.get("q")).toBe("kind:e2e_job");
  });

  test("route-mocked stale and drift badges render", async ({ page }) => {
    await page.route("**/api/kinds", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            kind: "mock_kind",
            display_name: "Mock Kind",
            description: "Descriptor health coverage",
            owner: "qa-platform",
            docs_url: null,
            tags: [],
            extra: {},
            descriptor_last_seen_at: "2026-03-01T00:00:00Z",
            descriptor_stale: true,
            descriptor_mismatch: true,
            job_count: 4,
            queue_count: 2,
            completed_last_hour: 1,
          },
        ]),
      });
    });

    await page.goto("/kinds");
    const kindsTable = page.getByRole("grid", { name: "Job kinds" });
    await expect(kindsTable.getByText("Descriptor stale", { exact: true })).toBeVisible();
    await expect(kindsTable.getByText("Descriptor drift", { exact: true })).toBeVisible();
    await expect(kindsTable.getByText("Descriptor drift across live runtimes")).toBeVisible();
  });
  test("a kind's declared output type and schema render", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/kinds") && r.ok()),
      page.goto("/kinds"),
    ]);

    const kindsTable = page.getByRole("grid", { name: "Job kinds" });
    const row = kindsTable.getByRole("row", { name: /E2E Job/ });
    await expect(row.getByText("E2eSummary", { exact: true })).toBeVisible();
    await row.getByText("Schema", { exact: true }).click();
    await expect(row.getByText("integer")).toBeVisible();

    // A kind that declares no output shows no output line.
    const legacyRow = kindsTable.getByRole("row", { name: /legacy_job/ });
    await expect(legacyRow.getByText(/^Output/)).toHaveCount(0);
  });

  function mockKind(overrides: Record<string, unknown> = {}) {
    return {
      kind: "mock_kind", display_name: null, description: null, owner: null, docs_url: null, tags: [], extra: {},
      output: null, descriptor_last_seen_at: "2026-03-01T00:00:00Z", descriptor_stale: false, descriptor_mismatch: false,
      job_count: 20, queue_count: 2, completed_last_hour: 0,
      ...overrides,
    };
  }

  test("Failed and Retryable columns show the server's per-kind counts", async ({ page }) => {
    await page.route("**/api/kinds", (route) =>
      route.fulfill({ json: [mockKind({ queues: ["a", "b"], available: 3, retryable: 4, running: 1, failed: 6, dead_lettered: 2 })] })
    );
    await page.goto("/kinds");
    const table = page.getByRole("grid", { name: "Job kinds" });
    await expect(table.getByRole("columnheader", { name: "Failed" })).toBeVisible();
    await expect(table.getByRole("columnheader", { name: "Retryable" })).toBeVisible();
    const row = table.getByRole("row", { name: /mock_kind/ });
    await expect(row.getByRole("gridcell")).toHaveText(["20", "2", "6", "4", "0", "—", ""]);
    await row.getByRole("link", { name: "Failed jobs of mock_kind: 6" }).click();
    await page.waitForURL(/\/jobs\?/);
    const url = new URL(page.url());
    expect(url.searchParams.get("state")).toBe("failed");
    expect(url.searchParams.get("dead_lettered")).toBe("false");
    expect(url.searchParams.get("q")).toBe("kind:mock_kind");
  });

  test("an older server's kinds keep the columns they had", async ({ page }) => {
    await page.route("**/api/kinds", (route) => route.fulfill({ json: [mockKind()] }));
    await page.goto("/kinds");
    const table = page.getByRole("grid", { name: "Job kinds" });
    await expect(table.getByRole("row", { name: /mock_kind/ })).toBeVisible();
    await expect(table.getByRole("columnheader", { name: "Failed" })).toHaveCount(0);
    await expect(table.getByRole("columnheader", { name: "Retryable" })).toHaveCount(0);
  });

  test("phone cards show a kind's failed and retrying counts", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.route("**/api/kinds", (route) => route.fulfill({ json: [mockKind({ queues: ["a", "b"], retryable: 4, failed: 6 })] }));
    await page.goto("/kinds");
    const cards = page.getByRole("list", { name: "Job kinds" });
    await expect(cards).toContainText("6 failed");
    await expect(cards).toContainText("4 retrying");
  });
});
