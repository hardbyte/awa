import { test, expect } from "@playwright/test";

function operation(overrides = {}) {
  return {
    id: "11111111-1111-4111-8111-111111111111",
    op_kind: "set_priority",
    filter: { queue: "e2e_test" },
    spec: { priority: 1 },
    state: "pending",
    submitted_by: "ui",
    submitted_at: new Date().toISOString(),
    started_at: null,
    finalized_at: null,
    cursor: null,
    total_matched: null,
    processed: 0,
    skipped: 0,
    errored: 0,
    last_error: null,
    runner_instance: null,
    retention_until: null,
    updated_at: new Date().toISOString(),
    ...overrides,
  };
}

async function stubSharedApi(page, operations) {
  await page.route("**/api/runtime", (route) =>
    route.fulfill({ json: { total_instances: 0, live_instances: 0, stale_instances: 0, healthy_instances: 0, leader_instances: 0, instances: [] } })
  );
  await page.route("**/api/capabilities", (route) =>
    route.fulfill({ json: { read_only: false, poll_interval_ms: 2000 } })
  );
  await page.route("**/api/batch-ops*", (route) => {
    if (route.request().method() !== "GET") return route.fallback();
    const url = new URL(route.request().url());
    if (url.pathname !== "/api/batch-ops") return route.fallback();
    return route.fulfill({ json: operations });
  });
}

test.describe("Batch operations page", () => {
  test("previews and submits a set-priority operation", async ({ page }) => {
    const operations = [];

    await stubSharedApi(page, operations);
    await page.route("**/api/batch-ops/preview", (route) =>
      route.fulfill({ json: { total_matched: 2, sample: [] } })
    );
    await page.route("**/api/batch-ops", async (route) => {
      if (route.request().method() === "POST") {
        const body = route.request().postDataJSON();
        expect(body).toMatchObject({
          op_kind: "set_priority",
          filter: { queue: "e2e_test" },
          spec: { priority: 1 },
        });
        const submitted = operation({
          filter: body.filter,
          spec: body.spec,
        });
        operations.push(submitted);
        await route.fulfill({
          json: submitted,
        });
        return;
      }
      await route.fallback();
    });

    await page.goto("/batch-ops");
    await expect(page.getByRole("heading", { name: "Batch Operations" })).toBeVisible();
    await page.getByLabel("Queue filter").fill("e2e_test");
    await page.getByRole("textbox", { name: "Priority" }).fill("1");
    await page.getByRole("button", { name: "Preview" }).click();
    await expect(page.getByText("Preview matched 2 job(s)")).toBeVisible();
    await page.getByRole("button", { name: "Submit" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toContainText(
      "Set priority to 1 on 2 available or scheduled jobs with queue e2e_test."
    );
    await dialog.getByRole("button", { name: "Submit" }).click();
    await expect(page.getByRole("gridcell", { name: "11111111" })).toBeVisible();
  });

  test("submits a move-queue operation with filters", async ({ page }) => {
    const operations = [];

    await stubSharedApi(page, operations);
    await page.route("**/api/batch-ops/preview", async (route) => {
      const body = route.request().postDataJSON();
      expect(body).toMatchObject({
        op_kind: "move_queue",
        filter: {
          queue: "e2e_test",
          kind: "e2e_job",
          ids: [800006],
        },
        spec: {
          queue: "escalations",
          priority: 2,
        },
      });
      await route.fulfill({ json: { total_matched: 1, sample: [] } });
    });
    await page.route("**/api/batch-ops", async (route) => {
      if (route.request().method() === "POST") {
        const body = route.request().postDataJSON();
        expect(body).toMatchObject({
          op_kind: "move_queue",
          filter: {
            queue: "e2e_test",
            kind: "e2e_job",
            ids: [800006],
          },
          spec: {
            queue: "escalations",
            priority: 2,
          },
        });
        const submitted = operation({
          op_kind: "move_queue",
          filter: body.filter,
          spec: body.spec,
        });
        operations.push(submitted);
        await route.fulfill({ json: submitted });
        return;
      }
      await route.fallback();
    });

    await page.goto("/batch-ops");
    await page.getByRole("combobox", { name: /^Operation/ }).selectOption("move_queue");
    await page.getByLabel("Queue filter").fill("e2e_test");
    await page.getByLabel("Kind filter").fill("e2e_job");
    await page.getByLabel("IDs filter").fill("800006");
    await page.getByRole("textbox", { name: "Priority" }).fill("2");
    await page.getByLabel("Destination queue").fill("escalations");
    await page.getByRole("button", { name: "Preview" }).click();
    await expect(page.getByText("Preview matched 1 job(s)")).toBeVisible();
    await page.getByRole("button", { name: "Submit" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toContainText(
      "Move 1 available or scheduled jobs with queue e2e_test, kind e2e_job, id 800006 to queue escalations at priority 2."
    );
    await dialog.getByRole("button", { name: "Submit" }).click();
    await expect(page.getByRole("gridcell", { name: "move queue" })).toBeVisible();
  });

  test("sends tags, state and created bounds in the shared filter", async ({ page }) => {
    await stubSharedApi(page, []);
    await page.route("**/api/batch-ops/preview", async (route) => {
      const body = route.request().postDataJSON();
      expect(body.filter).toEqual({
        queue: "e2e_test",
        tag: ["e2e", "priority"],
        state: "scheduled",
        created_at_gte: "2026-10-01T00:00:00.000Z",
      });
      await route.fulfill({ json: { total_matched: 4, sample: [] } });
    });

    await page.goto("/batch-ops?queue=e2e_test&tags=e2e%2C%20priority&state=scheduled");
    await page.getByLabel("Created from").fill("2026-10-01T00:00:00Z");
    await page.getByRole("button", { name: "Preview" }).click();
    await expect(page.getByTestId("batch-preview-count")).toHaveText(
      "4 eligible job(s) would change."
    );
  });

  test("Submit waits for a preview of the form's current filter", async ({ page }) => {
    await stubSharedApi(page, []);
    await page.route("**/api/batch-ops/preview", (route) =>
      route.fulfill({ json: { total_matched: 3, sample: [] } })
    );

    await page.goto("/batch-ops?queue=e2e_test");
    const submit = page.getByRole("button", { name: "Submit" });
    await expect(submit).toBeDisabled();
    await expect(page.getByTestId("batch-submit-hint")).toHaveText("Preview first");

    await page.getByRole("button", { name: "Preview" }).click();
    await expect(page.getByTestId("batch-preview-count")).toHaveText(
      "3 eligible job(s) would change."
    );
    await expect(submit).toBeEnabled();
    await expect(page.getByTestId("batch-submit-hint")).toHaveCount(0);

    await page.getByLabel("Kind filter").fill("e2e_job");
    await expect(submit).toBeDisabled();
    await expect(page.getByTestId("batch-submit-hint")).toHaveText(
      "Filter changed; preview again"
    );
    await expect(page.getByTestId("batch-preview-count")).toHaveCount(0);

    await page.getByLabel("State filter").selectOption("scheduled");
    await page.getByLabel("Kind filter").fill("");
    await expect(submit).toBeDisabled();

    // Back to the previewed filter, the preview stands for it again.
    await page.getByLabel("State filter").selectOption({ label: "Available or scheduled" });
    await expect(submit).toBeEnabled();
  });

  test("a state alone doesn't narrow the filter", async ({ page }) => {
    const previews: string[] = [];
    page.on("request", (r) => {
      if (r.url().includes("/api/batch-ops/preview")) previews.push(r.url());
    });
    await stubSharedApi(page, []);

    await page.goto("/batch-ops?state=available");
    await expect(page.getByTestId("batch-narrowing")).toHaveText(
      "Add a queue, kind, tag, ID or created bound."
    );
    await expect(page.getByRole("button", { name: "Preview" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Submit" })).toBeDisabled();

    await page.getByLabel("Tags filter").fill("e2e");
    await expect(page.getByTestId("batch-narrowing")).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Preview" })).toBeEnabled();
    expect(previews).toEqual([]);
  });

  test("the confirm step shows an at-least count and cancels without submitting", async ({ page }) => {
    const submits: string[] = [];
    page.on("request", (r) => {
      const url = new URL(r.url());
      if (r.method() === "POST" && url.pathname === "/api/batch-ops") submits.push(r.url());
    });
    await stubSharedApi(page, []);
    await page.route("**/api/batch-ops/preview", (route) =>
      route.fulfill({ json: { total_matched: { kind: "at_least", value: 10000 }, sample: [] } })
    );

    await page.goto("/batch-ops?kind=e2e_job&state=scheduled");
    await page.getByRole("button", { name: "Preview" }).click();
    await expect(page.getByTestId("batch-preview-count")).toHaveText(
      "At least 10,000 eligible job(s) would change."
    );
    await page.getByRole("button", { name: "Submit" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toContainText(
      "Set priority to 1 on at least 10,000 scheduled jobs with kind e2e_job."
    );
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).toHaveCount(0);
    expect(submits).toEqual([]);
  });

  test("a created date that doesn't parse blocks preview and submit", async ({ page }) => {
    await stubSharedApi(page, []);
    await page.goto("/batch-ops?queue=e2e_test");
    await page.getByLabel("Created before").fill("2026-10-32");
    await expect(page.getByRole("alert")).toHaveText("Created before is not a date");
    await expect(page.getByRole("button", { name: "Preview" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Submit" })).toBeDisabled();
  });

  test("an id beyond the exact range blocks preview and submit", async ({ page }) => {
    await stubSharedApi(page, []);
    await page.goto("/batch-ops?queue=e2e_test");
    await page.getByLabel("IDs filter").fill("9007199254740993");
    await expect(page.getByRole("alert")).toHaveText("IDs must be whole job ids");
    await expect(page.getByRole("button", { name: "Preview" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Submit" })).toBeDisabled();
  });

  test("requests cancellation for an active operation", async ({ page }) => {
    const operations = [
      operation({
        state: "running",
        total_matched: 10,
        processed: 3,
      }),
    ];

    await stubSharedApi(page, operations);
    await page.route("**/api/batch-ops/11111111-1111-4111-8111-111111111111", async (route) => {
      expect(route.request().method()).toBe("PATCH");
      expect(route.request().postDataJSON()).toMatchObject({ state: "cancelling" });
      operations[0] = {
        ...operations[0],
        state: "cancelling",
        updated_at: new Date().toISOString(),
      };
      await route.fulfill({ json: operations[0] });
    });

    await page.goto("/batch-ops");
    await expect(page.getByRole("gridcell", { name: "running" })).toBeVisible();
    await page.getByRole("button", { name: "Cancel" }).click();
    await expect(page.getByText("Cancellation requested")).toBeVisible();
    await expect(page.getByRole("gridcell", { name: "cancelling" })).toBeVisible();
  });
});
