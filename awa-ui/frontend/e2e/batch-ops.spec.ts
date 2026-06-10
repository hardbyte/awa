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
    await page.locator("select").selectOption("move_queue");
    await page.getByLabel("Queue filter").fill("e2e_test");
    await page.getByLabel("Kind filter").fill("e2e_job");
    await page.getByLabel("IDs filter").fill("800006");
    await page.getByRole("textbox", { name: "Priority" }).fill("2");
    await page.getByLabel("Destination queue").fill("escalations");
    await page.getByRole("button", { name: "Preview" }).click();
    await expect(page.getByText("Preview matched 1 job(s)")).toBeVisible();
    await page.getByRole("button", { name: "Submit" }).click();
    await expect(page.getByRole("gridcell", { name: "move queue" })).toBeVisible();
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
