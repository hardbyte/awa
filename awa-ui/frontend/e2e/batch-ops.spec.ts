import { test, expect } from "@playwright/test";

test.describe("Batch operations page", () => {
  test("previews and submits a set-priority operation", async ({ page }) => {
    let submitted = false;

    await page.route("**/api/runtime", (route) =>
      route.fulfill({ json: { total_instances: 0, live_instances: 0, stale_instances: 0, healthy_instances: 0, leader_instances: 0, instances: [] } })
    );
    await page.route("**/api/capabilities", (route) =>
      route.fulfill({ json: { read_only: false, poll_interval_ms: 2000 } })
    );
    await page.route("**/api/batch-ops?limit=50", (route) =>
      route.fulfill({
        json: submitted
          ? [
              {
                id: "11111111-1111-4111-8111-111111111111",
                op_kind: "set_priority",
                filter: { queue: "e2e_test" },
                spec: { op_kind: "set_priority", priority: 1 },
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
              },
            ]
          : [],
      })
    );
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
        submitted = true;
        await route.fulfill({
          json: {
            id: "11111111-1111-4111-8111-111111111111",
            op_kind: "set_priority",
            filter: body.filter,
            spec: body.spec,
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
          },
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
    await expect(page.getByText("11111111")).toBeVisible();
  });
});
