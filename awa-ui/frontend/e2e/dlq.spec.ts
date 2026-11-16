import { test, expect, type Page } from "@playwright/test";

const QUEUE = "e2e_dlq_drain";

function dlqRow(id: number) {
  return {
    id,
    kind: "e2e_dlq_job",
    queue: QUEUE,
    args: {},
    state: "failed",
    priority: 2,
    attempt: 3,
    run_lease: 1,
    max_attempts: 3,
    run_at: new Date().toISOString(),
    heartbeat_at: null,
    deadline_at: null,
    attempted_at: null,
    finalized_at: new Date().toISOString(),
    created_at: new Date().toISOString(),
    errors: null,
    metadata: {},
    tags: [],
    unique_key: null,
    callback_id: null,
    callback_timeout_at: null,
    callback_filter: null,
    callback_on_complete: null,
    callback_on_fail: null,
    callback_transform: null,
    progress: null,
    dlq_reason: "terminal_error",
    dlq_at: new Date().toISOString(),
    original_run_lease: 1,
  };
}

async function stubDlqPage(page: Page, retryResponses: Array<{ status: number; json: unknown }>) {
  const requests: unknown[] = [];
  await page.route("**/api/capabilities", (route) =>
    route.fulfill({ json: { read_only: false, poll_interval_ms: 2000 } })
  );
  await page.route("**/api/dlq**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname === "/api/dlq/depth") {
      return route.fulfill({
        json: { total: 3, by_queue: [{ queue: QUEUE, count: 3 }] },
      });
    }
    if (url.pathname === "/api/dlq/bulk-retry") {
      requests.push(route.request().postDataJSON());
      const next = retryResponses.shift() ?? { status: 200, json: { count: 0 } };
      return route.fulfill(next);
    }
    if (url.pathname === "/api/dlq") {
      return route.fulfill({ json: [dlqRow(700001), dlqRow(700002), dlqRow(700003)] });
    }
    return route.fallback();
  });
  return requests;
}

test.describe("DLQ bulk retry", () => {
  test("retries matching rows in batches until none remain", async ({ page }) => {
    const requests = await stubDlqPage(page, [
      { status: 200, json: { count: 2 } },
      { status: 200, json: { count: 1 } },
      { status: 200, json: { count: 0 } },
    ]);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await expect(page.getByText("Retried 3 DLQ row(s)")).toBeVisible();
    expect(requests).toEqual([
      { queue: QUEUE, batch_size: 1000 },
      { queue: QUEUE, batch_size: 1000 },
      { queue: QUEUE, batch_size: 1000 },
    ]);
  });

  test("stops on a unique conflict and names the job", async ({ page }) => {
    await stubDlqPage(page, [
      { status: 200, json: { count: 2 } },
      {
        status: 409,
        json: {
          error:
            "DLQ job 700003 conflicts with a live unique claim; its batch was not retried",
        },
      },
    ]);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await expect(
      page.getByText(/Retried 2 DLQ row\(s\), then stopped: DLQ job 700003 conflicts/)
    ).toBeVisible();
  });

  test("stops between batches when asked", async ({ page }) => {
    await stubDlqPage(page, []);
    await page.route("**/api/dlq/bulk-retry", async (route) => {
      await new Promise((resolve) => setTimeout(resolve, 200));
      await route.fulfill({ json: { count: 2 } });
    });

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await expect(page.getByRole("status").getByText(/2 of 3 retried/)).toBeVisible();
    await page.getByRole("button", { name: "Stop" }).click();
    await expect(page.getByText(/Stopped after retrying \d+ DLQ row\(s\)/)).toBeVisible();
  });
});
