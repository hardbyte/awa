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
      const next = retryResponses.shift() ?? { status: 200, json: { count: 0, skipped: 0, conflicts: [], done: true } };
      return route.fulfill(next);
    }
    if (url.pathname === "/api/dlq") {
      return route.fulfill({ json: [dlqRow(700001), dlqRow(700002), dlqRow(700003)] });
    }
    return route.fallback();
  });
  return requests;
}

/**
 * Hold each bulk-retry request until the test releases it, so a test can act
 * between batches and count exactly how many batches started.
 */
async function gateBatches(page: Page) {
  const pending: Array<(json: unknown) => void> = [];
  const waiters: Array<() => void> = [];
  let started = 0;
  await page.route("**/api/dlq/bulk-retry", async (route) => {
    started += 1;
    waiters.splice(0).forEach((wake) => wake());
    const json = await new Promise<unknown>((resolve) => pending.push(resolve));
    await route.fulfill({ json });
  });
  return {
    count: () => started,
    async started(n: number) {
      while (started < n) {
        await new Promise<void>((resolve) => waiters.push(resolve));
      }
    },
    release(json: unknown) {
      const next = pending.shift();
      if (!next) throw new Error("no batch is waiting");
      next(json);
    },
  };
}

function batch(count: number, after: number, extra: Record<string, unknown> = {}) {
  return {
    count,
    skipped: 0,
    conflicts: [],
    done: false,
    cursor: { stage: 0, after_job_id: after },
    ...extra,
  };
}

test.describe("DLQ bulk retry", () => {
  test("retries matching rows in batches until none remain", async ({ page }) => {
    const requests = await stubDlqPage(page, [
      { status: 200, json: batch(2, 700002) },
      { status: 200, json: batch(1, 700003, { done: true }) },
    ]);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await expect(page.getByText("Retried 3 DLQ row(s)")).toBeVisible();
    expect(requests).toEqual([
      { queue: QUEUE, batch_size: 1000 },
      { queue: QUEUE, batch_size: 1000, cursor: { stage: 0, after_job_id: 700002 } },
    ]);
  });

  test("skips a unique conflict, carries on, and names the job", async ({ page }) => {
    await stubDlqPage(page, [
      { status: 200, json: batch(2, 700003, { skipped: 1, conflicts: [700003] }) },
      { status: 200, json: batch(0, 700003, { done: true }) },
    ]);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await expect(
      page.getByText(
        "Retried 2 DLQ row(s); skipped 1 whose unique key a live job holds (700003)"
      )
    ).toBeVisible();
  });

  test("stops between batches when asked", async ({ page }) => {
    await stubDlqPage(page, []);
    const gate = await gateBatches(page);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await gate.started(1);
    gate.release(batch(2, 700002));
    await gate.started(2);
    await page.getByRole("button", { name: "Stop" }).click();
    gate.release(batch(2, 700004));

    await expect(page.getByText("Retried 4 DLQ row(s) before stopping")).toBeVisible();
    expect(gate.count()).toBe(2);
  });

  test("navigating away stops the retry", async ({ page }) => {
    await stubDlqPage(page, []);
    const gate = await gateBatches(page);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await gate.started(1);
    await page.getByRole("link", { name: "Queues" }).first().click();
    await page.waitForURL(/\/queues/);
    gate.release(batch(2, 700002));

    await expect(page.getByText("Retried 2 DLQ row(s) before stopping")).toBeVisible();
    expect(gate.count()).toBe(1);
  });

  test("waits while matching rows are held by another batch", async ({ page }) => {
    await stubDlqPage(page, [
      { status: 200, json: batch(0, 0, { busy: true }) },
      { status: 200, json: batch(3, 700003, { done: true }) },
    ]);

    await page.goto(`/dlq?q=queue:${QUEUE}`);
    await page.getByRole("button", { name: /Retry matching/ }).click();
    await page.getByRole("button", { name: "Retry", exact: true }).click();

    await expect(page.getByText("Retried 3 DLQ row(s)")).toBeVisible();
  });
});
