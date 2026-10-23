import { test, expect, type Page, type Response } from "@playwright/test";

function isQueuesListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return (
    response.ok() &&
    response.request().method() === "GET" &&
    url.pathname === "/api/queues"
  );
}

async function loadQueuesPage(page: Page) {
  await Promise.all([
    page.waitForResponse(isQueuesListResponse),
    page.goto("/queues"),
  ]);
}

test.describe("Queues page", () => {
  test("navigate to /queues, heading visible", async ({ page }) => {
    await page.goto("/queues");
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
  });

  test("queue table renders with seeded data", async ({ page }) => {
    await loadQueuesPage(page);

    // Desktop table (visible at default viewport)
    const queueTable = page.getByRole("table", { name: "Queue pressure" });
    await expect(queueTable).toBeVisible();

    for (const header of [
      "Queue",
      "Available",
      "Oldest available",
      "Running",
      "Retryable / failed",
      "Completed last hour",
      "Controls",
    ]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header, exact: true }),
      ).toBeVisible();
    }

    // Descriptor-backed queue shows display name and raw key
    await expect(queueTable.getByText("E2E Queue")).toBeVisible();
    await expect(queueTable.getByText("e2e_test")).toBeVisible();
    await expect(
      queueTable.getByRole("link", { name: "legacy_queue", exact: true }),
    ).toBeVisible();
  });

  test("queue table shows descriptor description", async ({ page }) => {
    await loadQueuesPage(page);

    const queueTable = page.getByRole("table", { name: "Queue pressure" });
    await expect(
      queueTable.getByText("End-to-end queue used for UI coverage"),
    ).toBeVisible();
  });

  test("queue table surfaces available wait and controls", async ({ page }) => {
    await loadQueuesPage(page);
    const queueTable = page.getByRole("table", { name: "Queue pressure" });
    await expect(
      queueTable.getByRole("columnheader", { name: "Oldest available" }),
    ).toBeVisible();
    await expect(
      queueTable.getByRole("columnheader", { name: "Controls" }),
    ).toBeVisible();
  });

  test("queue table surfaces DLQ policy from runtime config", async ({
    page,
  }) => {
    await page.route("**/api/queues", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            queue: "dlq_off_queue",
            display_name: "DLQ Off Queue",
            description: null,
            owner: null,
            docs_url: null,
            tags: [],
            extra: {},
            descriptor_last_seen_at: "2026-03-01T00:00:00Z",
            descriptor_stale: false,
            descriptor_mismatch: false,
            total_queued: 1,
            scheduled: 0,
            available: 1,
            retryable: 0,
            running: 0,
            failed: 1,
            waiting_external: 0,
            completed_last_hour: 0,
            lag_seconds: 1,
            paused: false,
          },
        ]),
      });
    });
    await page.route("**/api/queues/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            queue: "dlq_off_queue",
            instance_count: 1,
            live_instances: 1,
            stale_instances: 0,
            healthy_instances: 1,
            total_in_flight: 0,
            overflow_held_total: 0,
            config_mismatch: false,
            config: {
              mode: "weighted",
              max_workers: null,
              min_workers: 1,
              weight: 1,
              global_max_workers: 16,
              poll_interval_ms: 200,
              deadline_duration_secs: 300,
              priority_aging_interval_secs: 60,
              dlq_enabled: false,
              rate_limit: null,
            },
          },
        ]),
      });
    });

    await page.goto("/queues");

    const queueTable = page.getByRole("table", { name: "Queue pressure" });
    await expect(
      queueTable.getByRole("rowheader", { name: /DLQ Off Queue/ }),
    ).toBeVisible();
    await expect(
      queueTable.getByText("DLQ off", { exact: true }),
    ).toBeVisible();
  });

  test("click queue navigates to queue detail", async ({ page }) => {
    await loadQueuesPage(page);

    const queueTable = page.getByRole("table", { name: "Queue pressure" });
    const queueLink = queueTable.getByRole("link", { name: "E2E Queue" });
    await expect(queueLink).toBeVisible();

    await queueLink.click();

    await page.waitForURL(/\/queues\/e2e_test/);
    await expect(
      page.getByRole("heading", { name: "E2E Queue" }),
    ).toBeVisible();
  });

  test("queue detail renders descriptor metadata", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) => /\/api\/queues\/e2e_test$/.test(r.url()) && r.ok(),
      ),
      page.goto("/queues/e2e_test"),
    ]);

    await expect(
      page.getByRole("heading", { name: "E2E Queue" }),
    ).toBeVisible();
    await expect(page.getByText("e2e_test").first()).toBeVisible();
    await expect(
      page.getByText("End-to-end queue used for UI coverage"),
    ).toBeVisible();
    await expect(page.getByText("qa-platform")).toBeVisible();
    await expect(page.getByText("critical")).toBeVisible();
  });

  test("queue detail links to filtered DLQ rows when DLQ is enabled", async ({
    page,
  }) => {
    await page.route("**/api/queues/payments", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          queue: "payments",
          display_name: "Payments",
          description: "Customer payment processing",
          owner: "platform",
          docs_url: null,
          tags: ["critical"],
          extra: {},
          descriptor_last_seen_at: "2026-03-01T00:00:00Z",
          descriptor_stale: false,
          descriptor_mismatch: false,
          total_queued: 1,
          scheduled: 0,
          available: 1,
          retryable: 0,
          running: 0,
          failed: 1,
          waiting_external: 0,
          completed_last_hour: 0,
          lag_seconds: 1,
          paused: false,
        }),
      });
    });
    await page.route("**/api/queues/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            queue: "payments",
            instance_count: 1,
            live_instances: 1,
            stale_instances: 0,
            healthy_instances: 1,
            total_in_flight: 0,
            overflow_held_total: 0,
            config_mismatch: false,
            config: {
              mode: "weighted",
              max_workers: null,
              min_workers: 1,
              weight: 1,
              global_max_workers: 16,
              poll_interval_ms: 200,
              deadline_duration_secs: 300,
              priority_aging_interval_secs: 60,
              dlq_enabled: true,
              rate_limit: null,
            },
          },
        ]),
      });
    });
    await page.route("**/api/dlq/depth", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({ total: 0, by_queue: [] }),
      });
    });
    await page.route("**/api/dlq**", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([]),
      });
    });

    await page.goto("/queues/payments");

    const dlqLink = page.getByRole("link", { name: "View DLQ rows" });
    await expect(dlqLink).toBeVisible();
    await dlqLink.click();
    await expect(page).toHaveURL(/\/dlq\?q=queue%3Apayments$/);
  });

  test("legacy queue still renders without descriptors", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) => /\/api\/queues\/legacy_queue$/.test(r.url()) && r.ok(),
      ),
      page.goto("/queues/legacy_queue"),
    ]);

    await expect(
      page.getByRole("heading", { name: "legacy_queue" }),
    ).toBeVisible();
    await expect(page.getByText("Descriptor health")).toBeVisible();
    await expect(page.getByText("Not declared by a live worker")).toBeVisible();
  });

  test("route-mocked descriptor stale and drift badges render", async ({
    page,
  }) => {
    await page.route("**/api/queues", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            queue: "mock_queue",
            display_name: "Mock Queue",
            description: "Descriptor health coverage",
            owner: null,
            docs_url: null,
            tags: [],
            extra: {},
            descriptor_last_seen_at: "2026-03-01T00:00:00Z",
            descriptor_stale: true,
            descriptor_mismatch: true,
            total_queued: 1,
            scheduled: 0,
            available: 1,
            retryable: 0,
            running: 0,
            failed: 0,
            waiting_external: 0,
            completed_last_hour: 0,
            lag_seconds: 1,
            paused: false,
          },
        ]),
      });
    });
    await page.route("**/api/queues/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([]),
      });
    });

    await page.goto("/queues");
    const queueTable = page.getByRole("table", { name: "Queue pressure" });
    await expect(
      queueTable.getByText("Descriptor stale", { exact: true }),
    ).toBeVisible();
    await expect(
      queueTable.getByText("Descriptor drift across live runtimes"),
    ).toBeVisible();
  });

  function queueRow(page: Page) {
    return page
      .getByRole("table", { name: "Queue pressure" })
      .getByRole("row", { name: /E2E Queue/ });
  }

  test("Pause or Resume and Drain controls are available", async ({ page }) => {
    await loadQueuesPage(page);
    await expect(
      queueRow(page).getByRole("button", { name: /^(Pause|Resume)$/ }),
    ).toBeVisible();
    await expect(
      queueRow(page).getByRole("button", { name: "Drain…" }),
    ).toBeVisible();
  });

  test("Pause and Resume execute mutations", async ({ page }) => {
    await loadQueuesPage(page);
    const row = queueRow(page);
    if (
      await row.getByRole("button", { name: "Resume", exact: true }).isVisible()
    ) {
      await row.getByRole("button", { name: "Resume", exact: true }).click();
    }
    await expect(
      row.getByRole("button", { name: "Pause", exact: true }),
    ).toBeVisible();
    const [pauseResponse] = await Promise.all([
      page.waitForResponse(
        (response) =>
          response.url().endsWith("/pause") &&
          response.request().method() === "POST",
      ),
      row.getByRole("button", { name: "Pause", exact: true }).click(),
    ]);
    expect(pauseResponse.ok()).toBeTruthy();
    await expect(
      row.getByRole("button", { name: "Resume", exact: true }),
    ).toBeVisible();
    const [resumeResponse] = await Promise.all([
      page.waitForResponse(
        (response) =>
          response.url().endsWith("/resume") &&
          response.request().method() === "POST",
      ),
      row.getByRole("button", { name: "Resume", exact: true }).click(),
    ]);
    expect(resumeResponse.ok()).toBeTruthy();
  });

  test("Drain requires confirmation and can be cancelled", async ({ page }) => {
    await loadQueuesPage(page);
    await queueRow(page).getByRole("button", { name: "Drain…" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toBeVisible();
    await expect(
      dialog.getByText(/Running jobs will not be affected/),
    ).toBeVisible();
    await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
    await expect(dialog).not.toBeVisible();
  });
});
