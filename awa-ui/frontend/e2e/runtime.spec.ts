import { test, expect } from "@playwright/test";

test.describe("Runtime page", () => {
  test("navigate to /runtime, heading visible", async ({ page }) => {
    await page.goto("/runtime");
    await expect(page.getByRole("heading", { name: "Runtime" })).toBeVisible();
  });

  test("renders empty states when no runtime snapshots exist", async ({ page }) => {
    await page.route("**/api/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          total_instances: 0,
          live_instances: 0,
          stale_instances: 0,
          healthy_instances: 0,
          leader_instances: 0,
          instances: [],
        }),
      });
    });

    await page.route("**/api/queues/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([]),
      });
    });

    await page.goto("/runtime");

    await expect(
      page.getByText("No runtime snapshots yet. Start a worker to populate this view.")
    ).toBeVisible();
    await expect(page.getByText("No queue runtime snapshots yet.")).toBeVisible();
  });

  test("renders instance and queue runtime details from API data", async ({ page }) => {
    await page.route("**/api/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          total_instances: 2,
          live_instances: 2,
          stale_instances: 0,
          healthy_instances: 1,
          leader_instances: 1,
          instances: [
            {
              instance_id: "11111111-1111-4111-8111-111111111111",
              hostname: "worker-a",
              pid: 101,
              version: "0.4.0-dev",
              started_at: "2026-03-23T00:00:00Z",
              last_seen_at: "2026-03-23T00:00:05Z",
              snapshot_interval_ms: 10000,
              stale: false,
              healthy: true,
              postgres_connected: true,
              poll_loop_alive: true,
              heartbeat_alive: true,
              maintenance_alive: true,
              shutting_down: false,
              leader: true,
              global_max_workers: 16,
              queues: [
                {
                  queue: "email",
                  in_flight: 3,
                  overflow_held: 1,
                  config: {
                    mode: "weighted",
                    max_workers: null,
                    min_workers: 2,
                    weight: 3,
                    global_max_workers: 16,
                    poll_interval_ms: 200,
                    deadline_duration_secs: 300,
                    priority_aging_interval_secs: 60,
                    rate_limit: { max_rate: 5.5, burst: 10 },
                  },
                },
              ],
            },
            {
              instance_id: "22222222-2222-4222-8222-222222222222",
              hostname: "worker-b",
              pid: 202,
              version: "0.4.0-dev",
              started_at: "2026-03-23T00:01:00Z",
              last_seen_at: "2026-03-23T00:00:04Z",
              snapshot_interval_ms: 10000,
              stale: false,
              healthy: false,
              postgres_connected: true,
              poll_loop_alive: true,
              heartbeat_alive: false,
              maintenance_alive: true,
              shutting_down: false,
              leader: false,
              global_max_workers: 16,
              queues: [
                {
                  queue: "email",
                  in_flight: 1,
                  overflow_held: 0,
                  config: {
                    mode: "weighted",
                    max_workers: null,
                    min_workers: 2,
                    weight: 3,
                    global_max_workers: 16,
                    poll_interval_ms: 200,
                    deadline_duration_secs: 300,
                    priority_aging_interval_secs: 60,
                    rate_limit: { max_rate: 5.5, burst: 10 },
                  },
                },
              ],
            },
          ],
        }),
      });
    });

    await page.route("**/api/queues/runtime", async (route) => {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify([
          {
            queue: "email",
            instance_count: 2,
            live_instances: 2,
            stale_instances: 0,
            healthy_instances: 1,
            total_in_flight: 4,
            overflow_held_total: 1,
            config_mismatch: false,
            config: {
              mode: "weighted",
              max_workers: null,
              min_workers: 2,
              weight: 3,
              global_max_workers: 16,
              poll_interval_ms: 200,
              deadline_duration_secs: 300,
              priority_aging_interval_secs: 60,
              rate_limit: { max_rate: 5.5, burst: 10 },
            },
          },
        ]),
      });
    });

    await page.goto("/runtime");

    await expect(page.getByText("Cluster Summary")).toBeVisible();
    const instancesGrid = page.getByRole("grid", { name: "Runtime instances" });
    await expect(instancesGrid).toBeVisible();
    await expect(instancesGrid.getByRole("rowheader", { name: /worker-a/ })).toBeVisible();
    await expect(instancesGrid.getByRole("rowheader", { name: /worker-b/ })).toBeVisible();
    await expect(instancesGrid.getByText("instance 11111111")).toBeVisible();
    await expect(instancesGrid.getByText("Leader", { exact: true })).toBeVisible();
    await expect(instancesGrid.getByText("Degraded", { exact: true })).toBeVisible();
    const queueGrid = page.getByRole("grid", { name: "Queue runtime summary" });
    await expect(queueGrid).toBeVisible();
    await expect(queueGrid.getByRole("rowheader", { name: "email" })).toBeVisible();
    await expect(queueGrid.getByRole("gridcell", { name: "min 2 / w 3" })).toBeVisible();
    await expect(queueGrid.getByRole("gridcell", { name: "5.5/s (10)" })).toBeVisible();
    await expect(queueGrid.getByText("global 16")).toBeVisible();
    await expect(queueGrid.getByText("poll 200ms · deadline 300s · aging 60s")).toBeVisible();
    await expect(queueGrid.getByRole("gridcell", { name: "overflow held 1" })).toBeVisible();
  });
});
