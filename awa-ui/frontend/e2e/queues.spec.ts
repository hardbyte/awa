import { test, expect } from "@playwright/test";

test.describe("Queues page", () => {
  test("navigate to /queues, heading visible", async ({ page }) => {
    await page.goto("/queues");
    await expect(page.getByRole("heading", { name: "Queues" })).toBeVisible();
  });

  test("queue table renders with data", async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    // Check column headers
    for (const header of ["Queue", "Available", "Running", "Failed", "Status"]) {
      await expect(
        queueTable.getByRole("columnheader", { name: header })
      ).toBeVisible();
    }

    // Should have data rows
    const rows = queueTable.getByRole("row");
    await expect(rows).not.toHaveCount(1);
  });

  test("click a queue name, navigate to /queues/<name>", async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    await expect(queueTable).toBeVisible();

    // Find the first queue link in the table
    const queueLink = queueTable.getByRole("link").first();
    const queueName = await queueLink.textContent();
    expect(queueName).toBeTruthy();

    await queueLink.click();

    // Wait for navigation
    await page.waitForURL(/\/queues\/.+/);

    // Verify we are on the queue detail page
    expect(page.url()).toContain(`/queues/${queueName}`);
  });

  test("queue detail page shows queue name and stats", async ({ page }) => {
    // First get a queue name from the list
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok()),
      page.goto("/queues"),
    ]);

    const queueTable = page.getByRole("grid", { name: "Queues" });
    const queueLink = queueTable.getByRole("link").first();
    const queueName = await queueLink.textContent();

    await queueLink.click();
    await page.waitForURL(/\/queues\/.+/);

    // Wait for queue data to load on the detail page
    await page.waitForResponse((r) => r.url().includes("/api/queues") && r.ok());

    // Heading should contain the queue name
    await expect(
      page.getByRole("heading", { name: new RegExp(`Queue:.*${queueName}`) })
    ).toBeVisible();

    // Stats section should show available, running, etc.
    await expect(page.getByText(/Available:/)).toBeVisible();
    await expect(page.getByText(/Running:/)).toBeVisible();
    await expect(page.getByText(/Failed:/)).toBeVisible();
  });
});
