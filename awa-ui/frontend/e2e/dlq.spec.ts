import { test, expect } from "@playwright/test";

const DLQ_PAGE = "/dlq?q=queue%3Ae2e_dlq";

test.describe("DLQ page", () => {
  test("a failed list shows the server's message and keeps bulk actions off", async ({ page }) => {
    await page.route(
      (url) => url.pathname === "/api/dlq",
      (route) => route.fulfill({ status: 500, json: { error: "database unavailable" } })
    );
    await page.goto(DLQ_PAGE);
    // A 5xx is retried before the error shows.
    await expect(page.getByRole("alert")).toHaveText(
      "Failed to load the DLQ: database unavailable",
      { timeout: 15_000 }
    );
    await expect(page.getByText("DLQ is empty")).toHaveCount(0);
    await expect(page.getByRole("button", { name: /Retry matching/ })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Purge matching" })).toBeDisabled();
  });

  test("a refused list shows its message without retrying", async ({ page }) => {
    await page.route(
      (url) => url.pathname === "/api/dlq",
      (route) => route.fulfill({ status: 400, json: { error: "narrow the filter" } })
    );
    await page.goto(DLQ_PAGE);
    await expect(page.getByRole("alert")).toHaveText("Failed to load the DLQ: narrow the filter");
    await expect(page.getByRole("button", { name: /Retry matching/ })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Purge matching" })).toBeDisabled();
  });

  test("bulk actions wait for the list to load", async ({ page }) => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => (release = resolve));
    await page.route(
      (url) => url.pathname === "/api/dlq",
      async (route) => {
        await held;
        await route.fallback();
      }
    );
    await page.goto(DLQ_PAGE);
    await expect(page.getByRole("button", { name: /Retry matching/ })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Purge matching" })).toBeDisabled();

    release();
    await expect(page.getByRole("button", { name: /Retry matching/ })).toBeEnabled();
    await expect(page.getByRole("button", { name: "Purge matching" })).toBeEnabled();
  });
});
