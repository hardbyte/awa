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

  for (const { button, endpoint, opKind, confirm, verb } of [
    { button: /Retry matching/, endpoint: "/api/dlq/retry-matching", opKind: "retry_from_dlq", confirm: "Retry", verb: "retry" },
    { button: /Purge matching/, endpoint: "/api/dlq/purge-matching", opKind: "purge_dlq", confirm: "Purge", verb: "purge" },
  ]) {
    test(`${verb} matching submits a batch operation and links to it`, async ({ page }) => {
      const id = "0b2c4d6e-0000-4000-8000-000000000000";
      let body: unknown = null;
      await page.route(
        (url) => url.pathname === endpoint,
        async (route) => {
          body = route.request().postDataJSON();
          await route.fulfill({
            status: 202,
            json: {
              id,
              op_kind: opKind,
              filter: { queue: "e2e_dlq" },
              spec: { op_kind: opKind },
              state: "pending",
              submitted_by: "http",
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
        }
      );
      await page.goto(DLQ_PAGE);
      await page.getByRole("button", { name: button }).click();
      await page.getByRole("alertdialog").getByRole("button", { name: confirm, exact: true }).click();

      const status = page.getByTestId("dlq-batch-submitted");
      await expect(status).toContainText(`Submitted ${verb} of the matching DLQ rows as batch operation 0b2c4d6e`);
      expect(body).toEqual({ filter: { queue: "e2e_dlq" } });
      await status.getByRole("link", { name: "follow its progress" }).click();
      await expect(page).toHaveURL(new RegExp(`/batch-ops/${id}$`));
    });
  }

  test("a queue chip narrows the list and pressing it again widens it", async ({ page }) => {
    await page.route("**/api/dlq/depth", (route) =>
      route.fulfill({ json: { total: 3, by_queue: [{ queue: "e2e_dlq", count: 3 }] } })
    );
    await page.goto("/dlq");
    const chip = page.getByTestId("dlq-queue-chip").filter({ hasText: "e2e_dlq" });
    await expect(chip).toHaveAttribute("aria-pressed", "false");
    await chip.click();
    await expect(page).toHaveURL(/\/dlq\?q=queue%3Ae2e_dlq$/);
    await expect(chip).toHaveAttribute("aria-pressed", "true");
    await chip.click();
    await expect(page).toHaveURL(/\/dlq$/);
  });
});
