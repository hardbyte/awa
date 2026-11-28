import { test, expect, type Page } from "@playwright/test";

const DLQ_PAGE = "/dlq?q=queue%3Ae2e_dlq";

function bulk(page: Page, name: string) {
  return page.getByRole("toolbar", { name: "Bulk actions" }).getByRole("button", { name });
}

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
    await expect(bulk(page, "Retry…")).toBeDisabled();
    await expect(bulk(page, "Purge…")).toBeDisabled();
  });

  test("a refused list shows its message without retrying", async ({ page }) => {
    await page.route(
      (url) => url.pathname === "/api/dlq",
      (route) => route.fulfill({ status: 400, json: { error: "narrow the filter" } })
    );
    await page.goto(DLQ_PAGE);
    await expect(page.getByRole("alert")).toHaveText("Failed to load the DLQ: narrow the filter");
    await expect(bulk(page, "Retry…")).toBeDisabled();
    await expect(bulk(page, "Purge…")).toBeDisabled();
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
    await expect(bulk(page, "Retry…")).toBeDisabled();
    await expect(bulk(page, "Purge…")).toBeDisabled();

    release();
    await expect(bulk(page, "Retry…")).toBeEnabled();
    await expect(bulk(page, "Purge…")).toBeEnabled();
  });

  for (const { button, opKind, confirm, typed } of [
    { button: "Retry…", opKind: "retry_from_dlq", confirm: "Retry 1 dead letter", typed: false },
    { button: "Purge…", opKind: "purge_dlq", confirm: "Purge 1 dead letter", typed: true },
  ]) {
    test(`${button} on all matching previews the rows and opens the submitted operation`, async ({ page }) => {
      const id = "0b2c4d6e-0000-4000-8000-000000000000";
      let body: unknown = null;
      const op = {
        id, op_kind: opKind, filter: { queue: "e2e_dlq" }, spec: {}, state: "pending", submitted_by: "ui",
        submitted_at: new Date().toISOString(), started_at: null, finalized_at: null, cursor: null,
        total_matched: null, processed: 0, skipped: 0, errored: 0, last_error: null,
        runner_instance: null, retention_until: null, updated_at: new Date().toISOString(),
      };
      await page.route("**/api/batch-ops/preview", (route) =>
        route.fulfill({
          json: {
            total_matched: 1,
            sample: [{ id: 600001, kind: "dlq_job", queue: "e2e_dlq", state: "failed", kind_descriptor: null, queue_descriptor: null }],
          },
        })
      );
      await page.route(
        (url) => url.pathname === "/api/batch-ops",
        (route) => {
          if (route.request().method() !== "POST") return route.fallback();
          body = route.request().postDataJSON();
          return route.fulfill({ status: 201, json: op });
        }
      );
      await page.route(`**/api/batch-ops/${id}`, (route) => route.fulfill({ json: op }));
      await page.goto(DLQ_PAGE);
      await bulk(page, button).click();

      const dialog = page.getByRole("alertdialog");
      await expect(dialog.getByRole("heading", { name: `${confirm}?` })).toBeVisible();
      await expect(dialog).toContainText("dead-lettered jobs with queue e2e_dlq");
      await expect(dialog.getByTestId("bulk-sample").getByRole("link", { name: "#600001" })).toHaveAttribute("href", "/dlq/600001");
      const submit = dialog.getByRole("button", { name: confirm });
      if (typed) {
        await expect(dialog).toContainText("This can't be undone.");
        await expect(submit).toBeDisabled();
        await dialog.getByRole("textbox", { name: /Type 1/ }).fill("1");
      } else {
        await expect(dialog.getByRole("textbox")).toHaveCount(0);
      }
      await submit.click();

      await expect(page).toHaveURL(new RegExp(`/batch-ops/${id}$`));
      expect(body).toEqual({ op_kind: opKind, filter: { queue: "e2e_dlq" }, spec: {}, submitted_by: "ui" });
    });
  }

  test("a refused submit stays in the dialog with the server's reason", async ({ page }) => {
    await page.route("**/api/batch-ops/preview", (route) =>
      route.fulfill({ json: { total_matched: 1, sample: [] } })
    );
    await page.route(
      (url) => url.pathname === "/api/batch-ops",
      (route) =>
        route.request().method() === "POST"
          ? route.fulfill({ status: 400, json: { error: "runtime worker-1 reports 0.6.15, older than 0.7.0-alpha.2" } })
          : route.fallback()
    );
    await page.goto(DLQ_PAGE);
    await bulk(page, "Retry…").click();
    const dialog = page.getByRole("alertdialog");
    await dialog.getByRole("button", { name: "Retry 1 dead letter" }).click();
    await expect(dialog.getByTestId("confirm-error")).toHaveText("runtime worker-1 reports 0.6.15, older than 0.7.0-alpha.2");
    await expect(dialog).toBeVisible();
  });

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

test.describe("Dead letters on a phone", () => {
  test("cards list the reason and open a dead letter with the keyboard", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto(DLQ_PAGE);
    const cards = page.getByRole("list", { name: "Dead letters" });
    await expect(cards.getByTestId("row-card").first()).toBeVisible();
    await cards.getByRole("link").first().focus();
    await page.keyboard.press("Enter");
    await expect(page).toHaveURL(/\/dlq\/\d+$/);
  });
});

test.describe("Dead letter page", () => {
  test("Retry quotes the dead-letter reason and Purge says it can't be undone", async ({ page }) => {
    const posts: string[] = [];
    page.on("request", (r) => {
      if (r.method() !== "GET" && r.url().includes("/api/dlq/600001")) posts.push(r.url());
    });
    await page.goto("/dlq/600001");
    await page.getByRole("button", { name: "Retry…" }).click();
    let dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: "Retry dead letter #600001?" })).toBeVisible();
    await expect(dialog).toContainText("manual_test");
    await expect(dialog).not.toContainText("undone");
    await dialog.getByRole("button", { name: "Keep" }).click();
    await expect(dialog).toBeHidden();

    await page.getByRole("button", { name: "Purge…" }).click();
    dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: "Purge dead letter #600001?" })).toBeVisible();
    await expect(dialog).toContainText("This can't be undone.");
    await dialog.getByRole("button", { name: "Keep" }).click();
    expect(posts).toEqual([]);
  });

  test("on a read-only server Retry and Purge are disabled and say why", async ({ page }) => {
    await page.route("**/api/capabilities", async (route) => {
      const response = await route.fetch();
      await route.fulfill({ response, json: { ...(await response.json()), read_only: true } });
    });
    await page.goto("/dlq/600001");
    for (const name of ["Retry…", "Purge…"]) {
      const button = page.getByRole("button", { name });
      await expect(button).toBeDisabled();
      await expect(button).toHaveAccessibleDescription("Read-only server — admin actions are off");
    }
  });
});
