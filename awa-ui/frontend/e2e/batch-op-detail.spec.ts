import { test, expect, type Page } from "@playwright/test";

const ID = "44444444-4444-4444-8444-444444444444";

function operation(overrides = {}) {
  return {
    id: ID,
    op_kind: "retry",
    filter: { kind: "e2e_job", state: "failed" },
    spec: {},
    state: "completed",
    submitted_by: "ui",
    submitted_at: new Date(Date.now() - 120_000).toISOString(),
    started_at: new Date(Date.now() - 110_000).toISOString(),
    finalized_at: new Date(Date.now() - 60_000).toISOString(),
    cursor: null,
    total_matched: 3000,
    processed: 0,
    skipped: 3000,
    errored: 0,
    last_error: null,
    runner_instance: "0b0b0b0b-0000-4000-8000-000000000001",
    retention_until: new Date(Date.now() + 86_400_000).toISOString(),
    updated_at: new Date().toISOString(),
    ...overrides,
  };
}

async function serve(page: Page, op: ReturnType<typeof operation>) {
  await page.route(`**/api/batch-ops/${ID}`, (route) =>
    route.request().method() === "GET" ? route.fulfill({ json: op }) : route.fallback()
  );
}

test.describe("Bulk operation page", () => {
  test("an operation that skipped every job says so", async ({ page }) => {
    await serve(page, operation());
    await page.goto(`/batch-ops/${ID}`);
    await expect(page.getByRole("heading", { name: "Retry", exact: true })).toBeVisible();
    await expect(page.getByTestId("batch-op-outcome")).toHaveText(
      `0 changed, ${(3000).toLocaleString("en-US")} skipped`
    );
    await expect(page.getByTestId("batch-op-changed-nothing")).toBeVisible();
    await expect(page.getByText("ui", { exact: true })).toBeVisible();
  });

  test("a failed operation shows its last error", async ({ page }) => {
    await serve(
      page,
      operation({ state: "failed", processed: 400, skipped: 12, last_error: "chunk 5 failed: statement timeout" })
    );
    await page.goto(`/batch-ops/${ID}`);
    await expect(page.getByTestId("batch-op-last-error")).toContainText("chunk 5 failed: statement timeout");
    await expect(page.getByTestId("batch-op-outcome")).toHaveText("400 changed, 12 skipped");
  });

  test("the scope links to the matching jobs", async ({ page }) => {
    await serve(page, operation());
    await page.goto(`/batch-ops/${ID}`);
    await expect(page.getByTestId("batch-op-scope")).toContainText("failed jobs with kind e2e_job");
    await page.getByRole("link", { name: "Open matching jobs" }).click();
    await expect(page).toHaveURL(/\/jobs\?/);
    const url = new URL(page.url());
    expect(url.searchParams.get("state")).toBe("failed");
    expect(url.searchParams.get("q")).toBe("kind:e2e_job");
    expect(url.searchParams.get("dead_lettered")).toBe("false");
  });

  test("without a state the link narrows to the kind's one state, or says the list is wider", async ({ page }) => {
    let op = operation({ op_kind: "discard", filter: { kind: "e2e_job" } });
    await page.route(`**/api/batch-ops/${ID}`, (route) =>
      route.request().method() === "GET" ? route.fulfill({ json: op }) : route.fallback()
    );
    await page.goto(`/batch-ops/${ID}`);
    const link = page.getByRole("link", { name: "Open matching jobs" });
    await expect(link).toHaveAttribute("href", /^\/jobs\?/);
    const search = new URL((await link.getAttribute("href"))!, "http://x").searchParams;
    expect(Object.fromEntries(search)).toEqual({ state: "failed", q: "kind:e2e_job", dead_lettered: "false" });

    op = operation({ op_kind: "retry", filter: { kind: "e2e_job" } });
    await page.reload();
    const wider = page.getByRole("link", { name: "Open matching jobs in every state, not only failed or cancelled" });
    await expect(wider).toBeVisible();
    await expect(wider).not.toHaveAttribute("href", /state=/);
  });

  test("a DLQ operation links to the matching dead letters", async ({ page }) => {
    await serve(page, operation({ op_kind: "purge_dlq", filter: { queue: "e2e_dlq" } }));
    await page.goto(`/batch-ops/${ID}`);
    await page.getByRole("link", { name: "Open matching dead letters" }).click();
    await expect(page).toHaveURL(/\/dlq\?q=queue/);
  });

  test("a running operation can be cancelled", async ({ page }) => {
    let op = operation({ state: "running", finalized_at: null, processed: 10, skipped: 0, total_matched: 100 });
    await page.route("**/api/capabilities", (route) =>
      route.fulfill({ json: { read_only: false, poll_interval_ms: 2000, instance_name: null, instance_color: null, peers: [] } })
    );
    await page.route(`**/api/batch-ops/${ID}`, async (route) => {
      if (route.request().method() === "PATCH") {
        op = { ...op, state: "cancelling" };
        return route.fulfill({ json: op });
      }
      return route.fulfill({ json: op });
    });
    await page.goto(`/batch-ops/${ID}`);
    await page.getByRole("button", { name: "Cancel operation" }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toContainText("The 10 of 100 job(s) it has already changed stay changed.");
    await expect(dialog).not.toContainText("undone");
    await dialog.getByRole("button", { name: "Cancel operation" }).click();
    await expect(page.locator('[data-state="cancelling"]')).toBeVisible();
  });

  test("an unknown id shows not found", async ({ page }) => {
    await page.goto(`/batch-ops/${ID}`);
    const notFound = page.getByTestId("resource-not-found");
    await expect(notFound.getByRole("heading", { name: "Operation 44444444 not found" })).toBeVisible();
    await notFound.getByRole("link", { name: "Back to bulk operations" }).click();
    await expect(page).toHaveURL(/\/batch-ops$/);
  });

  test("a malformed id shows not found without asking the server", async ({ page }) => {
    await page.goto("/batch-ops/not-a-uuid");
    await expect(page.getByTestId("resource-not-found")).toBeVisible();
  });
});
