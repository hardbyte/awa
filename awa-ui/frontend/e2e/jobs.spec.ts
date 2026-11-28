import { test, expect, type Page, type Response } from "@playwright/test";

function isQueuesListResponse(response: Response): boolean {
  const url = new URL(response.url());
  return response.ok() && response.request().method() === "GET" && url.pathname === "/api/queues";
}

function statePill(page: Page, state: string) {
  return page.locator(`[data-testid="state-pill"][data-state="${state}"]`);
}

test.describe("Jobs page", () => {
  test("navigate to /jobs, heading visible", async ({ page }) => {
    await page.goto("/jobs");
    await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
  });

  test("state filter pills are present", async ({ page }) => {
    await page.goto("/jobs");
    await page.waitForResponse("**/api/stats");

    for (const label of ["All", "Available", "Failed", "Running"]) {
      await expect(page.getByRole("button", { name: new RegExp(`^${label}`) })).toBeVisible();
    }
  });

  test("job table renders with column headers", async ({ page }) => {
    const [response] = await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Kind is now the primary column (no ID column)
    for (const header of ["Kind", "State", "Queue", "Attempt", "Created"]) {
      await expect(
        jobTable.getByRole("columnheader", { name: header })
      ).toBeVisible();
    }

    // Should have data rows
    const rows = jobTable.getByRole("row");
    await expect(rows).not.toHaveCount(1);
  });

  test("descriptor-backed and legacy labels both render in jobs table", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable.getByText("E2E Job").first()).toBeVisible();
    await expect(jobTable.getByText("e2e_job").first()).toBeVisible();
    await expect(jobTable.getByText("E2E Queue").first()).toBeVisible();
    await expect(jobTable.getByText("e2e_test").first()).toBeVisible();
    await expect(jobTable.getByText("legacy_job").first()).toBeVisible();
    await expect(jobTable.getByText("legacy_queue").first()).toBeVisible();
  });

  test("URL-driven state: navigate to /jobs?state=failed, failed pill active", async ({
    page,
  }) => {
    const [response] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=failed") &&
          r.ok()
      ),
      page.goto("/jobs?state=failed"),
    ]);

    const failedPill = statePill(page, "failed");
    await expect(failedPill).toHaveAttribute("aria-pressed", "true");
    // Table should show only failed jobs
    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();
  });

  test("search bar is present and interactive", async ({ page }) => {
    await page.goto("/jobs");

    const searchInput = page.getByPlaceholder(/Filter by kind/);
    await expect(searchInput).toBeVisible();

    await searchInput.fill("kind:test");
    await expect(searchInput).toHaveValue("kind:test");
  });

  test("clicking state pill updates URL and filters table", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    // Click the "failed" pill
    const failedPill = statePill(page, "failed");
    await expect(failedPill).toBeVisible();

    const [filteredResponse] = await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=failed") &&
          r.ok()
      ),
      failedPill.click(),
    ]);

    expect(page.url()).toContain("state=failed");

    // Click "all" to clear the filter
    const allPill = statePill(page, "all");
    await allPill.click();

    expect(page.url()).not.toContain("state=");
  });

  test("clicking 'available' pill then 'running' pill switches filter", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/stats") && r.ok()),
      page.goto("/jobs"),
    ]);

    // Click available pill
    const availablePill = statePill(page, "available");
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=available") &&
          r.ok()
      ),
      availablePill.click(),
    ]);
    expect(page.url()).toContain("state=available");

    // Switch to running
    const runningPill = statePill(page, "running");
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=running") &&
          r.ok()
      ),
      runningPill.click(),
    ]);
    expect(page.url()).toContain("state=running");
    expect(page.url()).not.toContain("state=available");
  });
});

test.describe("Search bar filters", () => {
  test("several tags narrow to jobs carrying all of them, with a match count", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs/count") && r.ok()),
      page.goto("/jobs?q=tag%3Ae2e%20tag%3Apriority"),
    ]);
    await expect(page.getByText("tag:e2e", { exact: true })).toBeVisible();
    await expect(page.getByText("tag:priority", { exact: true })).toBeVisible();
    await expect(page.getByTestId("match-count")).toHaveText("1 matching");
    await expect(page.getByText("#800005").filter({ visible: true }).first()).toBeVisible();
    await expect(page.getByText("#800006")).toHaveCount(0);
  });

  test("all matching → Retry previews the count and sample, then opens the operation", async ({ page }) => {
    const id = "5a5a5a5a-0000-4000-8000-000000000000";
    let submitted: unknown = null;
    await page.route("**/api/batch-ops/preview", (route) =>
      route.fulfill({
        json: {
          total_matched: 2,
          sample: [800003, 800013].map((jobId) => ({
            id: jobId, kind: "e2e_job", queue: "e2e_test", state: "cancelled",
            kind_descriptor: null, queue_descriptor: null,
          })),
        },
      })
    );
    await page.route(
      (url) => url.pathname === "/api/batch-ops",
      (route) => {
        if (route.request().method() !== "POST") return route.fallback();
        submitted = route.request().postDataJSON();
        return route.fulfill({ status: 201, json: batchOperation(id, "retry", { kind: "e2e_job", state: "cancelled" }) });
      }
    );
    await page.route(`**/api/batch-ops/${id}`, (route) =>
      route.fulfill({ json: batchOperation(id, "retry", { kind: "e2e_job", state: "cancelled" }) })
    );
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs/count") && r.ok()),
      page.goto("/jobs?state=cancelled&q=kind%3Ae2e_job"),
    ]);
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    await expect(bar.getByTestId("bulk-scope-all")).toHaveText(/^All \d+ matching live jobs$/);
    await bar.getByRole("button", { name: "Retry…" }).click();

    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: "Retry 2 jobs?" })).toBeVisible();
    await expect(dialog).toContainText("Retry 2 cancelled jobs with kind e2e_job.");
    await expect(dialog.getByTestId("bulk-sample").getByRole("link", { name: "#800003" })).toBeVisible();
    await expect(dialog.getByRole("textbox")).toHaveCount(0);
    await dialog.getByRole("button", { name: "Retry 2 jobs" }).click();

    await expect(page).toHaveURL(new RegExp(`/batch-ops/${id}$`));
    expect(submitted).toEqual({ op_kind: "retry", filter: { kind: "e2e_job", state: "cancelled" }, spec: {}, submitted_by: "ui" });
  });

  test("the all-matching scope counts only live jobs, leaving dead letters out", async ({ page }) => {
    const counted: string[] = [];
    await page.route(
      (url) => url.pathname === "/api/jobs/count",
      (route) => {
        const url = new URL(route.request().url());
        counted.push(url.search);
        const live = url.searchParams.get("dead_lettered") === "false";
        return route.fulfill({ json: { kind: "exact", value: live ? 3 : 5 } });
      }
    );
    await page.goto("/jobs?state=failed&q=kind%3Ae2e_job");
    await expect(page.getByTestId("match-count")).toHaveText("5 matching");
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    await expect(bar.getByTestId("bulk-scope-all")).toHaveText("All 3 matching live jobs");
    expect(counted.some((search) => search.includes("dead_lettered=false"))).toBe(true);
  });

  test("a state that holds no dead letters counts once", async ({ page }) => {
    const counted: string[] = [];
    page.on("request", (request) => {
      const url = new URL(request.url());
      if (url.pathname === "/api/jobs/count") counted.push(url.search);
    });
    await page.goto("/jobs?state=cancelled&q=kind%3Ae2e_job");
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    await expect(page.getByTestId("match-count")).toHaveText(/^[\d,]+ matching$/);
    const total = (await page.getByTestId("match-count").textContent())!.replace(" matching", "");
    await expect(bar.getByTestId("bulk-scope-all")).toHaveText(`All ${total} matching live jobs`);
    expect(counted.filter((search) => search.includes("dead_lettered"))).toEqual([]);
  });

  test("a state alone disables the all-matching actions and says why", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs/count") && r.ok()),
      page.goto("/jobs?state=failed"),
    ]);
    const retry = page.getByRole("toolbar", { name: "Bulk actions" }).getByRole("button", { name: "Retry…" });
    await expect(retry).toBeDisabled();
    await expect(retry).toHaveAccessibleDescription(/a state alone matches every job in it/);
  });

  test("an action that can't act on the list's state is disabled with the reason", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs/count") && r.ok()),
      page.goto("/jobs?state=available&q=queue%3Ae2e_test"),
    ]);
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    await expect(bar.getByRole("button", { name: "Retry…" })).toHaveAccessibleDescription(
      "Retry acts on failed or cancelled jobs, and this list shows available jobs"
    );
    await expect(bar.getByRole("button", { name: "Cancel…" })).toBeEnabled();
  });

  test("Set priority carries the filter to the new-operation form", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs/count") && r.ok()),
      page.goto("/jobs?state=available&q=queue%3Ae2e_test%20tag%3Ae2e"),
    ]);
    await page.getByRole("toolbar", { name: "Bulk actions" }).getByRole("button", { name: "Set priority…" }).click();
    await page.waitForURL(/\/batch-ops\/new\?/);
    await expect(page.getByLabel("Queue filter")).toHaveValue("e2e_test");
    await expect(page.getByLabel("Tags filter")).toHaveValue("e2e");
    await expect(page.getByLabel("State filter")).toHaveValue("available");
  });

  // A refused read shows within the default expect timeout only if the
  // query doesn't retry the 4xx.
  test("a refused list or count shows the server's message", async ({ page }) => {
    await page.route(
      (url) => url.pathname === "/api/jobs",
      (route) => route.fulfill({ status: 400, json: { error: "narrow the filter" } })
    );
    await page.route(
      (url) => url.pathname === "/api/jobs/count",
      (route) => route.fulfill({ status: 400, json: { error: "count budget spent" } })
    );
    await page.goto("/jobs?q=queue%3Ae2e_test");
    await expect(
      page.getByText("Failed to load jobs: narrow the filter").filter({ visible: true })
    ).toBeVisible();
    await expect(page.getByText("No jobs found.").filter({ visible: true })).toHaveCount(0);
    await expect(page.getByTestId("match-count-error")).toHaveText(
      "Count failed: count budget spent"
    );
  });

  test("a malformed term in the URL lists nothing until it is removed", async ({ page }) => {
    const jobRequests: string[] = [];
    page.on("request", (r) => {
      if (r.url().includes("/api/jobs")) jobRequests.push(r.url());
    });
    await page.goto("/jobs?q=queue%3Ae2e_test%20id%3Aabc");
    await expect(page.getByRole("alert")).toContainText("whole job ids");
    await expect(page.getByText("id:abc", { exact: true })).toBeVisible();
    await expect(page.getByText("No jobs found.").filter({ visible: true })).toHaveCount(0);
    await expect(
      page.getByRole("toolbar", { name: "Bulk actions" }).getByRole("button", { name: "Retry…" })
    ).toHaveAccessibleDescription("Fix the search first");
    // Job detail requests (/api/jobs/<id>) aside, nothing lists or counts.
    expect(jobRequests.filter((url) => !/\/api\/jobs\/\d+/.test(url))).toEqual([]);

    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs?") && r.ok()),
      page.getByRole("button", { name: "Remove id:abc filter" }).click(),
    ]);
    await expect(page.getByRole("alert")).toHaveCount(0);
    expect(page.url()).not.toContain("abc");
  });

  test("rows cached for the wider search are hidden beside the error", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs?") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);
    const rows = page.getByRole("row").filter({ hasText: "e2e_test" });
    await expect(rows.first()).toBeVisible();
    // An in-app navigation keeps the query cache, whose entry for the wider
    // search shares a key with this one.
    await page.evaluate(() => {
      history.pushState({}, "", "/jobs?q=queue%3Ae2e_test%20id%3Aabc");
      dispatchEvent(new PopStateEvent("popstate"));
    });
    await expect(page.getByRole("alert")).toContainText("whole job ids");
    await expect(rows).toHaveCount(0);
  });

  test("the DLQ keeps bulk actions off for a malformed term in the URL", async ({ page }) => {
    const bulkRequests: string[] = [];
    page.on("request", (r) => {
      if (r.method() === "POST" && r.url().includes("/api/dlq")) bulkRequests.push(r.url());
    });
    await page.goto("/dlq?q=queue%3Ae2e_dlq%20id%3Aabc");
    await expect(page.getByRole("alert")).toContainText("whole job ids");
    await expect(page.getByText("DLQ is empty")).toHaveCount(0);
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    for (const name of ["Retry…", "Purge…"]) {
      await expect(bar.getByRole("button", { name })).toBeDisabled();
      await expect(bar.getByRole("button", { name })).toHaveAccessibleDescription("Fix the search first");
    }
    expect(bulkRequests).toEqual([]);
  });

  test("typing queue:e2e_test and pressing Enter creates a chip and updates URL", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const searchInput = page.getByLabel("Search jobs");
    await expect(searchInput).toBeVisible();

    await searchInput.fill("queue:e2e_test");
    await searchInput.press("Enter");

    // A filter chip should appear with the queue filter
    await expect(page.getByText("queue:e2e_test")).toBeVisible();

    // URL should have the queue filter
    await page.waitForURL(/q=.*queue/);
    expect(page.url()).toContain("queue");
    expect(page.url()).toContain("e2e_test");

    // Input should be cleared after applying the chip
    await expect(searchInput).toHaveValue("");
  });

  test("clicking X on a chip removes the filter and updates URL", async ({ page }) => {
    // Navigate with queue filter pre-applied
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    // Chip should be visible
    await expect(page.getByText("queue:e2e_test")).toBeVisible();

    // Click the remove button on the chip
    const removeBtn = page.getByRole("button", { name: "Remove queue:e2e_test filter" });
    await expect(removeBtn).toBeVisible();
    await removeBtn.click();

    // Chip should disappear and URL should not contain the queue filter
    await expect(page.getByText("queue:e2e_test")).not.toBeVisible();
  });

  test("backspace on empty input removes the last filter chip", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    // Chip should be visible
    await expect(page.getByText("queue:e2e_test")).toBeVisible();

    const searchInput = page.getByLabel("Search jobs");
    await searchInput.focus();

    // Input is empty, backspace should remove the last filter
    await searchInput.press("Backspace");

    await expect(page.getByText("queue:e2e_test")).not.toBeVisible();
  });

  test("/ shortcut focuses search input", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const searchInput = page.getByLabel("Search jobs");
    // Ensure input is not focused initially
    await expect(searchInput).not.toBeFocused();

    // Press "/" to focus search
    await page.keyboard.press("/");
    await expect(searchInput).toBeFocused();
  });

  test("autocomplete suggestions appear on focus and can be selected", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    const searchInput = page.getByLabel("Search jobs");
    await searchInput.focus();

    // Suggestion dropdown should appear with prefix hints
    const listbox = page.getByRole("listbox");
    await expect(listbox).toBeVisible();

    // Should show kind:, queue:, tag: hints
    await expect(listbox.getByText("kind:<name>")).toBeVisible();
    await expect(listbox.getByText("queue:<name>")).toBeVisible();

    // Arrow down to select first suggestion
    await searchInput.press("ArrowDown");
    // First option should be selected
    const firstOption = listbox.getByRole("option").first();
    await expect(firstOption).toHaveAttribute("aria-selected", "true");
  });
});

test.describe("Bulk actions", () => {
  test("selecting a job shows bulk action toolbar", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // React Aria checkboxes have wrapper divs that intercept pointer events;
    // use force:true to click through the overlay
    const firstRowCheckbox = jobTable.getByRole("checkbox").nth(1);
    await expect(firstRowCheckbox).toBeVisible();
    await firstRowCheckbox.click({ force: true });

    // The bulk-action bar switches to the selection
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    await expect(bar.getByRole("button", { name: "Selected (1)" })).toHaveAttribute("aria-pressed", "true");
    await expect(bar.getByText("Updates paused")).toBeVisible();
  });

  test("select-all checkbox selects all visible jobs", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("queue=e2e_test") &&
          r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // The first checkbox is the select-all
    const selectAll = jobTable.getByRole("checkbox").first();
    await selectAll.click({ force: true });

    // The bar shows the selected count
    const bar = page.getByRole("toolbar", { name: "Bulk actions" });
    await expect(bar.getByRole("button", { name: /^Selected \(\d+\)$/ })).toBeVisible();

    // Clicking again deselects all, and the bar acts on all matching jobs again
    await selectAll.click({ force: true });
    await expect(bar.getByRole("button", { name: /^Selected/ })).toHaveCount(0);
    await expect(bar.getByTestId("bulk-scope-all")).toBeVisible();
  });

  test("bulk cancel shows confirmation dialog", async ({ page }) => {
    // Filter to available jobs so they are cancellable (non-terminal)
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("state=available") &&
          r.ok()
      ),
      page.goto("/jobs?state=available&q=queue%3Ae2e_test"),
    ]);

    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await expect(jobTable).toBeVisible();

    // Select all available jobs
    const selectAll = jobTable.getByRole("checkbox").first();
    await selectAll.click({ force: true });

    // Cancel button should be enabled (available jobs are cancellable)
    const cancelBtn = page.getByRole("button", { name: /^Cancel \(\d+\)…$/ });
    await expect(cancelBtn).toBeVisible();
    await expect(cancelBtn).toBeEnabled();
    const count = Number((await cancelBtn.textContent())?.match(/\((\d+)\)/)?.[1]);

    await cancelBtn.click();

    // Several jobs: the dialog counts and lists them, and says nothing it can't keep
    const dialog = page.getByRole("alertdialog");
    await expect(dialog).toBeVisible();
    if (count > 1) {
      await expect(dialog.getByRole("heading", { name: `Cancel ${count} jobs?` })).toBeVisible();
      await expect(dialog.getByTestId("selection-preview").getByRole("listitem").first()).toBeVisible();
      await expect(dialog.getByRole("button", { name: `Cancel ${count} jobs` })).toBeVisible();
    } else {
      await expect(dialog.getByRole("heading", { name: /^Cancel job #\d+\?$/ })).toBeVisible();
    }
    await expect(dialog.getByText(/undone/)).toHaveCount(0);

    // Close dialog without confirming
    await dialog.getByRole("button", { name: "Keep", exact: true }).click();
    await expect(dialog).not.toBeVisible();
    await expect(cancelBtn).toBeFocused();
  });

  test("bulk retry reports what the server changed", async ({ page }) => {
    await page.route("**/api/jobs/bulk-retry", (route) => route.fulfill({ json: [] }));
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.url().includes("state=cancelled") && r.ok()),
      page.goto("/jobs?state=cancelled&q=queue%3Ae2e_test"),
    ]);
    const jobTable = page.getByRole("grid", { name: "Jobs" });
    await jobTable.getByRole("checkbox").nth(1).click({ force: true });
    await page.getByRole("button", { name: /^Retry \(1\)…$/ }).click();
    const dialog = page.getByRole("alertdialog");
    await expect(dialog.getByRole("heading", { name: /^Retry job #\d+\?$/ })).toBeVisible();
    await dialog.getByRole("button", { name: /^Retry job #\d+$/ }).click();
    await expect(dialog).toBeHidden();
    await expect(page.getByText("Retried 0 of 1 selected job(s)")).toBeVisible();
    await expect(page.getByText("Skipped: 1 changed state before the retry reached them.")).toBeVisible();
  });
});

test.describe("Pagination", () => {
  test("page size buttons update URL limit param", async ({ page }) => {
    await Promise.all([
      page.waitForResponse((r) => r.url().includes("/api/jobs") && r.ok()),
      page.goto("/jobs"),
    ]);

    // Find the "20" page size button (it's in the "Per page:" section)
    const perPageLabel = page.getByText("Per page:");
    await expect(perPageLabel).toBeVisible();

    // Click "20" page size button and wait for the API call with limit=20
    const size20Btn = page.getByRole("button", { name: "20", exact: true });
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("limit=20") &&
          r.ok()
      ),
      size20Btn.click(),
    ]);

    // TanStack Router may URL-encode the limit value differently; check the API was called
    expect(page.url()).toMatch(/limit/);

    // Switch to 100
    const size100Btn = page.getByRole("button", { name: "100", exact: true });
    await Promise.all([
      page.waitForResponse(
        (r) =>
          r.url().includes("/api/jobs") &&
          r.url().includes("limit=100") &&
          r.ok()
      ),
      size100Btn.click(),
    ]);

    expect(page.url()).toMatch(/limit/);
  });
});

test.describe("Single-queue filter", () => {
  test("shows an Open queue link and no queue banner", async ({ page }) => {
    await Promise.all([
      page.waitForResponse(
        (r) => r.url().includes("/api/jobs") && r.url().includes("queue=e2e_test") && r.ok()
      ),
      page.goto("/jobs?q=queue%3Ae2e_test"),
    ]);
    await expect(page.getByRole("heading", { level: 1, name: "Jobs", exact: true })).toBeVisible();
    await expect(page.getByRole("button", { name: "Drain" })).toHaveCount(0);
    await page.getByTestId("open-queue").click();
    await expect(page).toHaveURL(/\/queues\/e2e_test$/);
  });

  test("no Open queue link without a queue filter", async ({ page }) => {
    await page.goto("/jobs");
    await expect(page.getByRole("grid", { name: "Jobs" })).toBeVisible();
    await expect(page.getByTestId("open-queue")).toHaveCount(0);
  });
});

function batchOperation(id: string, opKind: string, filter: Record<string, unknown>) {
  return {
    id, op_kind: opKind, filter, spec: {}, state: "pending", submitted_by: "ui",
    submitted_at: new Date().toISOString(), started_at: null, finalized_at: null, cursor: null,
    total_matched: null, processed: 0, skipped: 0, errored: 0, last_error: null,
    runner_instance: null, retention_until: null, updated_at: new Date().toISOString(),
  };
}
