import { test, expect } from "@playwright/test";
import { mockOperations } from "./operations-fixture";

const artifacts =
  process.env.AWA_REVIEW_ARTIFACT_DIR ?? "test-results/operations-review";
test("busy overview communicates current work and opens targeted drilldowns", async ({
  page,
}) => {
  const fixture = await mockOperations(page);
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.goto("/");
  await expect(
    page.getByRole("heading", { name: "Work in motion" }),
  ).toBeVisible();
  await expect(
    page.locator(".work-counter").filter({ hasText: "Available" }),
  ).toContainText("237,192");
  await expect(
    page.locator(".work-counter").filter({ hasText: "Retryable" }),
  ).toContainText("2,070");
  await expect(page.locator(".worker-preview-row")).toHaveCount(4);
  await expect(page.locator(".worker-preview-row").first()).toContainText(
    "health unknown",
  );
  await expect(page.locator(".pressure-table tbody tr").first()).toContainText(
    "7m 38s",
  );
  await page.screenshot({
    path: `${artifacts}/overview-desktop.png`,
    fullPage: true,
  });
  await page.getByRole("link", { name: "184,200", exact: true }).click();
  await expect(page).toHaveURL(/state=available/);
  await expect(page).toHaveURL(/queue%3Aemail-delivery/);
  await expect(
    page.getByRole("button", { name: "available 184,200" }),
  ).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByText("State counts cover this queue")).toBeVisible();
  for (let index = 0; index < 3; index++) {
    await page.goto("/");
    await page.getByRole("link", { name: "worker-eu-01" }).click();
    await expect(page).toHaveURL(/runtime\/00000000/);
    await page.goBack();
    await expect(
      page.getByRole("heading", { name: "Work in motion" }),
    ).toBeVisible();
  }
  expect(fixture.mutations).toEqual([]);
  expect(errors).toEqual([]);
});

test("queue filters and core controls use only the synthetic API", async ({
  page,
}) => {
  const fixture = await mockOperations(page);
  await page.goto("/queues");
  await page.getByRole("searchbox", { name: "Find a queue" }).fill("invoice");
  await expect(page.locator(".pressure-table tbody tr")).toHaveCount(1);
  await page.getByRole("button", { name: "Resume", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "Drain…" }).click();
  await expect(page.getByRole("alertdialog")).toContainText(
    "Running jobs will not be affected",
  );
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(fixture.mutations).toHaveLength(2);
  await page.getByRole("button", { name: "Drain…" }).click();
  await page.getByRole("button", { name: "Drain queue", exact: true }).click();
  await expect(page.locator(".pressure-table tbody tr")).toContainText(
    "0 scheduled",
  );
  expect(fixture.mutations).toContain("POST /queues/invoice-export/drain");
  await page.getByRole("searchbox").fill("not-a-real-queue");
  await expect(page.getByText("No queues match these filters.")).toBeVisible();
  await page.getByRole("searchbox").fill("");
  await page.getByRole("combobox", { name: "Sort by" }).selectOption("failed");
  await expect(page.locator(".pressure-table tbody tr").first()).toContainText(
    "email-delivery",
  );
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.screenshot({
    path: `${artifacts}/queues-desktop.png`,
    fullPage: true,
  });
  await page.goto("/jobs/984213");
  await page.getByRole("button", { name: "Retry", exact: true }).click();
  await expect
    .poll(() => fixture.mutations)
    .toContain("POST /jobs/984213/retry");
  await page.getByRole("button", { name: "Cancel", exact: true }).click();
  await expect
    .poll(() => fixture.mutations)
    .toContain("POST /jobs/984213/cancel");
});

test("mobile navigation, controls, job drilldown and layout at narrow sizes", async ({
  page,
}) => {
  test.setTimeout(60_000);
  await mockOperations(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await expect(page.locator(".work-counter").first()).toContainText("237,192");
  await page.screenshot({
    path: `${artifacts}/overview-mobile.png`,
    fullPage: true,
  });
  await page.screenshot({
    path: `${artifacts}/overview-mobile-top.png`,
    fullPage: false,
  });
  for (let index = 0; index < 3; index++) {
    await page.getByRole("button", { name: "Toggle Sidebar" }).click();
    await page.getByRole("link", { name: "Queues", exact: true }).click();
    await expect(page.getByRole("searchbox")).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await page.getByRole("button", { name: "Toggle Sidebar" }).click();
    await page.getByRole("link", { name: "Overview", exact: true }).click();
    await expect(
      page.getByRole("heading", { name: "Work in motion" }),
    ).toBeVisible();
    await expect(page.getByRole("dialog")).toHaveCount(0);
  }
  await page.goto("/queues");
  await page.getByRole("searchbox").fill("email");
  await page.screenshot({
    path: `${artifacts}/queues-mobile.png`,
    fullPage: true,
  });
  const pauseBounds = await page
    .getByRole("button", { name: "Pause", exact: true })
    .boundingBox();
  expect(pauseBounds?.height).toBeGreaterThanOrEqual(44);
  await page.getByRole("link", { name: "236", exact: true }).click();
  await page.screenshot({
    path: `${artifacts}/jobs-mobile.png`,
    fullPage: false,
  });
  await page
    .getByRole("link", { name: /SendTransactionalEmail/ })
    .first()
    .click();
  await expect(page).toHaveURL(/jobs\/984213/);
  await page.screenshot({ path: `${artifacts}/job-detail-mobile.png` });
  for (const width of [320, 390, 768, 1024, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    for (const route of [
      "/",
      "/queues",
      "/jobs",
      "/queues/email-delivery",
      "/runtime",
    ]) {
      await page.goto(route);
      const heading =
        route === "/"
          ? "Work in motion"
          : route === "/queues"
            ? "Queues"
            : route === "/jobs"
              ? "Jobs"
              : route === "/runtime"
                ? "Workers"
                : "email-delivery";
      await expect(
        page.getByRole("heading", { name: heading, exact: true }),
      ).toBeVisible();
      if (route === "/jobs")
        await expect(
          page
            .getByText("SendTransactionalEmail")
            .filter({ visible: true })
            .first(),
        ).toBeVisible();
      if (route === "/runtime")
        await expect(
          page
            .getByText("worker-eu-02", { exact: true })
            .filter({ visible: true })
            .first(),
        ).toBeVisible();
      if (route === "/queues")
        await expect(
          page.getByRole("link", { name: "email-delivery", exact: true }),
        ).toBeVisible();
      if (route === "/")
        await expect(page.locator(".work-counter").first()).toContainText(
          "237,192",
        );
      await page.evaluate(() => document.fonts.ready);
      if (
        (width === 390 || width === 1440) &&
        (route === "/runtime" || route.startsWith("/queues/"))
      ) {
        const view = route === "/runtime" ? "workers" : "queue-detail";
        await page.screenshot({
          path: `${artifacts}/${view}-${width === 390 ? "mobile" : "desktop"}.png`,
        });
      }
      if (width === 768 && route === "/jobs")
        await page.screenshot({ path: `${artifacts}/jobs-tablet.png` });
      await expect
        .poll(
          () =>
            page.evaluate(
              () => document.documentElement.scrollWidth <= innerWidth,
            ),
          { message: `${route} at ${width}px should fit the viewport` },
        )
        .toBe(true);
    }
  }
});

for (const scenario of ["empty", "error", "loading"] as const) {
  test(`${scenario} state is explicit without invented zeroes`, async ({
    page,
  }) => {
    await mockOperations(page, scenario);
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.goto("/");
    if (scenario === "empty")
      await expect(
        page.getByText("No queues yet. Queues appear when registered or used."),
      ).toBeVisible();
    if (scenario === "error") {
      await expect(page.getByRole("alert")).toBeVisible({ timeout: 15000 });
      await expect(page.locator(".work-counter").first()).toContainText("—");
    }
    if (scenario === "loading")
      await expect(page.getByText("Loading snapshots…")).toBeVisible();
    await page.screenshot({
      path: `${artifacts}/${scenario}-desktop.png`,
      fullPage: true,
    });
  });
}

test("failed refresh retains known values and labels them out of date", async ({
  page,
}) => {
  const fixture = await mockOperations(page);
  await page.goto("/");
  await expect(page.locator(".work-counter").first()).toContainText("237,192");
  fixture.setReadFailure(true);
  await page.getByRole("button", { name: "Refresh data", exact: true }).click();
  await expect(page.getByText(/Refresh failed ·/)).toBeVisible({
    timeout: 15000,
  });
  await expect(page.locator(".work-counter").first()).toContainText("237,192");
  await page.screenshot({
    path: `${artifacts}/stale-desktop.png`,
    fullPage: true,
  });
  fixture.setReadFailure(false);
  await page.getByRole("button", { name: "Refresh data", exact: true }).click();
  await expect(page.getByText(/Snapshots received/)).toBeVisible();
});

test("read-only controls remain disabled and dark mode stays legible", async ({
  page,
}) => {
  const fixture = await mockOperations(page, "busy", true);
  await page.addInitScript(() => localStorage.setItem("theme", "dark"));
  await page.setViewportSize({ width: 1440, height: 1050 });
  await page.goto("/");
  await expect(
    page.getByText("Connected to a read-only database."),
  ).toBeVisible();
  await expect(page.locator("html")).toHaveClass(/dark/);
  await page.screenshot({
    path: `${artifacts}/overview-dark.png`,
    fullPage: true,
  });
  await page.goto("/queues");
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }).first(),
  ).toBeDisabled();
  await expect(
    page.getByRole("button", { name: "Drain…" }).first(),
  ).toBeDisabled();
  await page.goto("/jobs/984213");
  await expect(
    page.getByRole("button", { name: "Retry", exact: true }),
  ).toBeDisabled();
  expect(fixture.mutations).toEqual([]);
});

test("mobile drawer closes promptly with both motion preferences", async ({
  page,
}) => {
  await mockOperations(page);
  await page.setViewportSize({ width: 390, height: 844 });
  for (const reducedMotion of ["reduce", "no-preference"] as const) {
    await page.emulateMedia({ reducedMotion });
    await page.goto("/");
    for (const name of ["Queues", "Overview", "Workers", "Overview"]) {
      await page.getByRole("button", { name: "Toggle Sidebar" }).click();
      await page.getByRole("link", { name, exact: true }).click();
      await expect(page.getByRole("dialog", { name: "Sidebar" })).toHaveCount(
        0,
      );
      await expect(
        page.getByRole("heading", {
          name: name === "Overview" ? "Work in motion" : name,
          exact: true,
        }),
      ).toBeVisible();
    }
    await page.getByRole("button", { name: "Toggle Sidebar" }).click();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await expect(page.getByRole("dialog", { name: "Sidebar" })).toHaveCount(0);
    await expect(
      page.getByRole("button", { name: "Toggle Sidebar" }),
    ).toBeFocused();
  }
});
