import { test, expect } from "@playwright/test";

/**
 * Repro / regression test for the "clicking a nav link expands the
 * docked sidebar" issue. Loads the page at desktop width, collapses the
 * sidebar via the topbar trigger, asserts it is docked, clicks a nav
 * link, then asserts the sidebar is STILL docked after the route change.
 */
test("docked sidebar stays docked across nav", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto("http://localhost:4200/");

  // Find the desktop sidebar container (data-slot=sidebar + not inside a sheet).
  const sidebarWrapper = page.locator('div[data-slot="sidebar"][data-side="left"]').first();
  await expect(sidebarWrapper).toBeVisible();

  // Initial state should be expanded.
  await expect(sidebarWrapper).toHaveAttribute("data-state", "expanded");

  // Collapse via the topbar trigger.
  const trigger = page.locator('button[data-slot="sidebar-trigger"]').first();
  await trigger.click();
  await expect(sidebarWrapper).toHaveAttribute("data-state", "collapsed");
  await expect(sidebarWrapper).toHaveAttribute("data-collapsible", "dock");

  // Click the Jobs nav link by tooltip/aria-label fallback.
  const jobsLink = page.locator('a[href="/jobs"][data-slot="sidebar-item"]').first();
  await expect(jobsLink).toBeVisible();
  await jobsLink.click();

  // URL should have changed.
  await expect(page).toHaveURL(/\/jobs$/);

  // The sidebar should STILL be collapsed after navigation.
  await expect(sidebarWrapper).toHaveAttribute("data-state", "collapsed");
  await expect(sidebarWrapper).toHaveAttribute("data-collapsible", "dock");
});

test("docked sidebar stays docked across refresh", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await page.goto("http://localhost:4200/");

  const sidebarWrapper = page.locator('div[data-slot="sidebar"][data-side="left"]').first();
  await expect(sidebarWrapper).toHaveAttribute("data-state", "expanded");

  // Collapse, then hard-refresh the page.
  await page.locator('button[data-slot="sidebar-trigger"]').first().click();
  await expect(sidebarWrapper).toHaveAttribute("data-state", "collapsed");

  await page.reload();

  const sidebarAfter = page.locator('div[data-slot="sidebar"][data-side="left"]').first();
  await expect(sidebarAfter).toHaveAttribute("data-state", "collapsed");
});
