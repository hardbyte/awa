import { test, expect } from "@playwright/test";

const shortcut = process.platform === "darwin" ? "Meta+k" : "Control+k";

test.describe("Jump to (command palette)", () => {
  test("the shortcut opens it and a job id opens the job, following it to the DLQ", async ({ page }) => {
    await page.goto("/jobs");
    await expect(page.getByRole("heading", { level: 1, name: "Jobs" })).toBeVisible();
    await page.keyboard.press(shortcut);
    const dialog = page.getByRole("dialog", { name: "Jump to" });
    await expect(dialog).toBeVisible();
    await dialog.getByRole("searchbox").pressSequentially("600001");
    await expect(dialog.getByRole("menuitem", { name: "Open job #600001" })).toBeVisible();
    await page.keyboard.press("Enter");
    await expect(page).toHaveURL(/\/dlq\/600001$/);
    await expect(dialog).toBeHidden();
  });

  test("typing a queue name opens its hub", async ({ page }) => {
    await page.goto("/");
    await page.getByTestId("command-palette-trigger").click();
    const dialog = page.getByRole("dialog", { name: "Jump to" });
    await dialog.getByRole("searchbox").fill("e2e_test");
    const item = dialog.getByRole("menuitem").filter({ hasText: "E2E Queue" });
    await expect(item).toBeVisible();
    await item.click();
    await expect(page).toHaveURL(/\/queues\/e2e_test$/);
  });

  test("a page name opens that page", async ({ page }) => {
    await page.goto("/");
    await page.keyboard.press(shortcut);
    const dialog = page.getByRole("dialog", { name: "Jump to" });
    await dialog.getByRole("searchbox").fill("bulk");
    await dialog.getByRole("menuitem", { name: "Bulk operations" }).click();
    await expect(page).toHaveURL(/\/batch-ops$/);
  });

  test("Escape closes it and returns focus to the button that opened it", async ({ page }) => {
    await page.goto("/");
    const trigger = page.getByTestId("command-palette-trigger");
    await trigger.click();
    const dialog = page.getByRole("dialog", { name: "Jump to" });
    await expect(dialog.getByRole("searchbox")).toBeFocused();
    await expect(page.locator("[data-entering]")).toHaveCount(0);
    await page.keyboard.press("Escape");
    await expect(dialog).toBeHidden();
    await expect(trigger).toBeFocused();
  });
});
