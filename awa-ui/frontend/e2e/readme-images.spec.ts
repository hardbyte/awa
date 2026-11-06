import { test, expect } from "@playwright/test";
import { mockOperations, type Phase } from "./operations-fixture";

// Regenerates the README screenshots from the synthetic fleet:
//   AWA_README_IMAGES_DIR=../../docs/images npm run test:operations -- readme-images
const out = process.env.AWA_README_IMAGES_DIR;

test.use({ viewport: { width: 1440, height: 1180 }, deviceScaleFactor: 2 });

for (const [phase, verdict] of [
  ["incident", "1 queue not being worked"],
  ["recovery", "Recovering"],
] as [Phase, string][]) {
  test(`README image: overview during ${phase}`, async ({ page }) => {
    test.skip(!out, "set AWA_README_IMAGES_DIR to regenerate README images");
    await page.addInitScript(() => localStorage.setItem("theme", "dark"));
    await mockOperations(page, phase);
    await page.goto("/");
    await expect(page.getByTestId("verdict")).toHaveText(verdict);
    await expect(page.locator("tbody tr").first()).toBeVisible();
    await page.evaluate(() => document.fonts.ready);
    await page.screenshot({ path: `${out}/awa-ui-overview-${phase}-dark.png` });
  });
}
