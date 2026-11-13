import { afterEach, describe, expect, it, vi } from "vitest";
import { createMemoryHistory, createRootRoute, createRoute, createRouter } from "@tanstack/react-router";
import { apiPath, getBasePath, isExternalHref, mountRewrite } from "@/lib/base-path";
import { fetchCapabilities, fetchStorage } from "@/lib/api";

afterEach(() => vi.unstubAllGlobals());

function setBase(base: string | null, root = base?.replace(/\/$/, "") || "/") {
  vi.stubGlobal("document", {
    querySelector: () => base === null ? null : { getAttribute: (name: string) => name === "href" ? base : root },
  });
}

describe("mounted frontend URLs", () => {
  it("defaults to the root for Vite development and non-browser imports", () => {
    expect(getBasePath()).toBe("/");
    setBase(null);
    expect(apiPath("/jobs")).toBe("/api/jobs");
  });

  it.each(["/", "/awa/", "/admin/awa/"])("scopes both API fetch paths under %s", async (base) => {
    setBase(base);
    const fetch = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
    vi.stubGlobal("fetch", fetch);
    await fetchCapabilities();
    await fetchStorage({ history: true });
    expect(fetch.mock.calls.map(([path]) => path)).toEqual([
      `${base}api/capabilities`,
      `${base}api/storage?history=true`,
    ]);
  });

  it.each([
    ["/", "/"],
    ["/awa/", "/awa"],
    ["/awa/", "/awa/"],
    ["/jobs/jobs/", "/jobs/jobs"],
  ])("resolves deep routes and native link hrefs under %s (root %s)", async (base, mountRoot) => {
    setBase(base, mountRoot);
    const rewrite = mountRewrite();
    vi.unstubAllGlobals();
    const root = createRootRoute();
    const jobs = createRoute({ getParentRoute: () => root, path: "/jobs/$id" });
    const router = createRouter({
      routeTree: root.addChildren([jobs]),
      isServer: true,
      rewrite,
      history: createMemoryHistory({ initialEntries: [`${base}jobs/123`] }),
    });
    await router.load();
    expect(router.state.matches.slice(-1)[0]?.routeId).toBe("/jobs/$id");
    expect(router.state.location.pathname).toBe("/jobs/123");
    expect(router.buildLocation({ to: "/jobs/456" }).href).toBe(`${base}jobs/456`);
    expect(router.buildLocation({ to: "/jobs/456", search: { state: "failed" }, hash: "attempts" }).href)
      .toBe(`${base}jobs/456?state=failed#attempts`);
    expect(router.buildLocation({ to: "/" }).href).toBe(mountRoot);
    await router.navigate({ to: "/" });
    expect(router.state.location.pathname).toBe("/");
    expect(router.history.location.pathname).toBe(mountRoot);
  });

  it("preserves external docs links through react-aria's href adapter", () => {
    for (const href of ["https://example.com/docs", "mailto:ops@example.com", "//example.com/docs"]) {
      expect(isExternalHref(href)).toBe(true);
    }
    expect(isExternalHref("/jobs")).toBe(false);
  });
});
