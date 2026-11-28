import { describe, expect, it } from "vitest";
import { PALETTE_SECTION_LIMIT, paletteItems } from "@/lib/palette";

const data = {
  queues: [
    { queue: "email", display_name: "Outbound email" },
    { queue: "imports", display_name: null },
  ],
  kinds: [{ kind: "send_email", display_name: null }],
  schedules: [{ name: "nightly report", kind: "report" }],
  batchOps: [{ id: "a1b2c3d4-0000-4000-8000-000000000000", op_kind: "retry" }],
};

describe("paletteItems", () => {
  it("lists pages first when nothing is typed", () => {
    const items = paletteItems("", data);
    expect(items[0]).toMatchObject({ section: "Pages", label: "Overview", target: { to: "/" } });
    expect(items.filter((i) => i.section === "Pages").map((i) => i.label)).toContain("Dead letters");
  });

  it("offers a job for a number, with or without #", () => {
    expect(paletteItems("600001", data)[0]).toMatchObject({
      label: "Open job #600001",
      target: { to: "/jobs/$id", params: { id: "600001" } },
    });
    expect(paletteItems("#42", data)[0]?.target).toEqual({ to: "/jobs/$id", params: { id: "42" } });
    expect(paletteItems("0", data).some((i) => i.id === "open-job")).toBe(false);
  });

  it("matches queues by key or display name and shows the key beside a display name", () => {
    const byName = paletteItems("outbound", data).filter((i) => i.section === "Queues");
    expect(byName).toEqual([
      expect.objectContaining({ label: "Outbound email", detail: "email", target: { to: "/queues/$name", params: { name: "email" } } }),
    ]);
    const byKey = paletteItems("imports", data).filter((i) => i.section === "Queues");
    expect(byKey[0]?.label).toBe("imports");
    expect(byKey[0]?.detail).toBeUndefined();
  });

  it("ranks an exact match ahead of a partial one", () => {
    const queues = [{ queue: "email_bulk" }, { queue: "email" }];
    expect(paletteItems("email", { queues }).filter((i) => i.section === "Queues").map((i) => i.label)).toEqual([
      "email",
      "email_bulk",
    ]);
  });

  it("keeps schedule names with spaces as route params", () => {
    expect(paletteItems("nightly", data).find((i) => i.section === "Schedules")?.target).toEqual({
      to: "/cron/$name",
      params: { name: "nightly report" },
    });
  });

  it("keeps the id of an entry built from the input as the input changes", () => {
    expect(paletteItems("6", data)[0]?.id).toBe(paletteItems("600001", data)[0]?.id);
  });

  it("caps each section", () => {
    const queues = Array.from({ length: 20 }, (_, i) => ({ queue: `q${i}` }));
    expect(paletteItems("q", { queues }).filter((i) => i.section === "Queues")).toHaveLength(PALETTE_SECTION_LIMIT);
  });

  it("opens a bulk operation by full id or by its 8-character prefix", () => {
    const full = "A1B2C3D4-1111-4111-8111-111111111111";
    expect(paletteItems(full, data)[0]?.target).toEqual({ to: "/batch-ops/$id", params: { id: full.toLowerCase() } });
    expect(paletteItems("a1b2c3d4", data)[0]).toMatchObject({
      section: "Bulk operations",
      target: { to: "/batch-ops/$id", params: { id: data.batchOps[0]?.id } },
    });
    expect(paletteItems("ffffffff", data).some((i) => i.section === "Bulk operations")).toBe(false);
  });

  it("turns a filter term into a jobs search and other text with a colon into a tag", () => {
    expect(paletteItems("kind:send_email queue:email", data)[0]).toMatchObject({
      label: "Jobs matching kind:send_email queue:email",
      target: { to: "/jobs", search: { q: "kind:send_email queue:email" } },
    });
    expect(paletteItems("tenant:acme", data)[0]).toMatchObject({
      label: "Jobs tagged tenant:acme",
      target: { to: "/jobs", search: { q: "tag:tenant:acme" } },
    });
    expect(paletteItems("bad: term", data).some((i) => i.section === "Jobs")).toBe(false);
  });
});
