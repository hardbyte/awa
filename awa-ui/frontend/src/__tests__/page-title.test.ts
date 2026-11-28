import { describe, it, expect } from "vitest";
import { documentTitle, pageTitle } from "@/lib/page-title";

describe("documentTitle", () => {
  it("names the page for each route", () => {
    expect(documentTitle("/")).toBe("Dashboard · AWA");
    expect(documentTitle("/jobs")).toBe("Jobs · AWA");
    expect(documentTitle("/jobs/42")).toBe("Job #42 · AWA");
    expect(documentTitle("/dlq")).toBe("Dead letters · AWA");
    expect(documentTitle("/queues/email%20out")).toBe("Queue email out · AWA");
    expect(documentTitle("/batch-ops/")).toBe("Batch Ops · AWA");
    expect(documentTitle("/nope")).toBe("Not found · AWA");
  });
});

describe("pageTitle", () => {
  it("names detail pages from the decoded segment", () => {
    expect(pageTitle("/queues/email%20high")).toBe("Queue email high");
    expect(pageTitle("/jobs/42")).toBe("Job #42");
  });

  it("keeps a malformed escape as written instead of throwing", () => {
    expect(pageTitle("/queues/%")).toBe("Queue %");
  });
});
