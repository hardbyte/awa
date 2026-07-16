import { describe, it, expect } from "vitest";
import {
  DEFAULT_FAVICON_COLOR,
  faviconDataUri,
  faviconSvg,
  instanceTitle,
  normalizeInstanceColor,
} from "@/lib/instance";

describe("instanceTitle", () => {
  it("uses the plain product title without a name", () => {
    expect(instanceTitle(null)).toBe("AWA");
    expect(instanceTitle(undefined)).toBe("AWA");
    expect(instanceTitle("   ")).toBe("AWA");
  });

  it("prefixes the instance name", () => {
    expect(instanceTitle("cloudsql-prod")).toBe("cloudsql-prod — AWA");
    expect(instanceTitle("  alloydb  ")).toBe("alloydb — AWA");
  });
});

describe("normalizeInstanceColor", () => {
  it("accepts 3/4/6/8-digit hex", () => {
    expect(normalizeInstanceColor("#abc")).toBe("#abc");
    expect(normalizeInstanceColor("#abcd")).toBe("#abcd");
    expect(normalizeInstanceColor("#0ea5e9")).toBe("#0ea5e9");
    expect(normalizeInstanceColor("#0ea5e9ff")).toBe("#0ea5e9ff");
    expect(normalizeInstanceColor(" #0ea5e9 ")).toBe("#0ea5e9");
  });

  it("rejects anything else (defense against style injection)", () => {
    expect(normalizeInstanceColor(null)).toBeNull();
    expect(normalizeInstanceColor("red")).toBeNull();
    expect(normalizeInstanceColor("#abcde")).toBeNull();
    expect(normalizeInstanceColor("#0ea5e9;background:url(x)")).toBeNull();
    expect(normalizeInstanceColor('url("https://evil")')).toBeNull();
  });
});

describe("favicon", () => {
  it("tints the SVG with the instance color", () => {
    expect(faviconSvg("#ff0000")).toContain('stroke="#ff0000"');
  });

  it("falls back to the default tint for missing or invalid colors", () => {
    expect(faviconSvg(null)).toContain(`stroke="${DEFAULT_FAVICON_COLOR}"`);
    expect(faviconSvg("not-a-color")).toContain(
      `stroke="${DEFAULT_FAVICON_COLOR}"`
    );
  });

  it("produces an inline SVG data URI", () => {
    const uri = faviconDataUri("#0ea5e9");
    expect(uri.startsWith("data:image/svg+xml,")).toBe(true);
    expect(decodeURIComponent(uri)).toContain("#0ea5e9");
  });
});
