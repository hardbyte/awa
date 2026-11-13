import { afterEach, describe, it, expect, vi } from "vitest";
import { shouldRetry } from "@/lib/query";
import { ApiError, fetchJobs } from "@/lib/api";

describe("shouldRetry", () => {
  it("does not retry a 4xx", () => {
    expect(shouldRetry(0, new ApiError(400, "narrow the filter"))).toBe(false);
    expect(shouldRetry(0, new ApiError(404, "not found"))).toBe(false);
    expect(shouldRetry(0, new ApiError(401, "unauthorized"))).toBe(false);
  });

  it("retries a 5xx a few times", () => {
    const error = new ApiError(503, "unavailable");
    expect(shouldRetry(0, error)).toBe(true);
    expect(shouldRetry(1, error)).toBe(true);
    expect(shouldRetry(2, error)).toBe(false);
  });

  it("retries a network error a few times", () => {
    const error = new TypeError("Failed to fetch");
    expect(shouldRetry(0, error)).toBe(true);
    expect(shouldRetry(2, error)).toBe(false);
  });
});

describe("ApiError", () => {
  afterEach(() => vi.unstubAllGlobals());

  it("carries a failed response's status and the server's message", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json({ error: "narrow the filter" }, { status: 400 }))
    );
    const error = await fetchJobs().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ApiError);
    expect((error as ApiError).status).toBe(400);
    expect((error as ApiError).message).toBe("narrow the filter");
  });

  it("falls back to the status text without a JSON body", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("oops", { status: 502, statusText: "Bad Gateway" }))
    );
    const error = await fetchJobs().catch((e: unknown) => e);
    expect((error as ApiError).status).toBe(502);
    expect((error as ApiError).message).toBe("Bad Gateway");
  });
});
