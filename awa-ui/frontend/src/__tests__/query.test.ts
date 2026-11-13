import { afterEach, describe, it, expect, vi } from "vitest";
import { keepPreviousData, QueryClient, QueryObserver } from "@tanstack/react-query";
import { listMatchesFilter, nextPageAnchor, shouldRetry } from "@/lib/query";
import { ApiError, fetchJobs } from "@/lib/api";
import { HttpError } from "@/lib/observation/read-control";

describe("shouldRetry", () => {
  it("recognizes the HTTP errors emitted by observation reads", () => {
    expect(shouldRetry(0, new HttpError("narrow the filter", 400))).toBe(false);
    expect(shouldRetry(0, new HttpError("not found", 404))).toBe(false);
    expect(shouldRetry(0, new HttpError("unavailable", 503))).toBe(true);
  });
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
    expect(shouldRetry(0, error)).toBe(false);
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

describe("list reads after a filter change", () => {
  // The jobs and DLQ lists keep the previous filter's rows on screen while
  // the new filter's read runs. Drive a real observer through that change.
  function switchFilter() {
    const client = new QueryClient();
    client.setQueryData(["dlq", "kind:a"], [{ id: 3 }, { id: 2 }]);
    const options = (filter: string) => ({
      queryKey: ["dlq", filter],
      queryFn: async () => [] as Array<{ id: number }>,
      enabled: false,
      placeholderData: keepPreviousData,
    });
    const observer = new QueryObserver(client, options("kind:a"));
    const unsubscribe = observer.subscribe(() => {});
    observer.setOptions(options("kind:b"));
    return { client, observer, unsubscribe };
  }

  it("does not treat the previous filter's rows as this filter's", () => {
    const { client, observer, unsubscribe } = switchFilter();
    const placeholder = observer.getCurrentResult();
    // The hazard: placeholder rows are a success, not pending or failed.
    expect(placeholder.isPlaceholderData).toBe(true);
    expect(placeholder.isSuccess).toBe(true);
    expect(placeholder.isPending || placeholder.isError).toBe(false);
    expect(placeholder.data).toEqual([{ id: 3 }, { id: 2 }]);
    expect(listMatchesFilter(placeholder)).toBe(false);

    client.setQueryData(["dlq", "kind:b"], [{ id: 9 }]);
    expect(listMatchesFilter(observer.getCurrentResult())).toBe(true);
    unsubscribe();
  });

  it("takes no next-page cursor from placeholder rows", () => {
    const { client, observer, unsubscribe } = switchFilter();
    const placeholder = observer.getCurrentResult();
    expect(nextPageAnchor(placeholder.data ?? [], 2, placeholder)).toBeUndefined();

    client.setQueryData(["dlq", "kind:b"], [{ id: 9 }, { id: 8 }]);
    const current = observer.getCurrentResult();
    expect(nextPageAnchor(current.data ?? [], 2, current)).toEqual({ id: 8 });
    expect(nextPageAnchor(current.data ?? [], 3, current)).toBeUndefined();
    unsubscribe();
  });

  it("has no rows of its own while pending or after a failure", () => {
    const base = { isPending: false, isError: false, isPlaceholderData: false };
    expect(listMatchesFilter(base)).toBe(true);
    expect(listMatchesFilter({ ...base, isPending: true })).toBe(false);
    expect(listMatchesFilter({ ...base, isError: true })).toBe(false);
  });
});
