import { QueryClient } from "@tanstack/react-query";
import { ApiError } from "@/lib/api";

/** Retries after a failure. */
const MAX_RETRIES = 2;

/**
 * Whether a failed query is worth asking again. A 4xx says the request
 * itself is refused (a bad filter, one too wide for the server's read
 * budget), so asking again only delays showing why; network errors and
 * 5xx responses may pass.
 */
export function shouldRetry(failureCount: number, error: unknown): boolean {
  if (error instanceof ApiError && error.status >= 400 && error.status < 500) {
    return false;
  }
  return failureCount < MAX_RETRIES;
}

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Polling interval and staleTime are set per-query via
      // usePollInterval() so the server can control the rate.
      refetchInterval: false,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: "always",
      retry: shouldRetry,
    },
  },
});
