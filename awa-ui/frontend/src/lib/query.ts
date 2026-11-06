import { QueryClient } from "@tanstack/react-query";
import { HttpError } from "@/lib/observation/read-control";

/** Retries after a failure. */
const MAX_RETRIES = 2;

/**
 * Whether a failed query is worth asking again. A 4xx says the request
 * itself is refused (a bad filter, one too wide for the server's read
 * budget), so asking again only delays showing why; network errors and
 * 5xx responses may pass.
 */
export function shouldRetry(failureCount: number, error: unknown): boolean {
  if (error instanceof HttpError && error.status >= 400 && error.status < 500) {
    return false;
  }
  return failureCount < MAX_RETRIES;
}

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Managed views use the tab observation scheduler. Catalog views
      // retain their existing per-query polling and focus behavior.
      refetchInterval: false,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: "always",
      retry: shouldRetry,
    },
  },
});
