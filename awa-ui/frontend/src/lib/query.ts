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

/** The parts of a list query's state that say whose rows it holds. */
export interface ListReadState {
  isPending: boolean;
  isError: boolean;
  isPlaceholderData: boolean;
}

/**
 * Whether a list's rows belong to the filter and page now in the URL.
 * Actions that act on the whole filter, or a cursor taken from the last row,
 * are judged against those rows. A pending or failed read has none of its
 * own. Placeholder rows are the previous filter's, kept on screen while the
 * new read runs, and TanStack Query reports them with success status.
 */
export function listMatchesFilter(query: ListReadState): boolean {
  return !query.isPending && !query.isError && !query.isPlaceholderData;
}

/**
 * The row a "Next page" cursor is taken from: the last row of a full page
 * that belongs to the current filter. Undefined while the list shows a short
 * page, or the previous filter's placeholder rows, whose cursor would skip
 * or repeat rows of the new filter.
 */
export function nextPageAnchor<T>(rows: readonly T[], limit: number, query: ListReadState): T | undefined {
  return rows.length >= limit && listMatchesFilter(query) ? rows[rows.length - 1] : undefined;
}
