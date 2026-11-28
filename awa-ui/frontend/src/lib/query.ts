import { QueryClient } from "@tanstack/react-query";
import { HttpError } from "@/lib/api";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Polling interval and staleTime are set per-query via
      // usePollInterval() so the server can control the rate.
      refetchInterval: false,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: "always",
      retry: (failureCount, error) =>
        !(error instanceof HttpError && error.status === 404) && failureCount < 3,
    },
  },
});
