import { QueryClient } from "@tanstack/react-query";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Polling interval is set per-query via usePollInterval() so the
      // server can control the rate (e.g. slower for read-only replicas).
      // Individual routes opt-in to refetchInterval explicitly.
      refetchInterval: false,
      refetchIntervalInBackground: false,
      staleTime: 2_500,
      refetchOnWindowFocus: "always",
    },
  },
});
