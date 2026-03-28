import { QueryClient } from "@tanstack/react-query";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Polling interval and staleTime are set per-query via
      // usePollInterval() so the server can control the rate.
      refetchInterval: false,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: "always",
    },
  },
});
