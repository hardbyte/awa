import { QueryClient } from "@tanstack/react-query";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Managed views use the tab observation scheduler. Catalog views
      // retain their existing per-query polling and focus behavior.
      refetchInterval: false,
      refetchIntervalInBackground: false,
      refetchOnWindowFocus: "always",
    },
  },
});
