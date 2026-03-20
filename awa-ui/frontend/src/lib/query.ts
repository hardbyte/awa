import { QueryClient } from "@tanstack/react-query";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchInterval: 2000,
      refetchIntervalInBackground: false, // Pause polling when tab is hidden
      staleTime: 1000,
      refetchOnWindowFocus: true,
    },
  },
});
