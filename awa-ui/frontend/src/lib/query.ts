import { QueryClient } from "@tanstack/react-query";
import { POLL } from "@/lib/constants";

export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchInterval: POLL.DEFAULT,
      refetchIntervalInBackground: false, // Pause polling when tab is hidden
      staleTime: POLL.DEFAULT / 2,
      refetchOnWindowFocus: true,
    },
  },
});
