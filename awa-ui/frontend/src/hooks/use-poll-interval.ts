import { useQuery } from "@tanstack/react-query";
import { fetchCapabilities } from "@/lib/api";
import { POLL } from "@/lib/constants";

interface PollConfig {
  /** Interval between automatic refetches (ms). */
  interval: number;
  /** How long data is considered fresh before a refetch is needed (ms). */
  staleTime: number;
}

/**
 * Returns the server-suggested polling interval and a matching staleTime.
 *
 * Falls back to `POLL.DEFAULT` while the capabilities query is loading.
 * The server derives the interval from the cache TTL — there is no point
 * polling faster than the cache refreshes. staleTime is set to half the
 * interval so tab-focus refetches feel responsive without redundant fetches.
 */
export function usePollInterval(): PollConfig {
  const { data } = useQuery({
    queryKey: ["capabilities"],
    queryFn: fetchCapabilities,
    staleTime: 60_000,
  });

  const interval = data?.poll_interval_ms ?? POLL.DEFAULT;
  return { interval, staleTime: Math.floor(interval / 2) };
}
