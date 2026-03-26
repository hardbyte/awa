import { useQuery } from "@tanstack/react-query";
import { fetchCapabilities } from "@/lib/api";
import { POLL } from "@/lib/constants";

/**
 * Returns the server-suggested polling interval in milliseconds.
 *
 * Falls back to `POLL.DEFAULT` while the capabilities query is loading.
 * The server derives this from the cache TTL — there is no point polling
 * faster than the cache refreshes.
 */
export function usePollInterval(): number {
  const { data } = useQuery({
    queryKey: ["capabilities"],
    queryFn: fetchCapabilities,
    staleTime: 60_000,
  });

  return data?.poll_interval_ms ?? POLL.DEFAULT;
}
