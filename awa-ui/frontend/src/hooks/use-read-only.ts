import { useQuery } from "@tanstack/react-query";
import { fetchCapabilities } from "@/lib/api";

/**
 * Returns whether the server is in read-only mode.
 *
 * Defaults to `true` (safe) while the capabilities query is loading or
 * has errored, so mutation buttons are never briefly enabled on a
 * read-only replica.
 */
export function useReadOnly(): boolean {
  const capabilitiesQuery = useQuery({
    queryKey: ["capabilities"],
    queryFn: fetchCapabilities,
    staleTime: 60_000,
  });

  if (!capabilitiesQuery.isSuccess) {
    // Loading or error — assume read-only to avoid false-writable window
    return true;
  }

  return capabilitiesQuery.data.read_only;
}
