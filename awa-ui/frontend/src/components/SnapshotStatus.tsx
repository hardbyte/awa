import { useEffect, useState } from "react";
import { usePollInterval } from "@/hooks/use-poll-interval";

interface SnapshotSource {
  dataUpdatedAt: number;
  isError: boolean;
  isPending: boolean;
}
export function SnapshotStatus({ sources }: { sources: SnapshotSource[] }) {
  const poll = usePollInterval();
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  const updated = sources.map((source) => source.dataUpdatedAt);
  const oldest = Math.min(...updated);
  const failed = sources.some((source) => source.isError);
  const pending = sources.some((source) => source.isPending);
  const age = oldest ? Math.max(0, Math.floor((now - oldest) / 1000)) : null;
  const delayed =
    age !== null && age * 1000 > Math.max(30000, poll.interval * 2);
  return (
    <div
      className={`snapshot-status ${failed || delayed ? "snapshot-warning" : ""}`}
      role="status"
      aria-live="off"
    >
      <span className="status-dot" aria-hidden="true" />
      <span>
        {failed
          ? "Refresh failed · some data unavailable or out of date"
          : pending
            ? "Loading snapshots…"
            : delayed
              ? `Snapshot received ${age}s ago · refresh delayed`
              : `Snapshots received ${age ?? 0}s ago`}
        <span className="snapshot-cadence">
          {" "}
          · refresh every {poll.interval / 1000}s
        </span>
      </span>
    </div>
  );
}
