import type { ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import type { QueueStats, QueueRuntimeSummary } from "@/lib/api";
import { dlqPolicyLabel } from "@/components/RuntimeDisplay";
import { availableWait } from "@/lib/operations";

export function QueuePressure({
  queues,
  runtime,
  actions,
}: {
  queues: QueueStats[];
  runtime?: Map<string, QueueRuntimeSummary>;
  actions?: (queue: QueueStats) => ReactNode;
}) {
  return (
    <div className="queue-board">
      <table className="pressure-table" aria-label="Queue pressure">
        <thead>
          <tr>
            <th scope="col">Queue</th>
            <th scope="col">Available</th>
            <th scope="col">Oldest available</th>
            <th scope="col">Running</th>
            <th scope="col">Retryable / failed</th>
            <th scope="col">
              Completed <span>last hour</span>
            </th>
            {actions && <th scope="col">Controls</th>}
          </tr>
        </thead>
        <tbody>
          {queues.map((queue) => {
            const workers = runtime?.get(queue.queue);
            return (
              <tr key={queue.queue}>
                <th scope="row" className="queue-identity">
                  <Link to="/queues/$name" params={{ name: queue.queue }}>
                    {queue.display_name?.trim() || queue.queue}
                  </Link>
                  {queue.display_name?.trim() && <code>{queue.queue}</code>}
                  <div className="queue-context">
                    {queue.paused && (
                      <span className="signal signal-warning">Paused</span>
                    )}
                    <span>
                      {queue.scheduled.toLocaleString()} scheduled ·{" "}
                      {queue.waiting_external.toLocaleString()} waiting external
                    </span>
                  </div>
                  {runtime && queue.description && (
                    <p className="queue-context">{queue.description}</p>
                  )}
                  {queue.descriptor_stale && (
                    <div className="queue-context concern-text">
                      Descriptor stale
                    </div>
                  )}
                  {queue.descriptor_mismatch && (
                    <div className="queue-context concern-text">
                      Descriptor drift across live runtimes
                    </div>
                  )}
                  {workers && (
                    <div className="queue-context">
                      {workers.healthy_instances}/{workers.live_instances} live
                      instances healthy
                      {workers.config_mismatch && " · config mismatch"}
                    </div>
                  )}
                  {workers?.config && (
                    <div className="queue-context">
                      {dlqPolicyLabel(workers.config)}
                    </div>
                  )}
                </th>
                <td data-label="Available">
                  <Link
                    to="/jobs"
                    search={{ state: "available", q: `queue:${queue.queue}` }}
                  >
                    {queue.available.toLocaleString()}
                  </Link>
                </td>
                <td data-label="Oldest available">
                  <span
                    className={
                      queue.available > 0 ? "wait-value" : "text-muted-fg"
                    }
                  >
                    {availableWait(queue.lag_seconds)}
                  </span>
                </td>
                <td data-label="Running">
                  <Link
                    to="/jobs"
                    search={{ state: "running", q: `queue:${queue.queue}` }}
                  >
                    {queue.running.toLocaleString()}
                  </Link>
                </td>
                <td data-label="Retryable / failed" className="queue-failures">
                  <Link
                    className={
                      queue.retryable > 0 ? "retry-value" : "text-muted-fg"
                    }
                    to="/jobs"
                    search={{ state: "retryable", q: `queue:${queue.queue}` }}
                  >
                    {queue.retryable.toLocaleString()}
                  </Link>
                  <span aria-hidden="true"> / </span>
                  <Link
                    className={
                      queue.failed > 0 ? "failure-value" : "text-muted-fg"
                    }
                    to="/jobs"
                    search={{ state: "failed", q: `queue:${queue.queue}` }}
                  >
                    {queue.failed.toLocaleString()}
                  </Link>
                </td>
                <td data-label="Completed · last hour">
                  {queue.completed_last_hour.toLocaleString()}
                </td>
                {actions && (
                  <td className="queue-controls" data-label="Controls">
                    {actions(queue)}
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
