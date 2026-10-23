import { useQuery } from "@tanstack/react-query";
import { Link } from "@tanstack/react-router";
import {
  fetchStats,
  fetchQueues,
  fetchJobs,
  fetchRuntime,
  fetchDlqDepth,
} from "@/lib/api";
import { compareQueuePressure, runtimeConcern } from "@/lib/operations";
import { timeAgo } from "@/lib/time";
import { usePollInterval } from "@/hooks/use-poll-interval";
import { QueuePressure } from "@/components/QueuePressure";
import { SnapshotStatus } from "@/components/SnapshotStatus";

const STATES = [
  ["available", "Available", "Ready to be claimed"],
  ["running", "Running", "Executing now"],
  ["retryable", "Retryable", "Waiting for another attempt"],
  ["failed", "Failed", "Requires review"],
  ["scheduled", "Scheduled", "Deferred until run time"],
  ["waiting_external", "Waiting external", "Awaiting a callback"],
] as const;

export function DashboardPage() {
  const poll = usePollInterval();
  const polling = { refetchInterval: poll.interval, staleTime: poll.staleTime };
  const stats = useQuery({
    queryKey: ["stats"],
    queryFn: fetchStats,
    ...polling,
  });
  const queues = useQuery({
    queryKey: ["queues"],
    queryFn: fetchQueues,
    ...polling,
  });
  const runtime = useQuery({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
    ...polling,
  });
  const failures = useQuery({
    queryKey: ["jobs", { state: "failed", limit: 5 }],
    queryFn: () => fetchJobs({ state: "failed", limit: 5 }),
    ...polling,
  });
  const dlq = useQuery({
    queryKey: ["dlq-depth"],
    queryFn: fetchDlqDepth,
    refetchInterval: Math.max(poll.interval, 60_000),
    staleTime: Math.max(poll.staleTime, 60_000),
  });
  const topQueues = [...(queues.data ?? [])]
    .sort(compareQueuePressure)
    .slice(0, 6);
  const workers = [...(runtime.data?.instances ?? [])]
    .sort(
      (left, right) =>
        Number(right.stale || !right.healthy) -
        Number(left.stale || !left.healthy),
    )
    .slice(0, 4);
  const completed = queues.data?.reduce(
    (sum, queue) => sum + queue.completed_last_hour,
    0,
  );
  const paused = queues.data?.filter((queue) => queue.paused).length;
  const degraded = runtime.data?.instances.filter(
    (instance) => !instance.stale && !instance.healthy,
  ).length;
  const sources = [stats, queues, runtime, failures, dlq];
  const errors = [
    "Job counts",
    "Queues",
    "Workers",
    "Failed jobs",
    "Dead letter queue",
  ].filter((_, index) => sources[index]?.isError);

  return (
    <div className="operations-overview">
      <div className="page-intro">
        <div>
          <p className="eyebrow">Operations / overview</p>
          <h1>Work in motion</h1>
          <p>Queue pressure, worker health, and work that needs attention.</p>
        </div>
        <Link to="/jobs" className="operator-link">
          Explore jobs ↗
        </Link>
      </div>
      <SnapshotStatus sources={[stats, queues, runtime, failures]} />
      {errors.length > 0 && (
        <div className="source-error" role="alert">
          Could not refresh: {errors.join(", ")}. Previous values remain visible
          where available. Use Refresh data to retry.
        </div>
      )}

      <div className="flow-summary">
        <div className="completion-summary">
          <p className="eyebrow">Completed · last 60 minutes</p>
          <Link
            to="/jobs"
            search={{ state: "completed" }}
            className="completion-value"
          >
            {completed?.toLocaleString() ?? "—"}
            <span> jobs</span>
          </Link>
          <p>
            Completions retained from a rolling hour. Current counts below are
            snapshots.
          </p>
        </div>
        <div className="attention-summary">
          <p className="eyebrow">Review & intervene</p>
          <Link to="/queues">
            <strong>{paused ?? "—"}</strong>
            <span>paused queues</span>
            <span aria-hidden="true">↗</span>
          </Link>
          <Link to="/runtime" search={{ lifecycle: "all" }}>
            <strong>
              {runtime.data
                ? (
                    runtime.data.stale_instances + (degraded ?? 0)
                  ).toLocaleString()
                : "—"}
            </strong>
            <span>stale or degraded instances</span>
            <span aria-hidden="true">↗</span>
          </Link>
          <Link to="/dlq">
            <strong>{dlq.data?.total.toLocaleString() ?? "—"}</strong>
            <span>
              dead letter jobs
              <span className="block text-xs text-muted-fg">
                refresh every {Math.max(poll.interval, 60_000) / 1000}s
              </span>
            </span>
            <span aria-hidden="true">↗</span>
          </Link>
        </div>
      </div>

      <section aria-labelledby="current-work">
        <div className="section-label">
          <h2 id="current-work">Current work</h2>
          <span>Counts now · may lag behind execution</span>
        </div>
        <div className="work-counters">
          {STATES.map(([state, label, description]) => (
            <Link
              key={state}
              to="/jobs"
              search={{ state }}
              className={`work-counter counter-${state}`}
            >
              <span>{label}</span>
              <strong>
                {stats.data ? (stats.data[state] ?? 0).toLocaleString() : "—"}
              </strong>
              <small>{description}</small>
            </Link>
          ))}
        </div>
      </section>

      <section className="operations-panel" aria-labelledby="queue-pressure">
        <div className="panel-heading">
          <div>
            <h2 id="queue-pressure">Where work is waiting</h2>
            <p>
              Top {topQueues.length} of {queues.data?.length ?? "—"} queues ·
              longest available wait first
            </p>
          </div>
          <Link to="/queues" className="operator-link">
            All queues ↗
          </Link>
        </div>
        {topQueues.length ? (
          <QueuePressure queues={topQueues} />
        ) : (
          <p className="panel-empty">
            {queues.isPending
              ? "Loading queue snapshots…"
              : queues.isError
                ? "Queue data unavailable."
                : "No queues yet. Queues appear when registered or used."}
          </p>
        )}
        <p className="panel-footnote">
          Oldest available = time since the earliest available job’s run time.
          Scheduled, retryable, and external waits are separate; a single
          snapshot does not show whether a backlog is growing.
        </p>
      </section>

      <div className="overview-details">
        <section className="operations-panel" aria-labelledby="worker-health">
          <div className="panel-heading">
            <div>
              <h2 id="worker-health">Worker health</h2>
              <p>
                {runtime.data
                  ? `${runtime.data.healthy_instances} healthy · ${runtime.data.live_instances} live · ${runtime.data.stale_instances} stale · ${runtime.data.leader_instances} leader`
                  : "Runtime snapshots"}
              </p>
            </div>
            <Link
              to="/runtime"
              search={{ lifecycle: "all" }}
              className="operator-link"
            >
              All workers ↗
            </Link>
          </div>
          <div className="worker-preview">
            {workers.length ? (
              workers.map((instance) => (
                <Link
                  key={instance.instance_id}
                  to="/runtime/$instanceId"
                  params={{ instanceId: instance.instance_id }}
                  className="worker-preview-row"
                >
                  <div>
                    <strong>
                      {instance.hostname || `pid ${instance.pid}`}
                    </strong>
                    <span
                      className={
                        instance.stale || !instance.healthy
                          ? "concern-text"
                          : "healthy-text"
                      }
                    >
                      {runtimeConcern(instance)}
                    </span>
                  </div>
                  <span>
                    Seen {timeAgo(instance.last_seen_at)}
                    <span className="worker-role">
                      {instance.leader ? "Leader" : "Worker"} ·{" "}
                      {instance.version}
                    </span>
                  </span>
                </Link>
              ))
            ) : (
              <p className="panel-empty">
                {runtime.isPending
                  ? "Loading workers…"
                  : runtime.isError
                    ? "Worker data unavailable."
                    : "No runtime snapshots. Start a worker to report health."}
              </p>
            )}
          </div>
          <p className="panel-footnote">
            Showing up to 4 instances, stale and degraded first. Stale snapshots
            cannot establish current worker health.
          </p>
        </section>
        <section className="operations-panel" aria-labelledby="recent-failures">
          <div className="panel-heading">
            <div>
              <h2 id="recent-failures">Failed jobs to inspect</h2>
              <p>Latest 5 retained failed jobs · not a failure rate</p>
            </div>
            <Link
              to="/jobs"
              search={{ state: "failed" }}
              className="operator-link"
            >
              All failed ↗
            </Link>
          </div>
          <div className="failure-preview">
            {failures.data?.length ? (
              failures.data.map((job) => {
                const last = job.errors?.[job.errors.length - 1];
                const message =
                  typeof last === "object" && last && "error" in last
                    ? String(last.error)
                    : last
                      ? JSON.stringify(last)
                      : "No error details recorded";
                return (
                  <Link
                    key={job.id}
                    to="/jobs/$id"
                    params={{ id: String(job.id) }}
                    className="failure-preview-row"
                  >
                    <div>
                      <strong>
                        {job.kind_descriptor?.display_name?.trim() || job.kind}
                      </strong>
                      <span>
                        #{job.id} · {job.queue} · attempt {job.attempt}/
                        {job.max_attempts}
                      </span>
                    </div>
                    <p>{message}</p>
                    <span>
                      {job.finalized_at
                        ? `Finalized ${timeAgo(job.finalized_at)}`
                        : "Finalization time unavailable"}{" "}
                      ↗
                    </span>
                  </Link>
                );
              })
            ) : (
              <p className="panel-empty">
                {failures.isPending
                  ? "Loading failed jobs…"
                  : failures.isError
                    ? "Failed-job data unavailable."
                    : "No failed jobs in retained data."}
              </p>
            )}
          </div>
        </section>
      </div>
    </div>
  );
}
