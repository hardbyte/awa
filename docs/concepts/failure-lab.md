---
hide:
  - toc
---

# One job. How many receipts?

Can you complete one job and deliver two receipts? Operate a worker, stop it at different moments, and recover its unfinished work. The database and external mailbox show what each action changed.

<div class="awa-failure-launch" markdown>

**Try the crash window yourself.** Send a receipt, then stop the worker before saving completion. Recover the job and start a replacement. Does the mailbox change?

Then repeat with the provider remembering a stable operation key. Compare the handler starts, external requests and delivered receipts from your own runs.

[Open the interactive model <span aria-hidden="true">→</span>](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html){ .md-button .md-button--primary }

</div>

The model starts with a committed order and job. It is an Awa-specific teaching model, not a live system or a benchmark. No email is sent. Source links and technical assumptions are available inside it.

<details class="awa-failure-desktop">
<summary>Explore inside this page on a wider screen</summary>
<iframe
  class="awa-failure-lab"
  src="https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html"
  width="100%"
  height="1100"
  title="Interactive Awa job, worker and receipt model"
  loading="lazy"
  referrerpolicy="no-referrer"
  sandbox="allow-scripts allow-popups"
>
  <p><a href="https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html">Open the interactive Awa model.</a></p>
</iframe>
</details>

??? note "What the model assumes"
    The worker really stops and its database claim survives. “Recover for retry” compresses rescue, backoff and due-job promotion; it assumes healthy maintenance and sufficient retry budget. Clicks do not predict recovery latency. Database crashes, standby failover, live stale workers and retry exhaustion are outside this example. If the stopped attempt was the job's last, recovery fails the job instead of retrying it, even though its receipt may already have been delivered; see [poison jobs](job-lifecycle.md#poison-jobs).

    The provider-key option assumes an external provider atomically deduplicates a stable operation key across attempts. Awa does not supply that external guarantee, and enqueue uniqueness is a different boundary. The component identifies the development source revision it describes, rather than promising behavior across every release.

Continue with [transactional enqueue](transactional-enqueue.md), [job lifecycle and recovery](job-lifecycle.md), or [queue-storage architecture](../architecture.md).
