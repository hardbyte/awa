---
hide:
  - toc
---

# Interactive failure lab

Step through a crash, change one assumption, and replay the outcome. The lab follows one order and one Awa job across four boundaries:

| Experiment | What to inspect |
| --- | --- |
| The handoff | Whether the application write and enqueue commit in the same PostgreSQL transaction. |
| The replay | An external effect can succeed before a worker crashes without recording completion. |
| The late worker | Guarded completion rejects a closed attempt while external calls can still repeat. |
| The lost claim | An acknowledged queue-storage claim can disappear before its WAL is flushed, leaving the already durable job claimable again. |

[Open the lab in a full page](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html){ .md-button .md-button--primary }

<iframe
  class="awa-failure-lab"
  src="https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html"
  title="Interactive Awa failure windows simulation"
  loading="lazy"
  referrerpolicy="no-referrer"
>
  <p><a href="https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html">Open the interactive Awa failure lab.</a></p>
</iframe>

!!! note "A teaching model, not a benchmark"
    The animation uses deterministic events and compressed waits. Its playback speed does not predict recovery latency. It assumes a working maintenance path after recovery and sufficient attempts to retry. The provider-key comparison assumes the provider atomically deduplicates the same operation key across workers; Awa does not supply that external guarantee.

    The lab shows the Awa source revision it describes and links each transition to that source. Development-revision behavior is not a promise about every released version. The lost-claim experiment models a local PostgreSQL crash with an already durable enqueue and storage that honors flushes; it does not model arbitrary standby promotion or storage corruption.

If the embedded page is unavailable or difficult to use on a narrow screen, use the full-page link above. The written guides also explain [transactional enqueue](transactional-enqueue.md), [job lifecycle and recovery](job-lifecycle.md), and [queue-storage architecture](../architecture.md).
