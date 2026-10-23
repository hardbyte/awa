> For the complete AWA documentation index, see [`llms.txt`](../../llms.txt).

# Where does the guarantee end?

A committed enqueue, a worker claim and a successful external request protect different things. Follow one order through a crash to see the boundary you need to design around.

<div class="awa-failure-launch" markdown>

**Start with the failure you want to understand.** Each link opens a full-page lesson, with the current event, state and playback controls together.

- [Can an order commit without its job? →](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html?experiment=handoff) Compare a shared transaction with separate commits.
- [Could one job send two emails? →](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html?experiment=replay) Crash after the external effect, before completion is recorded.
- [What if an old worker returns? →](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html?experiment=fence) Inspect guarded completion and overlapping external requests.
- [Could an acknowledged claim disappear? →](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html?experiment=wal) Inspect a local crash before asynchronous claim WAL is flushed.

</div>

[Start the interactive lesson](https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html?experiment=handoff){ .md-button .md-button--primary }

<details class="awa-failure-desktop">
<summary>Explore inside this page on a wider screen</summary>
<iframe
  class="awa-failure-lab"
  src="https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html"
  width="100%"
  height="1450"
  title="Interactive Awa failure windows simulation"
  loading="lazy"
  referrerpolicy="no-referrer"
>
  <p><a href="https://hardbyte.github.io/postgresql-job-queue-benchmarking/lab/awa.html">Open the interactive Awa failure lab.</a></p>
</iframe>
</details>

??? note "What the teaching model assumes"
    The animation uses deterministic events and compressed waits. Its playback speed does not predict recovery latency. It assumes a working maintenance path after recovery and sufficient attempts to retry. The provider-key comparison assumes the provider atomically deduplicates the same operation key across workers; Awa does not supply that external guarantee.

    The lab shows the Awa source revision it describes and links each transition to that source. Development-revision behavior is not a promise about every released version. The lost-claim experiment models a local PostgreSQL crash with an already durable enqueue and storage that honors flushes; it does not model arbitrary standby promotion or storage corruption.

Continue with [transactional enqueue](../transactional-enqueue/index.md), [job lifecycle and recovery](../job-lifecycle/index.md), or [queue-storage architecture](../../architecture/index.md).
