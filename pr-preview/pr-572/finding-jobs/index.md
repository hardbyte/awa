> For the complete AWA documentation index, see [`llms.txt`](../llms.txt).

# Finding jobs

One filter describes "which jobs" everywhere Awa answers the question: the
admin UI's jobs page and DLQ page, `awa job list` and `awa dlq list`, Python's
`find_jobs`, and batch operations' preview and execution
([ADR-049](../adr/049-job-filter-and-indexed-tags/index.md)). The same filter selects
the same jobs on every one of them.

Finding jobs needs the queue-storage engine. On the deprecated canonical
engine ([ADR-037](../adr/037-canonical-engine-deprecation/index.md)) these calls
return an error naming the missing queue_storage backend. The exception is a
fresh install that holds no jobs yet: listing and counting there return
nothing, and batch operations are still refused.

## The filter

| Field | Meaning |
| --- | --- |
| `kind` | The job's kind. |
| `queue` | The job's queue. |
| `state` | One state, or a list: any of them. |
| `tag` | One tag, or a list: all of them (at most 20). |
| `dead_lettered` | `true` for only dead-lettered jobs, `false` for none of them. |
| `ids` | Only these job ids (at most 1,000). |
| `created_at_gte`, `created_at_lt` | The original insert time, unchanged by retries and moves. |

A field that is not set does not narrow the population. Unknown fields are an
error, so a typo cannot widen what a purge or batch operation acts on. In
Rust the lists are the `states` and `tags` fields.

A DLQ purge or bulk retry needs a field that narrows the DLQ (kind, queue,
tag, ids or a created bound), or `allow_all`. Over HTTP the full filter goes
to `POST /api/dlq/retry-matching` or `/api/dlq/purge-matching`:
`{"filter": {"queue": "billing", "tag": ["tenant:acme"]}}`, or
`{"filter": {}, "all": true}` for the whole DLQ.

Each job appears once, in its current state. Listing is newest first; page by
passing the last id you saw as `before_id`. A job whose claim is in flight
can be missed or shown in its previous state, and a job can change state
between pages.

## Tags are how you find "jobs about X"

Attach a tag when you enqueue, then find every retained job carrying it, in
any state:

=== "Rust"

    ```rust
    awa::insert_with(&pool, &SendInvoice { order_id: 123 }, InsertOpts {
        tags: vec!["order:123".into()],
        ..Default::default()
    }).await?;

    let jobs = awa::admin::find_jobs(
        &pool,
        &awa::JobFilter::new().tag("order:123"),
        awa::JobPage::default(),
    ).await?;
    ```

=== "Python"

    ```python
    client.insert(SendInvoice(order_id=123), tags=["order:123"])

    jobs = client.find_jobs(tag="order:123")
    count = client.count_jobs({"tag": ["tenant:acme", "region:eu"], "state": ["failed", "cancelled"]})
    ```

=== "HTTP"

    ```
    GET /api/jobs?tag=order:123
    GET /api/jobs?tag=tenant:acme&state=failed&state=cancelled
    GET /api/jobs/count?tag=tenant:acme
    ```

=== "CLI"

    ```
    awa job list --tag order:123
    awa job list --tag tenant:acme --state failed --state cancelled
    ```

On queue storage, tags are indexed by a partial GIN index on each partition
where a job's row outlives its claim. Untagged jobs add nothing to it.

A few things a tag cannot do:

- **It finds only jobs whose producers attached it.** "Never tagged", "pruned"
  and "never inserted" all look like an empty result.
- **It lives as long as the job's rows.** Retention prunes both together.
  History beyond retention belongs in change data capture or traces.
- **It is not private.** Tags show in the admin UI and logs and are indexed.
  Use an opaque identifier such as `order:123`, never an email address or a
  secret.

A job may carry at most 20 tags.

## Counting

`count_jobs` (HTTP `GET /api/jobs/count`) counts exactly up to 10,000 and
reports more as "10,000+". It reads the whole matching population, so the
admin UI fetches it when the filter changes rather than on every poll.

## From a search to a batch operation

On the jobs page, a filter that can match available or scheduled jobs offers
**Change priority or queue of matching jobs**, which opens the batch
operations form with the same filter. Batch operations act only on available
and scheduled jobs, so the preview counts those; running, waiting and finished
jobs in the same filter are left alone.

A batch operation whose filter uses several tags, several states or
`dead_lettered` is refused while a runtime older than 0.7.0-alpha.2 is live:
an older runner can't read them. A 0.6 server or runner refuses a list under
`tag` or `state` rather than acting on more jobs.

## Building the tag indexes

New queue-storage schemas, and the empty partitions of existing ones, get
their tag indexes when they are prepared or migrated. For populated
partitions, run as the migrator (the role that owns Awa's tables):

```
awa storage build-tag-index          # one pass; ring partitions are indexed once pruning empties them
awa storage build-tag-index --wait   # keep passing until every index exists
awa storage build-tag-index --now    # build populated partitions now, CONCURRENTLY
```

`--now` holds back vacuum while each index builds and makes pruning of that
partition report `Blocked` until it finishes. The build is safe to interrupt
and to repeat; an index an interrupted build left invalid is rebuilt.
`awa storage status` reports how many indexes exist. Until they all do, tag
lookups still work and scan the partitions that have no index.
