import { test } from "node:test";
import assert from "node:assert/strict";
import { CapacityError, RefreshScheduler, type Demand } from "../src/lib/observation/scheduler.ts";
import { boundedRead, checkedJson, HttpError, onHttpFailure, ReadProtocolError, ReadTimeoutError, readWithFence, retryAfter, withReadSignal } from "../src/lib/observation/read-control.ts";
import { ObservationDriver } from "../src/lib/observation/driver.ts";
import { bindPageLifecycle } from "../src/lib/observation/lifecycle.ts";
import { apiRead, fetchCapabilities, fetchDlq, fetchJobs, bulkRetryDlq, bulkPurgeDlq, bulkMoveFailedToDlq, purgeDlqJob, previewBatchOperation, submitBatchOperation, cancelBatchOperation } from "../src/lib/api.ts";

const settle = async () => { for (let n = 0; n < 30; n++) await Promise.resolve(); };
const demand = (id: string, run: Demand["run"], options: Partial<Demand> = {}): Demand => ({ id, run, stale: () => {}, ...options });

test("initial demand is admitted only after discovery; empty scope has no resource deadline", async () => {
  let reads = 0;
  const scheduler = new RefreshScheduler({}, () => 0, () => 1);
  assert.equal(scheduler.nextWakeAt(), null);
  const handle = scheduler.attach(demand("backend/admin/jobs/{}", async () => { reads++; }));
  scheduler.tick(); await settle(); assert.equal(reads, 0);
  scheduler.setReady(true); assert.equal(scheduler.nextWakeAt(), 0);
  scheduler.tick(); await settle(); assert.equal(reads, 1);
  handle.detach(); assert.equal(scheduler.nextWakeAt(), null);
});

test("16 exact keys are a hard cap; duplicate observers share admission and detached handles are idempotent", () => {
  const scheduler = new RefreshScheduler();
  const handles = Array.from({ length: 16 }, (_, n) => scheduler.attach(demand(String(n), async () => {})));
  const duplicate = scheduler.attach(demand("0", async () => {}));
  assert.throws(() => scheduler.attach(demand("17", async () => {})), CapacityError);
  const clear = scheduler.recordRejection(); assert.match(scheduler.readStatus(), /rejected/); clear(); clear();
  handles[0]!.detach(); assert.equal(scheduler.size, 16);
  duplicate.detach(); assert.equal(scheduler.size, 15);
  const replacement = scheduler.attach(demand("0", async () => {}));
  handles[0]!.detach(); assert.equal(scheduler.size, 16);
  handles.slice(1).forEach(handle => handle.detach()); replacement.detach();
  assert.equal(scheduler.size, 0);
});

test("a dirty mark during the first read survives its response and coalesces across resources", async t => {
  let now = 0;
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  t.after(async () => { scheduler.setVisible(false); await settle(); });
  const reads: string[] = []; const finish: Array<() => void> = []; const stale = new Map<string, boolean>();
  for (const id of ["jobs", "workers"]) scheduler.attach(demand(id, () => {
    reads.push(id); return new Promise<void>(resolve => finish.push(resolve));
  }, { stale: value => stale.set(id, value) }));
  scheduler.tick(); await settle(); assert.deepEqual(reads, ["jobs", "workers"]);
  now = 50; for (let n = 0; n < 1000; n++) { scheduler.mark("jobs"); scheduler.mark("workers"); }
  finish.splice(0).forEach(resolve => resolve()); await settle();
  assert.equal(stale.get("jobs"), true); assert.equal(stale.get("workers"), true);
  now = 1999; scheduler.tick(); await settle(); assert.equal(reads.length, 2);
  now = 2000; scheduler.tick(); await settle(); assert.equal(reads.length, 4);
});

test("debounce is anchored to the first mark, while polling and reconciliation keep separate deadlines", async t => {
  let now = 0; let reads = 0; let finish!: () => void;
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  t.after(async () => { scheduler.setVisible(false); await settle(); });
  scheduler.attach(demand("job", async () => { reads++; if (reads > 1) await new Promise<void>(resolve => { finish = resolve; }); }, { intervalMs: 60_000 }));
  scheduler.tick(); await settle(); assert.equal(scheduler.nextWakeAt(), 30_000);
  now = 29_000; scheduler.mark("job"); assert.equal(scheduler.nextWakeAt(), 29_100);
  now = 29_050; scheduler.mark("job"); assert.equal(scheduler.nextWakeAt(), 29_100);
  now = 29_100; scheduler.tick(); await settle(); assert.equal(reads, 2);
  assert.equal(scheduler.nextWakeAt(), 30_000); // A pending read/mark did not renew the watchdog.
  now = 30_000; scheduler.tick(); finish(); await settle();
  assert.match(scheduler.readStatus(), /awaiting/);
});

test("lost local invalidation converges through ordinary scheduled reads", async () => {
  let now = 0; let source = "running"; let displayed = "";
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  scheduler.attach(demand("job", async () => { displayed = source; }, { intervalMs: 60_000 }));
  scheduler.tick(); await settle(); source = "completed";
  now = 29_999; scheduler.tick(); await settle(); assert.equal(displayed, "running");
  now = 30_000; scheduler.tick(); // Watchdog marks the common dispatcher, with its debounce.
  now = 30_100; scheduler.tick(); await settle(); assert.equal(displayed, "completed");
});

test("counts and paused selections have no automatic watchdog; another observer cannot resume selection", async () => {
  let now = 0; const reads: string[] = [];
  const scheduler = new RefreshScheduler({}, () => now); scheduler.setReady(true);
  const selected = scheduler.attach(demand("jobs", async () => { reads.push("jobs"); }, { paused: true }));
  const other = scheduler.attach(demand("jobs", async () => { reads.push("jobs"); })); other.setPaused(false);
  scheduler.attach(demand("count", async () => { reads.push("count"); }, { manualOnly: true }));
  scheduler.tick(); await settle(); assert.deepEqual(reads, ["count"]); assert.equal(scheduler.nextWakeAt(), null);
  now = 100_000; scheduler.mark(); scheduler.tick(); await settle(); assert.deepEqual(reads, ["count"]);
  scheduler.mark(undefined, true); scheduler.tick(); await settle(); assert.deepEqual(reads, ["count", "jobs", "count"]);
  selected.detach(); other.detach();
});

test("client concurrency is two, with fairness and backend-wide Retry-After", async t => {
  let now = 0; const reads: string[] = []; const finish: Array<() => void> = [];
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  t.after(async () => { scheduler.setVisible(false); await settle(); });
  for (const id of ["a", "b", "c", "d"]) scheduler.attach(demand(id, () => { reads.push(id); return new Promise<void>(resolve => finish.push(resolve)); }));
  scheduler.tick(); await settle(); assert.deepEqual(reads, ["a", "b"]);
  scheduler.failure(new HttpError("busy", 429, 10_000)); finish.splice(0).forEach(resolve => resolve()); await settle();
  now = 9999; scheduler.tick(); await settle(); assert.equal(reads.length, 2);
  now = 10_000; scheduler.tick(); await settle(); assert.deepEqual(reads, ["a", "b", "c", "d"]);
});

test("hung or synchronous-failing reads release slots and late results cannot clear demand", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let now = 0; let finish!: () => void; let healthy = 0;
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  t.after(async () => { scheduler.setVisible(false); await settle(); });
  scheduler.attach(demand("hang", () => new Promise<void>(resolve => { finish = resolve; })));
  scheduler.attach(demand("throw", () => { throw new Error("sync failure"); }));
  scheduler.attach(demand("healthy", async () => { healthy++; }));
  scheduler.tick(); await settle(); scheduler.tick(); await settle(); assert.equal(healthy, 1);
  t.mock.timers.tick(35_001); await settle();
  assert.match(scheduler.readStatus(), /failed/); finish(); await settle(); assert.match(scheduler.readStatus(), /failed/);
  now = 1000; scheduler.tick(); await settle(); assert.equal(healthy, 1);
});

test("filter/backend replacement cancels old work, releases capacity and fences late completions", async () => {
  let finish!: () => void; let fresh = false;
  const scheduler = new RefreshScheduler({}, () => 0); scheduler.setReady(true);
  const old = scheduler.attach(demand("backend-a/admin/jobs/filter1/page1", () => new Promise<void>(resolve => { finish = resolve; })));
  scheduler.tick(); await settle(); old.detach(); await settle();
  scheduler.attach(demand("backend-b/admin/jobs/filter2/page1", async () => { fresh = true; }));
  scheduler.tick(); await settle(); finish(); await settle();
  assert.equal(fresh, true); assert.equal(scheduler.size, 1); assert.equal(scheduler.inFlight, 0);
  assert.equal(scheduler.readStatus(), "");
});

test("hidden and BFCache lifecycles cancel work and resume one paced demand", async () => {
  let now = 0; let reads = 0;
  const scheduler = new RefreshScheduler({}, () => now); scheduler.setReady(true);
  const host = new EventTarget(); const doc = Object.assign(new EventTarget(), { visibilityState: "visible" });
  const stop = bindPageLifecycle(host, doc, visible => { scheduler.setVisible(visible); if (visible) scheduler.mark(); });
  scheduler.attach(demand("jobs", async () => { reads++; }));
  scheduler.tick(); await settle(); host.dispatchEvent(new Event("pagehide"));
  now = 60_000; assert.equal(scheduler.nextWakeAt(), null); scheduler.tick(); assert.equal(reads, 1);
  const restore = new Event("pageshow"); Object.defineProperty(restore, "persisted", { value: true });
  host.dispatchEvent(restore); doc.dispatchEvent(new Event("visibilitychange"));
  now += 100; scheduler.tick(); await settle(); assert.equal(reads, 2);
  stop();
});

test("auth and protocol failures stop loops; failed network reads retain paced demand", async () => {
  let now = 0; let reads = 0;
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  scheduler.attach(demand("jobs", async () => { reads++; if (reads === 1) throw new Error("network"); }));
  scheduler.tick(); await settle(); now = 1000; scheduler.tick(); assert.equal(reads, 1);
  now = 2000; scheduler.tick(); await settle(); assert.equal(reads, 2);
  scheduler.failure(new HttpError("sign in", 403)); now = 60_000; scheduler.mark(); scheduler.tick(); assert.equal(reads, 2);
  scheduler.mark(undefined, true); scheduler.tick(); await settle(); assert.equal(reads, 3);
  const invalid = new RefreshScheduler({}, () => now); invalid.setReady(true); let invalidReads = 0;
  invalid.attach(demand("bad", async () => { invalidReads++; throw new ReadProtocolError("HTML"); }));
  invalid.tick(); await settle(); now += 60_000; invalid.mark(); invalid.tick(); assert.equal(invalidReads, 1);
  assert.equal(invalid.nextWakeAt(), null);
});

test("known-absent optional views stop automatic reads and can be explicitly reprobed", async () => {
  let now = 0; let reads = 0;
  const scheduler = new RefreshScheduler({}, () => now); scheduler.setReady(true);
  scheduler.attach(demand("storage", async () => { reads++; return null; }, { stopAfterNull: true }));
  scheduler.tick(); await settle(); assert.equal(scheduler.nextWakeAt(), null);
  now = 60_000; scheduler.mark(); scheduler.tick(); assert.equal(reads, 1);
  scheduler.mark(undefined, true); scheduler.tick(); await settle(); assert.equal(reads, 2);
});

test("HTTP 200 HTML/malformed JSON are errors, and overload is applied before a stalled error body", async () => {
  await assert.rejects(checkedJson(new Response("<html/>", { headers: { "content-type": "text/html" } })), ReadProtocolError);
  await assert.rejects(checkedJson(new Response("{", { headers: { "content-type": "application/json" } })), ReadProtocolError);
  let applied = false; const controller = new AbortController();
  onHttpFailure(error => { applied = error instanceof HttpError && error.retryAfterMs === 12_000; });
  const body = new ReadableStream({ start(stream) { controller.signal.addEventListener("abort", () => stream.error(new DOMException("aborted", "AbortError"))); } });
  const pending = boundedRead(controller, 20, () => checkedJson(new Response(body, { status: 429, headers: { "content-type": "application/json", "retry-after": "12" } })));
  await settle(); assert.equal(applied, true); await assert.rejects(pending, ReadTimeoutError); onHttpFailure();
  assert.equal(retryAfter("9".repeat(400)), 60_000);
});

test("the c09b23eb DLQ matching and whole-decimal filter transport are preserved", async () => {
  const original = globalThis.fetch; const calls: Array<{ url: string; body: unknown }> = [];
  try {
    globalThis.fetch = async (input, init) => { calls.push({ url: String(input), body: init?.body ? JSON.parse(String(init.body)) : null });
      return new Response(JSON.stringify([]), { headers: { "content-type": "application/json" } }); };
    await fetchDlq({ ids: [123], tags: ["a", "b"] }); await bulkRetryDlq({ ids: [123], tags: ["a", "b"] });
    assert.match(calls[0]!.url, /ids=123/); assert.equal(calls[1]!.url, "/api/dlq/retry-matching");
    assert.deepEqual(calls[1]!.body, { filter: { ids: [123], tag: ["a", "b"] } });
  } finally { globalThis.fetch = original; }
});

test("capability discovery is single-flight and rejects malformed pacing/read-only metadata", async () => {
  const original = globalThis.fetch; let calls = 0;
  try {
    globalThis.fetch = async () => { calls++; return new Response(JSON.stringify({ read_only: true, poll_interval_ms: 5000 }), { headers: { "content-type": "application/json" } }); };
    await Promise.all([fetchCapabilities(), fetchCapabilities(), fetchCapabilities()]); assert.equal(calls, 1);
    globalThis.fetch = async () => new Response(JSON.stringify({ read_only: false, poll_interval_ms: "bad" }), { headers: { "content-type": "application/json" } });
    await assert.rejects(fetchCapabilities(), ReadProtocolError);
  } finally { globalThis.fetch = original; }
});

test("configuration cannot disable admission, deadlines or reconciliation", () => {
  for (const options of [{ maxKeys: 0 }, { concurrency: 3 }, { debounceMs: 501 }, { reconcileMaxMs: 0 }, { readTimeoutMs: Infinity }, { readTimeoutMs: 30_000 }]) {
    assert.throws(() => new RefreshScheduler(options));
  }
});


test("an elapsed deadline fences publication even before the timeout callback runs", async () => {
  let now = 0; let published = false;
  const controller = new AbortController();
  await assert.rejects(boundedRead(controller, 20_000, async () => {
    const value = await readWithFence(controller.signal, async () => { now = 20_001; return "late"; });
    published = true; return value;
  }, () => now), ReadTimeoutError);
  assert.equal(published, false); assert.equal(controller.signal.aborted, true);
});


test("batch scans and DLQ mutations survive an overlapping observation deadline", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const original = globalThis.fetch;
  const calls: Array<{ url: string; signal: AbortSignal | null | undefined; finish: () => void }> = [];
  const actions: Array<Promise<unknown>> = [];
  let settledActions = 0;
  const controller = new AbortController();
  try {
    globalThis.fetch = (input, init) => new Promise<Response>((resolve, reject) => {
      const abort = () => reject(new DOMException("observation aborted", "AbortError"));
      init?.signal?.addEventListener("abort", abort, { once: true });
      calls.push({ url: String(input), signal: init?.signal, finish: () => {
        init?.signal?.removeEventListener("abort", abort);
        resolve(new Response(JSON.stringify({ count: 1 }), { headers: { "content-type": "application/json" } }));
      } });
    });
    const read = boundedRead(controller, 35_000, () => withReadSignal(controller.signal, () => {
      // Exercise the shared transport even inside an ambient read context.
      // These actions normally run separately through useMutation.
      const payload = { op_kind: "set_priority" as const, filter: { queue: "emails" }, spec: { priority: 1 } };
      actions.push(submitBatchOperation(payload), cancelBatchOperation("operation"),
        bulkRetryDlq({ queue: "emails" }), bulkPurgeDlq({ queue: "emails" }), bulkMoveFailedToDlq({ queue: "emails" }), purgeDlqJob(123));
      actions.forEach(action => { void action.then(() => { settledActions++; }); });
      return fetchJobs();
    }));
    const timedOut = assert.rejects(read, ReadTimeoutError);
    await settle();
    assert.equal(calls.length, 7);
    assert.equal(calls.find(call => call.url === "/api/jobs")?.signal, controller.signal);
    for (const call of calls.filter(call => call.url !== "/api/jobs")) assert.equal(call.signal, undefined, call.url);
    t.mock.timers.tick(35_001); await timedOut; await settle();
    assert.equal(controller.signal.aborted, true);
    assert.equal(settledActions, 0); // No read deadline settled/cancelled an action.
    for (const call of calls.filter(call => call.url !== "/api/jobs")) call.finish();
    await Promise.all(actions);
    assert.equal(settledActions, 6);
  } finally { controller.abort(); globalThis.fetch = original; }
});


test("explicit POST read intent inherits observation cancellation, including previews", async () => {
  const original = globalThis.fetch;
  const signals: Array<AbortSignal | null | undefined> = [];
  const controller = new AbortController();
  try {
    globalThis.fetch = async (_input, init) => { signals.push(init?.signal);
      return new Response("{}", { headers: { "content-type": "application/json" } }); };
    await withReadSignal(controller.signal, () => apiRead("/v1/jobs/query", { method: "POST", body: "{}" }));
    await withReadSignal(controller.signal, () => previewBatchOperation({ op_kind: "set_priority", filter: { queue: "emails" }, spec: {} }));
    const explicit = new AbortController();
    await apiRead("/v1/jobs/query", { method: "POST", body: "{}", signal: explicit.signal });
    assert.deepEqual(signals, [controller.signal, controller.signal, explicit.signal]);
  } finally { globalThis.fetch = original; }
});


test("a capability probe queued behind active data reads cannot pause the tab forever", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let now = 0; let probes = 0; let reads = 0; let finish!: () => void;
  const scheduler = new RefreshScheduler({}, () => now, () => 1);
  scheduler.attach(demand("jobs", async () => { reads++; if (reads === 2) await new Promise<void>(resolve => { finish = resolve; }); }));
  const driver = new ObservationDriver(scheduler, async () => { probes++; return { poll_interval_ms: 5000 }; }, () => now);
  t.after(() => driver.stop()); driver.start(); await settle();
  assert.equal(probes, 1); assert.equal(reads, 1);
  now = 2000; scheduler.mark("jobs", true); await settle(); assert.equal(reads, 2);
  now = 60_001; scheduler.mark("jobs"); await settle(); assert.equal(probes, 1);
  finish(); await settle(); assert.equal(probes, 2);
  now += 100; t.mock.timers.tick(100); await settle();
  assert.equal(reads, 3);
});

test("cancelled discovery resumes after backend cooldown without a competing fetch loop", async t => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let now = 0; let probes = 0; let reads = 0;
  const scheduler = new RefreshScheduler({}, () => now, () => 1);
  scheduler.attach(demand("jobs", async () => { reads++; }));
  const driver = new ObservationDriver(scheduler, async () => { probes++; if (probes === 1) await new Promise<void>(() => {}); return {}; }, () => now);
  t.after(() => driver.stop()); driver.start(); await settle(); assert.equal(probes, 1);
  scheduler.failure(new HttpError("busy", 429, 10_000)); driver.setVisible(false); await settle();
  driver.setVisible(true); await settle(); assert.equal(probes, 1); assert.equal(reads, 0);
  now = 9999; t.mock.timers.tick(9999); await settle(); assert.equal(probes, 1);
  now = 10_000; t.mock.timers.tick(1); await settle();
  assert.equal(probes, 2); assert.equal(reads, 1);
});


test("scheduled job reads mark manual counts stale without fetching them", async () => {
  let now = 0; let counts = 0; let stale = true;
  const scheduler = new RefreshScheduler({}, () => now, () => 1); scheduler.setReady(true);
  scheduler.attach(demand("jobs", async () => {}, { stalesCounts: true }));
  scheduler.attach(demand("count", async () => { counts++; }, { manualOnly: true, stale: value => { stale = value; } }));
  scheduler.tick(); await settle(); assert.equal(stale, false); assert.equal(counts, 1);
  now = 5000; scheduler.tick(); await settle();
  assert.equal(stale, true); assert.equal(counts, 1);
  now = 5100; scheduler.tick(); await settle(); assert.equal(counts, 1);
  scheduler.mark("count", true); scheduler.tick(); await settle(); assert.equal(stale, false); assert.equal(counts, 2);
});
