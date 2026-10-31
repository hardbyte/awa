import { boundedRead, HttpError, ReadProtocolError, ReadTimeoutError } from "./read-control.ts";

export interface Policy {
  maxKeys: number;
  concurrency: number;
  debounceMs: number;
  maxDebounceMs: number;
  reconcileMaxMs: number;
  readTimeoutMs: number;
}
export const DEFAULT_POLICY: Readonly<Policy> = Object.freeze({
  maxKeys: 16, concurrency: 2, debounceMs: 100, maxDebounceMs: 500,
  reconcileMaxMs: 30_000, readTimeoutMs: 35_000,
});
export class CapacityError extends Error {}
export interface Demand {
  id: string;
  cached?: boolean;
  manualOnly?: boolean;
  stalesCounts?: boolean;
  paused?: boolean;
  intervalMs?: number;
  stopAfterNull?: boolean;
  run: (signal: AbortSignal) => Promise<unknown>;
  stale: (stale: boolean) => void;
}
interface Entry {
  observers: Map<symbol, Demand>;
  dirty: boolean;
  pendingSince: number | null;
  during: boolean;
  manual: boolean;
  running: AbortController | null;
  lastStart: number;
  nextAllowed: number;
  nextPoll: number;
  nextReconcile: number | null;
  failures: number;
  blocked: boolean;
  absent: boolean;
  error: boolean;
}

/** Bounded exact-key demand. Polling, independent reconciliation and explicit
 * marks share this dispatcher; there is no second automatic fetch loop. */
export class RefreshScheduler {
  readonly policy: Readonly<Policy>;
  private entries = new Map<string, Entry>();
  private listeners = new Set<() => void>();
  private admissionListeners = new Set<() => void>();
  private active = 0;
  private rejected = 0;
  private visible = true;
  private ready = false;
  private halted = false;
  private until = 0;
  private pollMs = 5000;
  constructor(options: Partial<Policy> = {}, private readonly now = () => performance.now(), private readonly random = () => Math.random()) {
    const policy = { ...DEFAULT_POLICY, ...options };
    for (const key of Object.keys(policy) as Array<keyof Policy>) {
      if (!Number.isSafeInteger(policy[key]) || policy[key] < 1 || policy[key] > DEFAULT_POLICY[key]) throw new Error(`Invalid observation policy: ${key}`);
    }
    if (policy.readTimeoutMs <= 30_000) throw new Error("Client deadline must exceed the current 30-second server budget");
    if (policy.debounceMs > policy.maxDebounceMs) throw new Error("Invalid dirty-demand debounce");
    this.policy = Object.freeze(policy);
  }
  get size() { return this.entries.size; }
  get inFlight() { return this.active; }
  nextAdmissionAt(): number | null {
    return this.visible && !this.halted ? Math.max(this.now(), this.until) : null;
  }
  get canRead() { return this.visible && !this.halted && this.now() >= this.until; }
  subscribe(listener: () => void) { this.listeners.add(listener); return () => { this.listeners.delete(listener); }; }
  onCapacity(listener: () => void) { this.admissionListeners.add(listener); return () => { this.admissionListeners.delete(listener); }; }
  recordRejection() {
    this.rejected++; this.notify(); let active = true;
    return () => { if (active) { active = false; this.rejected--; this.notify(); } };
  }
  private notify() { for (const listener of this.listeners) listener(); }
  private representative(entry: Entry) { return entry.observers.values().next().value!; }
  private paused(entry: Entry) { return [...entry.observers.values()].some(observer => observer.paused); }
  private automatic(entry: Entry) { return !entry.absent && !entry.blocked && !this.representative(entry).manualOnly && !this.paused(entry); }
  private watchdog() { return this.now() + this.policy.reconcileMaxMs * (0.9 + Math.max(0, Math.min(1, this.random())) * 0.1); }
  private cadence(entry: Entry) { return Math.max(this.floor(entry), this.representative(entry).intervalMs ?? this.pollMs); }
  private floor(entry: Entry) { return this.representative(entry).cached ? this.pollMs : 2000; }
  private syncWatchdog(entry: Entry) {
    if (!this.automatic(entry)) entry.nextReconcile = null;
    else if (entry.nextReconcile === null) entry.nextReconcile = this.watchdog();
  }
  setReady(ready: boolean) { this.ready = ready; }
  setPollInterval(ms: number) {
    if (!Number.isSafeInteger(ms) || ms < 1) throw new Error("Invalid poll interval");
    this.pollMs = Math.max(5000, ms);
  }
  attach(demand: Demand) {
    if (demand.intervalMs !== undefined && (!Number.isSafeInteger(demand.intervalMs) || demand.intervalMs < 2000)) throw new Error("Invalid query cadence");
    let entry = this.entries.get(demand.id);
    if (!entry) {
      if (this.entries.size >= this.policy.maxKeys) throw new CapacityError(`At most ${this.policy.maxKeys} active observation keys are allowed`);
      entry = { observers: new Map(), dirty: true, pendingSince: null, during: false, manual: !!demand.manualOnly,
        running: null, lastStart: -Infinity, nextAllowed: 0, nextPoll: Infinity, nextReconcile: null,
        failures: 0, blocked: false, absent: false, error: false };
      this.entries.set(demand.id, entry);
    }
    const token = Symbol(); const observer = { ...demand }; let attached = true;
    entry.observers.set(token, observer);
    this.syncWatchdog(entry); // Armed before the first dispatch, except paused/manual views.
    if (observer.paused) { entry.running?.abort(); this.mark(demand.id); }
    observer.stale(entry.dirty || !!entry.running); this.notify();
    return {
      setPaused: (paused: boolean) => {
        if (!attached) return;
        const before = this.paused(entry!); observer.paused = paused;
        const after = this.paused(entry!); this.syncWatchdog(entry!);
        if (!before && after) entry!.running?.abort();
        if (before !== after) this.mark(demand.id);
      },
      detach: () => {
        if (!attached) return; attached = false;
        entry!.observers.delete(token);
        if (!entry!.observers.size) {
          entry!.running?.abort(); this.entries.delete(demand.id);
          for (const listener of [...this.admissionListeners]) listener();
        } else this.syncWatchdog(entry!);
        this.notify();
      },
    };
  }
  mark(id?: string, manual = false) {
    let stalesCounts = false;
    for (const [key, entry] of this.entries) {
      if (id !== undefined && key !== id) continue;
      if (entry.absent && !manual) continue;
      stalesCounts ||= !!this.representative(entry).stalesCounts;
      if (entry.running) entry.during = true; else entry.dirty = true;
      entry.pendingSince ??= this.now(); entry.manual ||= manual;
      if (manual) { entry.blocked = false; entry.absent = false; this.syncWatchdog(entry); }
      for (const observer of entry.observers.values()) observer.stale(true);
    }
    if (stalesCounts) for (const entry of this.entries.values()) {
      if (!this.representative(entry).manualOnly) continue;
      if (entry.running) entry.during = true; else entry.dirty = true;
      for (const observer of entry.observers.values()) observer.stale(true);
    }
    if (manual) this.halted = false;
    this.notify();
  }
  setVisible(visible: boolean) {
    if (this.visible === visible) return;
    this.visible = visible;
    if (!visible) for (const entry of this.entries.values()) entry.running?.abort();
    else this.mark();
    this.notify();
  }
  failure(error: unknown) {
    if (error instanceof HttpError) {
      if ([401, 403].includes(error.status)) this.halted = true;
      if ([429, 503].includes(error.status)) this.until = Math.max(this.until, this.now() + error.retryAfterMs);
    }
  }
  readStatus() {
    if (this.rejected) return `${this.rejected} view(s) rejected at the ${this.policy.maxKeys}-key limit; close a view to retry`;
    const entries = [...this.entries.values()].filter(entry => !entry.absent);
    const failed = entries.filter(entry => entry.error).length;
    const stale = entries.filter(entry => entry.dirty || entry.running || entry.during).length;
    return failed ? `${failed} view(s) failed to refresh; retained data is stale`
      : stale ? `${stale} view(s) awaiting authoritative refresh` : "";
  }
  private eligible(entry: Entry) {
    const debounce = entry.manual || entry.pendingSince === null ? -Infinity : entry.pendingSince + Math.min(this.policy.debounceMs, this.policy.maxDebounceMs);
    return Math.max(this.until, entry.nextAllowed, entry.lastStart + this.floor(entry), debounce);
  }
  private demanded(entry: Entry) {
    return entry.dirty && !entry.blocked && !entry.absent
      && (entry.manual || (!this.representative(entry).manualOnly && !this.paused(entry)));
  }
  nextWakeAt(): number | null {
    if (!this.ready || !this.visible || this.halted) return null;
    let next = Infinity;
    for (const entry of this.entries.values()) {
      if (this.automatic(entry)) next = Math.min(next, entry.nextPoll, entry.nextReconcile ?? Infinity);
      if (this.active < this.policy.concurrency && !entry.running && this.demanded(entry)) next = Math.min(next, this.eligible(entry));
    }
    return Number.isFinite(next) ? Math.max(this.now(), this.until, next) : null;
  }
  tick() {
    if (!this.ready || !this.canRead) return;
    for (const [id, entry] of [...this.entries]) {
      if (this.automatic(entry)) {
        if (this.now() >= (entry.nextReconcile ?? Infinity)) { this.mark(id); entry.nextReconcile = this.watchdog(); }
        if (this.now() >= entry.nextPoll) { this.mark(id); entry.nextPoll = this.now() + this.cadence(entry); }
      }
      if (entry.running || this.active >= this.policy.concurrency || !this.demanded(entry) || this.now() < this.eligible(entry)) continue;
      const demand = this.representative(entry); const controller = new AbortController();
      this.entries.delete(id); this.entries.set(id, entry); // Fair admission across slow views.
      entry.running = controller; entry.dirty = false; entry.during = false; entry.pendingSince = null; entry.manual = false;
      entry.lastStart = this.now(); entry.nextPoll = entry.lastStart + this.cadence(entry); this.active++;
      void boundedRead(controller, this.policy.readTimeoutMs, () => demand.run(controller.signal)).then(value => {
        if (controller.signal.aborted || this.entries.get(id) !== entry) return;
        entry.dirty = entry.during; entry.error = false; entry.failures = 0;
        if (demand.stopAfterNull && value === null) { entry.absent = true; entry.blocked = true; }
        entry.nextReconcile = this.automatic(entry) ? this.watchdog() : null;
        entry.nextPoll = this.now() + this.cadence(entry);
        for (const observer of entry.observers.values()) observer.stale(entry.dirty);
      }).catch(error => {
        if (this.entries.get(id) !== entry) return;
        entry.dirty = true;
        if (!controller.signal.aborted || error instanceof ReadTimeoutError) {
          this.failure(error); entry.error = true;
          entry.blocked = error instanceof ReadProtocolError || (error instanceof HttpError && [400, 413, 415, 422].includes(error.status));
          this.syncWatchdog(entry);
          entry.manual ||= !!demand.manualOnly;
          entry.failures = Math.min(entry.failures + 1, 6);
          entry.nextAllowed = this.now() + Math.min(60_000, 1000 * 2 ** entry.failures) * (0.9 + Math.max(0, Math.min(1, this.random())) * 0.1);
        }
      }).finally(() => { entry.running = null; this.active--; this.notify(); });
    }
  }
}
