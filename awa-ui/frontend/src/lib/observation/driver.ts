import { RefreshScheduler } from "./scheduler.ts";
import { boundedRead } from "./read-control.ts";

/** Metadata and data share the dispatcher. A queued probe keeps its own demand
 * while waiting for active reads or backend cooldown; ready=false cannot erase it. */
export class ObservationDriver {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private pumping = false;
  private known = false;
  private nextProbe = 0;
  private probe: AbortController | null = null;
  private epoch = 0;
  private running = false;
  private visible = true;
  private unsubscribe = () => {};
  constructor(private readonly scheduler: RefreshScheduler,
    private readonly discover: (signal: AbortSignal) => Promise<{ poll_interval_ms?: number }>,
    private readonly now = () => performance.now()) {}
  start() {
    this.running = true;
    this.unsubscribe = this.scheduler.subscribe(() => this.pump());
    this.pump();
  }
  stop() {
    this.running = false; this.stopTimer(); this.stopProbe(); this.unsubscribe();
    this.scheduler.setVisible(false); this.scheduler.setReady(false);
  }
  refreshMetadata() { this.known = false; this.nextProbe = 0; this.pump(); }
  setVisible(visible: boolean) {
    this.visible = visible;
    this.scheduler.setVisible(visible);
    if (!visible) { this.stopTimer(); this.stopProbe(); }
    else { this.nextProbe = 0; this.scheduler.mark(); this.pump(); }
  }
  private stopTimer() { if (this.timer !== undefined) clearTimeout(this.timer); this.timer = undefined; }
  private stopProbe() { this.epoch++; if (this.probe) this.known = false; this.probe?.abort(); this.probe = null; }
  private wake(at: number | null) {
    if (at !== null) this.timer = setTimeout(() => this.pump(), Math.max(1, Math.min(60_000, at - this.now())));
  }
  private pump() {
    if (!this.running || this.pumping) return;
    this.pumping = true; this.stopTimer();
    try {
      if (!this.visible || !this.scheduler.size) { this.stopProbe(); return; }
      const next = this.scheduler.nextWakeAt();
      if (!this.known || (this.now() >= this.nextProbe && next !== null)) {
        this.known = false; // Preserve queued metadata demand while data drains.
        this.scheduler.setReady(false);
        if (!this.scheduler.canRead) { this.wake(this.scheduler.nextAdmissionAt()); return; }
        if (this.probe || this.scheduler.inFlight) return;
        const controller = new AbortController(); this.probe = controller;
        const identity = ++this.epoch;
        void boundedRead(controller, this.scheduler.policy.readTimeoutMs, () => this.discover(controller.signal))
          .then(capabilities => {
            if (identity !== this.epoch || controller.signal.aborted) return;
            this.scheduler.setPollInterval(capabilities.poll_interval_ms ?? 5000);
          }).catch(error => {
            if (identity === this.epoch && !controller.signal.aborted) this.scheduler.failure(error);
            // Existing legacy reads remain available after failed discovery;
            // strict mixed-version protocols are outside this increment.
          }).finally(() => {
            if (identity !== this.epoch) return;
            this.probe = null; this.known = true; this.nextProbe = this.now() + 60_000;
            this.scheduler.setReady(true); this.pump();
          });
        return;
      }
      if (this.probe) return;
      this.scheduler.tick(); this.wake(this.scheduler.nextWakeAt());
    } finally { this.pumping = false; }
  }
}
