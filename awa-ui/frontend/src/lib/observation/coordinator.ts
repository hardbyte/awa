import { hashKey, type QueryKey } from "@tanstack/react-query";
import { queryClient } from "../query";
import { fetchCapabilities } from "../api";
import { RefreshScheduler } from "./scheduler.ts";
import { onHttpFailure, readWithFence, withReadSignal } from "./read-control.ts";
import { ObservationDriver } from "./driver.ts";
import { bindPageLifecycle } from "./lifecycle.ts";

export const refreshScheduler = new RefreshScheduler();
export const observationPolicy = refreshScheduler.policy;
export function managedQueryId(key: QueryKey) {
  const backend = typeof window === "undefined" ? "/api" : `${window.location.origin}/api`;
  return hashKey([backend, "trusted-admin", ...key]);
}
export function getReadStatus() { return refreshScheduler.readStatus(); }
export function subscribeReadStatus(listener: () => void) { return refreshScheduler.subscribe(listener); }
let started = false;
let requestProbe = () => {};

export function refreshAll() {
  refreshScheduler.mark(undefined, true);
  requestProbe();
  // Catalog reads retain their existing policy; capabilities are observed
  // passively and refreshed exclusively by this coordinator.
  void queryClient.invalidateQueries({ queryKey: ["kinds"] });
}

/** One timer exists only while visible managed demand needs service. A metadata
 * probe runs between data batches, never in addition to two active data reads. */
export function startObservation() {
  if (started) return () => {};
  started = true;
  const driver = new ObservationDriver(refreshScheduler, signal => {
    const cancel = () => { void queryClient.cancelQueries({ queryKey: ["capabilities"], exact: true }); };
    signal.addEventListener("abort", cancel, { once: true });
    return queryClient.fetchQuery({ queryKey: ["capabilities"], retry: false, staleTime: 0,
      queryFn: () => readWithFence(signal, () => withReadSignal(signal, fetchCapabilities)),
    }).finally(() => signal.removeEventListener("abort", cancel));
  });
  driver.start();
  const unsubscribeCache = queryClient.getQueryCache().subscribe(event => {
    if (event.type === "updated" && event.action.type === "invalidate") {
      refreshScheduler.mark(managedQueryId(event.query.queryKey));
      // Counts become stale, but their manual-only policy prevents a fetch.
      if (event.query.queryKey[0] !== "jobs-count") {
        for (const query of queryClient.getQueryCache().findAll({ queryKey: ["jobs-count"] })) {
          refreshScheduler.mark(managedQueryId(query.queryKey));
        }
      }
    }
  });
  onHttpFailure(error => refreshScheduler.failure(error));
  requestProbe = () => driver.refreshMetadata();
  const stopLifecycle = bindPageLifecycle(window, document, visible => driver.setVisible(visible));
  const focus = () => { refreshScheduler.mark(); };
  window.addEventListener("focus", focus); window.addEventListener("online", focus);
  return () => {
    started = false; driver.stop();
    unsubscribeCache(); stopLifecycle(); onHttpFailure(); requestProbe = () => {};
    window.removeEventListener("focus", focus); window.removeEventListener("online", focus);
  };
}
