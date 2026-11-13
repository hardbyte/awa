/** BFCache restoration does not require a network failure to trigger recovery. */
export function bindPageLifecycle(host: EventTarget, document: EventTarget & { visibilityState: string }, change: (visible: boolean) => void) {
  const visibility = () => change(document.visibilityState !== "hidden");
  const hide = () => change(false);
  const show = (event: Event) => { if ((event as PageTransitionEvent).persisted) visibility(); };
  document.addEventListener("visibilitychange", visibility);
  host.addEventListener("pagehide", hide); host.addEventListener("pageshow", show);
  visibility();
  return () => {
    document.removeEventListener("visibilitychange", visibility);
    host.removeEventListener("pagehide", hide); host.removeEventListener("pageshow", show);
  };
}
