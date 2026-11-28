import type { RuntimeInstance } from "@/lib/api";

/**
 * Fleet-wide capacity for a queue: the sum of the hard reservations of the
 * live instances serving it. Null when none serve it or any serves it
 * without a hard reservation.
 */
export function fleetCapacity(instances: RuntimeInstance[], queue: string): number | null {
  let total = 0;
  let serving = 0;
  for (const instance of instances) {
    if (instance.stale) continue;
    const config = instance.queues.find((entry) => entry.queue === queue)?.config;
    if (!config) continue;
    if (config.mode !== "hard_reserved" || config.max_workers == null) return null;
    total += config.max_workers;
    serving += 1;
  }
  return serving > 0 ? total : null;
}
