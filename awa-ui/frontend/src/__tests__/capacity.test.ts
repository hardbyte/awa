import { describe, expect, it } from "vitest";
import type { RuntimeInstance } from "@/lib/api";
import { fleetCapacity } from "@/lib/capacity";

function instance(
  id: string,
  queues: Array<{ queue: string; mode: "hard_reserved" | "weighted"; max_workers: number | null }>,
  stale = false
): RuntimeInstance {
  return {
    instance_id: id,
    stale,
    queues: queues.map(({ queue, mode, max_workers }) => ({
      queue,
      in_flight: 0,
      overflow_held: null,
      config: { mode, max_workers },
    })),
  } as unknown as RuntimeInstance;
}

describe("fleetCapacity", () => {
  it("sums the reservations of every live instance serving the queue", () => {
    const fleet = [
      instance("a", [{ queue: "billing", mode: "hard_reserved", max_workers: 20 }]),
      instance("b", [{ queue: "billing", mode: "hard_reserved", max_workers: 20 }]),
      instance("c", [{ queue: "billing", mode: "hard_reserved", max_workers: 20 }]),
      instance("d", [{ queue: "email", mode: "hard_reserved", max_workers: 5 }]),
    ];
    expect(fleetCapacity(fleet, "billing")).toBe(60);
  });

  it("ignores stale instances", () => {
    const fleet = [
      instance("a", [{ queue: "billing", mode: "hard_reserved", max_workers: 20 }]),
      instance("b", [{ queue: "billing", mode: "hard_reserved", max_workers: 20 }], true),
    ];
    expect(fleetCapacity(fleet, "billing")).toBe(20);
  });

  it("is unknown when any serving instance has no hard reservation", () => {
    const fleet = [
      instance("a", [{ queue: "billing", mode: "hard_reserved", max_workers: 20 }]),
      instance("b", [{ queue: "billing", mode: "weighted", max_workers: null }]),
    ];
    expect(fleetCapacity(fleet, "billing")).toBeNull();
  });

  it("is unknown when nothing serves the queue", () => {
    expect(fleetCapacity([], "billing")).toBeNull();
  });
});
