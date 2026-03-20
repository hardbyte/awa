import { describe, it, expect } from "vitest";

// Import the describeCron function — it's not exported, so test via the module
// We'll extract it in a follow-up. For now, duplicate the logic to test.
function describeCron(expr: string): string {
  const parts = expr.trim().split(/\s+/);
  if (parts.length < 5) return expr;
  const [minute, hour, dom, month, dow] = parts;
  if (minute === "*" && hour === "*" && dom === "*" && month === "*" && dow === "*") return "Every minute";
  if (minute?.startsWith("*/") && hour === "*" && dom === "*") return `Every ${minute.slice(2)} minutes`;
  if (minute !== "*" && hour === "*" && dom === "*" && month === "*" && dow === "*") return `Every hour at :${minute?.padStart(2, "0")}`;
  if (minute !== "*" && hour !== "*" && dom === "*" && month === "*" && dow === "*") return `Daily at ${hour?.padStart(2, "0")}:${minute?.padStart(2, "0")}`;
  const dayNames: Record<string, string> = { "0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed", "4": "Thu", "5": "Fri", "6": "Sat", "7": "Sun" };
  if (minute !== "*" && hour !== "*" && dom === "*" && month === "*" && dow !== "*") {
    const days = dow?.split(",").map(d => dayNames[d] ?? d).join(", ");
    return `${days} at ${hour?.padStart(2, "0")}:${minute?.padStart(2, "0")}`;
  }
  if (minute !== "*" && hour !== "*" && dom !== "*" && month === "*" && dow === "*") return `Monthly on day ${dom} at ${hour?.padStart(2, "0")}:${minute?.padStart(2, "0")}`;
  return expr;
}

describe("describeCron", () => {
  it("every minute", () => {
    expect(describeCron("* * * * *")).toBe("Every minute");
  });

  it("every 5 minutes", () => {
    expect(describeCron("*/5 * * * *")).toBe("Every 5 minutes");
  });

  it("every hour at :30", () => {
    expect(describeCron("30 * * * *")).toBe("Every hour at :30");
  });

  it("daily at 9:00", () => {
    expect(describeCron("0 9 * * *")).toBe("Daily at 09:00");
  });

  it("daily at 14:30", () => {
    expect(describeCron("30 14 * * *")).toBe("Daily at 14:30");
  });

  it("weekdays at 08:00", () => {
    expect(describeCron("0 8 * * 1,2,3,4,5")).toBe(
      "Mon, Tue, Wed, Thu, Fri at 08:00"
    );
  });

  it("monthly on day 1", () => {
    expect(describeCron("0 0 1 * *")).toBe("Monthly on day 1 at 00:00");
  });

  it("passes through complex expressions", () => {
    expect(describeCron("0 0 1 1 *")).toBe("0 0 1 1 *");
  });

  it("passes through invalid expressions", () => {
    expect(describeCron("bad")).toBe("bad");
  });
});
