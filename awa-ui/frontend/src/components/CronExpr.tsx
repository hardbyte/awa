import { Tooltip, TooltipTrigger, TooltipContent } from "@/components/ui/tooltip";

/** Simple human-readable description of common cron expressions. */
function describeCron(expr: string): string {
  const parts = expr.trim().split(/\s+/);
  if (parts.length < 5) return expr;

  const [minute, hour, dom, month, dow] = parts;

  // Every minute
  if (minute === "*" && hour === "*" && dom === "*" && month === "*" && dow === "*") {
    return "Every minute";
  }

  // Every N minutes
  if (minute?.startsWith("*/") && hour === "*" && dom === "*") {
    return `Every ${minute.slice(2)} minutes`;
  }

  // Every hour at :MM
  if (minute !== "*" && hour === "*" && dom === "*" && month === "*" && dow === "*") {
    return `Every hour at :${minute?.padStart(2, "0")}`;
  }

  // Daily at HH:MM
  if (minute !== "*" && hour !== "*" && dom === "*" && month === "*" && dow === "*") {
    return `Daily at ${hour?.padStart(2, "0")}:${minute?.padStart(2, "0")}`;
  }

  // Specific days of week
  const dayNames: Record<string, string> = {
    "0": "Sun", "1": "Mon", "2": "Tue", "3": "Wed",
    "4": "Thu", "5": "Fri", "6": "Sat", "7": "Sun",
  };

  if (minute !== "*" && hour !== "*" && dom === "*" && month === "*" && dow !== "*") {
    const days = dow?.split(",").map(d => dayNames[d] ?? d).join(", ");
    return `${days} at ${hour?.padStart(2, "0")}:${minute?.padStart(2, "0")}`;
  }

  // Monthly on day N
  if (minute !== "*" && hour !== "*" && dom !== "*" && month === "*" && dow === "*") {
    return `Monthly on day ${dom} at ${hour?.padStart(2, "0")}:${minute?.padStart(2, "0")}`;
  }

  return expr;
}

interface CronExprProps {
  expr: string;
}

export function CronExpr({ expr }: CronExprProps) {
  const description = describeCron(expr);
  const hasDescription = description !== expr;

  if (!hasDescription) {
    return <code className="font-mono">{expr}</code>;
  }

  return (
    <Tooltip>
      <TooltipTrigger className="cursor-default font-mono text-sm underline decoration-dotted decoration-muted-fg/50 underline-offset-4">
        {expr}
      </TooltipTrigger>
      <TooltipContent>
        <p>{description}</p>
      </TooltipContent>
    </Tooltip>
  );
}
