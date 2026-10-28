import { Card, CardHeader, CardContent } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { CopyButton } from "@/components/CopyButton";
import { JsonView } from "@/components/JsonView";

interface ShownOutput {
  value: unknown;
  /** The failed attempt that recorded it; null for the completion's output. */
  attempt: number | null;
}

// The completed attempt's output, else the output of the latest failed
// attempt that recorded one: a failure keeps its output in its error entry,
// and only the latest such entry keeps it (ADR-048).
export function outputToShow(
  output: unknown,
  errors: unknown[] | null | undefined,
): ShownOutput | null {
  if (output !== undefined && output !== null) {
    return { value: output, attempt: null };
  }
  for (const entry of [...(errors ?? [])].reverse()) {
    if (entry && typeof entry === "object") {
      const { output: failed, attempt } = entry as { output?: unknown; attempt?: unknown };
      if (failed !== undefined && failed !== null) {
        return { value: failed, attempt: typeof attempt === "number" ? attempt : 0 };
      }
    }
  }
  return null;
}

/** A job's output (ADR-048), labelled with the type its kind declares. */
export function OutputCard({
  output,
  errors,
  outputType,
}: {
  output: unknown;
  errors: unknown[] | null | undefined;
  outputType?: string | null;
}) {
  const shown = outputToShow(output, errors);
  if (!shown) return null;
  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-semibold">
              {shown.attempt === null
                ? "Output"
                : `Output from failed attempt ${shown.attempt}`}
            </span>
            {outputType && (
              <Badge intent="secondary" aria-label={`Output type ${outputType}`}>
                <code>{outputType}</code>
              </Badge>
            )}
          </div>
          <CopyButton
            value={JSON.stringify(shown.value, null, 2)}
            label="Copy output JSON"
          />
        </div>
      </CardHeader>
      <CardContent>
        <JsonView data={shown.value} />
      </CardContent>
    </Card>
  );
}
