/**
 * Syntax-highlighted JSON viewer. Renders JSON with coloured
 * keys, strings, numbers, booleans, and nulls.
 */

interface JsonViewProps {
  data: unknown;
}

export function JsonView({ data }: JsonViewProps) {
  return (
    <pre className="overflow-auto rounded-md bg-muted p-4 font-mono text-[13px] leading-relaxed">
      <JsonNode value={data} indent={0} />
    </pre>
  );
}

function JsonNode({ value, indent }: { value: unknown; indent: number }) {
  if (value === null) {
    return <span className="text-muted-fg italic">null</span>;
  }

  if (typeof value === "boolean") {
    return (
      <span className="text-info-subtle-fg font-semibold">
        {value ? "true" : "false"}
      </span>
    );
  }

  if (typeof value === "number") {
    return <span className="text-warning-subtle-fg">{value}</span>;
  }

  if (typeof value === "string") {
    return <span className="text-success-subtle-fg">"{value}"</span>;
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return <span>{"[]"}</span>;
    const pad = "  ".repeat(indent);
    const innerPad = "  ".repeat(indent + 1);
    return (
      <>
        {"[\n"}
        {value.map((item, i) => (
          <span key={i}>
            {innerPad}
            <JsonNode value={item} indent={indent + 1} />
            {i < value.length - 1 ? ",\n" : "\n"}
          </span>
        ))}
        {pad}
        {"]"}
      </>
    );
  }

  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return <span>{"{}"}</span>;
    const pad = "  ".repeat(indent);
    const innerPad = "  ".repeat(indent + 1);
    return (
      <>
        {"{\n"}
        {entries.map(([key, val], i) => (
          <span key={key}>
            {innerPad}
            <span className="text-primary-subtle-fg">"{key}"</span>
            {": "}
            <JsonNode value={val} indent={indent + 1} />
            {i < entries.length - 1 ? ",\n" : "\n"}
          </span>
        ))}
        {pad}
        {"}"}
      </>
    );
  }

  return <span>{String(value)}</span>;
}
