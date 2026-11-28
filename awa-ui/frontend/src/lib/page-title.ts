function safeDecode(segment: string): string {
  try {
    return decodeURIComponent(segment);
  } catch {
    return segment;
  }
}

/** Page name for a route path, used in the browser tab title. */
export function pageTitle(pathname: string): string {
  const [section, id] = pathname.replace(/^\/+|\/+$/g, "").split("/");
  const detail = id ? safeDecode(id) : null;
  switch (section) {
    case "":
    case undefined:
      return "Dashboard";
    case "jobs":
      return detail ? `Job #${detail}` : "Jobs";
    case "dlq":
      return detail ? `Dead letter #${detail}` : "Dead letters";
    case "queues":
      return detail ? `Queue ${detail}` : "Queues";
    case "kinds":
      return "Kinds";
    case "runtime":
      return detail ? "Runtime instance" : "Runtime";
    case "cron":
      return "Cron";
    case "batch-ops":
      return "Batch Ops";
    default:
      return "Not found";
  }
}

/** Browser tab title for a route path. */
export function documentTitle(pathname: string): string {
  return `${pageTitle(pathname)} · AWA`;
}
