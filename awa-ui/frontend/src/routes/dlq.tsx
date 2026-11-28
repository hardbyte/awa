import { useManagedQuery } from "@/hooks/use-managed-query";
import { dlqDepthQuery } from "@/lib/shared-queries";
import { DlqList } from "@/components/DlqList";
import { Page, PageHeader } from "@/components/Page";
import { Badge } from "@/components/ui/badge";

export function DlqPage() {
  const depthQuery = useManagedQuery(dlqDepthQuery);

  return (
    <Page className="space-y-4">
      <PageHeader
        title="Dead letters"
        status={depthQuery.data && (
          <div className="flex flex-wrap gap-2 items-center">
            <Badge intent="danger">
              {depthQuery.data.total.toLocaleString()} total
            </Badge>
            {depthQuery.data.by_queue.slice(0, 5).map((q) => (
              <Badge key={q.queue} intent="secondary">
                {q.queue}: {q.count.toLocaleString()}
              </Badge>
            ))}
          </div>
        )}
      />

      <DlqList />
    </Page>
  );
}
