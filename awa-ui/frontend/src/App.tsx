import { useEffect } from "react";
import { startObservation } from "./lib/observation/coordinator";
import {
  createRouter,
  createRootRoute,
  createRoute,
  redirect,
  RouterProvider,
} from "@tanstack/react-router";
import { QueryClientProvider } from "@tanstack/react-query";
import { queryClient } from "./lib/query";
import { Shell } from "./components/Shell";
import { DashboardPage } from "./routes/index";
import { JobsPage } from "./routes/jobs";
import { JobDetailPage } from "./routes/job-detail";
import { KindsPage } from "./routes/kinds";
import { KindDetailPage } from "./routes/kind-detail";
import { QueuesPage } from "./routes/queues";
import { QueueDetailPage } from "./routes/queue-detail";
import { CronPage } from "./routes/cron";
import { DlqPage } from "./routes/dlq";
import { DlqDetailPage } from "./routes/dlq-detail";
import { BatchOpsPage } from "./routes/batch-ops";
import { BatchOpNewPage } from "./routes/batch-op-new";
import { BatchOpDetailPage } from "./routes/batch-op-detail";
import { BATCH_PREFILL_KEYS } from "./lib/batch";
import { RuntimePage } from "./routes/runtime";
import { RuntimeInstancePage } from "./routes/runtime-detail";
import { NotFoundPage } from "./routes/not-found";
import { mountRewrite } from "./lib/base-path";

const rootRoute = createRootRoute({
  component: Shell,
  notFoundComponent: NotFoundPage,
});

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: DashboardPage,
});

const jobsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs",
  component: JobsPage,
});

const jobDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/jobs/$id",
  component: JobDetailPage,
});

const kindsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/kinds",
  component: KindsPage,
});

const kindDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/kinds/$kind",
  component: KindDetailPage,
});

const queuesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/queues",
  component: QueuesPage,
});

const queueDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/queues/$name",
  component: QueueDetailPage,
});

const cronRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/cron",
  component: CronPage,
});

const dlqRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/dlq",
  component: DlqPage,
});

const dlqDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/dlq/$id",
  component: DlqDetailPage,
});

const batchOpsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/batch-ops",
  // Links from other pages prefill the form with these keys.
  beforeLoad: ({ search }) => {
    const values = search as Record<string, unknown>;
    if (BATCH_PREFILL_KEYS.some((key) => values[key] !== undefined)) {
      throw redirect({ to: "/batch-ops/new", search: values, replace: true });
    }
  },
  component: BatchOpsPage,
});

const batchOpNewRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/batch-ops/new",
  component: BatchOpNewPage,
});

const batchOpDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/batch-ops/$id",
  component: BatchOpDetailPage,
});

const runtimeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runtime",
  component: RuntimePage,
});

const runtimeInstanceRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runtime/$instanceId",
  component: RuntimeInstancePage,
});

const routeTree = rootRoute.addChildren([
  dashboardRoute,
  jobsRoute,
  jobDetailRoute,
  kindsRoute,
  kindDetailRoute,
  queuesRoute,
  queueDetailRoute,
  runtimeRoute,
  runtimeInstanceRoute,
  cronRoute,
  dlqRoute,
  dlqDetailRoute,
  batchOpsRoute,
  batchOpNewRoute,
  batchOpDetailRoute,
]);

const router = createRouter({ routeTree, rewrite: mountRewrite() });

import { Toaster } from "@/components/ui/toast";
import { ErrorBoundary } from "@/components/ErrorBoundary";

export function App() {
  useEffect(startObservation, []);
  return (
    <ErrorBoundary>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
      <Toaster position="bottom-right" />
    </QueryClientProvider>
    </ErrorBoundary>
  );
}
