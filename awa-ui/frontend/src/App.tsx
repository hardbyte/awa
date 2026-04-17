import {
  createRouter,
  createRootRoute,
  createRoute,
  RouterProvider,
} from "@tanstack/react-router";
import { QueryClientProvider } from "@tanstack/react-query";
import { queryClient } from "./lib/query";
import { Shell } from "./components/Shell";
import { DashboardPage } from "./routes/index";
import { JobsPage } from "./routes/jobs";
import { JobDetailPage } from "./routes/job-detail";
import { KindsPage } from "./routes/kinds";
import { QueuesPage } from "./routes/queues";
import { QueueDetailPage } from "./routes/queue-detail";
import { CronPage } from "./routes/cron";
import { DlqPage } from "./routes/dlq";
import { DlqDetailPage } from "./routes/dlq-detail";
import { RuntimePage } from "./routes/runtime";
import { RuntimeInstancePage } from "./routes/runtime-detail";
import { NotFoundPage } from "./routes/not-found";

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
  queuesRoute,
  queueDetailRoute,
  runtimeRoute,
  runtimeInstanceRoute,
  cronRoute,
  dlqRoute,
  dlqDetailRoute,
]);

const router = createRouter({ routeTree });

import { Toaster } from "@/components/ui/toast";
import { ErrorBoundary } from "@/components/ErrorBoundary";

export function App() {
  return (
    <ErrorBoundary>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
      <Toaster position="bottom-right" />
    </QueryClientProvider>
    </ErrorBoundary>
  );
}
