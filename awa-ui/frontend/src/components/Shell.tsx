import { useEffect } from "react";
import { Outlet, useRouterState, useNavigate } from "@tanstack/react-router";
import { RouterProvider as AriaRouterProvider } from "react-aria-components";
import { useIsFetching, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTheme, type Theme } from "@/hooks/use-theme";
import { fetchCapabilities, fetchRuntime, type RuntimeOverview } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarInset,
  SidebarItem,
  SidebarLabel,
  SidebarProvider,
  SidebarSection,
  SidebarTrigger,
  useSidebar,
} from "@/components/ui/sidebar";
import { usePollInterval } from "@/hooks/use-poll-interval";

/**
 * Koru-river logo — a Maori koru (unfurling fern spiral) with
 * flowing water lines beneath, evoking "awa" (river).
 */
function KoruLogo({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      viewBox="0 0 36 36"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <path
        d="M18 4 C10 4, 5 10, 5 17 C5 24, 10 30, 18 30"
        stroke="currentColor"
        strokeWidth="2.8"
        strokeLinecap="round"
        fill="none"
      />
      <path
        d="M18 30 C22 30, 26 26, 26 21 C26 16, 22 13, 18 13"
        stroke="currentColor"
        strokeWidth="2.4"
        strokeLinecap="round"
        fill="none"
      />
      <path
        d="M18 13 C16 13, 14 14.5, 14 17 C14 19, 15.5 20.5, 18 20.5"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        fill="none"
      />
      <circle cx="18" cy="17" r="1.8" fill="currentColor" />
      <path
        d="M3 32 C8 30, 14 34, 20 31 C26 28, 30 32, 34 30"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        fill="none"
        opacity="0.6"
      />
      <path
        d="M5 35 C10 33.5, 15 36.5, 21 34 C27 31.5, 31 34.5, 33 33.5"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        fill="none"
        opacity="0.35"
      />
    </svg>
  );
}

/** Sun icon for light mode */
function SunIcon() {
  return (
    <svg data-slot="icon" viewBox="0 0 20 20" fill="currentColor">
      <path d="M10 2a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 10 2ZM10 15a.75.75 0 0 1 .75.75v1.5a.75.75 0 0 1-1.5 0v-1.5A.75.75 0 0 1 10 15ZM10 7a3 3 0 1 0 0 6 3 3 0 0 0 0-6ZM15.657 5.404a.75.75 0 1 0-1.06-1.06l-1.061 1.06a.75.75 0 0 0 1.06 1.061l1.061-1.06ZM6.464 14.596a.75.75 0 1 0-1.06-1.06l-1.06 1.06a.75.75 0 0 0 1.06 1.06l1.06-1.06ZM18 10a.75.75 0 0 1-.75.75h-1.5a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 18 10ZM5 10a.75.75 0 0 1-.75.75h-1.5a.75.75 0 0 1 0-1.5h1.5A.75.75 0 0 1 5 10ZM14.596 15.657a.75.75 0 0 0 1.06-1.06l-1.06-1.061a.75.75 0 1 0-1.06 1.06l1.06 1.061ZM5.404 6.464a.75.75 0 0 0 1.06-1.06l-1.06-1.06a.75.75 0 1 0-1.061 1.06l1.06 1.06Z" />
    </svg>
  );
}

/** Moon icon for dark mode */
function MoonIcon() {
  return (
    <svg data-slot="icon" viewBox="0 0 20 20" fill="currentColor">
      <path fillRule="evenodd" d="M7.455 2.004a.75.75 0 0 1 .26.77 7 7 0 0 0 9.958 7.967.75.75 0 0 1 1.067.853A8.5 8.5 0 1 1 6.647 1.921a.75.75 0 0 1 .808.083Z" clipRule="evenodd" />
    </svg>
  );
}

/** Monitor icon for system theme */
function MonitorIcon() {
  return (
    <svg data-slot="icon" viewBox="0 0 20 20" fill="currentColor">
      <path fillRule="evenodd" d="M2 4.25A2.25 2.25 0 0 1 4.25 2h11.5A2.25 2.25 0 0 1 18 4.25v8.5A2.25 2.25 0 0 1 15.75 15h-3.105a3.501 3.501 0 0 1 1.1 1.677A.75.75 0 0 1 13.026 17H6.974a.75.75 0 0 1-.719-.323 3.501 3.501 0 0 1 1.1-1.677H4.25A2.25 2.25 0 0 1 2 12.75v-8.5Zm1.5 0a.75.75 0 0 1 .75-.75h11.5a.75.75 0 0 1 .75.75v7.5a.75.75 0 0 1-.75.75H4.25a.75.75 0 0 1-.75-.75v-7.5Z" clipRule="evenodd" />
    </svg>
  );
}

/** Cycles through system → light → dark */
function ThemeToggle() {
  const { theme, updateTheme } = useTheme();

  const next: Record<Theme, Theme> = {
    system: "light",
    light: "dark",
    dark: "system",
  };

  const label: Record<Theme, string> = {
    system: "Theme: system",
    light: "Theme: light",
    dark: "Theme: dark",
  };

  return (
    <Button
      intent="plain"
      size="sq-sm"
      aria-label={label[theme]}
      onPress={() => updateTheme(next[theme])}
    >
      {theme === "light" && <SunIcon />}
      {theme === "dark" && <MoonIcon />}
      {theme === "system" && <MonitorIcon />}
    </Button>
  );
}

/** Refresh button with live indicator — click to force refresh all queries */
function RefreshControl() {
  const isFetching = useIsFetching();
  const queryClient = useQueryClient();
  const fetching = isFetching > 0;

  return (
    <Button
      intent="plain"
      size="sq-sm"
      aria-label="Refresh data"
      onPress={() => void queryClient.invalidateQueries()}
      className="relative"
    >
      <svg
        data-slot="icon"
        viewBox="0 0 20 20"
        fill="currentColor"
        className={fetching ? "animate-spin" : ""}
      >
        <path
          fillRule="evenodd"
          d="M15.312 11.424a5.5 5.5 0 0 1-9.201 2.466l-.312-.311h2.433a.75.75 0 0 0 0-1.5H4.598a.75.75 0 0 0-.75.75v3.634a.75.75 0 0 0 1.5 0v-2.033l.312.311a7 7 0 0 0 11.712-3.138.75.75 0 0 0-1.449-.39Zm-10.624-2.85a5.5 5.5 0 0 1 9.201-2.465l.312.31H11.768a.75.75 0 0 0 0 1.5h3.634a.75.75 0 0 0 .75-.75V3.535a.75.75 0 0 0-1.5 0v2.033l-.312-.31A7 7 0 0 0 2.628 8.395a.75.75 0 0 0 1.449.39l.611.789Z"
          clipRule="evenodd"
        />
      </svg>
      {/* Live indicator dot */}
      {fetching && (
        <span className="absolute -right-0.5 -top-0.5 flex size-2">
          <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-primary opacity-50" />
          <span className="relative inline-flex size-2 rounded-full bg-primary" />
        </span>
      )}
    </Button>
  );
}

/**
 * Aggregate a cluster-wide storage-capability label from live runtime
 * instances. Reads `storage_capability` on each instance; if the field
 * is absent (pre-v10 schema) every instance falls through as "canonical",
 * so the chip stays informative on deployments that haven't yet applied
 * the storage-transition-prep migration.
 */
function summariseCapabilities(runtime: RuntimeOverview | undefined): {
  label: string;
  hint: string;
} | null {
  if (!runtime) return null;
  const live = runtime.instances.filter((instance) => !instance.stale);
  if (live.length === 0) return null;

  const capabilities = new Set(
    live.map((instance) => instance.storage_capability ?? "canonical")
  );
  const ordered = Array.from(capabilities).sort();
  const count = live.length;
  if (ordered.length === 1 && ordered[0]) {
    return {
      label: ordered[0].replace(/_/g, " "),
      hint: `${count} live ${count === 1 ? "worker" : "workers"}`,
    };
  }
  return {
    label: "mixed",
    hint: ordered.join(" · "),
  };
}

// Closes the mobile sidebar sheet on route change. No-op on desktop.
function CloseMobileOnNavigate({ path }: { path: string }) {
  const { isMobile, isOpenOnMobile, setIsOpenOnMobile } = useSidebar();
  useEffect(() => {
    if (isMobile && isOpenOnMobile) {
      setIsOpenOnMobile(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [path]);
  return null;
}

/** Cluster status chip — health pulse + storage-capability summary. */
function ClusterStatusChip({ runtime }: { runtime: RuntimeOverview | undefined }) {
  const summary = summariseCapabilities(runtime);
  if (!summary) {
    return (
      <div className="flex items-center gap-2 rounded-md bg-muted/40 px-2.5 py-1.5 text-xs text-muted-fg">
        <span className="size-2 rounded-full bg-muted-fg/40" />
        <span>No live workers</span>
      </div>
    );
  }
  return (
    <div className="flex items-center gap-2 rounded-md bg-muted/40 px-2.5 py-1.5 text-xs">
      <span className="relative flex size-2">
        <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-primary opacity-60" />
        <span className="relative inline-flex size-2 rounded-full bg-primary" />
      </span>
      <span className="flex flex-col">
        <span className="font-medium capitalize text-fg">{summary.label}</span>
        <span className="text-muted-fg">{summary.hint}</span>
      </span>
    </div>
  );
}

// data-slot="icon" lets the Sidebar primitive render these as the dock-mode glyph.
const IconDashboard = () => (
  <svg data-slot="icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6">
    <rect x="2.5" y="2.5" width="6" height="6" rx="1" />
    <rect x="11.5" y="2.5" width="6" height="4" rx="1" />
    <rect x="2.5" y="11.5" width="6" height="6" rx="1" />
    <rect x="11.5" y="9.5" width="6" height="8" rx="1" />
  </svg>
);
const IconJobs = () => (
  <svg data-slot="icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round">
    <path d="M4 5h12M4 10h12M4 15h8" />
  </svg>
);
const IconKinds = () => (
  <svg data-slot="icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M10.5 2.5h5a2 2 0 0 1 2 2v5a1 1 0 0 1-.3.7l-7 7a1 1 0 0 1-1.4 0L3 11.5a1 1 0 0 1 0-1.4l7-7a1 1 0 0 1 .5-.6z" />
    <circle cx="13.5" cy="6.5" r="1.2" fill="currentColor" stroke="none" />
  </svg>
);
const IconQueues = () => (
  <svg data-slot="icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <path d="M10 3 2.5 6.5 10 10l7.5-3.5L10 3z" />
    <path d="M2.5 10 10 13.5 17.5 10" />
    <path d="M2.5 13.5 10 17l7.5-3.5" />
  </svg>
);
const IconRuntime = () => (
  <svg data-slot="icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3.5" width="14" height="5" rx="1.5" />
    <rect x="3" y="11.5" width="14" height="5" rx="1.5" />
    <circle cx="6.5" cy="6" r="0.8" fill="currentColor" stroke="none" />
    <circle cx="6.5" cy="14" r="0.8" fill="currentColor" stroke="none" />
  </svg>
);
const IconCron = () => (
  <svg data-slot="icon" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="10" cy="10" r="7.5" />
    <path d="M10 5.5V10l3 2" />
  </svg>
);

const NAV_ITEMS = [
  { to: "/", label: "Dashboard", Icon: IconDashboard },
  { to: "/jobs", label: "Jobs", Icon: IconJobs },
  { to: "/kinds", label: "Kinds", Icon: IconKinds },
  { to: "/queues", label: "Queues", Icon: IconQueues },
  { to: "/runtime", label: "Runtime", Icon: IconRuntime },
  { to: "/cron", label: "Cron", Icon: IconCron },
] as const;

export function Shell() {
  const routerState = useRouterState();
  const currentPath = routerState.location.pathname;
  const navigate = useNavigate();
  const poll = usePollInterval();

  // Routes react-aria-components Link clicks through TanStack Router so sidebar
  // nav is client-side. Modifier-key handling (cmd/ctrl/shift-click → new tab)
  // is handled inside RouterProvider via shouldClientNavigate.
  const ariaNavigate = (to: string) => {
    void navigate({ to });
  };

  // Show the banner only once we've confirmed read-only (not while loading)
  const capabilitiesQuery = useQuery({
    queryKey: ["capabilities"],
    queryFn: fetchCapabilities,
    staleTime: 60_000,
  });
  const showReadOnlyBanner = capabilitiesQuery.isSuccess && capabilitiesQuery.data.read_only;

  // Cluster status for the sidebar footer; poll-aligned with other pages.
  // On pre-v10 deployments `storage_capability` is absent per instance; the
  // aggregator treats that as canonical so the chip still renders.
  const runtimeQuery = useQuery({
    queryKey: ["runtime"],
    queryFn: fetchRuntime,
    refetchInterval: poll.interval,
    staleTime: poll.staleTime,
  });

  function isActive(to: string): boolean {
    if (to === "/") return currentPath === "/";
    return currentPath.startsWith(to);
  }

  return (
    <AriaRouterProvider navigate={ariaNavigate}>
    <SidebarProvider
      style={{ "--sidebar-width": "13.5rem" } as React.CSSProperties}
    >
      <Sidebar collapsible="dock">
        <SidebarHeader>
          <a
            href="/"
            className="flex items-center gap-2 px-2 py-1 no-underline"
          >
            <KoruLogo className="size-7 shrink-0 text-primary" />
            <span className="text-base font-semibold tracking-tight text-fg group-data-[collapsible=dock]:hidden">
              AWA
            </span>
          </a>
        </SidebarHeader>
        <SidebarContent>
          <SidebarSection title="Navigation">
            {NAV_ITEMS.map((item) => (
              <SidebarItem
                key={item.to}
                href={item.to}
                isCurrent={isActive(item.to)}
                tooltip={item.label}
              >
                <item.Icon />
                <SidebarLabel>{item.label}</SidebarLabel>
              </SidebarItem>
            ))}
          </SidebarSection>
        </SidebarContent>
        <SidebarFooter>
          <div className="group-data-[collapsible=dock]:hidden">
            <ClusterStatusChip runtime={runtimeQuery.data} />
          </div>
        </SidebarFooter>
        <CloseMobileOnNavigate path={currentPath} />
      </Sidebar>

      <SidebarInset>
        <header className="sticky top-0 z-10 flex items-center gap-2 border-b bg-bg/70 px-4 py-2.5 backdrop-blur sm:px-6">
          <SidebarTrigger className="-ml-1" />
          <div className="flex-1" />
          <div className="flex items-center gap-1">
            <RefreshControl />
            <ThemeToggle />
          </div>
        </header>

        <main className="mx-auto w-full max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
          {showReadOnlyBanner && (
            <div className="mb-6 rounded-lg border border-warning/40 bg-warning/10 px-4 py-3 text-sm text-warning-fg">
              Connected to a read-only database. Dashboard queries work, but admin actions are disabled.
            </div>
          )}
          <Outlet />
        </main>
      </SidebarInset>
    </SidebarProvider>
    </AriaRouterProvider>
  );
}
