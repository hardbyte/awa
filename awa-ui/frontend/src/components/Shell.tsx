import { Outlet, useRouterState } from "@tanstack/react-router";
import { useIsFetching, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTheme, type Theme } from "@/hooks/use-theme";
import { fetchCapabilities } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  NavbarProvider,
  Navbar,
  NavbarSection,
  NavbarItem,
  NavbarLabel,
  NavbarSpacer,
  NavbarMobile,
  NavbarTrigger,
  NavbarStart,
} from "@/components/ui/navbar";

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

const NAV_ITEMS = [
  { to: "/", label: "Dashboard" },
  { to: "/jobs", label: "Jobs" },
  { to: "/queues", label: "Queues" },
  { to: "/runtime", label: "Runtime" },
  { to: "/cron", label: "Cron" },
] as const;

export function Shell() {
  const routerState = useRouterState();
  const currentPath = routerState.location.pathname;
  // Show the banner only once we've confirmed read-only (not while loading)
  const capabilitiesQuery = useQuery({
    queryKey: ["capabilities"],
    queryFn: fetchCapabilities,
    staleTime: 60_000,
  });
  const showReadOnlyBanner = capabilitiesQuery.isSuccess && capabilitiesQuery.data.read_only;

  function isActive(to: string): boolean {
    if (to === "/") return currentPath === "/";
    return currentPath.startsWith(to);
  }

  return (
    <NavbarProvider>
      <Navbar isSticky>
        <NavbarStart>
          <a href="/" className="flex items-center gap-2 no-underline">
            <KoruLogo className="size-7 text-primary" />
            <span className="text-lg font-semibold tracking-tight text-fg">
              AWA
            </span>
          </a>
        </NavbarStart>

        <NavbarSection className="ml-6">
          {NAV_ITEMS.map((item) => (
            <NavbarItem
              key={item.to}
              href={item.to}
              isCurrent={isActive(item.to)}
            >
              <NavbarLabel>{item.label}</NavbarLabel>
            </NavbarItem>
          ))}
        </NavbarSection>

        <NavbarSpacer />

        <div className="flex items-center gap-2">
          <RefreshControl />
          <ThemeToggle />
        </div>
      </Navbar>

      <NavbarMobile>
        <NavbarTrigger />
        <a href="/" className="flex items-center gap-2 no-underline">
          <KoruLogo className="size-6 text-primary" />
          <span className="text-base font-semibold text-fg">AWA</span>
        </a>
        <div className="ml-auto flex items-center gap-2">
          <RefreshControl />
          <ThemeToggle />
        </div>
      </NavbarMobile>

      <main className="mx-auto w-full max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        {showReadOnlyBanner && (
          <div className="mb-6 rounded-lg border border-warning/40 bg-warning/10 px-4 py-3 text-sm text-warning-fg">
            Connected to a read-only database. Dashboard queries work, but admin actions are disabled.
          </div>
        )}
        <Outlet />
      </main>
    </NavbarProvider>
  );
}
