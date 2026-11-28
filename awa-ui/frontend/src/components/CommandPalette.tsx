import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { MagnifyingGlassIcon } from "@heroicons/react/20/solid";
import {
  CommandMenu,
  CommandMenuDescription,
  CommandMenuFooter,
  CommandMenuItem,
  CommandMenuLabel,
  CommandMenuList,
  CommandMenuSearch,
  CommandMenuSection,
} from "@/components/ui/command-menu";
import { Button } from "@/components/ui/button";
import { Keyboard } from "@/components/ui/keyboard";
import { fetchCronJobs, fetchKinds, fetchQueues, type BatchOperation } from "@/lib/api";
import { batchOpsQuery } from "@/lib/shared-queries";
import { paletteItems, type PaletteItem } from "@/lib/palette";

const PALETTE_STALE_MS = 60_000;

function isMac(): boolean {
  return typeof navigator !== "undefined" && /Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent);
}

/** Opens on ⌘K or Ctrl+K and jumps to a job, queue, kind, schedule, bulk operation or page. */
export function CommandPalette() {
  const [open, setOpen] = useState(false);
  const [input, setInput] = useState("");
  const navigate = useNavigate();
  const client = useQueryClient();
  const shortcut = isMac() ? "⌘K" : "Ctrl K";

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key.toLowerCase() === "k" && (event.metaKey || event.ctrlKey) && !event.altKey && !event.shiftKey) {
        event.preventDefault();
        setOpen((current) => !current);
      }
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  useEffect(() => {
    if (!open) setInput("");
  }, [open]);

  // Fetched when the palette opens, outside the polling scheduler.
  const lookup = { enabled: open, staleTime: PALETTE_STALE_MS, retry: false } as const;
  const queues = useQuery({ queryKey: ["palette", "queues"], queryFn: fetchQueues, ...lookup });
  const kinds = useQuery({ queryKey: ["palette", "kinds"], queryFn: fetchKinds, ...lookup });
  const schedules = useQuery({ queryKey: ["palette", "cron"], queryFn: fetchCronJobs, ...lookup });
  const batchOps = open ? client.getQueryData<BatchOperation[]>(batchOpsQuery.queryKey) : undefined;

  const items = useMemo(
    () =>
      paletteItems(input, {
        queues: queues.data,
        kinds: kinds.data,
        schedules: schedules.data,
        batchOps,
      }),
    [input, queues.data, kinds.data, schedules.data, batchOps]
  );
  const sections = useMemo(() => {
    const grouped = new Map<PaletteItem["section"], PaletteItem[]>();
    for (const item of items) grouped.set(item.section, [...(grouped.get(item.section) ?? []), item]);
    return [...grouped].map(([label, entries]) => ({ id: label, label, entries }));
  }, [items]);
  const failed = [queues, kinds, schedules].some((query) => query.isError);

  function go(key: React.Key) {
    const item = items.find((entry) => entry.id === key);
    if (!item) return;
    setOpen(false);
    void navigate(item.target);
  }

  return (
    <>
      <Button
        intent="outline"
        size="sm"
        onPress={() => setOpen(true)}
        aria-label="Jump to"
        aria-keyshortcuts={isMac() ? "Meta+K" : "Control+K"}
        className="gap-2 text-muted-fg"
        data-testid="command-palette-trigger"
      >
        <MagnifyingGlassIcon data-slot="icon" />
        <span className="hidden sm:inline">Jump to…</span>
        <Keyboard className="ml-2">{shortcut}</Keyboard>
      </Button>
      <CommandMenu
        isOpen={open}
        onOpenChange={setOpen}
        aria-label="Jump to"
        inputValue={input}
        onInputChange={setInput}
        filter={() => true}
        // Closing while the open animation still runs, as choosing an entry
        // right after typing does, can leave the overlay stuck on screen.
        className="animate-none!"
        overlay={{ className: "animate-none!" }}
        isPending={queues.isFetching || kinds.isFetching || schedules.isFetching}
      >
        <CommandMenuSearch placeholder="Job id, queue, kind, schedule, tag:… or page" />
        <CommandMenuList items={sections} onAction={go} aria-label="Results">
          {(section) => (
            <CommandMenuSection id={section.id} label={section.label} items={section.entries}>
              {(item) => (
                <CommandMenuItem id={item.id} textValue={item.label}>
                  <CommandMenuLabel>{item.label}</CommandMenuLabel>
                  {item.detail && <CommandMenuDescription className="font-mono">{item.detail}</CommandMenuDescription>}
                </CommandMenuItem>
              )}
            </CommandMenuSection>
          )}
        </CommandMenuList>
        {failed && (
          <CommandMenuFooter role="status">Some names couldn't be loaded; job ids and pages still work.</CommandMenuFooter>
        )}
      </CommandMenu>
    </>
  );
}
