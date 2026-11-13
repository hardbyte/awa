import type { LocationRewrite } from "@tanstack/react-router";

/** The server injects the router's mount path before any bundle resources.
 * Without an injected base (Vite dev server), the app remains rooted at /. */
export function getBasePath(): string {
  if (typeof document === "undefined") return "/";
  return document.querySelector("base")?.getAttribute("href") ?? "/";
}

export function apiPath(path: string): string {
  return `${getBasePath().replace(/\/$/, "")}/api${path}`;
}

/** Like a router basepath, with the exact root URL expected by axum's nest.
 * TanStack's basepath always emits a slash after the mount for the root. */
export function mountRewrite(): LocationRewrite {
  const prefix = getBasePath().replace(/\/$/, "");
  const root = typeof document === "undefined"
    ? "/"
    : document.querySelector("base")?.getAttribute("data-awa-root") ?? "/";
  return {
    input: ({ url }) => {
      if (url.pathname === prefix) url.pathname = "/";
      else if (url.pathname.startsWith(`${prefix}/`)) {
        url.pathname = url.pathname.slice(prefix.length);
      }
      return url;
    },
    output: ({ url }) => {
      url.pathname = url.pathname === "/" ? root : `${prefix}${url.pathname}`;
      return url;
    },
  };
}

/** Preserve external links when adapting react-aria hrefs through the router. */
export function isExternalHref(href: string): boolean {
  return /^(?:[a-z][a-z\d+.-]*:|\/\/)/i.test(href);
}
