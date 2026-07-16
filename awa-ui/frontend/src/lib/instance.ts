/**
 * Instance identity helpers (#437).
 *
 * The UI is single-backend by design — one `awa serve` per database. When
 * the operator names the instance (`--instance-name`, optionally
 * `--instance-color`), these helpers surface that identity in the browser
 * tab title and favicon so several open instances are distinguishable at a
 * glance.
 */

/** Browser tab title for an instance. */
export function instanceTitle(name: string | null | undefined): string {
  const trimmed = name?.trim();
  return trimmed ? `${trimmed} — AWA` : "AWA";
}

/**
 * Accept only CSS hex colors ('#' + 3/4/6/8 hex digits). The server
 * validates too; this is defense in depth before the value reaches inline
 * styles or the favicon SVG.
 */
export function normalizeInstanceColor(
  color: string | null | undefined
): string | null {
  if (!color) return null;
  const trimmed = color.trim();
  return /^#([0-9a-fA-F]{3,4}|[0-9a-fA-F]{6}|[0-9a-fA-F]{8})$/.test(trimmed)
    ? trimmed
    : null;
}

/** Matches the koru logo's default primary tint. */
export const DEFAULT_FAVICON_COLOR = "#0d9488";

/**
 * Koru favicon (same mark as the sidebar logo) tinted with the instance
 * accent color.
 */
export function faviconSvg(color: string | null): string {
  const accent = normalizeInstanceColor(color) ?? DEFAULT_FAVICON_COLOR;
  return (
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 36 36" fill="none">` +
    `<path d="M18 4 C10 4, 5 10, 5 17 C5 24, 10 30, 18 30" stroke="${accent}" stroke-width="2.8" stroke-linecap="round"/>` +
    `<path d="M18 30 C22 30, 26 26, 26 21 C26 16, 22 13, 18 13" stroke="${accent}" stroke-width="2.4" stroke-linecap="round"/>` +
    `<path d="M18 13 C16 13, 14 14.5, 14 17 C14 19, 15.5 20.5, 18 20.5" stroke="${accent}" stroke-width="2" stroke-linecap="round"/>` +
    `<circle cx="18" cy="17" r="1.8" fill="${accent}"/>` +
    `<path d="M3 32 C8 30, 14 34, 20 31 C26 28, 30 32, 34 30" stroke="${accent}" stroke-width="2" stroke-linecap="round" opacity="0.6"/>` +
    `</svg>`
  );
}

export function faviconDataUri(color: string | null): string {
  return `data:image/svg+xml,${encodeURIComponent(faviconSvg(color))}`;
}

/**
 * Apply the instance identity to the document: tab title and tinted
 * favicon. No-op until the capabilities query reports a name or color, so
 * unconfigured instances keep the browser defaults.
 */
export function applyInstanceIdentity(
  name: string | null,
  color: string | null
): void {
  const normalizedColor = normalizeInstanceColor(color);
  if (!name?.trim() && !normalizedColor) return;

  document.title = instanceTitle(name);

  let link = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
  if (!link) {
    link = document.createElement("link");
    link.rel = "icon";
    document.head.appendChild(link);
  }
  link.type = "image/svg+xml";
  link.href = faviconDataUri(normalizedColor);
}
