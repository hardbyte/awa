import { useCallback, useEffect, useRef, useState } from "react"

export type Theme = "light" | "dark" | "system"

const darkMetaColor = "oklch(0.17 0.006 285.885)"
const lightMetaColor = "#ffffff"
const themeStorageKey = "theme"

let cachedTheme: Theme | null = null
let cachedDark: boolean | null = null
let mediaQueryListener: ((this: MediaQueryList, ev: MediaQueryListEvent) => void) | null = null

function getStoredTheme(): Theme {
  if (cachedTheme !== null) {
    return cachedTheme
  }
  if (typeof window === "undefined") {
    cachedTheme = "system"
    return cachedTheme
  }
  const stored = localStorage.getItem(themeStorageKey) as Theme | null
  cachedTheme = stored || "system"
  return cachedTheme
}

function prefersDark(): boolean {
  if (cachedDark !== null) {
    return cachedDark
  }
  if (typeof window === "undefined") {
    cachedDark = false
    return cachedDark
  }
  cachedDark = window.matchMedia("(prefers-color-scheme: dark)").matches
  return cachedDark
}

function applyTheme(theme: Theme) {
  const isDark = theme === "dark" || (theme === "system" && prefersDark())
  document.documentElement.classList.toggle("dark", isDark)

  const metaThemeColor = document.querySelector(
    "meta[name='theme-color']",
  ) as HTMLMetaElement | null
  if (metaThemeColor) {
    metaThemeColor.content = isDark ? darkMetaColor : lightMetaColor
  }
}

const mediaQuery =
  typeof window !== "undefined" ? window.matchMedia("(prefers-color-scheme: dark)") : null

export function initializeTheme() {
  const savedTheme = getStoredTheme()
  applyTheme(savedTheme)

  if (mediaQuery && !mediaQueryListener) {
    mediaQueryListener = () => {
      cachedDark = null
      const currentTheme = getStoredTheme()
      if (currentTheme === "system") {
        applyTheme("system")
      }
    }
    mediaQuery.addEventListener("change", mediaQueryListener)
  }

  return () => {
    if (mediaQuery && mediaQueryListener) {
      mediaQuery.removeEventListener("change", mediaQueryListener)
      mediaQueryListener = null
    }
  }
}

export function useTheme() {
  const [theme, setTheme] = useState<Theme>(() => getStoredTheme())
  const [resolvedTheme, setResolvedTheme] = useState<"light" | "dark">(() =>
    prefersDark() ? "dark" : "light",
  )

  const savedThemeRef = useRef<Theme | null>(null)

  const updateTheme = useCallback((mode: Theme) => {
    cachedTheme = mode
    localStorage.setItem(themeStorageKey, mode)
    savedThemeRef.current = mode
    setTheme(mode)
    applyTheme(mode)
    setResolvedTheme(() => (mode === "system" ? (prefersDark() ? "dark" : "light") : mode))
  }, [])

  useEffect(() => {
    const savedTheme = getStoredTheme()
    savedThemeRef.current = savedTheme
    setTheme(savedTheme)
    setResolvedTheme(savedTheme === "system" ? (prefersDark() ? "dark" : "light") : savedTheme)

    if (mediaQuery) {
      const handler = () => {
        cachedDark = null
        if (savedThemeRef.current === "system") {
          const next = prefersDark() ? "dark" : "light"
          setResolvedTheme(next)
          applyTheme("system")
        }
      }
      mediaQuery.addEventListener("change", handler)
      return () => mediaQuery.removeEventListener("change", handler)
    }
  }, [])

  return { theme, updateTheme, resolvedTheme }
}
