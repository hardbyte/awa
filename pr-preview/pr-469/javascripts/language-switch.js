(function () {
  "use strict";

  const storageKey = "awa-docs-language";
  const languages = ["rust", "python"];
  const guidePaths = {
    rust: "getting-started-rust/",
    python: "getting-started-python/",
  };

  function savedLanguage() {
    try {
      const value = window.localStorage.getItem(storageKey);
      return languages.includes(value) ? value : null;
    } catch (_) {
      return null;
    }
  }

  function saveLanguage(language) {
    try {
      window.localStorage.setItem(storageKey, language);
    } catch (_) {
      // The preference is optional; private browsing may disable storage.
    }
  }

  function languageTabs() {
    return Array.from(document.querySelectorAll(".tabbed-labels > label")).filter(
      (label) => languages.includes(label.textContent.trim().toLowerCase()),
    );
  }

  function selectTabs(language) {
    languageTabs()
      .filter((label) => label.textContent.trim().toLowerCase() === language)
      .forEach((label) => label.click());
  }

  function updateButtons(language) {
    document.querySelectorAll("[data-awa-language]").forEach((button) => {
      button.setAttribute(
        "aria-pressed",
        String(button.dataset.awaLanguage === language),
      );
    });
  }

  function currentGuideLanguage() {
    return languages.find((language) =>
      window.location.pathname.endsWith(guidePaths[language]),
    );
  }

  function pairedGuideUrl(language) {
    const current = currentGuideLanguage();
    if (!current) return null;
    return new URL(
      window.location.href.replace(guidePaths[current], guidePaths[language]),
    );
  }

  function apply(language) {
    updateButtons(language);
    selectTabs(language);
  }

  function choose(language) {
    saveLanguage(language);
    apply(language);

    const destination = pairedGuideUrl(language);
    if (destination && destination.href !== window.location.href) {
      window.location.assign(destination);
    }
  }

  function initialise() {
    const switcher = document.querySelector("[data-awa-language-switch]");
    if (!switcher) return;

    const tabs = languageTabs();
    const hasPair = languages.every((language) =>
      tabs.some((label) => label.textContent.trim().toLowerCase() === language),
    );
    if (!currentGuideLanguage() && !hasPair) return;

    switcher.hidden = false;
    switcher.querySelectorAll("[data-awa-language]").forEach((button) => {
      button.addEventListener("click", () =>
        choose(button.dataset.awaLanguage),
      );
    });

    // Reflect the page's own language when on a guide page; fall back to the
    // saved preference for tabbed pages. Displaying is not choosing, so the
    // stored preference is left untouched.
    apply(currentGuideLanguage() || savedLanguage() || "rust");
  }

  if (typeof document$ !== "undefined") {
    document$.subscribe(initialise);
  } else {
    document.addEventListener("DOMContentLoaded", initialise);
  }
})();
