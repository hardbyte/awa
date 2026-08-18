(() => {
  const markdownUrl = () => new URL("index.md", window.location.href);

  const copyMarkdown = async (button) => {
    const response = await fetch(markdownUrl(), {
      headers: { Accept: "text/markdown" },
    });
    if (!response.ok) {
      throw new Error(`Markdown request failed with ${response.status}`);
    }
    await navigator.clipboard.writeText(await response.text());
    const original = button.textContent;
    button.textContent = "Copied";
    window.setTimeout(() => {
      button.textContent = original;
    }, 1600);
  };

  const mount = () => {
    const content = document.querySelector("article.md-content__inner");
    if (!content || content.querySelector(".awa-agent-actions")) return;

    const actions = document.createElement("div");
    actions.className = "awa-agent-actions";

    const copy = document.createElement("button");
    copy.type = "button";
    copy.className = "md-button md-button--primary awa-agent-copy";
    copy.textContent = "Copy Markdown";
    copy.addEventListener("click", async () => {
      copy.disabled = true;
      try {
        await copyMarkdown(copy);
      } catch (error) {
        copy.textContent = "Copy failed";
        console.error(error);
      } finally {
        copy.disabled = false;
      }
    });

    const view = document.createElement("a");
    view.className = "md-button awa-agent-view";
    view.href = markdownUrl();
    view.textContent = "View Markdown";

    actions.append(copy, view);
    content.insertBefore(actions, content.firstChild);
  };

  if (typeof document$ !== "undefined") {
    document$.subscribe(mount);
  } else {
    document.addEventListener("DOMContentLoaded", mount);
  }
})();
