#!/usr/bin/env python3
"""Build and validate agent-readable companions for the MkDocs site."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit, urlunsplit

import yaml


MARKDOWN_LINK = re.compile(r"(!?\[[^\]]*\])\(([^)]+)\)")
SNIPPET = re.compile(r'^([ \t]*)--8<--\s+"([^"]+)"[ \t]*$', re.MULTILINE)


def page_output(source: PurePosixPath) -> PurePosixPath:
    if source.name.lower() in {"index.md", "readme.md"}:
        return source.parent / "index.md"
    return source.with_suffix("") / "index.md"


def html_output(source: PurePosixPath) -> PurePosixPath:
    return page_output(source).with_suffix(".html")


def strip_front_matter(text: str) -> str:
    if not text.startswith("---\n"):
        return text
    marker = text.find("\n---\n", 4)
    return text[marker + 5 :] if marker >= 0 else text


def rewrite_link(
    destination: str,
    source: PurePosixPath,
    published_sources: set[PurePosixPath],
) -> str:
    wrapped = destination.startswith("<") and destination.endswith(">")
    value = destination[1:-1] if wrapped else destination
    parts = urlsplit(value)
    if parts.scheme or parts.netloc or value.startswith(("#", "/", "mailto:")):
        return destination

    resolved = PurePosixPath(os.path.normpath(str(source.parent / parts.path)))
    if parts.path.endswith(".md") and resolved not in published_sources:
        return urlunsplit(("https", "github.com", f"/hardbyte/awa/blob/main/docs/{resolved.as_posix()}", parts.query, parts.fragment))
    target = page_output(resolved) if parts.path.endswith(".md") else resolved
    current_dir = page_output(source).parent
    relative = os.path.relpath(target, current_dir).replace(os.sep, "/")
    rewritten = urlunsplit(("", "", relative, parts.query, parts.fragment))
    return f"<{rewritten}>" if wrapped else rewritten


def expand_snippets(text: str, repository_root: Path) -> str:
    root = repository_root.resolve()

    def replace(match: re.Match[str]) -> str:
        include = (root / match.group(2)).resolve()
        try:
            include.relative_to(root)
        except ValueError:
            raise SystemExit(f"snippet escapes repository: {match.group(2)}")
        if not include.is_file():
            raise SystemExit(f"snippet does not exist: {match.group(2)}")
        indent = match.group(1)
        return "\n".join(indent + line if line else "" for line in include.read_text().splitlines())

    return SNIPPET.sub(replace, text)


def markdown_variant(
    text: str,
    source: PurePosixPath,
    repository_root: Path,
    published_sources: set[PurePosixPath],
) -> str:
    body = strip_front_matter(text).lstrip()
    body = expand_snippets(body, repository_root)
    body = MARKDOWN_LINK.sub(
        lambda match: f"{match.group(1)}({rewrite_link(match.group(2), source, published_sources)})",
        body,
    )
    index_path = os.path.relpath("llms.txt", page_output(source).parent).replace(os.sep, "/")
    directive = (
        f"> For the complete AWA documentation index, see "
        f"[`llms.txt`]({index_path}).\n\n"
    )
    return directive + body.rstrip() + "\n"


def first_description(text: str) -> str:
    body = strip_front_matter(text)
    in_fence = False
    for block in re.split(r"\n\s*\n", body):
        candidate = block.strip()
        if candidate.startswith("```"):
            in_fence = not in_fence
            continue
        if (
            not candidate
            or in_fence
            or candidate.startswith(("#", ">", "- ", "* ", "<", "!!!", "???"))
        ):
            continue
        candidate = re.sub(r"\[([^]]+)\]\([^)]+\)", r"\1", candidate)
        candidate = re.sub(r"[`*_]", "", candidate)
        candidate = " ".join(candidate.split())
        if len(candidate) > 240:
            candidate = candidate[:240].rsplit(" ", 1)[0]
        return candidate.rstrip(" .,:;—-") + "."
    return "AWA documentation."


def nav_links(value: object) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    if not isinstance(value, list):
        return links
    for child in value:
        if not isinstance(child, dict):
            continue
        label, target = next(iter(child.items()))
        if isinstance(target, str):
            links.append((str(label), target))
        else:
            links.extend(nav_links(target))
    return links


def nav_sections(nav: list[object]) -> list[tuple[str, list[tuple[str, str]]]]:
    sections: list[tuple[str, list[tuple[str, str]]]] = []
    for item in nav:
        if not isinstance(item, dict):
            continue
        heading, value = next(iter(item.items()))
        links: list[tuple[str, str]] = []
        if isinstance(value, str):
            links.append((str(heading), value))
        elif isinstance(value, list):
            links.extend(nav_links(value))
        sections.append((str(heading), links))
    return sections


def build(docs_dir: Path, site_dir: Path, config_path: Path) -> None:
    config = yaml.safe_load(config_path.read_text())

    repository_root = config_path.resolve().parent
    source_paths = sorted(docs_dir.rglob("*.md"))
    published_sources = {
        PurePosixPath(path.relative_to(docs_dir).as_posix())
        for path in source_paths
        if (site_dir / html_output(PurePosixPath(path.relative_to(docs_dir).as_posix()))).exists()
    }
    generated: dict[PurePosixPath, Path] = {}
    for path in source_paths:
        source = PurePosixPath(path.relative_to(docs_dir).as_posix())
        if source not in published_sources:
            continue
        destination = site_dir / page_output(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            markdown_variant(path.read_text(), source, repository_root, published_sources)
        )
        generated[source] = destination

    lines = [
        "# AWA",
        "",
        "> Postgres-native background jobs for Rust and Python, with durable job state and transactional enqueue.",
        "",
        "These links are curated for agents. Each target is the Markdown representation of the corresponding documentation page.",
    ]
    indexed: set[PurePosixPath] = set()
    for section, links in nav_sections(config["nav"]):
        available = []
        for label, path_value in links:
            source = PurePosixPath(path_value)
            if source in generated:
                available.append((label, source))
                indexed.add(source)
        if not available:
            continue
        lines.extend(("", f"## {section}", ""))
        for label, source in available:
            url = page_output(source).as_posix()
            description = first_description((docs_dir / source).read_text())
            lines.append(f"- [{label}]({url}): {description}")

    lines.extend(
        (
            "",
            "## Optional",
            "",
            "- [GitHub repository](https://github.com/hardbyte/awa): Source code, issues, releases, and repository-only contributor material.",
        )
    )
    llms = "\n".join(lines) + "\n"
    if len(llms) >= 50_000:
        raise SystemExit(f"llms.txt is too large: {len(llms)} characters")
    (site_dir / "llms.txt").write_text(llms)

    expected_nav = {
        PurePosixPath(path)
        for _, links in nav_sections(config["nav"])
        for _, path in links
    }
    all_sources = {
        PurePosixPath(path.relative_to(docs_dir).as_posix()) for path in source_paths
    }
    validate(
        site_dir,
        generated,
        indexed,
        expected_nav,
        all_sources - published_sources,
    )


def validate(
    site_dir: Path,
    generated: dict[PurePosixPath, Path],
    indexed: set[PurePosixPath],
    expected_nav: set[PurePosixPath],
    excluded_sources: set[PurePosixPath],
) -> None:
    llms_path = site_dir / "llms.txt"
    llms = llms_path.read_text()
    errors: list[str] = []
    if not llms.startswith("# AWA\n\n> "):
        errors.append("llms.txt must start with an H1 and blockquote summary")
    if len(llms) >= 50_000:
        errors.append("llms.txt must remain below 50,000 characters")
    if not indexed:
        errors.append("llms.txt contains no documentation pages")
    missing_nav = expected_nav - indexed
    if missing_nav:
        errors.append(
            "llms.txt omits nav pages: " + ", ".join(sorted(map(str, missing_nav)))
        )

    for _, destination in MARKDOWN_LINK.findall(llms):
        parts = urlsplit(destination)
        if parts.scheme or parts.netloc:
            continue
        target = site_dir / unquote(parts.path)
        if not target.is_file():
            errors.append(f"llms.txt: target does not exist: {destination}")

    for source, output in generated.items():
        text = output.read_text()
        if "[`llms.txt`]" not in text:
            errors.append(f"{output}: missing llms.txt discovery directive")
        if len(text) >= 100_000:
            errors.append(f"{output}: Markdown representation exceeds 100,000 characters")
        if output.stat().st_size == 0:
            errors.append(f"{output}: empty Markdown representation")
        expected_html = site_dir / html_output(source)
        if not expected_html.exists():
            errors.append(f"{output}: corresponding HTML page is missing")
        else:
            html = expected_html.read_text()
            if 'rel="alternate"' not in html or 'type="text/markdown"' not in html:
                errors.append(f"{expected_html}: missing Markdown alternate link")
            if "llms.txt" not in html:
                errors.append(f"{expected_html}: missing llms.txt discovery link")
        for _, destination in MARKDOWN_LINK.findall(text):
            parts = urlsplit(destination.strip("<>"))
            if parts.scheme or parts.netloc or destination.startswith(("#", "/", "mailto:")):
                continue
            target = (output.parent / unquote(parts.path)).resolve()
            if parts.path and not target.exists():
                errors.append(f"{output}: local target does not exist: {destination}")

    for source in excluded_sources:
        output = site_dir / page_output(source)
        if output.exists():
            errors.append(f"excluded source unexpectedly emitted: {source}")

    if errors:
        raise SystemExit("agent documentation checks failed:\n- " + "\n- ".join(errors))
    print(
        f"agent documentation checks passed: {len(generated)} Markdown pages, "
        f"{len(indexed)} pages indexed, {len(llms)}-character llms.txt"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"))
    parser.add_argument("--site-dir", type=Path, default=Path("site"))
    parser.add_argument("--config", type=Path, default=Path("mkdocs.yml"))
    args = parser.parse_args()
    build(args.docs_dir, args.site_dir, args.config)


if __name__ == "__main__":
    main()
