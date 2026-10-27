#!/usr/bin/env python3
"""Check local Markdown links and heading fragments in repository documentation."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


ROOT = Path(__file__).resolve().parents[1]
LINK_RE = re.compile(r"(?<!!)\[[^\]]*\]\((?:<([^>]+)>|([^\s)]+))(?:\s+[^)]*)?\)")
FENCE_RE = re.compile(r"^\s*(```+|~~~+).*$", re.MULTILINE)
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*#*\s*$", re.MULTILINE)
HTML_ID_RE = re.compile(r"""(?:id|name)\s*=\s*["']([^"']+)["']""", re.IGNORECASE)


def markdown_files() -> list[Path]:
    files = [
        ROOT / "README.md",
        ROOT / "improvements.md",
        ROOT / "CHANGELOG.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "SECURITY.md",
    ]
    files.extend((ROOT / "docs").rglob("*.md"))
    return sorted(path for path in files if path.is_file())


def without_code(text: str) -> str:
    lines = text.splitlines()
    result: list[str] = []
    fence: str | None = None
    for line in lines:
        match = FENCE_RE.match(line)
        if match:
            marker = match.group(1)
            if fence is None:
                fence = marker[:3]
            elif marker.startswith(fence):
                fence = None
            continue
        if fence is None:
            result.append(line)
    return re.sub(r"`+[^`]*`+", "", "\n".join(result))


def anchors(path: Path) -> set[str]:
    text = without_code(path.read_text(encoding="utf-8"))
    found = set(HTML_ID_RE.findall(text))
    counts: dict[str, int] = {}
    for match in HEADING_RE.finditer(text):
        title = re.sub(r"<[^>]*>", "", match.group(2))
        title = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", title)
        base = re.sub(r"[^\w\- ]", "", title.lower())
        base = re.sub(r"\s+", "-", base).strip("-")
        count = counts.get(base, 0)
        counts[base] = count + 1
        found.add(base if count == 0 else f"{base}-{count}")
    return {anchor.lower() for anchor in found}


def resolve_case_sensitive(path: Path) -> Path | None:
    try:
        relative = path.resolve(strict=False).relative_to(ROOT)
    except ValueError:
        return None

    current = ROOT
    for part in relative.parts:
        if part in ("", "."):
            continue
        if part == "..":
            current = current.parent
            if current != ROOT and ROOT not in current.parents:
                return None
            continue
        if not current.is_dir():
            return None
        names = {child.name for child in current.iterdir()}
        if part not in names:
            return None
        current = current / part
    return current if current.exists() else None


def check() -> list[str]:
    errors: list[str] = []
    anchor_cache: dict[Path, set[str]] = {}
    for source in markdown_files():
        body = without_code(source.read_text(encoding="utf-8"))
        for match in LINK_RE.finditer(body):
            target = unquote(match.group(1) or match.group(2))
            parsed = urlsplit(target)
            if parsed.scheme or target.startswith("//"):
                continue

            relative_path = unquote(parsed.path)
            candidate = source.parent / relative_path if relative_path else source
            resolved = resolve_case_sensitive(candidate)
            line = body.count("\n", 0, match.start()) + 1
            if resolved is None:
                errors.append(f"{source.relative_to(ROOT)}:{line}: missing link target {target}")
                continue

            fragment = unquote(parsed.fragment).lower()
            if fragment and resolved.is_file() and resolved.suffix.lower() == ".md":
                if resolved not in anchor_cache:
                    anchor_cache[resolved] = anchors(resolved)
                if fragment not in anchor_cache[resolved]:
                    errors.append(f"{source.relative_to(ROOT)}:{line}: missing anchor {target}")

    return errors


def main() -> int:
    errors = check()
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Checked local Markdown links in {len(markdown_files())} files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
