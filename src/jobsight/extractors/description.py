from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

from jobsight.text import compact_text, stable_hash

DESCRIPTION_SELECTORS = ["[class*=description]", "[class*=details]", "[class*=content]", "[class*=overview]", "main", "article"]
SECTION_RE = re.compile(r"\b(about the role|key responsibilities|selection criteria|what you will do|about you|how to apply|position description)\b", re.IGNORECASE)


@dataclass(frozen=True)
class DescriptionResult:
    text: str | None
    html: str | None
    sections: list[dict[str, str]]
    hash: str | None
    status: str


def extract_description(soup: BeautifulSoup) -> DescriptionResult:
    node = None
    for selector in DESCRIPTION_SELECTORS:
        node = soup.select_one(selector)
        if node:
            break
    if not node:
        return DescriptionResult(text=None, html=None, sections=[], hash=None, status="missing")
    for bad in node.select("script, style, nav, footer, header, form"):
        bad.decompose()
    text = compact_text(node.get_text(" "))
    if len(text) < 40:
        return DescriptionResult(text=text or None, html=None, sections=[], hash=None, status="sparse")
    return DescriptionResult(text=text, html=str(node), sections=_sections(text), hash=stable_hash(text, length=32), status="fetched")


def _sections(text: str) -> list[dict[str, str]]:
    matches = list(SECTION_RE.finditer(text))
    if not matches:
        return []
    sections: list[dict[str, str]] = []
    for index, match in enumerate(matches):
        start = match.end()
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = compact_text(text[start:end])
        if body:
            sections.append({"heading": compact_text(match.group(0)).title(), "text": body})
    return sections[:8]
