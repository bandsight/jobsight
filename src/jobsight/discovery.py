from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from jobsight.text import compact_text, stable_hash


ENDPOINT_WORD_RE = re.compile(
    r"(api|jobs?|careers?|vacanc|position|recruit|requisition|graphql|feed|rss|sitemap)",
    re.IGNORECASE,
)
LINK_ENDPOINT_RE = re.compile(
    r"(api|json|xml|rss|feed|sitemap|graphql|ajax|service|odata|endpoint)",
    re.IGNORECASE,
)
ENDPOINT_LITERAL_RE = re.compile(
    r"(?P<quote>['\"])(?P<url>(?:https?://|/|\.\.?/)[^'\"]{1,360}?"
    r"(?:api|jobs?|careers?|vacanc|position|recruit|requisition|graphql|feed|rss|sitemap)"
    r"[^'\"]{0,360}?)(?P=quote)",
    re.IGNORECASE,
)
NOISE_EXTENSIONS = (
    ".css",
    ".gif",
    ".ico",
    ".jpg",
    ".jpeg",
    ".png",
    ".svg",
    ".webp",
    ".woff",
    ".woff2",
)


def same_origin(base_url: str, candidate_url: str) -> bool:
    base = urlsplit(base_url)
    candidate = urlsplit(candidate_url)
    return bool(
        candidate.scheme in {"http", "https"}
        and candidate.netloc
        and candidate.scheme == base.scheme
        and candidate.netloc.lower() == base.netloc.lower()
    )


def clean_candidate_url(base_url: str, raw_url: str) -> str:
    candidate = compact_text(raw_url)
    if not candidate:
        return ""
    candidate = candidate.replace("\\/", "/")
    candidate = candidate.split("#", 1)[0]
    if candidate.startswith(("mailto:", "tel:", "javascript:", "data:")):
        return ""
    absolute = urljoin(base_url, candidate)
    parsed = urlsplit(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    path = parsed.path
    if path.lower().endswith(NOISE_EXTENSIONS):
        return ""
    return urlunsplit((parsed.scheme, parsed.netloc.lower(), path, parsed.query, ""))


def hint_key(hint: dict[str, Any]) -> str:
    return stable_hash(hint.get("url"), hint.get("kind"), hint.get("evidence"), length=16)


def add_hint(hints: list[dict[str, Any]], seen: set[str], base_url: str, raw_url: str, kind: str, evidence: str) -> None:
    url = clean_candidate_url(base_url, raw_url)
    if not url or not same_origin(base_url, url):
        return
    haystack = f"{url} {evidence}"
    if not ENDPOINT_WORD_RE.search(haystack):
        return
    key = url.lower()
    if key in seen:
        return
    seen.add(key)
    hints.append({
        "id": stable_hash(kind, url, evidence, length=16),
        "kind": kind,
        "url": url,
        "evidence": compact_text(evidence)[:240],
    })


def endpoint_paths_from_text(base_url: str, text: str, *, kind: str) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    seen: set[str] = set()
    for match in ENDPOINT_LITERAL_RE.finditer(text or ""):
        add_hint(hints, seen, base_url, match.group("url"), kind, match.group(0))
    return hints


def discover_endpoint_hints(
    base_url: str,
    html: str,
    *,
    client: Any | None = None,
    script_limit: int = 3,
    max_hints: int = 12,
) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    hints: list[dict[str, Any]] = []
    seen: set[str] = set()

    for tag in soup.select("link[href], a[href], form[action]"):
        attr = "action" if tag.name == "form" else "href"
        raw_url = tag.get(attr) or ""
        label = compact_text(" ".join([
            tag.get("rel") if isinstance(tag.get("rel"), str) else " ".join(tag.get("rel") or []),
            tag.get("type") or "",
            tag.get_text(" "),
            tag.get("title") or "",
            tag.get("aria-label") or "",
        ]))
        kind = "form_action" if tag.name == "form" else "feed_or_link"
        is_structured_link = bool(LINK_ENDPOINT_RE.search(f"{raw_url} {label}"))
        if tag.name == "form" or is_structured_link:
            add_hint(hints, seen, base_url, raw_url, kind, label)

    for script in soup.select("script:not([src])"):
        for hint in endpoint_paths_from_text(base_url, script.get_text(" "), kind="inline_script"):
            if hint["url"].lower() not in seen:
                seen.add(hint["url"].lower())
                hints.append(hint)

    script_count = 0
    if client is not None:
        for script in soup.select("script[src]"):
            if script_count >= script_limit or len(hints) >= max_hints:
                break
            src = clean_candidate_url(base_url, script.get("src") or "")
            if not src or not same_origin(base_url, src):
                continue
            script_count += 1
            fetched = client.get(src)
            if getattr(fetched, "error", None):
                continue
            for hint in endpoint_paths_from_text(base_url, fetched.text, kind="linked_script"):
                if hint["url"].lower() not in seen:
                    seen.add(hint["url"].lower())
                    hints.append(hint)
                    if len(hints) >= max_hints:
                        break

    return hints[:max_hints]


def json_ld_job_items(html: str, base_url: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html or "", "lxml")
    items: list[dict[str, Any]] = []
    for script in soup.select('script[type*="ld+json"]'):
        text = script.get_text(" ").strip()
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        for item in walk_json(payload):
            if isinstance(item, dict) and is_jobposting(item):
                normalised = dict(item)
                if normalised.get("url"):
                    normalised["url"] = urljoin(base_url, compact_text(normalised.get("url")))
                items.append(normalised)
    return items


def walk_json(value: Any) -> list[Any]:
    found = [value]
    if isinstance(value, dict):
        for child in value.values():
            found.extend(walk_json(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(walk_json(child))
    return found


def is_jobposting(item: dict[str, Any]) -> bool:
    item_type = item.get("@type") or item.get("type")
    if isinstance(item_type, list):
        types = [compact_text(value).lower() for value in item_type]
    else:
        types = [compact_text(item_type).lower()]
    return "jobposting" in types or "job posting" in types


def same_origin_feed_urls(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html or "", "lxml")
    urls: list[str] = []
    seen: set[str] = set()
    for tag in soup.select("link[href], a[href]"):
        raw_url = tag.get("href") or ""
        label = compact_text(" ".join([
            tag.get("type") or "",
            tag.get("rel") if isinstance(tag.get("rel"), str) else " ".join(tag.get("rel") or []),
            tag.get_text(" "),
        ]))
        if not re.search(r"(rss|feed|sitemap|xml)", f"{raw_url} {label}", re.I):
            continue
        url = clean_candidate_url(base_url, raw_url)
        if url and same_origin(base_url, url) and url.lower() not in seen:
            seen.add(url.lower())
            urls.append(url)
    return urls[:5]
