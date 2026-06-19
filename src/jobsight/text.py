from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit


SPACE_RE = re.compile(r"\s+")
PLACEHOLDER_RUN_RE = re.compile(r"(?:\?{3,}|\ufffd+)")


def now_utc() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def compact_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u00a0", " ")
    text = PLACEHOLDER_RUN_RE.sub(" ", text)
    return SPACE_RE.sub(" ", text).strip()


def text_excerpt(value: Any, limit: int = 300) -> str:
    text = compact_text(value)
    if len(text) <= limit:
        return text
    excerpt = text[:limit].rsplit(" ", 1)[0].rstrip(" ,.;:")
    return f"{excerpt}..."


def normalise_key(value: Any) -> str:
    text = compact_text(value).upper().replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return SPACE_RE.sub(" ", text).strip()


def stable_hash(*parts: Any, length: int = 20) -> str:
    material = "\x1f".join(compact_text(part).lower() for part in parts if part is not None)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:length]


def canonical_url(url: str) -> str:
    parsed = urlsplit(compact_text(url))
    query = "&".join(
        part for part in parsed.query.split("&")
        if part and not part.lower().startswith(("utm_", "fbclid=", "gclid="))
    )
    return urlunsplit((parsed.scheme, parsed.netloc.lower(), parsed.path.rstrip("/") or "/", query, ""))
