from __future__ import annotations

import json
import bz2
from pathlib import Path
from typing import Any

from jobsight.text import normalise_key, stable_hash


def load_sources(path: Path) -> list[dict[str, Any]]:
    source_path = path
    if not source_path.exists() and path.with_suffix(path.suffix + ".bz2").exists():
        source_path = path.with_suffix(path.suffix + ".bz2")
    if source_path.suffix == ".bz2":
        payload = json.loads(bz2.decompress(source_path.read_bytes()).decode("utf-8"))
    else:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
    sources = payload.get("sources", [])
    active = []
    for source in sources:
        if source.get("enabled", True) is False:
            continue
        item = dict(source)
        item.setdefault("source_id", source_id(item))
        item.setdefault("council_key", normalise_key(item.get("short_name") or item.get("council_name")))
        active.append(item)
    return active


def source_id(source: dict[str, Any]) -> str:
    return stable_hash(source.get("council_name"), source.get("platform"), source.get("url"), length=16)
