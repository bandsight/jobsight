from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jobsight.extractors.band import extract_band
from jobsight.extractors.closing_date import extract_closing_date
from jobsight.extractors.salary import extract_salary
from jobsight.models import make_observation
from jobsight.outputs import append_jsonl, rebuild_outputs, write_descriptions
from jobsight.text import compact_text, normalise_key, now_utc, stable_hash, text_excerpt


def import_seed(
    input_path: Path,
    data_root: Path,
    sources: list[dict[str, Any]],
    observed_at: str | None = None,
    include_descriptions: bool = False,
) -> dict[str, Any]:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    rows = payload.get("jobs") or []
    observed = observed_at or payload.get("saved_at") or now_utc()
    run_id = f"seed-{observed[:10].replace('-', '')}"
    source_lookup = {normalise_key(source.get("short_name") or source.get("council_name")): source for source in sources}
    observations: list[dict[str, Any]] = []
    descriptions: list[dict[str, Any]] = []
    for row in rows:
        source = source_lookup.get(normalise_key(row.get("short_name") or row.get("council_name"))) or seed_source(row)
        raw, description = raw_from_seed_row(row, source, include_description=include_descriptions)
        if raw.get("title") and raw.get("url"):
            observations.append(make_observation(run_id=run_id, observed_at=observed, source=source, raw=raw))
        if description:
            descriptions.append(description)
    append_jsonl(data_root / "observations" / f"{observed[:10]}-seed.jsonl", observations)
    if include_descriptions:
        write_descriptions(data_root, descriptions, observed)
    return rebuild_outputs(data_root, sources, run_id=run_id)


def seed_source(row: dict[str, Any]) -> dict[str, Any]:
    council = row.get("council_name") or row.get("short_name")
    platform = row.get("source_family") or "seed"
    return {
        "source_id": stable_hash(council, platform, length=16),
        "council_name": council,
        "short_name": row.get("short_name") or council,
        "council_key": normalise_key(row.get("short_name") or council),
        "platform": platform,
        "url": row.get("job_url") or "",
    }


def raw_from_seed_row(
    row: dict[str, Any],
    source: dict[str, Any],
    include_description: bool = False,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    title = compact_text(row.get("job_title") or row.get("title"))
    description_text = compact_text(row.get("description_text"))
    salary = extract_salary(row.get("advertised_salary_text") or "", description_text)
    closing = extract_closing_date(row.get("closing_at_text") or "", row.get("closing_at") or "")
    band = extract_band(title, row.get("advertised_salary_text") or "", description_text)
    status = band.status
    if status == "unclassified" and salary.text:
        status = "salary_only"
    if status == "unclassified" and description_text:
        status = "description_only"
    description_hash = stable_hash(description_text, length=32) if description_text else None
    description = description_payload_from_text(description_text, row.get("job_url"), source) if include_description else None
    raw = {
        "title": title,
        "url": row.get("job_url"),
        "location_text": row.get("location_text"),
        "work_type": row.get("work_type"),
        "closing_date": closing.date,
        "closing_text": closing.text or row.get("closing_at_text"),
        "advertised_salary_text": row.get("advertised_salary_text") or salary.text,
        "advertised_salary_min": row.get("advertised_salary_min") or salary.minimum,
        "advertised_salary_max": row.get("advertised_salary_max") or salary.maximum,
        "advertised_salary_period": row.get("advertised_salary_period") or salary.period,
        "classification_status": status,
        "band": band.band,
        "evidence": {key: value for key, value in {"band_text": band.evidence_text, "salary_text": salary.evidence_text}.items() if value},
        "description_excerpt": text_excerpt(description_text),
        "description_hash": description["hash"] if description else description_hash,
        "description_status": "excerpt" if description_text and not description else ("fetched" if description else "missing"),
    }
    return raw, description


def description_payload_from_text(text: str, url: str | None, source: dict[str, Any]) -> dict[str, Any] | None:
    if len(text) < 40:
        return None
    digest = stable_hash(text, length=32)
    return {
        "schema_version": "jobsight.description.v1",
        "hash": digest,
        "source_url": url,
        "source_id": source.get("source_id"),
        "text": text_excerpt(text),
        "html": None,
        "sections": [],
    }
