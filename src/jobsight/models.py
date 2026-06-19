from __future__ import annotations

from typing import Any

from jobsight.extractors.salary import normalise_salary_fields
from jobsight.extractors.title import best_job_title
from jobsight.text import canonical_url, compact_text, normalise_key, stable_hash, text_excerpt


def make_observation(
    *,
    run_id: str,
    observed_at: str,
    source: dict[str, Any],
    raw: dict[str, Any],
) -> dict[str, Any]:
    url = canonical_url(raw.get("url") or source.get("url"))
    council_name = compact_text(source.get("council_name"))
    short_name = compact_text(source.get("short_name") or council_name)
    title = best_job_title(
        raw.get("title"),
        fallback_title=raw.get("fallback_title"),
        url=url,
        body=raw.get("description_excerpt"),
        council_key=short_name or council_name,
    )
    job_id = stable_hash(normalise_key(council_name), title, url)
    salary = normalise_salary_fields(
        salary_text(raw),
        raw.get("advertised_salary_min"),
        raw.get("advertised_salary_max"),
        raw.get("advertised_salary_period"),
    )
    observation = {
        "schema_version": "jobsight.observation.v1",
        "observation_id": stable_hash(run_id, job_id, source.get("source_id"), url, length=24),
        "job_id": job_id,
        "run_id": run_id,
        "observed_at": observed_at,
        "source_id": source.get("source_id"),
        "source_platform": source.get("platform"),
        "source_url": source.get("url"),
        "council_name": council_name,
        "short_name": short_name,
        "council_key": normalise_key(short_name or council_name),
        "title": title,
        "url": url,
        "location_text": compact_text(raw.get("location_text")),
        "work_type": compact_text(raw.get("work_type")),
        "closing_date": raw.get("closing_date"),
        "closing_text": compact_text(raw.get("closing_text")),
        "advertised_salary_text": salary.text,
        "advertised_salary_min": salary.minimum,
        "advertised_salary_max": salary.maximum,
        "advertised_salary_period": salary.period,
        "classification_status": raw.get("classification_status") or "unclassified",
        "band": raw.get("band"),
        "evidence": raw.get("evidence") or {},
        "field_evidence": raw.get("field_evidence") or {},
        "source_strategy": raw.get("source_strategy"),
        "description_excerpt": compact_text(raw.get("description_excerpt")),
        "description_hash": raw.get("description_hash"),
        "description_status": raw.get("description_status") or "missing",
    }
    return {key: value for key, value in observation.items() if value not in ("", None, {})}


def salary_text(raw: dict[str, Any]) -> str:
    text = compact_text(raw.get("advertised_salary_text"))
    if len(text) <= 360:
        return text_excerpt(text, 300)
    return salary_range_label(raw)


def salary_range_label(raw: dict[str, Any]) -> str:
    minimum = raw.get("advertised_salary_min")
    maximum = raw.get("advertised_salary_max")
    period = compact_text(raw.get("advertised_salary_period"))
    if not isinstance(minimum, (int, float)) and not isinstance(maximum, (int, float)):
        return ""
    low = minimum if isinstance(minimum, (int, float)) else maximum
    high = maximum if isinstance(maximum, (int, float)) else minimum
    if not isinstance(low, (int, float)) or not isinstance(high, (int, float)):
        return ""
    if abs(high - low) < 0.01:
        label = money_label(low)
    else:
        label = f"{money_label(low)}-{money_label(high)}"
    return f"{label}/{period}" if period else label


def money_label(value: int | float) -> str:
    if abs(value - round(value)) < 0.01:
        return f"${round(value):,}"
    return f"${value:,.2f}"
