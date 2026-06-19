from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from jobsight.discovery import discover_endpoint_hints, json_ld_job_items, same_origin_feed_urls
from jobsight.extractors.band import extract_band
from jobsight.extractors.closing_date import extract_closing_date
from jobsight.extractors.salary import extract_salary
from jobsight.extractors.title import best_job_title
from jobsight.platforms.generic import candidate_links, is_probable_job_raw, job_from_detail
from jobsight.text import compact_text, now_utc, stable_hash, text_excerpt


PROFILE_SCHEMA = "jobsight.source_profiles.v1"
HEALTH_SCHEMA = "jobsight.source_health.v1"
QUARANTINE_SCHEMA = "jobsight.quarantine.v1"
ADAPTER_PLATFORMS = {
    "pageup",
    "applynow",
    "successfactors",
    "elmo_talent",
    "t1cloud",
    "bigredsky",
    "recruitmenthub",
}
ENDPOINT_PLATFORMS = {"oracle_hcm", "pulse"} | ADAPTER_PLATFORMS
SEVERE_SOURCE_JOB_LIMIT = 50


@dataclass
class StrategyResult:
    name: str
    jobs: list[dict[str, Any]]
    descriptions: list[dict[str, Any]] = field(default_factory=list)
    status: str = "ok"
    message: str = ""
    discovered_endpoints: list[dict[str, Any]] = field(default_factory=list)
    diagnostics: dict[str, Any] = field(default_factory=dict)


@dataclass
class SmartParseResult:
    jobs: list[dict[str, Any]]
    descriptions: list[dict[str, Any]]
    health: dict[str, Any]
    profile: dict[str, Any]
    quarantine: list[dict[str, Any]]


def empty_profiles() -> dict[str, Any]:
    return {
        "schema_version": PROFILE_SCHEMA,
        "updated_at": None,
        "global_rules": {
            "reject_title_exact": [],
        },
        "sources": {},
    }


def load_source_profiles(path: Path) -> dict[str, Any]:
    if not path.exists():
        return empty_profiles()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty_profiles()
    if payload.get("schema_version") != PROFILE_SCHEMA:
        return empty_profiles()
    payload.setdefault("global_rules", {}).setdefault("reject_title_exact", [])
    payload.setdefault("sources", {})
    return payload


def save_source_profiles(path: Path, profiles: dict[str, Any], updated_at: str | None = None) -> None:
    profiles["schema_version"] = PROFILE_SCHEMA
    profiles["updated_at"] = updated_at or now_utc()
    profiles.setdefault("global_rules", {}).setdefault("reject_title_exact", [])
    profiles.setdefault("sources", {})
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(profiles, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def write_source_health(path: Path, health_rows: list[dict[str, Any]], *, run_id: str | None, generated_at: str) -> None:
    summary = {
        "sources": len(health_rows),
        "ok": sum(1 for row in health_rows if row.get("status") == "ok"),
        "degraded": sum(1 for row in health_rows if row.get("status") == "degraded"),
        "failed": sum(1 for row in health_rows if row.get("status") == "failed"),
        "fallback_used": sum(1 for row in health_rows if row.get("fallback_used")),
        "quarantined_rows": sum(int(row.get("quarantined_rows") or 0) for row in health_rows),
    }
    payload = {
        "schema_version": HEALTH_SCHEMA,
        "generated_at": generated_at,
        "run_id": run_id,
        "summary": summary,
        "platforms": platform_health_summary(health_rows),
        "sources": sorted(health_rows, key=lambda row: (row.get("status") != "degraded", row.get("source_id", ""))),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")


def platform_health_summary(health_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in health_rows:
        grouped[compact_text(row.get("platform")) or "unknown"].append(row)

    summaries: dict[str, dict[str, Any]] = {}
    for platform, rows in sorted(grouped.items()):
        strategy_counts = Counter(compact_text(row.get("chosen_strategy")) or "unknown" for row in rows)
        drift_counts = Counter(
            compact_text(flag.get("code"))
            for row in rows
            for flag in (row.get("drift_flags") or [])
            if isinstance(flag, dict) and compact_text(flag.get("code"))
        )
        accepted = sum(int((row.get("quality") or {}).get("accepted_count") or row.get("accepted_count") or 0) for row in rows)
        candidates = sum(int((row.get("quality") or {}).get("candidate_count") or row.get("candidate_count") or 0) for row in rows)
        rejected = sum(int((row.get("quality") or {}).get("rejected_count") or row.get("rejected_count") or 0) for row in rows)
        band_count = sum(int((row.get("quality") or {}).get("band_count") or 0) for row in rows)
        salary_count = sum(int((row.get("quality") or {}).get("salary_count") or 0) for row in rows)
        description_count = sum(int((row.get("quality") or {}).get("description_count") or 0) for row in rows)
        degraded = sum(1 for row in rows if row.get("status") == "degraded")
        failed = sum(1 for row in rows if row.get("status") == "failed")
        summaries[platform] = {
            "sources": len(rows),
            "status": "degraded" if degraded or failed else "ok",
            "ok": sum(1 for row in rows if row.get("status") == "ok"),
            "degraded": degraded,
            "failed": failed,
            "fallback_used": sum(1 for row in rows if row.get("fallback_used")),
            "candidate_count": candidates,
            "accepted_count": accepted,
            "rejected_count": rejected,
            "quarantined_rows": sum(int(row.get("quarantined_rows") or 0) for row in rows),
            "junk_rate": round(rejected / candidates, 4) if candidates else 0.0,
            "band_coverage": round(band_count / accepted, 4) if accepted else 0.0,
            "salary_coverage": round(salary_count / accepted, 4) if accepted else 0.0,
            "description_coverage": round(description_count / accepted, 4) if accepted else 0.0,
            "chosen_strategies": dict(sorted(strategy_counts.items())),
            "drift_flags": dict(sorted(drift_counts.items())),
        }
    return summaries


def append_quarantine(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            payload = {"schema_version": QUARANTINE_SCHEMA, **row}
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")


def source_profile_for(profiles: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    source_id = compact_text(source.get("source_id"))
    existing = (profiles.get("sources") or {}).get(source_id) or {}
    return {
        "source_id": source_id,
        "council_key": source.get("council_key"),
        "council_name": source.get("council_name"),
        "platform": source.get("platform"),
        "preferred_strategy": existing.get("preferred_strategy"),
        "known_endpoints": list(existing.get("known_endpoints") or []),
        "selector_hints": list(existing.get("selector_hints") or []),
        "promoted_rules": {
            "reject_title_exact": list((existing.get("promoted_rules") or {}).get("reject_title_exact") or []),
        },
        "last_good_count": int(existing.get("last_good_count") or 0),
        "last_good_score": int(existing.get("last_good_score") or 0),
        "last_good_run_id": existing.get("last_good_run_id"),
        "failure_streak": int(existing.get("failure_streak") or 0),
        "quality_history": list(existing.get("quality_history") or [])[-12:],
        "health": existing.get("health") or "unknown",
    }


def parse_source_smart(
    source: dict[str, Any],
    client: Any,
    profiles: dict[str, Any],
    *,
    observed_at: str,
    run_id: str,
) -> SmartParseResult:
    profile = source_profile_for(profiles, source)
    strategies = run_strategies(source, client, profile)
    selected = choose_strategy(strategies, profile)
    selected.discovered_endpoints = merge_strategy_endpoint_hints(strategies, selected.discovered_endpoints)
    global_rules = profiles.get("global_rules") or {}
    accepted, rejected = split_accepted_jobs(selected.jobs, source, selected.name, global_rules, profile)
    metrics = strategy_metrics(selected, accepted, rejected)
    flags = drift_flags(metrics, selected, profile)
    severe = any(flag.get("severity") == "severe" for flag in flags)
    status = "failed" if selected.status == "failed" and not accepted else "degraded" if severe else "ok"
    fallback_used = severe or (selected.status == "failed" and profile.get("last_good_count", 0) > 0)

    quarantine = quarantine_rows(
        source,
        selected,
        rejected,
        observed_at=observed_at,
        run_id=run_id,
        reason="severe_drift" if severe else "quality_rejected",
        flags=flags,
    )
    if fallback_used:
        accepted = []

    annotated = [annotate_evidence(job, selected.name) for job in accepted]
    updated_profile = update_source_profile(profile, selected, metrics, flags, observed_at=observed_at, run_id=run_id, accepted=annotated)
    health = {
        "source_id": source.get("source_id"),
        "council_key": source.get("council_key"),
        "council_name": source.get("council_name"),
        "platform": source.get("platform"),
        "status": status,
        "chosen_strategy": selected.name,
        "fallback_used": fallback_used,
        "candidate_count": len(selected.jobs),
        "accepted_count": len(annotated),
        "rejected_count": len(rejected),
        "quarantined_rows": len(quarantine),
        "score": metrics["score"],
        "quality": metrics,
        "drift_flags": flags,
        "discovered_endpoints": selected.discovered_endpoints,
        "strategy_results": [strategy_summary(result, global_rules, profile, source) for result in strategies],
    }
    return SmartParseResult(
        jobs=annotated,
        descriptions=selected.descriptions if not fallback_used else [],
        health=health,
        profile=updated_profile,
        quarantine=quarantine,
    )


def run_strategies(source: dict[str, Any], client: Any, profile: dict[str, Any]) -> list[StrategyResult]:
    strategies: list[StrategyResult] = []
    preferred = compact_text(profile.get("preferred_strategy"))
    known_endpoints = [item for item in profile.get("known_endpoints", []) if item.get("url")]
    accepted_known_endpoints = [item for item in known_endpoints if item.get("status") == "accepted"]

    if preferred.startswith("known_endpoint") or preferred.startswith("discovered_endpoint"):
        for endpoint in (accepted_known_endpoints or known_endpoints)[:2]:
            strategies.append(parse_endpoint_json_strategy(source, client, endpoint["url"], name="known_endpoint"))

    strategies.append(parse_default_strategy(source, client))

    discovery = discover_source(source, client)
    if str(source.get("platform") or "") in ENDPOINT_PLATFORMS or accepted_known_endpoints:
        for endpoint in discovery.discovered_endpoints[:3]:
            strategies.append(parse_endpoint_json_strategy(source, client, endpoint["url"], name="discovered_endpoint", hint=endpoint))
    if discovery.jobs or discovery.discovered_endpoints:
        strategies.append(discovery)
    feed = parse_feed_strategy(source, client, discovery.diagnostics.get("listing_html") or "")
    if feed.jobs:
        strategies.append(feed)

    if not strategies:
        strategies.append(StrategyResult(name="empty", jobs=[], status="failed", message="no strategies available"))
    return strategies


def parse_default_strategy(source: dict[str, Any], client: Any) -> StrategyResult:
    from jobsight.platforms import parse_source

    name = f"platform:{source.get('platform') or 'generic_html'}"
    try:
        jobs, descriptions = parse_source(source, client)
    except Exception as exc:
        return StrategyResult(name=name, jobs=[], status="failed", message=str(exc))
    return StrategyResult(name=name, jobs=jobs, descriptions=descriptions)


def discover_source(source: dict[str, Any], client: Any) -> StrategyResult:
    fetched = client.get(source["url"])
    if getattr(fetched, "error", None):
        return StrategyResult(name="discovery", jobs=[], status="failed", message=fetched.error)
    endpoints = discover_endpoint_hints(fetched.url, fetched.text, client=client)
    json_jobs = parse_json_ld_items(source, fetched.url, fetched.text)
    return StrategyResult(
        name="json_ld",
        jobs=json_jobs,
        discovered_endpoints=endpoints,
        diagnostics={"listing_url": fetched.url, "listing_html": fetched.text},
    )


def parse_json_ld_strategy(source: dict[str, Any], client: Any) -> StrategyResult:
    fetched = client.get(source["url"])
    if getattr(fetched, "error", None):
        return StrategyResult(name="json_ld", jobs=[], status="failed", message=fetched.error)
    endpoints = discover_endpoint_hints(fetched.url, fetched.text, client=client)
    return StrategyResult(
        name="json_ld",
        jobs=parse_json_ld_items(source, fetched.url, fetched.text),
        discovered_endpoints=endpoints,
        diagnostics={"listing_url": fetched.url, "listing_html": fetched.text},
    )


def parse_json_ld_items(source: dict[str, Any], base_url: str, html: str) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for item in json_ld_job_items(html, base_url):
        title = compact_text(item.get("title") or item.get("name"))
        if not title:
            continue
        url = compact_text(item.get("url")) or source.get("url")
        body = json_scalar_text(item)
        salary = extract_salary(body)
        band = extract_band(title, body)
        closing = extract_closing_date(item.get("validThrough") or "", body)
        jobs.append({
            "title": title,
            "url": urljoin(base_url, url),
            "location_text": location_from_json_ld(item),
            "work_type": compact_text(item.get("employmentType")),
            "closing_date": closing.date,
            "closing_text": closing.text or compact_text(item.get("validThrough")),
            "advertised_salary_text": salary.text,
            "advertised_salary_min": salary.minimum,
            "advertised_salary_max": salary.maximum,
            "advertised_salary_period": salary.period,
            "classification_status": band.status if band.band else ("salary_only" if salary.text else "unclassified"),
            "band": band.band,
            "evidence": {k: v for k, v in {"band_text": band.evidence_text, "salary_text": salary.evidence_text}.items() if v},
            "description_excerpt": text_excerpt(item.get("description") or body),
            "description_status": "fetched" if item.get("description") else "missing",
            "field_evidence": field_evidence("json_ld", has_document=False),
        })
    return jobs


def parse_endpoint_json_strategy(
    source: dict[str, Any],
    client: Any,
    url: str,
    *,
    name: str,
    hint: dict[str, Any] | None = None,
) -> StrategyResult:
    if not endpoint_url_looks_structured(url):
        return StrategyResult(name=name, jobs=[], status="skipped", message="endpoint hint is not structured JSON")
    fetched = client.get(url)
    if getattr(fetched, "error", None):
        return StrategyResult(name=name, jobs=[], status="failed", message=fetched.error, discovered_endpoints=[hint] if hint else [])
    text = compact_text(fetched.text)
    if not text.startswith(("{", "[")):
        return StrategyResult(name=name, jobs=[], status="skipped", message="endpoint did not return JSON", discovered_endpoints=[hint] if hint else [])
    try:
        payload = json.loads(fetched.text)
    except json.JSONDecodeError as exc:
        return StrategyResult(name=name, jobs=[], status="failed", message=str(exc), discovered_endpoints=[hint] if hint else [])
    items = json_job_like_items(payload)
    jobs = [raw_from_endpoint_item(source, fetched.url, item) for item in items]
    jobs = [job for job in jobs if job.get("title")]
    return StrategyResult(
        name=name,
        jobs=jobs,
        status="ok",
        discovered_endpoints=[hint] if hint else [{"url": fetched.url, "kind": "known_endpoint", "evidence": "source profile"}],
        diagnostics={"endpoint_url": fetched.url, "items": len(items)},
    )


def endpoint_url_looks_structured(url: str) -> bool:
    parsed = urlsplit(url)
    haystack = f"{parsed.path} {parsed.query}".lower()
    if parsed.path.lower().endswith((".js", ".css", ".png", ".jpg", ".svg", ".ico")):
        return False
    return any(token in haystack for token in ("api", "json", "jobs", "job", "vacanc", "position", "recruit", "requisition", "graphql"))


def json_job_like_items(payload: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for value in walk_json(payload):
        if not isinstance(value, dict):
            continue
        keys = {compact_text(key).lower() for key in value.keys()}
        has_title = bool(keys & {"title", "jobtitle", "name", "positiontitle", "job title"})
        has_url = bool(keys & {"url", "uri", "link", "joburl", "externalurl"})
        has_context = bool(keys & {"description", "closingdate", "validthrough", "salary", "compensation", "location", "employmenttype"})
        if has_title and (has_url or has_context):
            items.append(value)
    return items[:120]


def walk_json(value: Any) -> list[Any]:
    found = [value]
    if isinstance(value, dict):
        for child in value.values():
            found.extend(walk_json(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(walk_json(child))
    return found


def raw_from_endpoint_item(source: dict[str, Any], base_url: str, item: dict[str, Any]) -> dict[str, Any]:
    title = first_text(item, "Title", "title", "JobTitle", "jobTitle", "Name", "name", "PositionTitle", "positionTitle")
    url = first_text(item, "Url", "URL", "url", "Link", "link", "JobUrl", "jobUrl", "ExternalUrl", "externalUrl")
    body = json_scalar_text(item)
    band = extract_band(title, body)
    salary = extract_salary(body)
    closing = extract_closing_date(
        first_text(item, "ClosingDate", "closingDate", "Closing", "closing", "validThrough"),
        body,
    )
    return {
        "title": best_job_title(title, fallback_title=title, body=body, council_key=source.get("short_name") or source.get("council_name")),
        "url": urljoin(base_url, url or source.get("url") or base_url),
        "location_text": first_text(item, "Location", "location", "PrimaryLocation", "primaryLocation"),
        "work_type": first_text(item, "WorkType", "workType", "EmploymentType", "employmentType", "JobSchedule", "jobSchedule"),
        "closing_date": closing.date,
        "closing_text": closing.text,
        "advertised_salary_text": salary.text,
        "advertised_salary_min": salary.minimum,
        "advertised_salary_max": salary.maximum,
        "advertised_salary_period": salary.period,
        "classification_status": band.status if band.band else ("salary_only" if salary.text else "unclassified"),
        "band": band.band,
        "evidence": {k: v for k, v in {"band_text": band.evidence_text, "salary_text": salary.evidence_text}.items() if v},
        "description_excerpt": text_excerpt(first_text(item, "Description", "description") or body),
        "description_status": "fetched" if first_text(item, "Description", "description") else "missing",
        "field_evidence": field_evidence("endpoint_json", has_document=False),
    }


def parse_feed_strategy(source: dict[str, Any], client: Any, listing_html: str) -> StrategyResult:
    urls = same_origin_feed_urls(source["url"], listing_html)
    if not urls:
        return StrategyResult(name="sitemap_feed", jobs=[], status="skipped")
    jobs: list[dict[str, Any]] = []
    descriptions: list[dict[str, Any]] = []
    for feed_url in urls[:2]:
        fetched = client.get(feed_url)
        if getattr(fetched, "error", None):
            continue
        soup = BeautifulSoup(fetched.text, "lxml")
        links = candidate_links(soup, fetched.url)[: int(source.get("max_jobs", 40))]
        for title, url in links[: int(source.get("detail_limit", min(len(links), 25)))]:
            detail = client.get(url)
            if getattr(detail, "error", None):
                continue
            raw, description = job_from_detail(source, title, detail.url, BeautifulSoup(detail.text, "lxml"), client=client)
            raw["field_evidence"] = field_evidence("sitemap_feed", has_document=bool((raw.get("evidence") or {}).get("document_urls")))
            jobs.append(raw)
            if description:
                descriptions.append(description)
    return StrategyResult(name="sitemap_feed", jobs=jobs, descriptions=descriptions, diagnostics={"feed_urls": urls})


def choose_strategy(results: list[StrategyResult], profile: dict[str, Any]) -> StrategyResult:
    preferred = compact_text(profile.get("preferred_strategy"))
    scored: list[tuple[int, int, StrategyResult]] = []
    for index, result in enumerate(results):
        accepted, rejected = split_accepted_jobs(result.jobs, {}, result.name, {"reject_title_exact": []}, profile)
        metrics = strategy_metrics(result, accepted, rejected)
        preferred_bonus = 8 if preferred and result.name == preferred else 0
        endpoint_bonus = 5 if result.name in {"known_endpoint", "discovered_endpoint"} and accepted else 0
        scored.append((metrics["score"] + preferred_bonus + endpoint_bonus, -index, result))
    scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
    return scored[0][2]


def merge_strategy_endpoint_hints(results: list[StrategyResult], selected_hints: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for hint in selected_hints:
        url = compact_text(hint.get("url"))
        if url:
            by_url[url.lower()] = dict(hint)
    for result in results:
        for hint in result.discovered_endpoints:
            url = compact_text(hint.get("url"))
            if url and url.lower() not in by_url:
                by_url[url.lower()] = dict(hint)
    return list(by_url.values())[:12]


def split_accepted_jobs(
    jobs: list[dict[str, Any]],
    source: dict[str, Any],
    strategy: str,
    global_rules: dict[str, Any],
    profile: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for job in jobs:
        reason = reject_reason(job, global_rules, profile)
        if reason is None and is_probable_job_raw(job):
            accepted.append(job)
        else:
            row = dict(job)
            row["_reject_reason"] = reason or "weak_vacancy_signal"
            row["_strategy"] = strategy
            row["_source_id"] = source.get("source_id")
            rejected.append(row)
    return accepted, rejected


def reject_reason(job: dict[str, Any], global_rules: dict[str, Any], profile: dict[str, Any]) -> str | None:
    title = compact_text(job.get("title")).lower()
    exact_global = {compact_text(value).lower() for value in global_rules.get("reject_title_exact") or []}
    exact_source = {
        compact_text(value).lower()
        for value in (profile.get("promoted_rules") or {}).get("reject_title_exact") or []
    }
    if title and title in exact_global:
        return "global_reject_title"
    if title and title in exact_source:
        return "source_reject_title"
    if not has_compensation_signal(job):
        return "missing_salary_or_band"
    return None


def strategy_metrics(result: StrategyResult, accepted: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_count = len(result.jobs)
    accepted_count = len(accepted)
    rejected_count = len(rejected)
    duplicate_count = max(0, accepted_count - len({dedupe_key(job) for job in accepted}))
    band_count = sum(1 for job in accepted if has_band(job))
    salary_count = sum(1 for job in accepted if has_salary(job))
    description_count = sum(1 for job in accepted if compact_text(job.get("description_excerpt")))
    closing_count = sum(1 for job in accepted if compact_text(job.get("closing_date") or job.get("closing_text")))
    junk_rate = rejected_count / candidate_count if candidate_count else 0
    duplicate_rate = duplicate_count / accepted_count if accepted_count else 0
    score = (
        accepted_count * 45
        + band_count * 18
        + salary_count * 12
        + description_count * 8
        + closing_count * 5
        - rejected_count * 45
        - duplicate_count * 20
    )
    if result.status == "failed":
        score -= 100
    return {
        "candidate_count": candidate_count,
        "accepted_count": accepted_count,
        "rejected_count": rejected_count,
        "duplicate_count": duplicate_count,
        "band_count": band_count,
        "salary_count": salary_count,
        "description_count": description_count,
        "closing_count": closing_count,
        "junk_rate": round(junk_rate, 4),
        "duplicate_rate": round(duplicate_rate, 4),
        "score": score,
    }


def drift_flags(metrics: dict[str, Any], result: StrategyResult, profile: dict[str, Any]) -> list[dict[str, Any]]:
    flags: list[dict[str, Any]] = []
    accepted = int(metrics["accepted_count"])
    candidates = int(metrics["candidate_count"])
    baseline = int(profile.get("last_good_count") or 0)
    if result.status == "failed":
        flags.append({"code": "strategy_failed", "severity": "severe" if baseline else "moderate", "message": result.message})
    if candidates >= 5 and metrics["junk_rate"] > 0.5:
        flags.append({"code": "junk_inflation", "severity": "severe", "message": f"junk rate {metrics['junk_rate']}"})
    if accepted > SEVERE_SOURCE_JOB_LIMIT:
        flags.append({"code": "source_count_spike", "severity": "severe", "message": f"{accepted} accepted jobs exceeds {SEVERE_SOURCE_JOB_LIMIT}"})
    if baseline >= 3 and accepted > max(SEVERE_SOURCE_JOB_LIMIT, baseline * 4):
        flags.append({"code": "historical_count_spike", "severity": "severe", "message": f"{accepted} accepted jobs vs baseline {baseline}"})
    if baseline >= 5 and accepted == 0 and candidates >= 5:
        flags.append({"code": "historical_count_drop", "severity": "severe", "message": f"0 accepted jobs vs baseline {baseline}"})
    if candidates >= 10 and metrics["duplicate_rate"] > 0.5:
        flags.append({"code": "duplicate_inflation", "severity": "moderate", "message": f"duplicate rate {metrics['duplicate_rate']}"})
    return flags


def update_source_profile(
    profile: dict[str, Any],
    result: StrategyResult,
    metrics: dict[str, Any],
    flags: list[dict[str, Any]],
    *,
    observed_at: str,
    run_id: str,
    accepted: list[dict[str, Any]],
) -> dict[str, Any]:
    severe = any(flag.get("severity") == "severe" for flag in flags)
    updated = dict(profile)
    updated["health"] = "degraded" if severe else "ok"
    updated["failure_streak"] = int(profile.get("failure_streak") or 0) + 1 if severe else 0
    if not severe:
        updated["last_good_count"] = int(metrics["accepted_count"])
        updated["last_good_score"] = int(metrics["score"])
        updated["last_good_run_id"] = run_id
        if metrics["accepted_count"] > 0 and metrics["score"] >= int(profile.get("last_good_score") or -10**9):
            updated["preferred_strategy"] = result.name
    updated["known_endpoints"] = merge_known_endpoints(profile.get("known_endpoints") or [], result, observed_at, metrics)
    updated["quality_history"] = (list(profile.get("quality_history") or []) + [{
        "run_id": run_id,
        "observed_at": observed_at,
        "strategy": result.name,
        "status": updated["health"],
        "accepted_count": metrics["accepted_count"],
        "candidate_count": metrics["candidate_count"],
        "score": metrics["score"],
        "band_count": metrics["band_count"],
        "salary_count": metrics["salary_count"],
        "description_count": metrics["description_count"],
        "drift_flags": [flag.get("code") for flag in flags],
    }])[-20:]
    updated["promoted_rules"] = promote_source_rules(updated.get("promoted_rules") or {}, accepted)
    return updated


def merge_known_endpoints(
    existing: list[dict[str, Any]],
    result: StrategyResult,
    observed_at: str,
    metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    by_url = {compact_text(item.get("url")).lower(): dict(item) for item in existing if item.get("url")}
    endpoint_confirmed = result.name in {"known_endpoint", "discovered_endpoint"} and metrics["accepted_count"] > 0
    for hint in result.discovered_endpoints:
        url = compact_text(hint.get("url"))
        if not url:
            continue
        current = by_url.get(url.lower(), {"url": url, "kind": hint.get("kind"), "first_seen_at": observed_at})
        current["last_seen_at"] = observed_at
        current["last_strategy"] = result.name
        if endpoint_confirmed:
            current["last_score"] = metrics["score"]
            current["last_accepted_count"] = metrics["accepted_count"]
            current["status"] = "accepted"
        else:
            current.setdefault("last_score", 0)
            current.setdefault("last_accepted_count", 0)
            current["status"] = current.get("status") if current.get("status") == "accepted" else "candidate"
        current["evidence"] = hint.get("evidence")
        by_url[url.lower()] = current
    return sorted(by_url.values(), key=lambda item: (item.get("status") != "accepted", item.get("url", "")))[:12]


def promote_source_rules(promoted_rules: dict[str, Any], accepted: list[dict[str, Any]]) -> dict[str, Any]:
    promoted_rules.setdefault("reject_title_exact", [])
    return promoted_rules


def update_global_rules_from_quarantine(profiles: dict[str, Any], quarantine_rows: list[dict[str, Any]]) -> list[str]:
    title_sources: dict[str, set[str]] = {}
    title_platforms: dict[str, set[str]] = {}
    for row in quarantine_rows:
        title = compact_text(row.get("title")).lower()
        if not title or row.get("reason") not in {"weak_vacancy_signal", "quality_rejected"}:
            continue
        title_sources.setdefault(title, set()).add(compact_text(row.get("source_id")))
        title_platforms.setdefault(title, set()).add(compact_text(row.get("platform")))
    existing = set(profiles.setdefault("global_rules", {}).setdefault("reject_title_exact", []))
    added: list[str] = []
    for title, sources in title_sources.items():
        platforms = title_platforms.get(title, set())
        if (len(sources) >= 3 or len(platforms) >= 2) and title not in existing:
            existing.add(title)
            added.append(title)
    profiles["global_rules"]["reject_title_exact"] = sorted(existing)
    return added


def quarantine_rows(
    source: dict[str, Any],
    result: StrategyResult,
    rejected: list[dict[str, Any]],
    *,
    observed_at: str,
    run_id: str,
    reason: str,
    flags: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for job in rejected[:200]:
        rows.append({
            "observed_at": observed_at,
            "run_id": run_id,
            "source_id": source.get("source_id"),
            "council_key": source.get("council_key"),
            "council_name": source.get("council_name"),
            "platform": source.get("platform"),
            "strategy": result.name,
            "reason": job.get("_reject_reason") or reason,
            "title": job.get("title"),
            "url": job.get("url"),
            "evidence": {
                "strategy_status": result.status,
                "strategy_message": result.message,
                "drift_flags": [flag.get("code") for flag in flags],
                "field_evidence": job.get("field_evidence"),
            },
        })
    if reason == "severe_drift" and not rows:
        rows.append({
            "observed_at": observed_at,
            "run_id": run_id,
            "source_id": source.get("source_id"),
            "council_key": source.get("council_key"),
            "council_name": source.get("council_name"),
            "platform": source.get("platform"),
            "strategy": result.name,
            "reason": reason,
            "title": None,
            "url": source.get("url"),
            "evidence": {"drift_flags": flags, "candidate_count": len(result.jobs)},
        })
    return rows


def strategy_summary(result: StrategyResult, global_rules: dict[str, Any], profile: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    accepted, rejected = split_accepted_jobs(result.jobs, source, result.name, global_rules, profile)
    metrics = strategy_metrics(result, accepted, rejected)
    return {
        "strategy": result.name,
        "status": result.status,
        "message": result.message,
        "candidate_count": len(result.jobs),
        "accepted_count": len(accepted),
        "rejected_count": len(rejected),
        "score": metrics["score"],
        "discovered_endpoint_count": len(result.discovered_endpoints),
    }


def annotate_evidence(job: dict[str, Any], strategy: str) -> dict[str, Any]:
    annotated = dict(job)
    has_document = bool((job.get("evidence") or {}).get("document_urls"))
    annotated.setdefault("field_evidence", field_evidence(strategy, has_document=has_document))
    annotated["source_strategy"] = strategy
    return annotated


def field_evidence(strategy: str, *, has_document: bool) -> dict[str, str]:
    if strategy in {"known_endpoint", "discovered_endpoint", "endpoint_json"}:
        base = "endpoint_json"
    elif strategy == "json_ld":
        base = "json_ld"
    elif strategy == "sitemap_feed":
        base = "sitemap_feed"
    elif "oracle_hcm" in strategy or "pulse" in strategy:
        base = "endpoint_json"
    else:
        base = "detail_html"
    description = "pdf_or_docx" if has_document else base
    classification = "pdf_or_docx" if has_document else base
    return {
        "title": base,
        "salary": classification,
        "band": classification,
        "closing_date": classification,
        "description": description,
        "strategy": strategy,
    }


def dedupe_key(job: dict[str, Any]) -> tuple[str, str]:
    return (compact_text(job.get("title")).lower(), compact_text(job.get("url")).lower())


def has_salary(job: dict[str, Any]) -> bool:
    return any(job.get(key) not in ("", None) for key in ("advertised_salary_text", "advertised_salary_min", "advertised_salary_max"))


def has_band(job: dict[str, Any]) -> bool:
    evidence = job.get("evidence") if isinstance(job.get("evidence"), dict) else {}
    for value in (
        job.get("band"),
        job.get("standard_band_number"),
        job.get("classification_band"),
        evidence.get("band_text"),
    ):
        if value in ("", None):
            continue
        try:
            band = int(str(value).strip())
        except (TypeError, ValueError):
            band = 0
        if 1 <= band <= 8:
            return True
        if extract_band(compact_text(value)).band:
            return True
    return False


def has_compensation_signal(job: dict[str, Any]) -> bool:
    return has_salary(job) or has_band(job)


def first_text(item: dict[str, Any], *keys: str) -> str:
    lower_keys = {key.lower(): key for key in item.keys()}
    for key in keys:
        actual = lower_keys.get(key.lower())
        if actual is not None:
            value = item.get(actual)
            if isinstance(value, (str, int, float)):
                return compact_text(value)
    return ""


def json_scalar_text(value: Any) -> str:
    parts: list[str] = []
    if isinstance(value, dict):
        for child in value.values():
            parts.append(json_scalar_text(child))
    elif isinstance(value, list):
        for child in value:
            parts.append(json_scalar_text(child))
    elif isinstance(value, (str, int, float)):
        parts.append(compact_text(value))
    return compact_text(" ".join(parts))


def location_from_json_ld(item: dict[str, Any]) -> str:
    location = item.get("jobLocation") or item.get("applicantLocationRequirements")
    if isinstance(location, dict):
        return json_scalar_text(location)
    if isinstance(location, list):
        return json_scalar_text(location[:2])
    return compact_text(location)


def promote_source_reject_rules_from_quarantine(profile: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(compact_text(row.get("title")).lower() for row in rows if row.get("source_id") == profile.get("source_id"))
    rules = profile.setdefault("promoted_rules", {}).setdefault("reject_title_exact", [])
    existing = {compact_text(value).lower() for value in rules}
    for title, count in counts.items():
        if title and count >= 3 and title not in existing:
            rules.append(title)
            existing.add(title)
    return profile
