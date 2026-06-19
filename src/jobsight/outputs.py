from __future__ import annotations

import json
import shutil
import gzip
import bz2
import re
from collections import Counter, defaultdict
from email.utils import format_datetime
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape
from datetime import datetime, timezone

from jobsight.extractors.band import extract_band
from jobsight.extractors.salary import extract_salary, normalise_salary_fields
from jobsight.extractors.title import clean_role_title
from jobsight.extractors.title import title_from_text_blob as recover_title_from_text_blob
from jobsight.extractors.title import title_from_url as recover_title_from_url
from jobsight.text import compact_text, now_utc, text_excerpt


SALARY_BAND_CONFLICT_STATUS = "salary_band_conflict"
SalaryBandSamples = dict[str, dict[int, list[tuple[float, float, str]]]]

METRO_COUNCIL_KEYS = {
    "BANYULE",
    "BAYSIDE",
    "BOROONDARA",
    "BRIMBANK",
    "CARDINIA",
    "CASEY",
    "DAREBIN",
    "FRANKSTON",
    "GLEN EIRA",
    "GREATER DANDENONG",
    "HOBSONS BAY",
    "HUME",
    "KINGSTON",
    "KNOX",
    "MANNINGHAM",
    "MARIBYRNONG",
    "MAROONDAH",
    "MELBOURNE",
    "MELTON",
    "MERRI BEK",
    "MONASH",
    "MOONEE VALLEY",
    "MOORABOOL",
    "MORNINGTON PENINSULA",
    "NILLUMBIK",
    "PORT PHILLIP",
    "STONNINGTON",
    "WHITEHORSE",
    "WHITTLESEA",
    "WYNDHAM",
    "YARRA",
    "YARRA RANGES",
}

JUNK_TITLES = {
    "breadcrumb",
    "skip to content",
}

NON_JOB_TITLES = {
    "careers",
    "careers at council",
    "careers at yarra ranges",
    "careers with us",
    "breadcrumb",
    "current opportunities",
    "current vacancies",
    "employment",
    "employment opportunities",
    "job vacancies",
    "jobs",
    "jobs and careers",
    "jobs and opportunities",
    "gtranslate",
    "main navigation",
    "menu",
    "listen",
    "about us",
    "acknowledgement of country",
    "alerts",
    "apply for a job",
    "business jobs and investment",
    "careers at yarra ranges",
    "contact us",
    "cookie policy",
    "employment hero work app",
    "facebook",
    "find a local job",
    "help centre",
    "hr login",
    "instagram",
    "linkedin",
    "media centre",
    "newsroom",
    "our organisation",
    "privacy policy",
    "recommended for you",
    "terms and conditions",
    "youtube",
    "positions vacant",
    "work with us",
    "working for council",
}
NON_JOB_TITLE_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bselect this as your preferred language\b",
        r"^javascript is (?:turned off|not available)",
        r"^\d+\s+dismissed announcement$",
        r"^search [0-9,]+ jobs? now$",
        r"^(about|business workshops|business,? jobs|contact|cookie|data processing|employee benefits|employment resources|employment support|financial disclosure|harvest work|implementation hub|integrations|looking to invest|payday super|products|quick demos|sales|service centre|solutions|support for your business|tenders and contracts|trust and legal|upper yarra local|why work for council)\b",
        r"^(canada|malaysia|new zealand|singapore|united kingdom)$",
        r"^employment (hero|shouldn|intelligently)",
        r"^find your next role with employment hero jobs$",
        r"^region of choice$",
        r"^start or grow your business$",
    )
]
NON_JOB_HOST_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"(^|\.)facebook\.com$",
        r"(^|\.)instagram\.com$",
        r"(^|\.)linkedin\.com$",
        r"(^|\.)tiktok\.com$",
        r"(^|\.)youtube\.com$",
        r"^help\.employmenthero\.com$",
        r"^secure\.employmenthero\.com$",
        r"^employmenthero\.page\.link$",
    )
]
ROLE_TITLE_RE = re.compile(
    r"\b("
    r"accountant|administrator|adviser|advisor|analyst|assistant|auditor|business partner|case manager|coordinator|"
    r"crew|developer|educator|engineer|graduate|inspector|labou?rer|lead|leader|librarian|manager|nurse|officer|operator|"
    r"planner|project|ranger|receptionist|specialist|supervisor|surveyor|team leader|technician|trainee|worker"
    r")\b",
    re.IGNORECASE,
)
VACANCY_CONTEXT_RE = re.compile(
    r"\b(applications? close|apply now|closing date|fixed term|full[- ]time|part[- ]time|permanent|position|"
    r"responsibilities|role|salary|selection criteria|superannuation|successful applicant)\b",
    re.IGNORECASE,
)
MAX_PUBLIC_JOBS_PER_COUNCIL_PER_RUN = 50


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def write_descriptions(root: Path, descriptions: list[dict[str, Any]], observed_at: str) -> int:
    count = 0
    for description in descriptions:
        digest = description.get("hash")
        if not digest:
            continue
        item = dict(description)
        item.setdefault("extracted_at", observed_at)
        path = root / "descriptions" / digest[:2] / f"{digest}.json"
        if path.exists():
            continue
        write_json(path, item)
        count += 1
    return count


def load_description_texts(data_root: Path) -> dict[str, str]:
    descriptions: dict[str, str] = {}
    root = data_root / "descriptions"
    if not root.exists():
        return descriptions
    for path in root.glob("*/*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        digest = compact_text(payload.get("hash") or path.stem)
        text = compact_text(payload.get("text"))
        if digest and text:
            descriptions[digest] = text
    return descriptions


def load_jsonl_files(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.exists():
            continue
        try:
            if path.suffix == ".gz":
                text = gzip.decompress(path.read_bytes()).decode("utf-8")
            elif path.suffix == ".bz2":
                text = bz2.decompress(path.read_bytes()).decode("utf-8")
            else:
                text = path.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"Skipping unreadable observation archive {path}: {exc}")
            continue
        for line in text.splitlines():
            if line.strip():
                rows.append(json.loads(line))
    return rows


def load_previous_jobs(data_root: Path) -> list[dict[str, Any]]:
    for filename in ("all-jobs.json", "current-jobs.json"):
        path = data_root / filename
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        jobs = payload.get("jobs")
        if isinstance(jobs, list):
            return [dict(job) for job in jobs if isinstance(job, dict) and job.get("job_id")]
    return []


def dedupe_observations(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("job_id", "")),
            str(row.get("observed_at", "")),
            str(row.get("run_id", "")),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def rebuild_outputs(
    data_root: Path,
    sources: list[dict[str, Any]],
    run_id: str | None = None,
    failed_source_ids: set[str] | None = None,
) -> dict[str, Any]:
    observation_root = data_root / "observations"
    observation_paths = (
        sorted(observation_root.glob("*.jsonl"))
        + sorted(observation_root.glob("*.jsonl.gz"))
        + sorted(observation_root.glob("*.jsonl.bz2"))
    )
    observations = load_jsonl_files(observation_paths)
    has_seed_rows = any(str(row.get("run_id", "")).startswith("seed-") for row in observations)
    if not has_seed_rows:
        observations = dedupe_observations(load_previous_jobs(data_root) + observations)
    latest_row = max(observations, key=lambda row: row.get("observed_at", ""), default={})
    latest_run = run_id or latest_row.get("run_id")
    failed_sources = failed_source_ids or set()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in observations:
        grouped[row["job_id"]].append(row)

    all_jobs = []
    current_candidates = []
    for rows in grouped.values():
        rows.sort(key=lambda item: item.get("observed_at", ""))
        latest = dict(rows[-1])
        latest["first_seen_at"] = rows[0].get("observed_at")
        latest["last_seen_at"] = rows[-1].get("observed_at")
        latest["sighting_count"] = len(rows)
        if latest.get("run_id") == latest_run:
            latest["observed_status"] = "seen_latest_run"
        elif latest.get("source_id") in failed_sources:
            latest["observed_status"] = "source_unavailable_latest_run"
        else:
            latest["observed_status"] = "not_seen_latest_run"
        all_jobs.append(latest)
        if latest["observed_status"] == "seen_latest_run":
            current_candidates.append(latest)

    description_lookup = load_description_texts(data_root)
    current_jobs = normalise_report_jobs(current_candidates, description_lookup)
    report_jobs = normalise_report_jobs(all_jobs, description_lookup)
    all_jobs.sort(key=lambda item: (item.get("council_key", ""), item.get("title", "")))
    current_jobs.sort(key=lambda item: (item.get("council_key", ""), item.get("title", "")))
    report_jobs.sort(key=lambda item: (item.get("council_key", ""), item.get("title", "")))
    summary = build_summary(current_jobs, report_jobs, all_jobs, observations, sources, latest_run)
    write_json(data_root / "current-jobs.json", {"schema_version": "jobsight.current.v1", "summary": summary, "jobs": current_jobs})
    write_json(data_root / "report-jobs.json", {"schema_version": "jobsight.report.v1", "summary": summary, "jobs": report_jobs})
    write_json(data_root / "all-jobs.json", {"schema_version": "jobsight.all.v1", "summary": summary, "jobs": all_jobs})
    board_payload = build_job_board_data(report_jobs, sources, summary, data_root=data_root)
    validate_board_payload(board_payload)
    write_json(data_root / "job-board-data.json", board_payload)
    write_json(data_root / "run-summary.json", summary)
    write_rss(data_root.parent / "jobs.xml", report_jobs, summary)
    return summary


def build_job_board_data(
    jobs: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    summary: dict[str, Any],
    data_root: Path | None = None,
) -> dict[str, Any]:
    description_lookup = load_description_texts(data_root) if data_root else {}
    band_profiles = salary_band_profiles(jobs)
    board_pairs = [(job, board_job(job, description_lookup, band_profiles)) for job in jobs]
    board_pairs = dedupe_board_pairs([(job, row) for job, row in board_pairs if is_likely_board_job(row)])
    public_jobs = [job for job, _ in board_pairs]
    board_jobs = [row for _, row in board_pairs]
    salary_signal_jobs = sum(1 for job in board_jobs if has_salary_signal(job))
    description_jobs = sum(1 for job in board_jobs if compact_text(job.get("description_text")))
    councils_with_jobs = len({job.get("council_key") for job in board_jobs if job.get("council_key")})
    sources_configured = int(summary.get("sources_configured") or len(sources) or 0)
    coverage_rate = round((councils_with_jobs / sources_configured) * 100) if sources_configured else 0
    return {
        "schema_version": "jobsight.board.v1",
        "saved_at": summary.get("generated_at"),
        "as_of_date": summary.get("generated_at"),
        "summary": {
            **summary,
            "report_jobs": len(board_jobs),
            "salary_signal_jobs": salary_signal_jobs,
            "description_excerpt_jobs": description_jobs,
            "coverage": {
                "covered_councils": councils_with_jobs,
                "configured_sources": sources_configured,
                "coverage_rate": coverage_rate,
            },
        },
        "councils": board_councils(sources),
        "jobs": board_jobs,
        "visuals": {
            "evidence_flow": [
                {
                    "label": "Sources",
                    "value": sources_configured,
                    "detail": "public council boards",
                },
                {
                    "label": "Observed",
                    "value": len(board_jobs),
                    "detail": "quality-approved report jobs",
                },
                {
                    "label": "Salary",
                    "value": salary_signal_jobs,
                    "detail": "advertised salary signal",
                },
                {
                    "label": "Descriptions",
                    "value": description_jobs,
                    "detail": "excerpt captured",
                },
            ],
            "month_counts": month_counts(public_jobs),
            "source_counts": dict(sorted(Counter(job.get("source_family", "unknown") for job in board_jobs).items())),
        },
    }


def normalise_report_jobs(jobs: list[dict[str, Any]], description_lookup: dict[str, str]) -> list[dict[str, Any]]:
    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    band_profiles = salary_band_profiles(jobs)
    for job in jobs:
        board_row = board_job(job, description_lookup, band_profiles)
        if not is_likely_board_job(board_row):
            continue
        item = dict(job)
        item.update({
            "title": board_row.get("job_title"),
            "url": board_row.get("job_url"),
            "classification_status": board_row.get("classification_status"),
            "band": board_row.get("standard_band_number"),
            "advertised_salary_text": board_row.get("advertised_salary_text"),
            "advertised_salary_min": board_row.get("advertised_salary_min"),
            "advertised_salary_max": board_row.get("advertised_salary_max"),
            "advertised_salary_period": board_row.get("advertised_salary_period"),
            "location_text": board_row.get("location_text"),
            "work_type": board_row.get("work_type"),
            "closing_date": board_row.get("closing_at"),
            "closing_text": board_row.get("closing_at_text"),
            "description_excerpt": board_row.get("description_text"),
            "description_status": board_row.get("description_status"),
            "field_evidence": board_row.get("field_evidence"),
            "source_strategy": board_row.get("source_strategy"),
        })
        pairs.append((drop_empty(item), board_row))
    return [job for job, _ in dedupe_board_pairs(pairs)]


def validate_board_payload(payload: dict[str, Any]) -> None:
    if payload.get("schema_version") != "jobsight.board.v1":
        raise ValueError("Public board payload has an invalid schema version")
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise ValueError("Public board payload is missing a jobs array")
    counts_by_council_and_run = Counter()
    bad_rows: list[str] = []
    bad_compensation_rows: list[str] = []
    bad_evidence_rows: list[str] = []
    for index, job in enumerate(jobs):
        if not isinstance(job, dict):
            bad_rows.append(f"row {index}: not an object")
            continue
        title = compact_text(job.get("job_title"))
        url = compact_text(job.get("job_url"))
        council_key = compact_text(job.get("council_key"))
        if not title or not url or not council_key:
            bad_rows.append(f"row {index}: missing title, url, or council")
            continue
        if not has_compensation_signal(job):
            bad_compensation_rows.append(f"{council_key}: {title}")
            continue
        if is_generic_job_shell(job) or is_non_job_url(url) or not strong_vacancy_signal(job):
            bad_rows.append(f"{council_key}: {title}")
        evidence_errors = field_evidence_errors(job)
        if evidence_errors:
            bad_evidence_rows.append(f"{council_key}: {title} missing {', '.join(evidence_errors)}")
        run_id = compact_text(job.get("last_seen_run_id") or job.get("run_id") or job.get("latest_run_id"))
        counts_by_council_and_run[(council_key, run_id)] += 1
    if bad_compensation_rows:
        sample = "; ".join(bad_compensation_rows[:10])
        raise ValueError(f"Public board compensation gate rejected {len(bad_compensation_rows)} row(s): {sample}")
    if bad_rows:
        sample = "; ".join(bad_rows[:10])
        raise ValueError(f"Public board quality gate rejected {len(bad_rows)} row(s): {sample}")
    if bad_evidence_rows:
        sample = "; ".join(bad_evidence_rows[:10])
        raise ValueError(f"Public board evidence gate rejected {len(bad_evidence_rows)} row(s): {sample}")
    outliers = {
        key: count
        for key, count in counts_by_council_and_run.items()
        if count > MAX_PUBLIC_JOBS_PER_COUNCIL_PER_RUN
    }
    if outliers:
        detail = ", ".join(
            f"{council}/{run_id or 'unknown-run'}={count}"
            for (council, run_id), count in sorted(outliers.items())
        )
        raise ValueError(f"Public board quality gate rejected per-run council count outlier(s): {detail}")


def field_evidence_errors(job: dict[str, Any]) -> list[str]:
    evidence = job.get("field_evidence")
    if not isinstance(evidence, dict):
        return ["field_evidence"]
    required = {"strategy", "title"}
    if has_salary_signal(job):
        required.add("salary")
    if job.get("standard_band_number") or compact_text(job.get("classification_band")).lower().startswith("band "):
        required.add("band")
    if job.get("closing_at") or job.get("closing_at_text"):
        required.add("closing_date")
    if job.get("description_text") or job.get("description_status"):
        required.add("description")
    return sorted(field for field in required if not compact_text(evidence.get(field)))


def board_councils(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for source in sources:
        key = compact_text(source.get("council_key") or source.get("short_name") or source.get("council_name")).upper()
        if not key or key in rows:
            continue
        rows[key] = drop_empty({
            "short_name": source.get("short_name"),
            "long_name": source.get("council_name"),
            "official_name": source.get("council_name"),
            "spatial_key": key,
            "map_join_key": key,
            "source_url": source.get("url"),
            "entry_url": source.get("entry_url"),
            "source_platform": source.get("platform"),
            "council_category": "Metropolitan" if key in METRO_COUNCIL_KEYS else "Regional",
        })
    return sorted(rows.values(), key=lambda row: row.get("short_name", ""))


def board_job(
    job: dict[str, Any],
    description_lookup: dict[str, str] | None = None,
    band_profiles: SalaryBandSamples | None = None,
) -> dict[str, Any]:
    band_conflict = (
        compact_text(job.get("classification_status")) == SALARY_BAND_CONFLICT_STATUS
        or salary_band_conflict(job, band_profiles or {})
    )
    band = None if band_conflict else resolve_band(job, description_lookup or {})
    classification_status = SALARY_BAND_CONFLICT_STATUS if band_conflict else board_classification_status(job, band)
    observed_month = compact_text(job.get("observed_at"))[:7]
    salary = board_salary_fields(job)
    source_strategy = public_strategy_label(job)
    row = {
        "job_uid": job.get("job_id"),
        "dedupe_key": job.get("job_id"),
        "job_title": board_title(job),
        "job_url": job.get("url"),
        "external_links": [job.get("url")] if job.get("url") else [],
        "council_key": job.get("council_key"),
        "council_name": job.get("council_name"),
        "short_name": job.get("short_name"),
        "source_family": job.get("source_platform"),
        "source_name": job.get("source_id") or job.get("source_platform"),
        "source_url": job.get("source_url"),
        "source_strategy": source_strategy,
        "board_status": "current" if job.get("observed_status") == "seen_latest_run" else "historical",
        "public_board_signal": True,
        "classification_status": classification_status,
        "classification_band": f"Band {band}" if band else classification_status,
        "standard_band_number": band,
        "advertised_salary_text": salary["text"],
        "advertised_salary_min": salary["minimum"],
        "advertised_salary_max": salary["maximum"],
        "advertised_salary_period": salary["period"],
        "annual_salary_min": salary["minimum"] if salary["period"] == "year" else None,
        "annual_salary_max": salary["maximum"] if salary["period"] == "year" else None,
        "annual_salary_source": "advertised" if salary["period"] == "year" else None,
        "location_text": job.get("location_text"),
        "work_type": job.get("work_type"),
        "closing_at": job.get("closing_date"),
        "closing_at_text": text_excerpt(job.get("closing_text"), 140),
        "description_text": text_excerpt(job.get("description_excerpt"), 300),
        "description_status": job.get("description_status"),
        "canonical_reference_month": observed_month,
        "first_seen_at": job.get("first_seen_at"),
        "last_seen_at": job.get("last_seen_at"),
        "last_seen_run_id": job.get("run_id"),
        "sighting_count": job.get("sighting_count"),
        "observed_status": job.get("observed_status"),
    }
    row["field_evidence"] = public_field_evidence(job, row, source_strategy)
    return drop_empty(row)


def public_strategy_label(job: dict[str, Any]) -> str:
    strategy = compact_text(job.get("source_strategy"))
    if strategy:
        return strategy
    platform = compact_text(job.get("source_platform") or job.get("source_family"))
    if platform:
        return f"historical:platform:{platform}"
    return "historical:deterministic_rule"


def public_field_evidence(job: dict[str, Any], row: dict[str, Any], strategy: str) -> dict[str, str]:
    existing = job.get("field_evidence") if isinstance(job.get("field_evidence"), dict) else {}
    evidence = {compact_text(key): compact_text(value) for key, value in existing.items() if compact_text(key) and compact_text(value)}
    base = public_evidence_source(job, strategy)
    document_source = public_document_evidence_source(job, row)

    if row.get("job_title"):
        evidence.setdefault("title", base)
    if has_salary_signal(row):
        evidence.setdefault("salary", document_source or base)
    if row.get("standard_band_number") or compact_text(row.get("classification_band")).lower().startswith("band "):
        evidence.setdefault("band", document_source or base)
    if row.get("closing_at") or row.get("closing_at_text"):
        evidence.setdefault("closing_date", document_source or base)
    if row.get("description_text") or row.get("description_status"):
        evidence.setdefault("description", document_source or base)

    evidence.setdefault("strategy", strategy or "historical:deterministic_rule")
    return evidence


def public_evidence_source(job: dict[str, Any], strategy: str) -> str:
    strategy_text = compact_text(strategy).lower()
    platform = compact_text(job.get("source_platform") or job.get("source_family")).lower()
    if any(token in strategy_text for token in ("known_endpoint", "discovered_endpoint", "endpoint_json")):
        return "endpoint_json"
    if "json_ld" in strategy_text:
        return "json_ld"
    if "sitemap" in strategy_text or "feed" in strategy_text:
        return "sitemap_feed"
    if "pulse" in strategy_text or "oracle_hcm" in strategy_text or platform in {"pulse", "oracle_hcm"}:
        return "endpoint_json"
    if strategy_text.startswith(("platform:", "historical:platform:")) or platform:
        return "detail_html"
    return "deterministic_rule"


def public_document_evidence_source(job: dict[str, Any], row: dict[str, Any]) -> str:
    raw_evidence = job.get("evidence") if isinstance(job.get("evidence"), dict) else {}
    if raw_evidence.get("document_urls") or compact_text(job.get("description_status") or row.get("description_status")) == "document":
        return "pdf_or_docx"
    return ""


def title_key(value: Any) -> str:
    text = compact_text(value).lower().replace("&", " and ")
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def salary_band_profiles(jobs: list[dict[str, Any]]) -> SalaryBandSamples:
    profiles: SalaryBandSamples = defaultdict(lambda: defaultdict(list))
    for job in jobs:
        band = existing_band_number(job)
        interval = annual_salary_interval(job)
        council_key = compact_text(job.get("council_key")).upper()
        if not band or not interval or not council_key:
            continue
        profiles[council_key][band].append((interval[0], interval[1], salary_profile_identity(job)))
    return {council: dict(bands) for council, bands in profiles.items()}


def salary_profile_identity(job: dict[str, Any]) -> str:
    url = compact_text(job.get("job_url") or job.get("url"))
    if url:
        return (
            url.split("#", 1)[0]
            .rstrip("/")
            .lower()
            .removeprefix("https://www.")
            .removeprefix("http://www.")
            .removeprefix("https://")
            .removeprefix("http://")
        )
    return compact_text(job.get("job_id"))


def existing_band_number(job: dict[str, Any]) -> int | None:
    for value in (job.get("standard_band_number"), job.get("band"), job.get("classification_band")):
        parsed = parse_band(value)
        if parsed:
            return parsed
        result = extract_band(compact_text(value))
        if result.band:
            return result.band
    return None


def annual_salary_interval(job: dict[str, Any]) -> tuple[float, float] | None:
    salary = normalise_salary_fields(
        job.get("advertised_salary_text"),
        job.get("advertised_salary_min"),
        job.get("advertised_salary_max"),
        job.get("advertised_salary_period"),
    )
    if salary.period != "year" or not isinstance(salary.minimum, (int, float)):
        return None
    high_value = salary.maximum if isinstance(salary.maximum, (int, float)) else salary.minimum
    low = min(float(salary.minimum), float(high_value))
    high = max(float(salary.minimum), float(high_value))
    if high < 30000 or low <= 0:
        return None
    return low, high


def salary_band_conflict(job: dict[str, Any], profiles: SalaryBandSamples) -> bool:
    declared_band = existing_band_number(job)
    interval = annual_salary_interval(job)
    council_key = compact_text(job.get("council_key")).upper()
    if not declared_band or not interval or not council_key:
        return False
    if band_and_salary_are_coupled(job, declared_band):
        return False
    council_profiles = profiles.get(council_key) or {}
    declared_profile = peer_salary_span(council_profiles.get(declared_band, []), job)
    if not declared_profile:
        return False
    if intervals_overlap(interval, declared_profile):
        return False
    interval_width = max(1.0, interval[1] - interval[0])
    midpoint = (interval[0] + interval[1]) / 2
    for band, samples in council_profiles.items():
        if band == declared_band:
            continue
        profile = peer_salary_span(samples, job)
        if not profile:
            continue
        overlap_ratio = interval_overlap_length(interval, profile) / interval_width
        if overlap_ratio >= 0.5 or profile[0] <= midpoint <= profile[1]:
            return True
    return False


def band_and_salary_are_coupled(job: dict[str, Any], band: int) -> bool:
    band_re = re.compile(rf"\bband\s*{band}[a-d]?\b", re.IGNORECASE)
    for key in ("advertised_salary_text", "description_excerpt", "description_text"):
        text = compact_text(job.get(key))
        if not text:
            continue
        for match in band_re.finditer(text):
            window = text[max(0, match.start() - 160): match.end() + 220]
            if extract_salary(window).minimum is not None:
                return True
    return False


def peer_salary_span(samples: list[tuple[float, float, str]], job: dict[str, Any]) -> tuple[float, float] | None:
    identity = salary_profile_identity(job)
    peers = [(low, high) for low, high, sample_id in samples if sample_id != identity]
    if not peers:
        return None
    return min(low for low, _ in peers), max(high for _, high in peers)


def intervals_overlap(first: tuple[float, float], second: tuple[float, float]) -> bool:
    return interval_overlap_length(first, second) > 0


def interval_overlap_length(first: tuple[float, float], second: tuple[float, float]) -> float:
    return max(0.0, min(first[1], second[1]) - max(first[0], second[0]))


def resolve_band(job: dict[str, Any], description_lookup: dict[str, str] | None = None) -> int | None:
    if compact_text(job.get("classification_status")) == SALARY_BAND_CONFLICT_STATUS:
        return None
    existing = existing_band_number(job)
    if existing:
        return existing
    evidence = job.get("evidence") if isinstance(job.get("evidence"), dict) else {}
    description_text = ""
    if description_lookup:
        description_text = description_lookup.get(compact_text(job.get("description_hash")), "")
    result = extract_band(
        job.get("job_title") or job.get("title"),
        job.get("classification_band"),
        job.get("classification_status"),
        evidence.get("band_text"),
        job.get("advertised_salary_text"),
        job.get("description_excerpt") or job.get("description_text"),
        description_text,
        job.get("closing_text") or job.get("closing_at_text"),
        job.get("location_text"),
        job.get("work_type"),
    )
    return result.band


def board_classification_status(job: dict[str, Any], band: int | None) -> str:
    status = compact_text(job.get("classification_status")) or "unclassified"
    if band and status in {"", "unclassified", "description_only", "salary_only", "parse_warning"}:
        return "explicit_band"
    return status


def has_vacancy_signal(job: dict[str, Any]) -> bool:
    has_band = has_band_signal(job)
    has_salary = has_salary_signal(job) or bool(salary_range_label(job))
    has_detail = any(
        compact_text(job.get(key))
        for key in ("location_text", "work_type", "closing_at", "closing_at_text", "closing_date", "closing_text", "description_text")
    )
    return has_band or has_salary or has_detail


def role_title_signal(job: dict[str, Any]) -> bool:
    return bool(ROLE_TITLE_RE.search(compact_text(job.get("job_title") or job.get("title"))))


def vacancy_context_signal(job: dict[str, Any]) -> bool:
    text = " ".join(
        compact_text(job.get(key))
        for key in ("description_text", "description_excerpt", "closing_at_text", "closing_text", "work_type")
    )
    return bool(VACANCY_CONTEXT_RE.search(text))


def strong_vacancy_signal(job: dict[str, Any]) -> bool:
    if has_band_signal(job):
        return True
    has_salary = has_salary_signal(job) or bool(salary_range_label(job))
    has_closing = bool(compact_text(job.get("closing_at") or job.get("closing_at_text") or job.get("closing_date") or job.get("closing_text")))
    has_location_or_work = bool(compact_text(job.get("location_text")) or compact_text(job.get("work_type")))
    has_role = role_title_signal(job)
    has_context = vacancy_context_signal(job)
    return bool(has_role and (has_salary or has_closing or has_location_or_work or has_context))


def is_generic_job_shell(job: dict[str, Any]) -> bool:
    title = title_key(job.get("job_title") or job.get("title"))
    if not title:
        return True
    if title in NON_JOB_TITLES:
        return True
    if any(pattern.search(title) for pattern in NON_JOB_TITLE_PATTERNS):
        return True
    if re.match(r"^careers?( at| with|$)", title):
        return True
    if re.match(r"^(current )?(job )?vacanc(y|ies)$", title):
        return True
    if re.search(r"jobs? (and|or) (careers|opportunities)", title):
        return True
    return False


def is_non_job_url(url: Any) -> bool:
    text = compact_text(url)
    if not text:
        return False
    rest = text.split("://", 1)[-1]
    host, _, path = rest.partition("/")
    host = host.lower().removeprefix("www.")
    if host in {"au.jora.com", "jora.com"} and path.strip("/") == "":
        return True
    return any(pattern.search(host) for pattern in NON_JOB_HOST_PATTERNS)


def is_likely_board_job(job: dict[str, Any]) -> bool:
    if not compact_text(job.get("job_title")) or not job.get("job_url") or not job.get("council_key"):
        return False
    if is_generic_job_shell(job):
        return False
    if is_non_job_url(job.get("job_url")):
        return False
    if not has_compensation_signal(job):
        return False
    status = compact_text(job.get("classification_status")).lower()
    if status == "parser warning":
        status = "parse_warning"
    if status in {"parse_warning", "unclassified", "description_only"} and not strong_vacancy_signal(job):
        return False
    if not strong_vacancy_signal(job) and not role_title_signal(job):
        return False
    return True


def canonical_url_path(url: Any) -> str:
    text = compact_text(url)
    if not text:
        return ""
    without_query = text.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    return without_query.lower().removeprefix("https://www.").removeprefix("http://www.").removeprefix("https://").removeprefix("http://")


def vacancy_dedupe_key(job: dict[str, Any]) -> tuple[str, ...]:
    return (
        compact_text(job.get("council_key")).upper(),
        title_key(job.get("job_title")),
        canonical_url_path(job.get("job_url")),
        compact_text(job.get("location_text")).lower(),
        compact_text(job.get("work_type")).lower(),
        compact_text(job.get("closing_at") or job.get("closing_at_text")).lower(),
        compact_text(salary_range_label(job) or job.get("advertised_salary_text")).lower(),
    )


def vacancy_title_key(job: dict[str, Any]) -> tuple[str, ...]:
    return (
        compact_text(job.get("council_key")).upper(),
        title_key(job.get("job_title")),
    )


def title_words_for_similarity(value: Any) -> set[str]:
    return {
        word
        for word in re.split(r"[^a-z0-9]+", title_key(value))
        if word
        and word
        not in {
            "and",
            "the",
            "of",
            "at",
            "in",
            "for",
            "with",
            "jan",
            "january",
            "feb",
            "february",
            "mar",
            "march",
            "apr",
            "april",
            "may",
            "jun",
            "june",
            "jul",
            "july",
            "aug",
            "august",
            "sep",
            "sept",
            "september",
            "oct",
            "october",
            "nov",
            "november",
            "dec",
            "december",
        }
    }


def likely_same_listing_title(first: Any, second: Any) -> bool:
    first_words = title_words_for_similarity(first)
    second_words = title_words_for_similarity(second)
    if not first_words or not second_words:
        return False
    overlap = first_words & second_words
    if first_words <= second_words or second_words <= first_words:
        return True
    return len(overlap) / min(len(first_words), len(second_words)) >= 0.8


def url_host(job: dict[str, Any]) -> str:
    return canonical_url_path(job.get("job_url")).split("/", 1)[0]


def is_mirror_listing_url(job: dict[str, Any]) -> bool:
    host = url_host(job)
    return host in {"councildirect.com.au", "au.jora.com", "jora.com"} or any(
        host.endswith(domain) for domain in ("indeed.com", "seek.com.au")
    )


def salary_signature(job: dict[str, Any]) -> str:
    return compact_text(salary_range_label(job) or job.get("advertised_salary_text")).lower()


def matching_detail_signal(first: dict[str, Any], second: dict[str, Any]) -> bool:
    for key in ("work_type", "location_text", "closing_at", "closing_at_text"):
        first_value = compact_text(first.get(key)).lower()
        second_value = compact_text(second.get(key)).lower()
        if first_value and second_value and first_value == second_value:
            return True
    first_salary = salary_signature(first)
    second_salary = salary_signature(second)
    return bool(first_salary and second_salary and first_salary == second_salary)


def weak_mirror_duplicate_signal(first: dict[str, Any], second: dict[str, Any]) -> bool:
    mirror = first if is_mirror_listing_url(first) else second if is_mirror_listing_url(second) else None
    other = second if mirror is first else first if mirror is second else None
    if not mirror or not other:
        return False
    mirror_has_hard_signal = bool(
        parse_band(mirror.get("standard_band_number"))
        or salary_signature(mirror)
        or compact_text(mirror.get("closing_at") or mirror.get("closing_at_text"))
        or compact_text(mirror.get("work_type"))
    )
    other_has_hard_signal = bool(
        parse_band(other.get("standard_band_number"))
        or salary_signature(other)
        or compact_text(other.get("closing_at") or other.get("closing_at_text"))
        or compact_text(other.get("work_type") or other.get("location_text"))
    )
    return not mirror_has_hard_signal and other_has_hard_signal


def likely_same_mirrored_listing(first: dict[str, Any], second: dict[str, Any]) -> bool:
    if compact_text(first.get("council_key")).upper() != compact_text(second.get("council_key")).upper():
        return False
    if not likely_same_listing_title(first.get("job_title"), second.get("job_title")):
        return False
    if canonical_url_path(first.get("job_url")) == canonical_url_path(second.get("job_url")):
        return True
    first_host = url_host(first)
    second_host = url_host(second)
    if first_host and first_host == second_host and matching_detail_signal(first, second):
        return True
    if not (is_mirror_listing_url(first) or is_mirror_listing_url(second)):
        return False
    return matching_detail_signal(first, second) or weak_mirror_duplicate_signal(first, second)


def board_quality_score(job: dict[str, Any]) -> int:
    score = 0
    if parse_band(job.get("standard_band_number")):
        score += 40
    if compact_text(job.get("classification_status")) == "explicit_band":
        score += 30
    if has_salary_signal(job):
        score += 20
    if compact_text(job.get("description_text")):
        score += 10
    if job.get("observed_status") == "seen_latest_run":
        score += 5
    host = canonical_url_path(job.get("job_url")).split("/", 1)[0]
    if host.endswith(".vic.gov.au") or "pageuppeople.com" in host or "pulsesoftware.com" in host:
        score += 3
    if any(host.endswith(domain) for domain in ("jora.com", "indeed.com", "seek.com.au")):
        score -= 20
    return score


def dedupe_board_pairs(pairs: list[tuple[dict[str, Any], dict[str, Any]]]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    best_by_exact: dict[tuple[str, ...], tuple[dict[str, Any], dict[str, Any]]] = {}
    for raw_job, board_row in pairs:
        key = vacancy_dedupe_key(board_row)
        current = best_by_exact.get(key)
        if current is None or board_quality_score(board_row) > board_quality_score(current[1]):
            best_by_exact[key] = (raw_job, board_row)
    best_by_url: dict[tuple[str, str], list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for raw_job, board_row in best_by_exact.values():
        url_key = (compact_text(board_row.get("council_key")).upper(), canonical_url_path(board_row.get("job_url")))
        url_group = best_by_url.setdefault(url_key, [])
        for index, current in enumerate(url_group):
            if likely_same_listing_title(board_row.get("job_title"), current[1].get("job_title")):
                if board_quality_score(board_row) > board_quality_score(current[1]):
                    url_group[index] = (raw_job, board_row)
                break
        else:
            url_group.append((raw_job, board_row))
    deduped: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for group in best_by_url.values():
        for raw_job, board_row in group:
            for index, current in enumerate(deduped):
                if likely_same_mirrored_listing(board_row, current[1]):
                    if board_quality_score(board_row) > board_quality_score(current[1]):
                        deduped[index] = (raw_job, board_row)
                    break
            else:
                deduped.append((raw_job, board_row))
    return deduped


def drop_empty(row: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if value not in ("", None, [], {})}


def board_title(job: dict[str, Any]) -> str:
    title = clean_role_title(job.get("title"), job.get("council_key"))
    raw_salary_text = compact_text(job.get("advertised_salary_text"))
    if title.lower() in JUNK_TITLES or is_generic_job_shell({"job_title": title}):
        candidate = recover_title_from_text_blob(job.get("description_excerpt") or raw_salary_text)
        if candidate:
            return candidate
        candidate = recover_title_from_url(job.get("url"), job.get("council_key"))
        if candidate:
            return candidate
    return text_excerpt(title, 140)


def board_salary_text(job: dict[str, Any]) -> str:
    text = compact_text(job.get("advertised_salary_text"))
    if len(text) > 360:
        return salary_range_label(job)
    return text_excerpt(text, 300)


def board_salary_fields(job: dict[str, Any]) -> dict[str, Any]:
    salary = normalise_salary_fields(
        board_salary_text(job),
        job.get("advertised_salary_min"),
        job.get("advertised_salary_max"),
        job.get("advertised_salary_period"),
    )
    return {"text": salary.text, "minimum": salary.minimum, "maximum": salary.maximum, "period": salary.period}


def salary_range_label(job: dict[str, Any]) -> str:
    minimum = job.get("advertised_salary_min")
    maximum = job.get("advertised_salary_max")
    period = compact_text(job.get("advertised_salary_period"))
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


def parse_band(value: Any) -> int | None:
    try:
        band = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return band if 1 <= band <= 8 else None


def has_band_signal(job: dict[str, Any]) -> bool:
    for key in ("standard_band_number", "band", "classification_band"):
        value = job.get(key)
        if value in ("", None):
            continue
        if parse_band(value):
            return True
        if extract_band(compact_text(value)).band:
            return True
    return False


def has_salary_signal(job: dict[str, Any]) -> bool:
    return any(job.get(key) not in ("", None) for key in ("advertised_salary_text", "advertised_salary_min", "advertised_salary_max"))


def has_compensation_signal(job: dict[str, Any]) -> bool:
    return has_band_signal(job) or has_salary_signal(job) or bool(salary_range_label(job))


def month_counts(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(compact_text(job.get("observed_at"))[:7] or "unknown" for job in jobs)
    return [{"month": month, "count": count} for month, count in sorted(counts.items())]


def build_summary(
    current_jobs: list[dict[str, Any]],
    report_jobs: list[dict[str, Any]],
    all_jobs: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    sources: list[dict[str, Any]],
    latest_run: str | None,
) -> dict[str, Any]:
    status_counts = Counter(job.get("classification_status", "unclassified") for job in report_jobs)
    platform_counts = Counter(job.get("source_platform", "unknown") for job in report_jobs)
    current_status_counts = Counter(job.get("classification_status", "unclassified") for job in current_jobs)
    current_platform_counts = Counter(job.get("source_platform", "unknown") for job in current_jobs)
    return {
        "schema_version": "jobsight.run_summary.v1",
        "generated_at": now_utc(),
        "latest_run_id": latest_run,
        "sources_configured": len(sources),
        "current_jobs": len(current_jobs),
        "report_jobs": len(report_jobs),
        "all_jobs": len(all_jobs),
        "observations": len(observations),
        "councils_with_current_jobs": len({job.get("council_key") for job in current_jobs if job.get("council_key")}),
        "councils_with_report_jobs": len({job.get("council_key") for job in report_jobs if job.get("council_key")}),
        "classification_status_counts": dict(sorted(status_counts.items())),
        "source_platform_counts": dict(sorted(platform_counts.items())),
        "current_classification_status_counts": dict(sorted(current_status_counts.items())),
        "current_source_platform_counts": dict(sorted(current_platform_counts.items())),
    }


def write_rss(path: Path, jobs: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    items = []
    for job in jobs[:250]:
        parts = [
            f"<p><strong>Council:</strong> {job.get('council_name', '')}</p>",
            f"<p><strong>Status:</strong> {job.get('classification_status', 'unclassified')}</p>",
        ]
        if job.get("band"):
            parts.append(f"<p><strong>Band:</strong> {job['band']}</p>")
        if job.get("advertised_salary_text"):
            parts.append(f"<p><strong>Salary:</strong> {job['advertised_salary_text']}</p>")
        if job.get("closing_text") or job.get("closing_date"):
            parts.append(f"<p><strong>Closes:</strong> {job.get('closing_text') or job.get('closing_date')}</p>")
        items.append(
            "    <item>\n"
            f"      <guid>{escape(job.get('job_id') or job.get('url') or '')}</guid>\n"
            f"      <title>{escape(job.get('title', 'Untitled role'))} - {escape(job.get('short_name', job.get('council_name', 'Council')))}</title>\n"
            f"      <link>{escape(job.get('url') or '')}</link>\n"
            f"      <description><![CDATA[{''.join(parts)}]]></description>\n"
            f"      <pubDate>{escape(_rss_date(job.get('observed_at') or summary['generated_at']))}</pubDate>\n"
            "    </item>"
        )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n'
        '  <channel>\n'
        '    <title>JobSight: Victorian Council Jobs</title>\n'
        '    <link>https://github.com/bandsight/jobsight</link>\n'
        '    <description>Deterministic Victorian council job observations.</description>\n'
        '    <language>en</language>\n'
        f"{chr(10).join(items)}\n"
        '  </channel>\n'
        '</rss>\n'
    )
    path.write_text(xml, encoding="utf-8")


def _rss_date(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = datetime.now(timezone.utc)
    return format_datetime(parsed)


def build_public_dir(root: Path, data_root: Path, site_root: Path, public_root: Path) -> None:
    if public_root.exists():
        shutil.rmtree(public_root)
    public_root.mkdir(parents=True, exist_ok=True)
    if site_root.exists():
        shutil.copytree(site_root, public_root, dirs_exist_ok=True)
    for pattern in (
        "index.html",
        "app.js",
        "styles.css",
        "enhance.css",
        "map-interactions.js",
        "map-data-*.js",
    ):
        for asset in root.glob(pattern):
            shutil.copy2(asset, public_root / asset.name)
    shutil.copytree(data_root, public_root / "data", dirs_exist_ok=True)
    board_jobs = data_root / "job-board-data.json"
    if not board_jobs.exists():
        raise FileNotFoundError("Public build requires data/job-board-data.json")
    if board_jobs.exists():
        board_text = board_jobs.read_text(encoding="utf-8")
        (public_root / "embedded-data.js").write_text(f"window.JOBSIGHT_BOARD_DATA = {board_text};\n", encoding="utf-8")
        index_path = public_root / "index.html"
        if index_path.exists():
            index_text = index_path.read_text(encoding="utf-8")
            safe_json = board_text.replace("</", "<\\/")
            index_text = index_text.replace(
                '<script id="jobsight-board-data" type="application/json"></script>',
                f'<script id="jobsight-board-data" type="application/json">{safe_json}</script>',
            )
            index_path.write_text(index_text, encoding="utf-8")
    jobs_xml = root / "jobs.xml"
    if jobs_xml.exists():
        shutil.copy2(jobs_xml, public_root / "jobs.xml")
