from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup

from jobsight.extractors.band import extract_band
from jobsight.extractors.closing_date import extract_closing_date
from jobsight.extractors.salary import extract_salary
from jobsight.extractors.title import best_job_title
from jobsight.http import HttpClient
from jobsight.platforms.generic import candidate_links, description_payload, job_from_detail
from jobsight.text import compact_text, stable_hash, text_excerpt


def parse_pulse(source: dict[str, Any], client: HttpClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fetched = client.get(source["url"])
    if fetched.error:
        return [], [{"source_id": source.get("source_id"), "url": source["url"], "status": "failed", "message": fetched.error}]
    text = fetched.text.strip()
    if text.startswith("{"):
        return _parse_json(source, text, client)
    api = client.get(_pulse_jobs_api_url(fetched.url))
    if not api.error and api.text.strip().startswith("{"):
        return _parse_json(source, api.text, client)
    soup = BeautifulSoup(fetched.text, "lxml")
    links = candidate_links(soup, fetched.url)[: source.get("max_jobs", 60)]
    detail_limit = int(source.get("detail_limit", min(len(links), int(source.get("max_jobs", 60)), 40)))
    jobs: list[dict[str, Any]] = []
    descriptions: list[dict[str, Any]] = []
    for index, (title, url) in enumerate(links):
        if index >= detail_limit:
            jobs.append({
                "title": title,
                "url": url,
                "classification_status": "parse_warning",
                "description_status": "detail_skipped",
            })
            continue
        detail = client.get(url)
        if detail.error:
            continue
        raw, description = job_from_detail(source, title, detail.url, BeautifulSoup(detail.text, "lxml"))
        jobs.append(raw)
        if description:
            descriptions.append(description)
    return jobs, descriptions


def _parse_json(source: dict[str, Any], text: str, client: HttpClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    payload = json.loads(text)
    items = payload.get("Jobs") or payload.get("jobs") or []
    jobs: list[dict[str, Any]] = []
    descriptions: list[dict[str, Any]] = []
    detail_limit = int(source.get("detail_limit", min(len(items), int(source.get("max_jobs", 80)), 80)))
    for index, item in enumerate(items):
        if isinstance(item.get("JobInfo"), dict):
            raw = _raw_from_pulse_job_info(source, item)
            if raw:
                if raw.get("url") and index < detail_limit:
                    detail = client.get(raw["url"])
                    if not detail.error:
                        detail_raw, detail_description = job_from_detail(
                            source,
                            raw.get("title") or "",
                            detail.url,
                            BeautifulSoup(detail.text, "lxml"),
                            client=client,
                        )
                        raw = _merge_with_detail_raw(raw, detail_raw)
                        if detail_description:
                            descriptions.append(detail_description)
                jobs.append(raw)
                description = _description_payload_from_raw(raw, source)
                if description:
                    descriptions.append(description)
            continue
        title = compact_text(item.get("Title") or item.get("JobTitle") or item.get("Name"))
        if not title:
            continue
        url = item.get("Url") or item.get("URL") or item.get("JobUrl") or ""
        if url:
            url = urljoin(source["url"], url)
        body = " ".join(compact_text(value) for value in item.values())
        detail_raw: dict[str, Any] = {}
        if url and index < detail_limit:
            detail = client.get(url)
            if not detail.error:
                detail_raw, detail_description = job_from_detail(source, title, detail.url, BeautifulSoup(detail.text, "lxml"), client=client)
                if detail_description:
                    descriptions.append(detail_description)
        detail_text = " ".join(
            compact_text(detail_raw.get(key))
            for key in ("title", "advertised_salary_text", "closing_text", "description_excerpt")
        )
        title = better_title(title, detail_raw.get("title"))
        band = extract_band(title, body, detail_raw.get("evidence", {}).get("band_text"), detail_text)
        salary = extract_salary(body, detail_raw.get("advertised_salary_text"), detail_raw.get("description_excerpt"))
        closing = extract_closing_date(body, detail_raw.get("closing_text"), detail_raw.get("description_excerpt"))
        salary_text = salary.text or detail_raw.get("advertised_salary_text")
        jobs.append({
            "title": title,
            "url": detail_raw.get("url") or url or source["url"],
            "location_text": compact_text(item.get("Location") or detail_raw.get("location_text")),
            "work_type": compact_text(item.get("WorkType") or item.get("EmploymentType") or detail_raw.get("work_type")),
            "closing_date": closing.date or detail_raw.get("closing_date"),
            "closing_text": closing.text or detail_raw.get("closing_text"),
            "advertised_salary_text": salary_text,
            "advertised_salary_min": salary.minimum or detail_raw.get("advertised_salary_min"),
            "advertised_salary_max": salary.maximum or detail_raw.get("advertised_salary_max"),
            "advertised_salary_period": salary.period or detail_raw.get("advertised_salary_period"),
            "classification_status": band.status if band.band else ("salary_only" if salary_text else "unclassified"),
            "band": band.band,
            "evidence": {
                k: v for k, v in {
                    "band_text": band.evidence_text,
                    "salary_text": salary.evidence_text or detail_raw.get("evidence", {}).get("salary_text"),
                    "document_urls": detail_raw.get("evidence", {}).get("document_urls"),
                }.items() if v
            },
            "description_excerpt": detail_raw.get("description_excerpt") or text_excerpt(body),
            "description_hash": detail_raw.get("description_hash"),
            "description_status": detail_raw.get("description_status") or "missing",
        })
    return jobs, descriptions


def _pulse_jobs_api_url(listing_url: str) -> str:
    parsed = urlsplit(listing_url)
    return urlunsplit((parsed.scheme, parsed.netloc, "/WebServices/RCM/Jobs/Jobs", "internalOnly=false&workArrangement=&employmentType=", ""))


def _raw_from_pulse_job_info(source: dict[str, Any], item: dict[str, Any]) -> dict[str, Any]:
    info = item.get("JobInfo") or {}
    title = compact_text(info.get("Title"))
    if not title:
        return {}
    description_text = compact_text(BeautifulSoup(info.get("Description") or "", "lxml").get_text(" "))
    compensation = compact_text(info.get("Compensation"))
    body = " ".join(compact_text(value) for value in info.values() if not isinstance(value, (dict, list)))
    band = extract_band(title, compensation, description_text, body)
    salary = extract_salary(compensation, description_text, body)
    closing = extract_closing_date(info.get("ClosingDate") or "", description_text)
    url = _pulse_job_url(source.get("url") or "", item.get("LinkId"), title)
    status = band.status if band.band else ("salary_only" if salary.text else "unclassified")
    return {
        "title": title,
        "url": url or source.get("url"),
        "location_text": compact_text(info.get("Location")),
        "work_type": compact_text(info.get("EmploymentType") or info.get("WorkArrangement")),
        "closing_date": closing.date,
        "closing_text": closing.text or info.get("ClosingDate"),
        "advertised_salary_text": salary.text or compensation,
        "advertised_salary_min": salary.minimum,
        "advertised_salary_max": salary.maximum,
        "advertised_salary_period": salary.period,
        "classification_status": status,
        "band": band.band,
        "evidence": {k: v for k, v in {"band_text": band.evidence_text, "salary_text": salary.evidence_text}.items() if v},
        "description_excerpt": text_excerpt(description_text or body),
        "description_hash": stable_hash(description_text, length=32) if description_text else None,
        "description_status": "fetched" if description_text else "missing",
    }


def _pulse_job_url(listing_url: str, link_id: Any, title: str) -> str:
    if not link_id:
        return listing_url
    parsed = urlsplit(listing_url)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", title).strip("-")
    return urlunsplit((parsed.scheme, parsed.netloc, f"/Pulse/job/{link_id}/{slug}", "source=public", ""))


def _description_payload_from_raw(raw: dict[str, Any], source: dict[str, Any]) -> dict[str, Any] | None:
    text = compact_text(raw.get("description_excerpt"))
    digest = raw.get("description_hash")
    if not digest or len(text) < 40:
        return None
    return {
        "schema_version": "jobsight.description.v1",
        "hash": digest,
        "source_url": raw.get("url"),
        "source_id": source.get("source_id"),
        "text": text_excerpt(text),
        "html": None,
        "sections": [],
    }


def _merge_with_detail_raw(raw: dict[str, Any], detail_raw: dict[str, Any]) -> dict[str, Any]:
    if not detail_raw:
        return raw
    merged = dict(raw)
    merged["title"] = better_title(raw.get("title") or "", detail_raw.get("title"))
    for key in ("url", "location_text", "work_type", "closing_date", "closing_text"):
        merged[key] = detail_raw.get(key) or merged.get(key)
    for key in ("advertised_salary_text", "advertised_salary_min", "advertised_salary_max", "advertised_salary_period"):
        merged[key] = merged.get(key) or detail_raw.get(key)
    if detail_raw.get("band") and not merged.get("band"):
        merged["band"] = detail_raw.get("band")
        merged["classification_status"] = detail_raw.get("classification_status") or "explicit_band"
    elif merged.get("band"):
        merged["classification_status"] = "explicit_band"
    elif merged.get("advertised_salary_text"):
        merged["classification_status"] = "salary_only"
    merged_evidence = dict(merged.get("evidence") or {})
    detail_evidence = detail_raw.get("evidence") if isinstance(detail_raw.get("evidence"), dict) else {}
    for key, value in detail_evidence.items():
        merged_evidence.setdefault(key, value)
    merged["evidence"] = {key: value for key, value in merged_evidence.items() if value}
    raw_description = compact_text(merged.get("description_excerpt"))
    detail_description = compact_text(detail_raw.get("description_excerpt"))
    if len(detail_description) > len(raw_description):
        merged["description_excerpt"] = detail_raw.get("description_excerpt")
        merged["description_hash"] = detail_raw.get("description_hash") or merged.get("description_hash")
        merged["description_status"] = detail_raw.get("description_status") or merged.get("description_status")
    return merged


def better_title(existing: str, detail_title: Any) -> str:
    return best_job_title(detail_title, fallback_title=existing)
