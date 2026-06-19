from __future__ import annotations

from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from jobsight.extractors.band import extract_band
from jobsight.extractors.closing_date import extract_closing_date
from jobsight.extractors.salary import extract_salary
from jobsight.http import HttpClient
from jobsight.text import compact_text, text_excerpt


def parse_oracle_hcm(source: dict[str, Any], client: HttpClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    api_url = _api_url(source["url"], source.get("site_number") or "CX_1001")
    fetched = client.get(api_url)
    if fetched.error:
        return [], [{"source_id": source.get("source_id"), "url": api_url, "status": "failed", "message": fetched.error}]
    try:
        payload = fetched.text and fetched.json()  # type: ignore[attr-defined]
    except Exception:
        import json
        payload = json.loads(fetched.text)
    items = payload.get("items") or payload.get("Items") or payload.get("jobs") or []
    jobs: list[dict[str, Any]] = []
    for item in items:
        title = compact_text(item.get("Title") or item.get("title") or item.get("Name"))
        if not title:
            continue
        detail_url = item.get("ExternalUrl") or item.get("externalUrl") or _job_url(source["url"], item)
        body = " ".join(compact_text(item.get(key)) for key in item.keys())
        band = extract_band(title, body)
        salary = extract_salary(body)
        closing = extract_closing_date(body)
        jobs.append({
            "title": title,
            "url": detail_url,
            "location_text": compact_text(item.get("PrimaryLocation") or item.get("Location") or item.get("primaryLocation")),
            "work_type": compact_text(item.get("WorkerType") or item.get("JobSchedule")),
            "closing_date": closing.date,
            "closing_text": closing.text,
            "advertised_salary_text": salary.text,
            "advertised_salary_min": salary.minimum,
            "advertised_salary_max": salary.maximum,
            "advertised_salary_period": salary.period,
            "classification_status": band.status if band.band else ("salary_only" if salary.text else "unclassified"),
            "band": band.band,
            "evidence": {k: v for k, v in {"band_text": band.evidence_text, "salary_text": salary.evidence_text}.items() if v},
            "description_excerpt": text_excerpt(body),
            "description_status": "missing",
        })
    return jobs, []


def _api_url(listing_url: str, site_number: str) -> str:
    parsed = urlsplit(listing_url)
    path = "/hcmRestApi/resources/latest/recruitingCEJobRequisitions"
    query = urlencode({
        "finder": f"BySiteNumber;siteNumber={site_number}",
        "onlyData": "true",
        "expand": "requisitionList.workLocation,requisitionList.otherWorkLocations",
        "limit": "100",
    })
    return urlunsplit((parsed.scheme, parsed.netloc, path, query, ""))


def _job_url(listing_url: str, item: dict[str, Any]) -> str:
    parsed = urlsplit(listing_url)
    site = "CX_1001"
    if "/sites/" in parsed.path:
        site = parsed.path.split("/sites/", 1)[1].split("/", 1)[0]
    job_id = item.get("Id") or item.get("RequisitionId") or item.get("JobId") or item.get("jobId")
    slug = str(job_id or "").strip()
    return urlunsplit((parsed.scheme, parsed.netloc, f"/hcmUI/CandidateExperience/en/sites/{site}/job/{slug}", "", ""))
