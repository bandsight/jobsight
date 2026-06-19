from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from jobsight.extractors.band import extract_band
from jobsight.extractors.closing_date import extract_closing_date
from jobsight.extractors.description import DescriptionResult, extract_description
from jobsight.extractors.documents import extract_document_texts
from jobsight.extractors.salary import extract_salary
from jobsight.extractors.title import best_job_title, is_valid_role_title
from jobsight.http import HttpClient
from jobsight.text import compact_text, text_excerpt


JOB_LINK_RE = re.compile(r"\b(job|jobs|career|careers|vacanc|position|recruit|opening|role)s?\b", re.IGNORECASE)
BAD_TITLE_RE = re.compile(
    r"^(alerts|apply|apply for a job|login|register|view|read more|learn more|next|previous|"
    r"about us|contact us|cookie policy|privacy policy|terms|facebook|instagram|linkedin|"
    r"tiktok|youtube|email|hr login|help centre|main navigation|gtranslate|"
    r"javascript is not available\.?|javascript is turned off.*)$",
    re.IGNORECASE,
)
BAD_TITLE_FRAGMENT_RE = re.compile(
    r"(select this as your preferred language|employment hero|payday super|"
    r"support for your business|start or grow your business|tenders and contracts|"
    r"looking to invest|business workshops|business,? jobs and investment)",
    re.IGNORECASE,
)
BAD_HOST_RE = re.compile(
    r"(^|\.)facebook\.com$|(^|\.)instagram\.com$|(^|\.)linkedin\.com$|(^|\.)tiktok\.com$|"
    r"(^|\.)youtube\.com$|(^|\.)x\.com$|(^|\.)twitter\.com$|^help\.employmenthero\.com$|^secure\.employmenthero\.com$|"
    r"^employmenthero\.page\.link$",
    re.IGNORECASE,
)
VACANCY_CONTEXT_RE = re.compile(
    r"\b(applications? close|apply now|closing date|fixed term|full[- ]time|part[- ]time|permanent|position|"
    r"responsibilities|role|salary|selection criteria|superannuation|successful applicant)\b",
    re.IGNORECASE,
)


def parse_generic(source: dict[str, Any], client: HttpClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    fetched = client.get(source["url"])
    if fetched.error:
        return [], [{"source_id": source.get("source_id"), "url": source["url"], "status": "failed", "message": fetched.error}]
    if is_blocked_response(fetched.status_code, fetched.text):
        return [], [{
            "source_id": source.get("source_id"),
            "url": fetched.url,
            "status": "failed",
            "message": "blocked_by_waf",
        }]
    soup = BeautifulSoup(fetched.text, "lxml")
    links = candidate_links(soup, fetched.url)[: source.get("max_jobs", 40)]
    detail_limit = int(source.get("detail_limit", min(len(links), int(source.get("max_jobs", 40)), 40)))
    jobs: list[dict[str, Any]] = []
    descriptions: list[dict[str, Any]] = []
    for index, (title, url) in enumerate(links):
        if index >= detail_limit:
            continue
        detail = client.get(url)
        if detail.error:
            continue
        detail_soup = BeautifulSoup(detail.text, "lxml")
        raw, description = job_from_detail(source, title, detail.url, detail_soup, client=client)
        if not is_probable_job_raw(raw):
            continue
        jobs.append(raw)
        if description:
            descriptions.append(description)
    return jobs, descriptions


def is_blocked_response(status_code: int | None, text: str) -> bool:
    lowered = (text or "").lower()
    return bool(
        status_code in {401, 403, 429}
        or (
            status_code == 202
            and ("awswafcookiedomainlist" in lowered or "window.gokuprops" in lowered)
        )
    )


def candidate_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    seen: set[str] = set()
    links: list[tuple[str, str]] = []
    for anchor in soup.select("a[href]"):
        href = anchor.get("href") or ""
        if href.startswith(("#", "mailto:", "tel:", "javascript:")):
            continue
        title = compact_text(anchor.get_text(" "))
        if len(title) < 4 or BAD_TITLE_RE.match(title) or BAD_TITLE_FRAGMENT_RE.search(title):
            continue
        url = urljoin(base_url, href)
        parsed = urlsplit(url)
        if BAD_HOST_RE.search(parsed.netloc.lower().removeprefix("www.")):
            continue
        if url in seen:
            continue
        haystack = f"{title} {parsed.path} {parsed.query}"
        if JOB_LINK_RE.search(haystack):
            seen.add(url)
            links.append((title, url))
    return links


def job_from_detail(
    source: dict[str, Any],
    fallback_title: str,
    url: str,
    soup: BeautifulSoup,
    client: HttpClient | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    title = compact_text(soup.select_one("h1, h2").get_text(" ") if soup.select_one("h1, h2") else fallback_title)
    body = page_search_text(soup)
    description = extract_description(soup)
    documents = extract_document_texts(soup, url, client) if client else None
    document_text = documents.text if documents else ""
    structured_text = structured_job_detail_text(soup)
    band = extract_band(title, document_text, description.text or "", structured_text, body_window_around(title, body))
    salary = extract_salary(structured_text, description.text or "", document_text, body)
    closing = extract_closing_date(structured_text, body, description.text or "", document_text)
    title = best_job_title(
        title,
        fallback_title=fallback_title,
        url=url,
        body=description.text or document_text or structured_text or body,
        council_key=source.get("short_name") or source.get("council_name"),
    )
    status = band.status
    if status == "unclassified" and salary.text:
        status = "salary_only"
    raw = {
        "title": title,
        "url": url,
        "location_text": "",
        "closing_date": closing.date,
        "closing_text": closing.text,
        "advertised_salary_text": salary.text,
        "advertised_salary_min": salary.minimum,
        "advertised_salary_max": salary.maximum,
        "advertised_salary_period": salary.period,
        "classification_status": status,
        "band": band.band,
        "evidence": {
            key: value for key, value in {
                "band_text": band.evidence_text,
                "salary_text": salary.evidence_text,
                "document_urls": documents.urls if documents and documents.urls else None,
            }.items() if value
        },
        "description_excerpt": text_excerpt(description.text or document_text),
        "description_hash": description.hash,
        "description_status": "document" if document_text and description.status in {"missing", "sparse"} else description.status,
    }
    return raw, description_payload(description, url, source)


def structured_job_detail_text(soup: BeautifulSoup) -> str:
    parts: list[str] = []
    for selector in (
        "[class*=salary]",
        "[class*=salery]",
        "[class*=remuneration]",
        "[class*=package]",
        "[itemprop*=baseSalary]",
    ):
        for node in soup.select(selector)[:4]:
            parts.append(node.get_text(" "))
    for meta in soup.select("meta[name=description], meta[property='og:description']"):
        parts.append(meta.get("content") or "")
    for script in soup.select('script[type*="ld+json"]'):
        text = script.get_text(" ")
        for key in ("title", "validThrough", "employmentType"):
            match = re.search(rf'"{key}"\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
            if match:
                parts.append(match.group(1))
    return compact_text(" ".join(parts))


def minimal_job(source: dict[str, Any], title: str, url: str, status: str) -> dict[str, Any]:
    return {
        "title": title,
        "url": url,
        "classification_status": "parse_warning",
        "description_status": status,
    }


def is_probable_job_raw(raw: dict[str, Any]) -> bool:
    title = compact_text(raw.get("title"))
    url = compact_text(raw.get("url"))
    host = urlsplit(url).netloc.lower().removeprefix("www.") if url else ""
    path = urlsplit(url).path.lower() if url else ""
    if not title or BAD_TITLE_RE.match(title) or BAD_TITLE_FRAGMENT_RE.search(title):
        return False
    if host == "employmenthero.com" and "/jobs/position/" not in path:
        return False
    if not is_valid_role_title(title):
        return False
    has_band = raw.get("band") not in ("", None)
    has_salary = any(raw.get(key) not in ("", None) for key in ("advertised_salary_text", "advertised_salary_min", "advertised_salary_max"))
    has_closing = any(raw.get(key) not in ("", None) for key in ("closing_date", "closing_text"))
    has_context = bool(VACANCY_CONTEXT_RE.search(" ".join(
        compact_text(raw.get(key))
        for key in ("description_excerpt", "work_type", "location_text")
    )))
    return bool(has_band or has_salary or has_closing or has_context)


def page_search_text(soup: BeautifulSoup) -> str:
    parts = [soup.get_text(" ")]
    for meta in soup.select("meta[content]"):
        parts.append(meta.get("content") or "")
    for node in soup.select("[aria-label], [title]"):
        parts.append(node.get("aria-label") or "")
        parts.append(node.get("title") or "")
    for script in soup.select('script[type*="json"]'):
        parts.append(script.get_text(" "))
    return compact_text(" ".join(parts))


def body_window_around(title: str, body: str, window: int = 1400) -> str:
    clean_title = compact_text(title)
    clean_body = compact_text(body)
    if len(clean_title) < 4 or not clean_body:
        return ""
    index = clean_body.lower().find(clean_title.lower())
    if index < 0:
        return ""
    return clean_body[max(0, index - window // 3): index + len(clean_title) + window]


def description_payload(
    description: DescriptionResult,
    url: str,
    source: dict[str, Any],
) -> dict[str, Any] | None:
    if not description.hash or not description.text:
        return None
    return {
        "schema_version": "jobsight.description.v1",
        "hash": description.hash,
        "source_url": url,
        "source_id": source.get("source_id"),
        "text": text_excerpt(description.text),
        "html": None,
        "sections": [],
    }
