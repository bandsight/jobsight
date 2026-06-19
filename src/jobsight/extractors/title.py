from __future__ import annotations

import re
from typing import Any

from jobsight.text import compact_text, text_excerpt


ROLE_TITLE_RE = re.compile(
    r"\b("
    r"accountant|administrator|adviser|advisor|analyst|assistant|auditor|business partner|case manager|coordinator|crew|"
    r"developer|educator|engineer|graduate|inspector|labou?rer|lead|leader|librarian|manager|nurse|officer|operator|"
    r"planner|project|ranger|receptionist|specialist|supervisor|surveyor|team leader|technician|trainee|worker"
    r")\b",
    re.IGNORECASE,
)
GENERIC_TITLE_RE = re.compile(
    r"^(breadcrumb|recommended for you|apply for a job|careers?|current (opportunities|vacancies)|gtranslate|"
    r"javascript is (?:turned off|not available).*|"
    r"\d+\s+dismissed announcement|"
    r"search [0-9,]+ jobs? now|"
    r"jobs?( and (careers|opportunities))?|employment opportunities|positions vacant|work(ing)? for council)$",
    re.IGNORECASE,
)
DROP_SLUG_WORDS = {
    "job",
    "jobs",
    "career",
    "careers",
    "vic",
    "victoria",
    "australia",
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
SMALL_TITLE_WORDS = {"and", "at", "for", "in", "of", "on", "the", "to", "with"}
MONTH_WORD_RE = re.compile(
    r"\s+(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|"
    r"sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)(?:\s+20\d{2})?$",
    re.IGNORECASE,
)


def best_job_title(
    primary: Any,
    *,
    fallback_title: Any = None,
    url: Any = None,
    body: Any = None,
    council_key: Any = None,
) -> str:
    title = clean_role_title(primary, council_key)
    fallback = clean_role_title(fallback_title, council_key)
    if is_valid_role_title(title):
        return text_excerpt(title, 140)
    for candidate in (
        title_from_url(url, council_key),
        title_from_text_blob(body),
        fallback if fallback != title else "",
        title,
    ):
        clean = compact_text(candidate)
        if is_valid_role_title(clean):
            return text_excerpt(clean, 140)
    return text_excerpt(title or fallback, 140)


def is_valid_role_title(value: Any) -> bool:
    title = compact_text(value)
    if not 4 <= len(title) <= 140:
        return False
    if GENERIC_TITLE_RE.match(title):
        return False
    return bool(ROLE_TITLE_RE.search(title))


def title_from_url(url: Any, council_key: Any = None) -> str:
    path = compact_text(url).split("?", 1)[0].rstrip("/")
    slug = path.rsplit("/", 1)[-1].lower()
    if not slug:
        return ""
    words = [word for word in re.split(r"[^a-z0-9]+", slug) if word]
    while words and (words[-1] in DROP_SLUG_WORDS or re.fullmatch(r"[0-9a-f]{4,}", words[-1])):
        words.pop()
    council_words = [word for word in re.split(r"[^a-z0-9]+", compact_text(council_key).lower()) if word]
    if council_words and words[-len(council_words):] == council_words:
        words = words[:-len(council_words)]
    words = [word for word in words if not re.fullmatch(r"[0-9a-f]{8,}", word)]
    if not words:
        return ""
    candidate = title_case_slug(words)
    return candidate if is_valid_role_title(candidate) else ""


def title_from_text_blob(value: Any) -> str:
    candidate = compact_text(value)
    for pattern in (
        r"^(?:created\s+\d{1,2}/\d{1,2}/\d{4}\s+)?(?:page\s+\d+\s+)?position description\s+(?P<title>[A-Z][A-Z0-9 &/'(),.-]{3,90}?)(?:\s+(?:Created|Classification|Employment Status|Position Details|Reports To|[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,4}\s+Council)\b)",
        r"^(?:page\s+\d+\s+)?(?:position description\s+)?(?P<title>[A-Z][A-Z0-9 &/'(),.-]{3,90})\s+CLASSIFICATION\s*:",
        r"^(?:page\s+\d+\s+)?position description\s+(?P<title>[A-Z][A-Z0-9 &/'(),.-]{3,90})\s+Created\s+\d{1,2}/\d{1,2}/\d{4}",
        r"^(?:page\s+\d+\s+)?(?P<title>[A-Z][A-Z0-9 &/'(),.-]{3,90})\s+Created\s+\d{1,2}/\d{1,2}/\d{4}",
    ):
        match = re.search(pattern, candidate, flags=re.IGNORECASE)
        if match:
            title = clean_role_title(title_case_words(match.group("title")))
            if is_valid_role_title(title):
                return title
    for marker in (" Visit ", " Kiosk mode ", " Add to favourites ", " Closing on:", " Closing date:"):
        candidate = candidate.split(marker, 1)[0]
    if " | " in candidate:
        candidate = compact_text(candidate.split(" | ", 1)[0])
    if " - " in candidate:
        candidate = compact_text(candidate.split(" - ", 1)[0])
    candidate = re.sub(r"^New to you\s+", "", candidate, flags=re.IGNORECASE).strip()
    lead_match = re.match(
        r"^(?P<title>[A-Z][A-Za-z0-9 &/'(),.-]{3,90}?)(?:\s+(?:New role|Full[- ]time|Part[- ]time|Permanent|Temporary|Fixed[- ]term|Casual|\$|Band|Classification|Salary)\b)",
        candidate,
    )
    if lead_match:
        title = clean_role_title(lead_match.group("title"))
        if is_valid_role_title(title):
            return title
    return candidate if is_valid_role_title(candidate) else ""


def clean_role_title(value: Any, council_key: Any = None) -> str:
    title = compact_text(value)
    if not title:
        return ""
    title = re.sub(r"^(?:browse\s+jobs|search\s+jobs)\s+", "", title, flags=re.IGNORECASE)
    title = re.sub(r"(?:\s+[0-9a-f]{4,}){2,}$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s+(?:vic|victoria)\s+australia(?:\s+[0-9a-f]{4,})*$", "", title, flags=re.IGNORECASE)
    title = re.split(r"\s+\$\d", title, 1)[0]
    detail_match = re.search(
        r"\s+(?:casual|permanent|temporary|fixed[- ]term|full[- ]time|part[- ]time)\s+position\b",
        title,
        flags=re.IGNORECASE,
    )
    if detail_match and detail_match.start() > 8:
        title = title[:detail_match.start()]
    metadata_match = re.search(r"\s+(?:Type|Duration|Salary)\b", title)
    if metadata_match and metadata_match.start() > 8:
        title = title[:metadata_match.start()]
    title = MONTH_WORD_RE.sub("", title)
    title = re.sub(r"\s+[\(\[\{]+$", "", title)
    title = re.sub(r"\s+[-,/:;]+$", "", title)
    council_words = compact_text(council_key).split()
    if council_words:
        council_tail = r"\s+" + r"\s+".join(re.escape(word) for word in council_words) + r"$"
        title = re.sub(council_tail, "", title, flags=re.IGNORECASE)
    return compact_text(title)


def title_case_slug(words: list[str]) -> str:
    titled = []
    for index, word in enumerate(words):
        if index and word in SMALL_TITLE_WORDS:
            titled.append(word)
        else:
            titled.append(re.sub(r"[a-z]+", lambda match: match.group(0).capitalize(), word.lower()))
    return " ".join(titled)


def title_case_words(value: str) -> str:
    return title_case_slug([word.lower() for word in re.split(r"\s+", compact_text(value)) if word])
