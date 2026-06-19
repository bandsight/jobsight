from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from jobsight.text import compact_text

CLOSING_HINT_RE = re.compile(r"\b(closing|closes|applications close|apply by|deadline|closing date)\b.{0,120}", re.IGNORECASE)
NUMERIC_DATE_RE = re.compile(r"\b(?P<day>\d{1,2})[/-](?P<month>\d{1,2})[/-](?P<year>\d{2,4})\b")
WORD_DATE_RE = re.compile(
    r"\b(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(?P<month>jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\s+(?P<year>20\d{2})\b",
    re.IGNORECASE,
)
MONTHS = {"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12}


@dataclass(frozen=True)
class ClosingResult:
    date: str | None
    text: str | None


def extract_closing_date(*texts: str) -> ClosingResult:
    candidates: list[str] = []
    for text in texts:
        clean = compact_text(text)
        if not clean:
            continue
        candidates.extend(match.group(0) for match in CLOSING_HINT_RE.finditer(clean))
        candidates.append(clean)
    for candidate in candidates:
        parsed = _parse_date(candidate)
        if parsed:
            return ClosingResult(date=parsed.isoformat(), text=compact_text(candidate))
    return ClosingResult(date=None, text=None)


def _parse_date(text: str) -> date | None:
    numeric = NUMERIC_DATE_RE.search(text)
    if numeric:
        year = int(numeric.group("year"))
        if year < 100:
            year += 2000
        return _safe_date(year, int(numeric.group("month")), int(numeric.group("day")))
    word = WORD_DATE_RE.search(text)
    if word:
        month = MONTHS[word.group("month")[:3].lower()]
        return _safe_date(int(word.group("year")), month, int(word.group("day")))
    return None


def _safe_date(year: int, month: int, day: int) -> date | None:
    if year < 2020:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None
