from __future__ import annotations

import re
from dataclasses import dataclass

from jobsight.text import compact_text


NUMBER_PATTERN = r"(?:\d{1,3}(?:,\d{3})+|\d{2,6})(?:\.\d+)?"
SALARY_RE = re.compile(
    rf"(?:(?:AUD|AU\$)\s*)?\$?\s*(?P<first>{NUMBER_PATTERN})(?P<first_k>[kK])?\s*"
    r"(?:AUD)?\s*"
    rf"(?:-|to|\?|\u2013|\u2014|\u00e2\u20ac\u201c|\u00e2\u20ac\u201d)\s*(?:(?:AUD|AU\$)\s*)?\$?\s*"
    rf"(?P<second>{NUMBER_PATTERN})(?P<second_k>[kK])?(?:\s*AUD)?|"
    rf"\$\s*(?P<single>{NUMBER_PATTERN})(?P<single_k>[kK])?",
    re.IGNORECASE,
)
PERIOD_PATTERNS = [
    ("hour", re.compile(r"\b(hour|hourly|p/h|per hour)\b", re.IGNORECASE)),
    ("week", re.compile(r"\b(week|weekly|per week)\b", re.IGNORECASE)),
    ("fortnight", re.compile(r"\b(fortnight|fortnightly)\b", re.IGNORECASE)),
    ("year", re.compile(r"\b(year|annual|annum|p\.?a\.?|per annum)\b", re.IGNORECASE)),
]
SALARY_CONTEXT_RE = re.compile(r"\b(salary|pay|rate|remuneration|package|annum|hour|weekly|fortnight)\b", re.IGNORECASE)
REFERENCE_CODE_RE = re.compile(r"^\d{1,3}-20\d{2}$")
SALARY_SUFFIX_BOUNDARY_MARKERS = (
    " Description ",
    " Role Type ",
    " Job ",
    " Apply ",
    " Information pack ",
    " Position description ",
    " Categories: ",
    " About the team ",
    " About the team",
    " About the role ",
    " About the role",
    " Closing ",
    " Location:",
    " Tool of trade ",
    " Join us ",
    " Join our ",
    " You will ",
)


@dataclass(frozen=True)
class SalaryResult:
    text: str | None
    minimum: float | None
    maximum: float | None
    period: str | None
    evidence_text: str | None


def _number(value: str | None, suffix: str | None = None) -> float | None:
    if not value:
        return None
    number = float(value.replace(",", ""))
    return number * 1000 if suffix and suffix.lower() == "k" else number


def _period(text: str) -> str | None:
    for name, pattern in PERIOD_PATTERNS:
        if pattern.search(text):
            return name
    return None


def extract_salary(*texts: str) -> SalaryResult:
    for text in texts:
        clean = compact_text(text)
        if not clean:
            continue
        candidates = salary_candidates(clean)
        if candidates:
            return candidates[0]
    return SalaryResult(text=None, minimum=None, maximum=None, period=None, evidence_text=None)


def salary_candidates(text: str) -> list[SalaryResult]:
    candidates: list[tuple[int, SalaryResult]] = []
    for match in SALARY_RE.finditer(text):
        first = _number(match.group("first") or match.group("single"), match.group("first_k") or match.group("single_k"))
        second = _number(match.group("second") or match.group("single"), match.group("second_k") or match.group("single_k"))
        if first is None or second is None:
            continue
        window_start = max(0, match.start() - 90)
        window = compact_text(text[window_start: min(len(text), match.end() + 90)])
        evidence = compact_text(match.group(0))
        if is_reference_code(evidence, first, second):
            continue
        if max(first, second) > 350000 and not re.search(r"\b(salary|remuneration|package|pay|rate)\b", window, re.IGNORECASE):
            continue
        if "$" not in evidence and "aud" not in window.lower() and not SALARY_CONTEXT_RE.search(window):
            continue
        local_start = max(0, match.start() - window_start)
        local_end = local_start + len(match.group(0))
        period = normalise_period(_period_near(window, local_start, local_end), first, second, window)
        score = salary_score(first, second, period, window, match.group("second") is not None)
        candidates.append((
            score,
            SalaryResult(
                text=salary_text_from_window(window, evidence),
                minimum=first,
                maximum=second,
                period=period,
                evidence_text=evidence,
            ),
        ))
    candidates.sort(key=lambda item: item[0], reverse=True)
    return [result for _, result in candidates]


def normalise_salary_fields(
    text: str | None,
    minimum: float | int | None,
    maximum: float | int | None,
    period: str | None,
) -> SalaryResult:
    clean = compact_text(text)
    first = float(minimum) if isinstance(minimum, (int, float)) else None
    second = float(maximum) if isinstance(maximum, (int, float)) else first
    if clean and first is not None and second is not None and is_reference_code(clean, first, second):
        return SalaryResult(text=None, minimum=None, maximum=None, period=None, evidence_text=None)
    parsed = salary_candidates(clean) if clean else []
    if parsed:
        best = parsed[0]
        if re.search(r"\d{1,3},\d{4,}", clean):
            return best
        parsed_high = max(value for value in (best.minimum, best.maximum) if value is not None)
        parsed_low = min(value for value in (best.minimum, best.maximum) if value is not None)
        existing_high = max(value for value in (first, second) if value is not None) if first is not None or second is not None else None
        existing_low = min(value for value in (first, second) if value is not None) if first is not None or second is not None else None
        if (
            existing_high is None
            or (existing_high < 1000 <= parsed_high)
            or (existing_low is not None and existing_low < 1000 <= existing_high and normalise_period(compact_text(period), first or 0, second or 0, clean) == "year")
        ):
            return best
        if (
            existing_low is not None
            and existing_high is not None
            and abs(existing_low - parsed_low) < 0.01
            and abs(existing_high - parsed_high) < 0.01
            and best.text
            and len(best.text) < len(clean)
        ):
            return best
    high = max(value for value in (first, second) if value is not None) if first is not None or second is not None else None
    normalised_period = compact_text(period).lower() or None
    if high is not None and high >= 30000 and normalised_period in {"hour", "week", "fortnight"}:
        normalised_period = "year"
    if high is not None and high >= 30000 and normalised_period == "year":
        low = min(value for value in (first, second) if value is not None)
        if low < 1000:
            first = high
            second = high
    if high is not None and high <= 300 and normalised_period is None and "$" in clean:
        normalised_period = "hour"
    return SalaryResult(
        text=clean or None,
        minimum=first,
        maximum=second,
        period=normalised_period,
        evidence_text=None,
    )


def normalise_period(period: str | None, first: float, second: float, context: str) -> str | None:
    high = max(first, second)
    if high >= 30000:
        return "year"
    return period or inferred_period(first, second, context)


def is_reference_code(evidence: str, first: float, second: float) -> bool:
    clean = compact_text(evidence).replace("\u2013", "-").replace("\u2014", "-")
    return (
        "$" not in clean
        and REFERENCE_CODE_RE.fullmatch(clean) is not None
        and min(first, second) <= 999
        and 2000 <= max(first, second) <= 2099
    )


def inferred_period(first: float, second: float, context: str) -> str | None:
    high = max(first, second)
    low = min(first, second)
    if high >= 30000:
        return "year"
    if high <= 250 and re.search(r"\b(casual|hourly|rate)\b", context, re.IGNORECASE):
        return "hour"
    if low >= 1000 and high < 10000:
        return "fortnight"
    return None


def _period_near(text: str, start: int, end: int) -> str | None:
    matches: list[tuple[int, str]] = []
    for name, pattern in PERIOD_PATTERNS:
        for match in pattern.finditer(text):
            distance = min(abs(match.start() - end), abs(start - match.end()))
            matches.append((distance, name))
    if not matches:
        return None
    matches.sort(key=lambda item: item[0])
    return matches[0][1]


def salary_score(first: float, second: float, period: str | None, context: str, is_range: bool) -> int:
    high = max(first, second)
    score = 0
    if is_range:
        score += 10
    if period:
        score += 8
    if period == "year":
        score += 4
    if high >= 30000:
        score += 4
    if "$" in context or "aud" in context.lower():
        score += 3
    if SALARY_CONTEXT_RE.search(context):
        score += 3
    if high < 20:
        score -= 8
    return score


def salary_text_from_window(window: str, evidence: str) -> str:
    start = max(0, window.find(evidence))
    prefix = window[:start].rsplit(".", 1)[-1].rsplit(";", 1)[-1]
    raw_suffix = window[start + len(evidence):]
    protected_suffix = re.sub(
        r"\bp\.?\s*a\.?(?=\s|$|\+)",
        lambda match: match.group(0).replace(".", "<dot>"),
        raw_suffix,
        flags=re.IGNORECASE,
    )
    suffix = re.split(r"(?<!\d)\.(?!\d)|;", protected_suffix, 1)[0].replace("<dot>", ".")
    if re.search(r"\d{1,3},\d{3}$", evidence) and re.match(r"^\d+\b", suffix):
        suffix = re.sub(r"^\d+", "", suffix, count=1)
    for marker in SALARY_SUFFIX_BOUNDARY_MARKERS:
        suffix = suffix.split(marker, 1)[0]
    if (
        re.search(r"\b(p\.?\s*a\.?|per annum|annum|year)\b", suffix, re.IGNORECASE)
        and re.search(r"\(\s*\$?\d", suffix)
    ):
        suffix = suffix.split(" (", 1)[0]
    if "$" in prefix or len(prefix) > 35 or re.search(r"\bband\s*[1-8]\b", prefix, re.IGNORECASE):
        prefix = ""
    if len(suffix) > 90:
        suffix = suffix[:90].rsplit(" ", 1)[0]
    return compact_text(f"{prefix}{evidence}{suffix}")[:300]
