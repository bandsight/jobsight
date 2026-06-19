from __future__ import annotations

import re
from dataclasses import dataclass

from jobsight.text import compact_text


BAND_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
}
BAND_TOKEN = r"(?P<band>[1-8]|one|two|three|four|five|six|seven|eight)"
BAND_PATTERNS = [
    re.compile(
        rf"\b(?:classification|classified|salary|remuneration|position)?\s*"
        rf"(?:band|banding|band\s*/\s*level|level\s*/\s*band)\s*"
        rf"(?:no\.?|number|level|range|grade|:|-)?\s*{BAND_TOKEN}"
        rf"(?:\s*(?:/|-|to|or)\s*(?:[1-8]|one|two|three|four|five|six|seven|eight))?[a-d]?\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b(?:classification|classified|municipal\s+employee|local\s+government\s+employee|employee|me)\s+"
        rf"(?:level|grade)\s*(?:no\.?|number|:|-)?\s*{BAND_TOKEN}[a-d]?\b",
        re.IGNORECASE,
    ),
    re.compile(
        rf"\b(?:level|grade)\s*{BAND_TOKEN}[a-d]?\b.{{0,48}}"
        rf"\b(?:municipal|local\s+government|classification|classified|award|band)\b",
        re.IGNORECASE,
    ),
]


@dataclass(frozen=True)
class BandResult:
    band: int | None
    status: str
    evidence_text: str | None = None


def extract_band(*texts: str) -> BandResult:
    for text in texts:
        clean = compact_text(text)
        if not clean:
            continue
        for pattern in BAND_PATTERNS:
            match = pattern.search(clean)
            if match:
                return BandResult(
                    band=band_value(match.group("band")),
                    status="explicit_band",
                    evidence_text=match.group(0),
                )
    return BandResult(band=None, status="unclassified")


def band_value(value: str) -> int:
    token = compact_text(value).lower()
    if token.isdigit():
        return int(token)
    return BAND_WORDS[token]
