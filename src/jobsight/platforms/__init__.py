from __future__ import annotations

from typing import Any, Callable

from jobsight.http import HttpClient
from jobsight.platforms.generic import parse_generic
from jobsight.platforms.oracle_hcm import parse_oracle_hcm
from jobsight.platforms.pulse import parse_pulse

Parser = Callable[[dict[str, Any], HttpClient], tuple[list[dict[str, Any]], list[dict[str, Any]]]]

PARSERS: dict[str, Parser] = {
    "oracle_hcm": parse_oracle_hcm,
    "pulse": parse_pulse,
}


def parse_source(source: dict[str, Any], client: HttpClient) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parser = PARSERS.get(str(source.get("platform") or ""), parse_generic)
    return parser(source, client)
