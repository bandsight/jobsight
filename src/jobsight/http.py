from __future__ import annotations

from dataclasses import dataclass

import requests

try:
    import truststore

    truststore.inject_into_ssl()
except ImportError:
    pass


DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "User-Agent": "JobSight/2.0 (+https://github.com/bandsight/jobsight)",
}


@dataclass
class FetchResult:
    url: str
    status_code: int | None
    text: str
    content_type: str
    error: str | None = None


@dataclass
class FetchBytesResult:
    url: str
    status_code: int | None
    content: bytes
    content_type: str
    error: str | None = None


class HttpClient:
    def __init__(self, timeout: int = 20) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)

    def get(self, url: str) -> FetchResult:
        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            return FetchResult(
                url=response.url,
                status_code=response.status_code,
                text=response.text if response.ok else "",
                content_type=response.headers.get("content-type", ""),
                error=None if response.ok else f"HTTP {response.status_code}",
            )
        except requests.RequestException as exc:
            return FetchResult(url=url, status_code=None, text="", content_type="", error=str(exc))

    def get_bytes(self, url: str) -> FetchBytesResult:
        try:
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            return FetchBytesResult(
                url=response.url,
                status_code=response.status_code,
                content=response.content if response.ok else b"",
                content_type=response.headers.get("content-type", ""),
                error=None if response.ok else f"HTTP {response.status_code}",
            )
        except requests.RequestException as exc:
            return FetchBytesResult(url=url, status_code=None, content=b"", content_type="", error=str(exc))
