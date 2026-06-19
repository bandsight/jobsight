from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Any
from urllib.parse import parse_qs, unquote, urljoin, urlsplit
from zipfile import BadZipFile, ZipFile
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

from jobsight.http import HttpClient
from jobsight.text import compact_text


DOCUMENT_HINTS = ("position description", "job specification", "pd ", "download", "document")
DOCUMENT_EXTENSIONS = (".pdf", ".docx")


@dataclass(frozen=True)
class DocumentTextResult:
    text: str
    urls: list[str]


def document_urls_from_soup(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for tag in soup.select("a[href], iframe[src], embed[src], object[data]"):
        attr = "href" if tag.has_attr("href") else "src" if tag.has_attr("src") else "data"
        raw_url = compact_text(tag.get(attr))
        if not raw_url:
            continue
        embedded = embedded_document_url(raw_url)
        candidates = [urljoin(base_url, embedded)] if embedded else [urljoin(base_url, raw_url)]
        label = compact_text(" ".join([tag.get_text(" "), tag.get("title") or "", tag.get("aria-label") or ""]))
        for candidate in candidates:
            if not is_document_candidate(candidate, label) or candidate in seen:
                continue
            seen.add(candidate)
            urls.append(candidate)
    return urls


def embedded_document_url(url: str) -> str:
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)
    for key in ("file", "src", "url"):
        for value in query.get(key, []):
            decoded = unquote(value)
            if any(decoded.lower().split("?", 1)[0].endswith(ext) for ext in DOCUMENT_EXTENSIONS):
                return decoded
    return ""


def is_document_candidate(url: str, label: str = "") -> bool:
    path = urlsplit(url).path.lower()
    if path.endswith(DOCUMENT_EXTENSIONS):
        return True
    haystack = f"{url} {label}".lower()
    return any(hint in haystack for hint in DOCUMENT_HINTS)


def extract_document_texts(
    soup: BeautifulSoup,
    base_url: str,
    client: HttpClient,
    *,
    limit: int = 2,
) -> DocumentTextResult:
    texts: list[str] = []
    urls: list[str] = []
    for url in document_urls_from_soup(soup, base_url)[:limit]:
        fetched = client.get_bytes(url)
        if fetched.error or not fetched.content:
            continue
        text = text_from_document_bytes(fetched.content, fetched.url, fetched.content_type)
        if text:
            urls.append(fetched.url)
            texts.append(text)
    return DocumentTextResult(text=compact_text(" ".join(texts)), urls=urls)


def text_from_document_bytes(content: bytes, url: str = "", content_type: str = "") -> str:
    kind = f"{url} {content_type}".lower()
    if ".pdf" in kind or "pdf" in kind:
        return pdf_text(content)
    if ".docx" in kind or "wordprocessingml" in kind:
        return docx_text(content)
    return ""


def pdf_text(content: bytes) -> str:
    try:
        from pypdf import PdfReader
    except ImportError:
        return ""
    try:
        reader = PdfReader(BytesIO(content))
        return compact_text(" ".join(page.extract_text() or "" for page in reader.pages[:12]))
    except Exception:
        return ""


def docx_text(content: bytes) -> str:
    try:
        with ZipFile(BytesIO(content)) as archive:
            xml = archive.read("word/document.xml")
    except (BadZipFile, KeyError, OSError):
        return ""
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return ""
    return compact_text(" ".join(node.text or "" for node in root.iter() if node.text))
