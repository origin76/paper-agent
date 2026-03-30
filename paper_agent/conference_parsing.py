from __future__ import annotations

import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urljoin, urlparse


BLOCK_TAGS = {
    "article",
    "aside",
    "blockquote",
    "div",
    "figcaption",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "section",
    "table",
    "td",
    "th",
    "tr",
    "ul",
}
HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
SKIP_TAGS = {"script", "style", "noscript"}
TITLE_META_KEYS = ("citation_title", "dc.title", "og:title", "twitter:title")
AUTHOR_META_KEYS = ("citation_author", "dc.creator", "author")
PDF_META_KEYS = ("citation_pdf_url", "pdf_url")
DOI_META_KEYS = ("citation_doi", "dc.identifier", "doi")
TITLE_NOISE_MARKERS = (
    "call for papers",
    "accepted papers",
    "program",
    "schedule",
    "session",
    "home",
    "registration",
    "sponsors",
)


@dataclass
class HTMLLink:
    text: str
    url: str


@dataclass
class HTMLDocument:
    url: str
    final_url: str
    title: str
    headings: list[str] = field(default_factory=list)
    links: list[HTMLLink] = field(default_factory=list)
    meta: dict[str, list[str]] = field(default_factory=dict)
    text_blocks: list[str] = field(default_factory=list)


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_title_key(text: str) -> str:
    normalized = normalize_space(text).lower()
    normalized = normalized.replace("&", "and")
    normalized = re.sub(r"[\u2010-\u2015]", "-", normalized)
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def clean_extracted_title(text: str) -> str:
    normalized = normalize_space(text)
    normalized = normalized.replace("{", "").replace("}", "")
    normalized = re.sub(r"\s+([:;,.!?])", r"\1", normalized)
    return normalize_space(normalized)


def titles_match(left: str, right: str, threshold: float = 0.93) -> bool:
    left_key = normalize_title_key(left)
    right_key = normalize_title_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    if left_key in right_key and len(left_key) >= 24:
        return True
    if right_key in left_key and len(right_key) >= 24:
        return True
    return SequenceMatcher(None, left_key, right_key).ratio() >= threshold


def split_authors(raw_text: str) -> list[str]:
    normalized = normalize_space(raw_text)
    if not normalized:
        return []

    candidate = normalized.replace(" and ", ", ")
    if ";" in candidate:
        parts = [part.strip() for part in candidate.split(";")]
    else:
        parts = [part.strip() for part in candidate.split(",")]

    authors = [part for part in parts if part and len(part) > 1]
    if len(authors) >= 2:
        return authors
    return [normalized]


def looks_like_paper_title(text: str) -> bool:
    normalized = clean_extracted_title(text)
    if len(normalized) < 12 or len(normalized) > 260:
        return False
    lowered = normalized.lower()
    if any(marker in lowered for marker in TITLE_NOISE_MARKERS):
        return False
    if normalized.endswith(":"):
        return False
    word_count = len(normalized.split())
    if word_count < 3:
        return False
    return True


def looks_like_author_line(text: str) -> bool:
    normalized = normalize_space(text)
    lowered = normalized.lower()
    if len(normalized) < 4 or len(normalized) > 300:
        return False
    if any(marker in lowered for marker in TITLE_NOISE_MARKERS):
        return False
    return ("," in normalized or " and " in lowered or ";" in normalized) and "http" not in lowered


def looks_like_pdf_url(url: str) -> bool:
    lowered = str(url or "").strip().lower()
    if not lowered:
        return False
    return (
        lowered.endswith(".pdf")
        or ".pdf?" in lowered
        or "/pdf/" in lowered
        or "format=pdf" in lowered
        or "download=1" in lowered
    )


def maybe_promote_to_pdf_url(url: str) -> str:
    normalized = str(url or "").strip()
    lowered = normalized.lower()
    if "arxiv.org/abs/" in lowered:
        return re.sub(r"/abs/", "/pdf/", normalized, count=1).rstrip("/") + ".pdf"
    return normalized


def _candidate_link_score(link: HTMLLink) -> int:
    text = normalize_space(link.text).lower()
    url = link.url.lower()
    score = 0
    if looks_like_pdf_url(url):
        score += 6
    if "citation_pdf_url" in text:
        score += 3
    if any(marker in text for marker in ("pdf", "pre-print", "preprint", "download", "full text", "paper", "file attached")):
        score += 3
    if "/doi/pdf/" in url:
        score += 3
    if "arxiv.org/abs/" in url or "arxiv.org/pdf/" in url:
        score += 2
    if "openreview.net/pdf" in url or "proceedings.mlr.press" in url:
        score += 2
    return score


def collect_pdf_candidate_urls(document: HTMLDocument) -> list[str]:
    candidates: list[tuple[int, str]] = []
    seen: set[str] = set()

    for meta_key in PDF_META_KEYS:
        for value in document.meta.get(meta_key, []):
            url = maybe_promote_to_pdf_url(urljoin(document.final_url, value))
            if url not in seen:
                candidates.append((10, url))
                seen.add(url)

    for link in document.links:
        score = _candidate_link_score(link)
        if score <= 0:
            continue
        url = maybe_promote_to_pdf_url(link.url)
        if url in seen:
            continue
        candidates.append((score, url))
        seen.add(url)

    candidates.sort(key=lambda item: item[0], reverse=True)
    return [url for _, url in candidates]


def first_meta(document: HTMLDocument, *keys: str) -> str | None:
    for key in keys:
        values = document.meta.get(key.lower(), [])
        for value in values:
            normalized = normalize_space(value)
            if normalized:
                return normalized
    return None


def meta_values(document: HTMLDocument, *keys: str) -> list[str]:
    values: list[str] = []
    for key in keys:
        for value in document.meta.get(key.lower(), []):
            normalized = normalize_space(value)
            if normalized and normalized not in values:
                values.append(normalized)
    return values


def extract_document_paper_metadata(document: HTMLDocument) -> dict[str, Any]:
    title = first_meta(document, *TITLE_META_KEYS) or document.title or (document.headings[0] if document.headings else "")
    authors = meta_values(document, *AUTHOR_META_KEYS)
    if len(authors) == 1 and authors[0]:
        split = split_authors(authors[0])
        if len(split) > 1:
            authors = split

    doi_url = None
    doi_value = first_meta(document, *DOI_META_KEYS)
    if doi_value:
        doi_url = doi_value if doi_value.startswith("http") else f"https://doi.org/{doi_value}"

    if not doi_url:
        for link in document.links:
            if "doi.org/" in link.url.lower():
                doi_url = link.url
                break

    preprint_url = None
    for link in document.links:
        text = normalize_space(link.text).lower()
        lowered = link.url.lower()
        if any(marker in text for marker in ("pre-print", "preprint", "arxiv")) or "arxiv.org/" in lowered:
            preprint_url = maybe_promote_to_pdf_url(link.url) if looks_like_pdf_url(link.url) else link.url
            break

    pdf_candidates = collect_pdf_candidate_urls(document)
    pdf_url = pdf_candidates[0] if pdf_candidates else None

    return {
        "title": clean_extracted_title(title),
        "authors": authors,
        "doi_url": doi_url,
        "preprint_url": preprint_url,
        "pdf_candidates": pdf_candidates,
        "pdf_url": pdf_url,
    }


def extract_title_author_blocks(document: HTMLDocument) -> list[tuple[str, list[str]]]:
    pairs: list[tuple[str, list[str]]] = []
    seen_titles: set[str] = set()

    for block in document.text_blocks:
        lines = [normalize_space(line) for line in block.splitlines() if normalize_space(line)]
        if len(lines) < 2:
            continue
        title = lines[0]
        authors_line = " ".join(lines[1:3])
        if not looks_like_paper_title(title) or not looks_like_author_line(authors_line):
            continue
        cleaned_title = clean_extracted_title(title)
        title_key = normalize_title_key(cleaned_title)
        if title_key in seen_titles:
            continue
        seen_titles.add(title_key)
        pairs.append((cleaned_title, split_authors(authors_line)))

    return pairs


class _DocumentHTMLParser(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.skip_depth = 0
        self.capture_title = False
        self.current_heading_tag: str | None = None
        self.current_heading_parts: list[str] = []
        self.current_anchor_url: str | None = None
        self.current_anchor_parts: list[str] = []
        self.title_parts: list[str] = []
        self.headings: list[str] = []
        self.links: list[HTMLLink] = []
        self.meta: dict[str, list[str]] = {}
        self.text_blocks: list[str] = []
        self.current_block_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered in SKIP_TAGS:
            self.skip_depth += 1
            return
        if self.skip_depth > 0:
            return

        attr_map = {key.lower(): value or "" for key, value in attrs}
        if lowered == "meta":
            meta_key = attr_map.get("property") or attr_map.get("name") or attr_map.get("http-equiv")
            meta_value = normalize_space(unescape(attr_map.get("content", "")))
            if meta_key and meta_value:
                self.meta.setdefault(meta_key.lower(), []).append(meta_value)
            return

        if lowered == "title":
            self.capture_title = True
        if lowered in HEADING_TAGS:
            self.current_heading_tag = lowered
            self.current_heading_parts = []
        if lowered == "a":
            href = normalize_space(attr_map.get("href", ""))
            self.current_anchor_url = urljoin(self.base_url, href) if href else None
            self.current_anchor_parts = []
        if lowered == "br":
            self.current_block_parts.append("\n")
        elif lowered in BLOCK_TAGS and self.current_block_parts:
            self._flush_block()

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in SKIP_TAGS and self.skip_depth > 0:
            self.skip_depth -= 1
            return
        if self.skip_depth > 0:
            return

        if lowered == "title":
            self.capture_title = False
        if lowered == "a":
            text = normalize_space("".join(self.current_anchor_parts))
            if self.current_anchor_url and text:
                self.links.append(HTMLLink(text=text, url=self.current_anchor_url))
            self.current_anchor_url = None
            self.current_anchor_parts = []
        if lowered in HEADING_TAGS:
            heading = normalize_space("".join(self.current_heading_parts))
            if heading:
                self.headings.append(heading)
                self.current_block_parts.append(heading)
            self.current_heading_tag = None
            self.current_heading_parts = []
        if lowered in BLOCK_TAGS:
            self._flush_block()

    def handle_data(self, data: str) -> None:
        if self.skip_depth > 0:
            return
        text = unescape(data)
        if not text:
            return
        if self.capture_title:
            self.title_parts.append(text)
        if self.current_heading_tag:
            self.current_heading_parts.append(text)
        if self.current_anchor_url:
            self.current_anchor_parts.append(text)
        self.current_block_parts.append(text)

    def close(self) -> None:
        super().close()
        self._flush_block()

    def _flush_block(self) -> None:
        if not self.current_block_parts:
            return
        raw_text = "".join(self.current_block_parts)
        lines = [normalize_space(line) for line in raw_text.splitlines()]
        normalized = "\n".join(line for line in lines if line)
        if normalized and normalized not in self.text_blocks:
            self.text_blocks.append(normalized)
        self.current_block_parts = []


def parse_html_document(html_text: str, url: str, final_url: str | None = None) -> HTMLDocument:
    parser = _DocumentHTMLParser(final_url or url)
    parser.feed(html_text)
    parser.close()
    title = normalize_space("".join(parser.title_parts))
    return HTMLDocument(
        url=url,
        final_url=final_url or url,
        title=title,
        headings=parser.headings,
        links=parser.links,
        meta=parser.meta,
        text_blocks=parser.text_blocks,
    )


def infer_doi_pdf_candidate(url: str) -> str | None:
    normalized = str(url or "").strip()
    if not normalized:
        return None
    parsed = urlparse(normalized)
    doi_value = parsed.path.lstrip("/")
    if "doi.org" in parsed.netloc.lower() and doi_value.startswith("10.1145/"):
        return f"https://dl.acm.org/doi/pdf/{doi_value}"
    if "dl.acm.org" in parsed.netloc.lower() and "/doi/" in parsed.path and "/pdf/" not in parsed.path:
        return normalized.replace("/doi/", "/doi/pdf/", 1)
    return None
