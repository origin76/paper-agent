from __future__ import annotations

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from http.cookiejar import LoadError, MozillaCookieJar
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from paper_agent.conference_parsing import (
    extract_document_paper_metadata,
    infer_doi_pdf_candidate,
    looks_like_pdf_url,
    maybe_promote_to_pdf_url,
    normalize_title_key,
    parse_html_document,
    titles_match,
)
from paper_agent.conference_types import ConferenceManifest, ConferencePaper
from paper_agent.playwright_download import (
    DEFAULT_ACM_BROWSER_FALLBACK_ENV,
    DEFAULT_PLAYWRIGHT_BROWSER_EXECUTABLE_ENV,
    DEFAULT_PLAYWRIGHT_CDP_URL_ENV,
    DEFAULT_PLAYWRIGHT_HEADLESS_ENV,
    DEFAULT_PLAYWRIGHT_LAUNCH_TIMEOUT_MS_ENV,
    DEFAULT_PLAYWRIGHT_NAVIGATION_TIMEOUT_MS_ENV,
    DEFAULT_PLAYWRIGHT_PROFILE_DIRECTORY_ENV,
    DEFAULT_PLAYWRIGHT_USER_DATA_DIR_ENV,
    BrowserPDFDownloader,
    PlaywrightPDFDownloader,
    build_playwright_download_config,
    resolve_playwright_env_config,
)
from paper_agent.runtime import append_stage_trace, configure_logging, log_event
from paper_agent.utils import sanitize_filename, write_json
from paper_agent.venue_osdi import OSDIAdapter
from paper_agent.venue_pldi import PLDIAdapter
from paper_agent.venue_sosp import SOSPAdapter


ATOM_NAMESPACE = {"atom": "http://www.w3.org/2005/Atom"}
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
DEFAULT_ACCEPT_LANGUAGE = "zh-CN,zh;q=0.9,en;q=0.8"
DEFAULT_COOKIE_HEADER_ENV = "PAPER_AGENT_COOKIE_HEADER"
DEFAULT_COOKIE_FILE_ENV = "PAPER_AGENT_COOKIE_FILE"
DEFAULT_ACM_COOKIE_HEADER_ENV = "PAPER_AGENT_ACM_COOKIE_HEADER"
DEFAULT_ACM_COOKIE_FILE_ENV = "PAPER_AGENT_ACM_COOKIE_FILE"
ACM_PDF_HOST = "dl.acm.org"


def derive_pdf_download_referer(url: str) -> str | None:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host != ACM_PDF_HOST:
        return None
    pdf_prefix = "/doi/pdf/"
    if not parsed.path.startswith(pdf_prefix):
        return None
    doi_suffix = parsed.path[len(pdf_prefix) :].lstrip("/")
    if not doi_suffix:
        return None
    return f"{parsed.scheme or 'https'}://{parsed.netloc}/doi/{doi_suffix}"


def _looks_like_netscape_cookie_file(raw_text: str) -> bool:
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    if not lines:
        return False
    if lines[0].startswith("# Netscape HTTP Cookie File"):
        return True
    for line in lines:
        if line.startswith("#"):
            continue
        parts = line.split("\t")
        return len(parts) >= 7
    return False


def _load_cookie_header_file(path: Path) -> str:
    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        return ""
    first_non_empty_line = next((line.strip() for line in raw_text.splitlines() if line.strip()), "")
    if first_non_empty_line.lower().startswith("cookie:"):
        return first_non_empty_line.split(":", 1)[1].strip()
    return raw_text


class CookieHeaderSource:
    def __init__(
        self,
        *,
        static_header: str | None = None,
        cookie_jar: MozillaCookieJar | None = None,
        source_label: str | None = None,
    ) -> None:
        self.static_header = (static_header or "").strip() or None
        self.cookie_jar = cookie_jar
        self.source_label = source_label

    @classmethod
    def from_inputs(
        cls,
        *,
        cookie_header: str | None = None,
        cookie_file: Path | None = None,
        source_label: str | None = None,
    ) -> CookieHeaderSource | None:
        normalized_header = (cookie_header or "").strip()
        if normalized_header:
            return cls(static_header=normalized_header, source_label=source_label)
        if cookie_file is None:
            return None
        raw_text = cookie_file.read_text(encoding="utf-8").strip()
        if not raw_text:
            return None
        if _looks_like_netscape_cookie_file(raw_text):
            cookie_jar = MozillaCookieJar(str(cookie_file))
            try:
                cookie_jar.load(ignore_discard=True, ignore_expires=True)
            except LoadError as exc:
                raise RuntimeError(f"Failed to load Netscape cookie jar: {cookie_file}") from exc
            return cls(cookie_jar=cookie_jar, source_label=source_label or str(cookie_file))
        return cls(
            static_header=_load_cookie_header_file(cookie_file),
            source_label=source_label or str(cookie_file),
        )

    def header_for_url(self, url: str) -> str | None:
        if self.static_header:
            return self.static_header
        if self.cookie_jar is None:
            return None
        request = Request(url)
        self.cookie_jar.add_cookie_header(request)
        return request.get_header("Cookie")


class ReturnedPDFError(RuntimeError):
    def __init__(self, url: str, final_url: str):
        super().__init__(f"URL returned PDF bytes instead of HTML: {url}")
        self.url = url
        self.final_url = final_url


class ConferenceHTTPClient:
    def __init__(
        self,
        timeout_seconds: int,
        html_max_bytes: int,
        download_max_bytes: int,
        retry_attempts: int,
        retry_backoff_seconds: float,
        user_agent: str = DEFAULT_USER_AGENT,
        browser_like_headers: bool = True,
        default_cookie_source: CookieHeaderSource | None = None,
        acm_cookie_source: CookieHeaderSource | None = None,
        browser_pdf_downloader: BrowserPDFDownloader | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.html_max_bytes = html_max_bytes
        self.download_max_bytes = download_max_bytes
        self.retry_attempts = max(1, retry_attempts)
        self.retry_backoff_seconds = max(0.0, retry_backoff_seconds)
        self.user_agent = user_agent
        self.browser_like_headers = browser_like_headers
        self.default_cookie_source = default_cookie_source
        self.acm_cookie_source = acm_cookie_source
        self.browser_pdf_downloader = browser_pdf_downloader

    def _cookie_header_for_url(self, url: str) -> str | None:
        host = urlparse(url).netloc.lower()
        if host == ACM_PDF_HOST and self.acm_cookie_source is not None:
            return self.acm_cookie_source.header_for_url(url)
        if self.default_cookie_source is not None:
            return self.default_cookie_source.header_for_url(url)
        return None

    def _build_request(self, url: str, accept: str, *, referer: str | None = None) -> Request:
        headers = {
            "User-Agent": self.user_agent,
            "Accept": accept,
            "Accept-Encoding": "identity",
        }
        if self.browser_like_headers:
            headers["Accept-Language"] = DEFAULT_ACCEPT_LANGUAGE
            headers["Cache-Control"] = "no-cache"
            headers["Pragma"] = "no-cache"
            headers["Upgrade-Insecure-Requests"] = "1"
        if referer:
            headers["Referer"] = referer

        cookie_header = self._cookie_header_for_url(url)
        if cookie_header:
            headers["Cookie"] = cookie_header

        return Request(url, headers=headers)

    def fetch_text(self, url: str) -> tuple[str, str, str]:
        start_time = time.perf_counter()
        log_event("info", "Conference HTTP text request started", url=url)
        for attempt in range(1, self.retry_attempts + 1):
            try:
                with urlopen(self._build_request(url, "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"), timeout=self.timeout_seconds) as response:
                    final_url = response.geturl() or url
                    content_type = response.headers.get("Content-Type", "")
                    raw_bytes = response.read(self.html_max_bytes)
                break
            except Exception as exc:
                should_retry = attempt < self.retry_attempts and self._is_retryable_error(exc)
                log_event(
                    "warning" if should_retry else "error",
                    "Conference HTTP text request failed",
                    url=url,
                    attempt=attempt,
                    retrying=should_retry,
                    duration_seconds=f"{time.perf_counter() - start_time:.2f}",
                    error=str(exc),
                )
                if not should_retry:
                    raise
                time.sleep(self.retry_backoff_seconds * attempt)

        if content_type.lower().startswith("application/pdf") or raw_bytes.startswith(b"%PDF"):
            raise ReturnedPDFError(url=url, final_url=final_url)

        text = raw_bytes.decode(self._extract_charset(content_type), errors="replace")
        log_event(
            "info",
            "Conference HTTP text request finished",
            url=url,
            final_url=final_url,
            content_type=content_type,
            byte_count=len(raw_bytes),
            duration_seconds=f"{time.perf_counter() - start_time:.2f}",
        )
        return text, final_url, content_type

    def fetch_document(self, url: str):
        html_text, final_url, _ = self.fetch_text(url)
        return parse_html_document(html_text, url=url, final_url=final_url)

    def fetch_json(self, url: str) -> tuple[Any, str]:
        text, final_url, _ = self.fetch_text(url)
        return json.loads(text), final_url

    def fetch_xml_root(self, url: str) -> tuple[ElementTree.Element, str]:
        text, final_url, _ = self.fetch_text(url)
        return ElementTree.fromstring(text), final_url

    def download_pdf(self, url: str, destination: Path) -> dict[str, Any]:
        try:
            return self._download_pdf_via_http(url, destination)
        except Exception as exc:
            if not self._should_attempt_browser_fallback(url, exc):
                raise
            log_event(
                "warning",
                "Conference PDF switching to Playwright fallback",
                url=url,
                destination=destination,
                error=str(exc),
            )
            return self.browser_pdf_downloader.download_pdf(
                url,
                destination,
                referer=derive_pdf_download_referer(url),
            )

    def _download_pdf_via_http(self, url: str, destination: Path) -> dict[str, Any]:
        start_time = time.perf_counter()
        log_event("info", "Conference PDF download started", url=url, destination=destination)
        destination.parent.mkdir(parents=True, exist_ok=True)

        byte_count = 0
        prefix = b""
        final_url = url
        content_type = ""
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        if tmp_path.exists():
            tmp_path.unlink()

        for attempt in range(1, self.retry_attempts + 1):
            try:
                byte_count = 0
                prefix = b""
                if tmp_path.exists():
                    tmp_path.unlink()
                with urlopen(
                    self._build_request(
                        url,
                        "application/pdf,*/*;q=0.8",
                        referer=derive_pdf_download_referer(url),
                    ),
                    timeout=self.timeout_seconds,
                ) as response:
                    final_url = response.geturl() or url
                    content_type = response.headers.get("Content-Type", "")
                    with tmp_path.open("wb") as handle:
                        while True:
                            chunk = response.read(64 * 1024)
                            if not chunk:
                                break
                            if len(prefix) < 16:
                                prefix += chunk[: 16 - len(prefix)]
                            byte_count += len(chunk)
                            if byte_count > self.download_max_bytes:
                                raise RuntimeError(f"Download exceeded size limit ({self.download_max_bytes} bytes)")
                            handle.write(chunk)
                break
            except Exception as exc:
                should_retry = attempt < self.retry_attempts and self._is_retryable_error(exc)
                if tmp_path.exists():
                    tmp_path.unlink()
                log_event(
                    "warning" if should_retry else "error",
                    "Conference PDF download failed",
                    url=url,
                    destination=destination,
                    attempt=attempt,
                    retrying=should_retry,
                    duration_seconds=f"{time.perf_counter() - start_time:.2f}",
                    error=str(exc),
                )
                if not should_retry:
                    raise
                time.sleep(self.retry_backoff_seconds * attempt)

        if not prefix.startswith(b"%PDF"):
            if tmp_path.exists():
                tmp_path.unlink()
            raise RuntimeError(f"Downloaded content is not a PDF: {final_url} ({content_type or 'unknown content-type'})")

        tmp_path.replace(destination)
        log_event(
            "info",
            "Conference PDF download finished",
            url=url,
            final_url=final_url,
            destination=destination,
            byte_count=byte_count,
            content_type=content_type,
            duration_seconds=f"{time.perf_counter() - start_time:.2f}",
        )
        return {
            "url": url,
            "final_url": final_url,
            "destination": str(destination),
            "byte_count": byte_count,
            "content_type": content_type,
            "transport": "http",
        }

    def _should_attempt_browser_fallback(self, url: str, exc: Exception) -> bool:
        if self.browser_pdf_downloader is None:
            return False
        if urlparse(url).netloc.lower() != ACM_PDF_HOST:
            return False
        if isinstance(exc, HTTPError):
            return exc.code in {401, 403, 429}
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "forbidden",
                "not a pdf",
                "cloudflare",
                "challenge",
                "blocked",
            )
        )

    @staticmethod
    def _extract_charset(content_type: str, default: str = "utf-8") -> str:
        match = re.search(r"charset=([A-Za-z0-9._-]+)", content_type, flags=re.IGNORECASE)
        if match:
            return match.group(1)
        return default

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        if isinstance(exc, HTTPError):
            return exc.code == 429 or exc.code >= 500
        if isinstance(exc, URLError):
            message = str(exc.reason).lower()
        else:
            message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "timed out",
                "timeout",
                "temporarily unavailable",
                "connection reset",
                "unexpected eof",
                "incomplete read",
                "ssl",
            )
        )


class ConferenceFetchService:
    def __init__(
        self,
        output_root: Path,
        run_dir: Path,
        *,
        timeout_seconds: int,
        html_max_bytes: int,
        download_max_bytes: int,
        resolve_workers: int,
        download_workers: int,
        retry_attempts: int,
        retry_backoff_seconds: float,
        skip_existing: bool,
        dry_run: bool,
        enable_supplemental_lookups: bool,
        limit_per_venue: int | None,
        http_user_agent: str = DEFAULT_USER_AGENT,
        browser_like_http: bool = True,
        cookie_source: CookieHeaderSource | None = None,
        acm_cookie_source: CookieHeaderSource | None = None,
        browser_pdf_downloader: BrowserPDFDownloader | None = None,
    ) -> None:
        self.output_root = output_root
        self.run_dir = run_dir
        self.resolve_workers = max(1, resolve_workers)
        self.download_workers = max(1, download_workers)
        self.skip_existing = skip_existing
        self.dry_run = dry_run
        self.enable_supplemental_lookups = enable_supplemental_lookups
        self.limit_per_venue = limit_per_venue
        self.http = ConferenceHTTPClient(
            timeout_seconds=timeout_seconds,
            html_max_bytes=html_max_bytes,
            download_max_bytes=download_max_bytes,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
            user_agent=http_user_agent,
            browser_like_headers=browser_like_http,
            default_cookie_source=cookie_source,
            acm_cookie_source=acm_cookie_source,
            browser_pdf_downloader=browser_pdf_downloader,
        )
        self.adapters = {
            "osdi": OSDIAdapter(),
            "pldi": PLDIAdapter(),
            "sosp": SOSPAdapter(),
        }
        self.manifests_dir = self.output_root / "manifests"
        self.downloads_dir = self.output_root / "downloads"
        self.unresolved_dir = self.output_root / "unresolved"
        self.indexes_dir = self.output_root / "indexes"
        for path in (self.manifests_dir, self.downloads_dir, self.unresolved_dir, self.indexes_dir):
            path.mkdir(parents=True, exist_ok=True)

    def run(self, venues: list[str], years: list[int]) -> dict[str, Any]:
        summary_items: list[dict[str, Any]] = []
        downloaded_papers: list[dict[str, Any]] = []
        started_at = datetime.now(UTC).isoformat()
        append_stage_trace(self.run_dir, "conference_fetch", "started", venues=venues, years=years)
        log_event("info", "Conference fetch run started", venues=venues, years=years, output_root=self.output_root)

        for venue in venues:
            adapter = self.adapters[venue]
            for year in years:
                summary = self._process_venue_year(adapter, venue, year)
                summary_items.append(summary)

                manifest_path = summary.get("manifest_path")
                if manifest_path:
                    try:
                        manifest_payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
                    except Exception:
                        manifest_payload = {}
                    for item in manifest_payload.get("items", []):
                        if item.get("status") in {"downloaded", "existing"} and item.get("download_path"):
                            downloaded_papers.append(item)

        finished_at = datetime.now(UTC).isoformat()
        summary = {
            "started_at": started_at,
            "finished_at": finished_at,
            "output_root": str(self.output_root),
            "run_dir": str(self.run_dir),
            "venues": venues,
            "years": years,
            "completed_count": sum(1 for item in summary_items if item.get("status") == "completed"),
            "failed_count": sum(1 for item in summary_items if item.get("status") == "failed"),
            "paper_count": sum(int(item.get("paper_count") or 0) for item in summary_items),
            "downloaded_count": sum(int(item.get("downloaded_count") or 0) for item in summary_items),
            "unresolved_count": sum(int(item.get("unresolved_count") or 0) for item in summary_items),
            "pending_count": sum(int(item.get("pending_count") or 0) for item in summary_items),
            "items": summary_items,
        }
        write_json(self.run_dir / "fetch_summary.json", summary)
        write_json(self.indexes_dir / "fetch_summary.json", summary)
        write_json(
            self.indexes_dir / "download_index.json",
            {
                "generated_at": finished_at,
                "output_root": str(self.output_root),
                "downloaded_papers": downloaded_papers,
            },
        )
        append_stage_trace(
            self.run_dir,
            "conference_fetch",
            "finished",
            completed_count=summary["completed_count"],
            failed_count=summary["failed_count"],
            paper_count=summary["paper_count"],
            downloaded_count=summary["downloaded_count"],
            unresolved_count=summary["unresolved_count"],
            pending_count=summary["pending_count"],
        )
        log_event(
            "info",
            "Conference fetch run finished",
            completed_count=summary["completed_count"],
            failed_count=summary["failed_count"],
            downloaded_count=summary["downloaded_count"],
            unresolved_count=summary["unresolved_count"],
            pending_count=summary["pending_count"],
        )
        return summary

    def _process_venue_year(self, adapter: Any, venue: str, year: int) -> dict[str, Any]:
        stage_prefix = f"{venue}.{year}"
        manifest_path = self.manifests_dir / f"{venue}-{year}.json"
        unresolved_path = self.unresolved_dir / f"{venue}-{year}.json"

        try:
            append_stage_trace(self.run_dir, f"discover.{stage_prefix}", "started")
            log_event("info", "Conference discovery started", venue=venue, year=year)
            index_url, papers = adapter.discover_papers(year, self.http)
            if self.limit_per_venue is not None:
                papers = papers[: max(0, self.limit_per_venue)]
            if not papers:
                raise RuntimeError("No papers discovered from venue index")
            append_stage_trace(self.run_dir, f"discover.{stage_prefix}", "finished", paper_count=len(papers), index_url=index_url)
            log_event("info", "Conference discovery finished", venue=venue, year=year, paper_count=len(papers), index_url=index_url)

            papers = self._parallel_map(
                papers,
                self.resolve_workers,
                lambda paper: self._enrich_paper(adapter, paper),
            )

            if self.enable_supplemental_lookups:
                append_stage_trace(self.run_dir, f"supplement.{stage_prefix}", "started", paper_count=len(papers))
                papers = self._parallel_map(
                    papers,
                    self.resolve_workers,
                    self._supplement_paper,
                )
                append_stage_trace(self.run_dir, f"supplement.{stage_prefix}", "finished", paper_count=len(papers))

            if not self.dry_run:
                append_stage_trace(self.run_dir, f"download.{stage_prefix}", "started", paper_count=len(papers))
                papers = self._parallel_map(
                    papers,
                    self.download_workers,
                    self._download_paper,
                )
                append_stage_trace(
                    self.run_dir,
                    f"download.{stage_prefix}",
                    "finished",
                    paper_count=len(papers),
                    downloaded_count=sum(1 for item in papers if item.status in {"downloaded", "existing"}),
                    unresolved_count=sum(1 for item in papers if item.status not in {"downloaded", "existing"}),
                )

            manifest = ConferenceManifest(
                venue=venue,
                year=year,
                index_url=index_url,
                generated_at=datetime.now(UTC).isoformat(),
                status="completed",
                items=sorted(papers, key=lambda item: normalize_title_key(item.title)),
                manifest_path=str(manifest_path),
                unresolved_path=str(unresolved_path),
            )
            manifest_payload = manifest.to_dict()
            write_json(manifest_path, manifest_payload)

            unresolved_items = [item.to_dict() for item in manifest.items if item.status == "unresolved"]
            write_json(
                unresolved_path,
                {
                    "venue": venue,
                    "year": year,
                    "generated_at": datetime.now(UTC).isoformat(),
                    "unresolved_count": len(unresolved_items),
                    "pending_count": manifest_payload["pending_count"],
                    "items": unresolved_items,
                },
            )

            return {
                "venue": venue,
                "year": year,
                "status": "completed",
                "index_url": index_url,
                "paper_count": len(manifest.items),
                "downloaded_count": manifest_payload["downloaded_count"],
                "unresolved_count": manifest_payload["unresolved_count"],
                "pending_count": manifest_payload["pending_count"],
                "manifest_path": str(manifest_path),
                "unresolved_path": str(unresolved_path),
            }
        except Exception as exc:
            log_event("error", "Conference venue-year failed", venue=venue, year=year, error=str(exc))
            append_stage_trace(self.run_dir, f"discover.{stage_prefix}", "error", error=str(exc))
            failure_manifest = ConferenceManifest(
                venue=venue,
                year=year,
                index_url=adapter.build_index_url(year),
                generated_at=datetime.now(UTC).isoformat(),
                status="failed",
                items=[],
                manifest_path=str(manifest_path),
                unresolved_path=str(unresolved_path),
                error=str(exc),
            )
            write_json(manifest_path, failure_manifest.to_dict())
            write_json(
                unresolved_path,
                {
                    "venue": venue,
                    "year": year,
                    "generated_at": datetime.now(UTC).isoformat(),
                    "unresolved_count": 0,
                    "pending_count": 0,
                    "items": [],
                    "error": str(exc),
                },
            )
            return {
                "venue": venue,
                "year": year,
                "status": "failed",
                "index_url": adapter.build_index_url(year),
                "paper_count": 0,
                "downloaded_count": 0,
                "unresolved_count": 0,
                "pending_count": 0,
                "manifest_path": str(manifest_path),
                "unresolved_path": str(unresolved_path),
                "error": str(exc),
            }

    def _parallel_map(self, papers: list[ConferencePaper], worker_count: int, fn: Any) -> list[ConferencePaper]:
        if len(papers) <= 1 or worker_count <= 1:
            return [fn(paper) for paper in papers]

        results: list[ConferencePaper | None] = [None] * len(papers)
        with ThreadPoolExecutor(max_workers=min(worker_count, len(papers))) as executor:
            future_map = {executor.submit(fn, paper): index for index, paper in enumerate(papers)}
            for future in as_completed(future_map):
                index = future_map[future]
                results[index] = future.result()
        return [paper for paper in results if paper is not None]

    def _enrich_paper(self, adapter: Any, paper: ConferencePaper) -> ConferencePaper:
        log_event("info", "Conference paper enrich started", venue=paper.venue, year=paper.year, title=paper.title)
        try:
            enriched = adapter.enrich_paper(paper, self.http)
            enriched.add_trace("enrich:adapter_completed")
            log_event(
                "info",
                "Conference paper enrich finished",
                venue=enriched.venue,
                year=enriched.year,
                title=enriched.title,
                has_pdf=bool(enriched.pdf_url),
                has_preprint=bool(enriched.preprint_url),
                has_doi=bool(enriched.doi_url),
            )
            return enriched
        except ReturnedPDFError as exc:
            paper.pdf_url = maybe_promote_to_pdf_url(exc.final_url)
            paper.add_source_url(paper.pdf_url)
            paper.add_trace("enrich:detail_url_redirected_to_pdf")
            return paper
        except Exception as exc:
            paper.add_trace(f"enrich:error={exc}")
            log_event("warning", "Conference paper enrich failed", venue=paper.venue, year=paper.year, title=paper.title, error=str(exc))
            return paper

    def _supplement_paper(self, paper: ConferencePaper) -> ConferencePaper:
        if paper.pdf_url and paper.preprint_url and paper.doi_url and paper.authors:
            paper.add_trace("supplement:skipped_already_rich")
            return paper

        paper = self._supplement_from_dblp(paper)
        if not paper.pdf_url and not paper.preprint_url:
            paper = self._supplement_from_openalex(paper)
        if not paper.pdf_url and not paper.preprint_url and not paper.alternate_urls:
            paper = self._supplement_from_arxiv(paper)
        return paper

    @staticmethod
    def _looks_like_preprint_host(url: str) -> bool:
        lowered = str(url or "").strip().lower()
        return any(
            marker in lowered
            for marker in (
                "arxiv.org",
                "openreview.net",
                "hal.science",
                "hal.archives-ouvertes.fr",
                "researchgate.net",
                "zenodo.org",
                "osf.io",
            )
        )

    @staticmethod
    def _looks_like_downloadable_source(url: str) -> bool:
        lowered = str(url or "").strip().lower()
        if not lowered:
            return False
        if looks_like_pdf_url(lowered):
            return True
        return any(
            marker in lowered
            for marker in (
                "/pdf/",
                "/papers/",
                "/paper/",
                "/article/download",
                "/download/",
                "author-accepted",
                "accepted-manuscript",
            )
        )

    def _supplement_from_dblp(self, paper: ConferencePaper) -> ConferencePaper:
        try:
            params = urlencode({"q": f'"{paper.title}"', "h": 5, "format": "json"})
            payload, final_url = self.http.fetch_json(f"https://dblp.org/search/publ/api?{params}")
            hits = (((payload or {}).get("result") or {}).get("hits") or {}).get("hit") or []
            if isinstance(hits, dict):
                hits = [hits]

            matched_hit = None
            for hit in hits:
                info = hit.get("info") or {}
                candidate_title = str(info.get("title") or "").strip()
                candidate_year = str(info.get("year") or "").strip()
                if candidate_year and candidate_year != str(paper.year):
                    continue
                if titles_match(candidate_title, paper.title):
                    matched_hit = info
                    break
            if matched_hit is None:
                paper.add_trace("supplement:dblp_no_exact_match")
                return paper

            if not paper.authors:
                raw_authors = ((matched_hit.get("authors") or {}).get("author")) or []
                if isinstance(raw_authors, str):
                    raw_authors = [raw_authors]
                paper.authors = [str(item).strip() for item in raw_authors if str(item).strip()]

            dblp_url = str(matched_hit.get("url") or final_url).strip()
            paper.add_source_url(dblp_url)
            paper.metadata["dblp_url"] = dblp_url

            ee_links = matched_hit.get("ee") or []
            if isinstance(ee_links, str):
                ee_links = [ee_links]
            external_candidates: list[str] = []
            matched_hit_doi = str(matched_hit.get("doi") or "").strip()
            canonical_doi_url = f"https://doi.org/{matched_hit_doi}" if matched_hit_doi else ""
            for link in ee_links:
                normalized = str(link).strip()
                if not normalized:
                    continue
                paper.add_source_url(normalized)
                lowered = normalized.lower()
                if not paper.pdf_url and looks_like_pdf_url(lowered):
                    paper.pdf_url = maybe_promote_to_pdf_url(normalized)
                elif not paper.preprint_url and self._looks_like_preprint_host(lowered):
                    paper.preprint_url = maybe_promote_to_pdf_url(normalized) if looks_like_pdf_url(lowered) else normalized
                elif not paper.doi_url and ("doi.org/" in lowered or "dl.acm.org/" in lowered):
                    paper.doi_url = normalized
                else:
                    external_candidates.append(normalized)

            for candidate in external_candidates:
                if candidate != paper.doi_url and candidate != paper.preprint_url and candidate != paper.pdf_url:
                    paper.add_alternate_url(candidate)

            if canonical_doi_url:
                current_doi_url = str(paper.doi_url or "").strip()
                if current_doi_url and current_doi_url != canonical_doi_url:
                    paper.metadata["pre_dblp_doi_url"] = current_doi_url
                    if current_doi_url not in {paper.pdf_url, paper.preprint_url}:
                        paper.add_alternate_url(current_doi_url)
                    paper.add_trace("supplement:dblp_corrected_doi")
                paper.doi_url = canonical_doi_url
                paper.add_source_url(paper.doi_url)

            if external_candidates:
                paper.metadata["dblp_ee_urls"] = external_candidates
            paper.add_trace("supplement:dblp_match_applied")
            return paper
        except Exception as exc:
            paper.add_trace(f"supplement:dblp_error={exc}")
            return paper

    def _supplement_from_openalex(self, paper: ConferencePaper) -> ConferencePaper:
        try:
            if paper.doi_url:
                query_url = f"https://api.openalex.org/works?filter=doi:{paper.doi_url}&per-page=5"
            else:
                params = urlencode({"search": paper.title, "per-page": 5})
                query_url = f"https://api.openalex.org/works?{params}"

            payload, final_url = self.http.fetch_json(query_url)
            results = list((payload or {}).get("results") or [])
            matched_result = None
            for result in results:
                candidate_title = str(result.get("display_name") or "").strip()
                publication_year = str(result.get("publication_year") or "").strip()
                if publication_year and publication_year != str(paper.year):
                    continue
                if titles_match(candidate_title, paper.title):
                    matched_result = result
                    break
            if matched_result is None:
                paper.add_trace("supplement:openalex_no_exact_match")
                return paper

            oa_candidates: list[str] = []
            host_candidates: list[str] = []
            locations = []
            for key in ("best_oa_location", "primary_location"):
                location = matched_result.get(key)
                if isinstance(location, dict):
                    locations.append(location)
            for location in matched_result.get("locations") or []:
                if isinstance(location, dict):
                    locations.append(location)

            for location in locations:
                pdf_url = str(location.get("pdf_url") or "").strip()
                landing_url = str(location.get("landing_page_url") or "").strip()
                source = location.get("source") or {}
                source_name = str(source.get("display_name") or "").strip()

                if pdf_url:
                    if self._looks_like_preprint_host(pdf_url):
                        if not paper.preprint_url:
                            paper.preprint_url = pdf_url
                        paper.add_source_url(pdf_url)
                    elif not paper.pdf_url and self._looks_like_downloadable_source(pdf_url):
                        paper.pdf_url = pdf_url
                        paper.add_source_url(pdf_url)
                    else:
                        oa_candidates.append(pdf_url)

                if landing_url:
                    if self._looks_like_preprint_host(landing_url):
                        if not paper.preprint_url:
                            paper.preprint_url = landing_url
                        paper.add_source_url(landing_url)
                    else:
                        host_candidates.append(landing_url)

                if source_name:
                    paper.metadata.setdefault("openalex_sources", [])
                    if source_name not in paper.metadata["openalex_sources"]:
                        paper.metadata["openalex_sources"].append(source_name)

            for candidate in oa_candidates + host_candidates:
                if candidate not in {paper.pdf_url, paper.preprint_url, paper.doi_url}:
                    paper.add_alternate_url(candidate)

            open_access = matched_result.get("open_access") or {}
            oa_url = str(open_access.get("oa_url") or "").strip()
            if oa_url and oa_url not in {paper.pdf_url, paper.preprint_url, paper.doi_url}:
                if self._looks_like_preprint_host(oa_url) and not paper.preprint_url:
                    paper.preprint_url = oa_url
                    paper.add_source_url(oa_url)
                else:
                    paper.add_alternate_url(oa_url)

            ids = matched_result.get("ids") or {}
            if not paper.doi_url:
                doi_candidate = str(ids.get("doi") or "").strip()
                if doi_candidate:
                    paper.doi_url = doi_candidate
                    paper.add_source_url(doi_candidate)

            paper.metadata["openalex_id"] = matched_result.get("id")
            paper.metadata["openalex_query_url"] = final_url
            paper.add_trace("supplement:openalex_match_applied")
            return paper
        except Exception as exc:
            paper.add_trace(f"supplement:openalex_error={exc}")
            return paper

    def _supplement_from_arxiv(self, paper: ConferencePaper) -> ConferencePaper:
        try:
            params = urlencode({"search_query": f'ti:"{paper.title}"', "start": 0, "max_results": 5})
            root, final_url = self.http.fetch_xml_root(f"https://export.arxiv.org/api/query?{params}")
            best_entry = None
            for entry in root.findall("atom:entry", ATOM_NAMESPACE):
                title = (entry.findtext("atom:title", default="", namespaces=ATOM_NAMESPACE) or "").strip()
                if titles_match(title, paper.title):
                    best_entry = entry
                    break
            if best_entry is None:
                paper.add_trace("supplement:arxiv_no_exact_match")
                return paper

            entry_id = (best_entry.findtext("atom:id", default="", namespaces=ATOM_NAMESPACE) or "").strip()
            if entry_id:
                pdf_url = maybe_promote_to_pdf_url(entry_id)
                paper.preprint_url = entry_id
                paper.pdf_url = paper.pdf_url or pdf_url
                paper.add_source_url(entry_id)
                paper.add_source_url(pdf_url)
            if not paper.authors:
                authors = []
                for author in best_entry.findall("atom:author", ATOM_NAMESPACE):
                    name = (author.findtext("atom:name", default="", namespaces=ATOM_NAMESPACE) or "").strip()
                    if name:
                        authors.append(name)
                paper.authors = authors
            paper.metadata["arxiv_query_url"] = final_url
            paper.add_trace("supplement:arxiv_match_applied")
            return paper
        except Exception as exc:
            paper.add_trace(f"supplement:arxiv_error={exc}")
            return paper

    def _resolve_pdf_url(self, paper: ConferencePaper) -> str | None:
        candidates: list[str] = []
        for label, candidate in (
            ("pdf_url", paper.pdf_url),
            ("preprint_url", paper.preprint_url),
            *[(f"alternate_url_{index}", url) for index, url in enumerate(paper.alternate_urls)],
            ("doi_url", paper.doi_url),
            ("detail_url", paper.detail_url),
            ("landing_page_url", paper.landing_page_url),
        ):
            normalized = str(candidate or "").strip()
            if not normalized:
                continue
            if label == "landing_page_url" and self._looks_like_listing_page_url(normalized):
                continue
            if normalized not in candidates:
                candidates.append(normalized)

        for candidate in candidates:
            resolved = self._resolve_pdf_candidate(candidate)
            if resolved:
                paper.add_trace(f"resolve:pdf_candidate={candidate} -> {resolved}")
                return resolved

        direct_doi_pdf = infer_doi_pdf_candidate(paper.doi_url or "")
        if direct_doi_pdf:
            paper.add_trace(f"resolve:doi_pdf_guess={direct_doi_pdf}")
            return direct_doi_pdf

        paper.add_trace("resolve:no_pdf_url_found")
        return None

    @staticmethod
    def _looks_like_listing_page_url(url: str) -> bool:
        normalized = str(url or "").strip().lower()
        return any(
            marker in normalized
            for marker in (
                "/technical-sessions",
                "/track/",
                "/accepted.html",
            )
        )

    def _resolve_pdf_candidate(self, url: str) -> str | None:
        candidate = maybe_promote_to_pdf_url(url)
        visited: set[str] = set()

        while candidate and candidate not in visited:
            visited.add(candidate)
            if looks_like_pdf_url(candidate):
                return candidate

            guessed_pdf = infer_doi_pdf_candidate(candidate)
            if guessed_pdf and guessed_pdf not in visited:
                candidate = guessed_pdf
                continue

            try:
                document = self.http.fetch_document(candidate)
            except ReturnedPDFError as exc:
                return maybe_promote_to_pdf_url(exc.final_url or candidate)
            except Exception:
                return None

            metadata = extract_document_paper_metadata(document)
            pdf_candidates = list(metadata.get("pdf_candidates") or [])
            if pdf_candidates:
                return str(pdf_candidates[0]).strip()

            preprint_url = str(metadata.get("preprint_url") or "").strip()
            if preprint_url and preprint_url not in visited:
                candidate = preprint_url
                continue

            guessed_pdf = infer_doi_pdf_candidate(document.final_url)
            if guessed_pdf:
                return guessed_pdf

            return None

        return None

    def _destination_for_paper(self, paper: ConferencePaper) -> Path:
        directory = self.downloads_dir / paper.venue / str(paper.year)
        filename = f"{sanitize_filename(paper.title, fallback=paper.paper_id(), max_length=150)} [{paper.venue.upper()} {paper.year}].pdf"
        return directory / filename

    def _download_paper(self, paper: ConferencePaper) -> ConferencePaper:
        destination = self._destination_for_paper(paper)

        if self.skip_existing and destination.exists() and destination.stat().st_size > 0:
            paper.status = "existing"
            paper.download_path = str(destination)
            paper.download_url = paper.download_url or paper.pdf_url or paper.preprint_url or paper.doi_url
            paper.add_trace("download:reused_existing_file")
            return paper

        resolved_url = self._resolve_pdf_url(paper)
        if not resolved_url:
            paper.status = "unresolved"
            paper.download_error = "No PDF URL could be resolved"
            paper.add_note("未能解析到可下载 PDF，已写入 unresolved 清单。")
            return paper

        try:
            download_meta = self.http.download_pdf(resolved_url, destination)
            paper.download_url = str(download_meta["final_url"] or resolved_url)
            paper.download_path = str(destination)
            paper.status = "downloaded"
            paper.metadata["download_content_type"] = download_meta["content_type"]
            paper.metadata["download_bytes"] = download_meta["byte_count"]
            if download_meta.get("transport"):
                paper.metadata["download_transport"] = download_meta["transport"]
            paper.add_source_url(resolved_url)
            paper.add_source_url(paper.download_url)
            paper.add_trace(f"download:success={paper.download_url}")
        except Exception as exc:
            paper.status = "unresolved"
            paper.download_error = str(exc)
            paper.add_note("PDF 下载失败，详情见 resolution_trace 与 run.log。")
            paper.add_trace(f"download:error={exc}")
        return paper


def _default_recent_years() -> list[int]:
    current_year = datetime.now().year
    return [current_year - 3, current_year - 2, current_year - 1]


def _parse_csv_items(raw_value: str) -> list[str]:
    return [item.strip().lower() for item in raw_value.split(",") if item.strip()]


def _parse_years(raw_value: str | None) -> list[int]:
    if not raw_value:
        return _default_recent_years()

    years: list[int] = []
    for token in [part.strip() for part in raw_value.split(",") if part.strip()]:
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            start_year = int(start_text)
            end_year = int(end_text)
            step = 1 if end_year >= start_year else -1
            years.extend(list(range(start_year, end_year + step, step)))
        else:
            years.append(int(token))
    return sorted({year for year in years})


def _resolve_env_value(explicit_env_name: str | None, fallback_env_name: str) -> tuple[str | None, str | None]:
    candidate_names = [explicit_env_name] if explicit_env_name else []
    if fallback_env_name not in candidate_names:
        candidate_names.append(fallback_env_name)
    for env_name in candidate_names:
        if not env_name:
            continue
        value = os.getenv(env_name)
        if value:
            return value, env_name
    return None, None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover and download conference paper PDFs for OSDI / SOSP / PLDI into a local batch workspace.",
    )
    parser.add_argument("--venues", default="osdi,sosp,pldi", help="Comma-separated venues. Supported: osdi,sosp,pldi")
    parser.add_argument("--years", help="Comma-separated years or ranges, for example 2023,2024,2025 or 2023-2025")
    parser.add_argument("--output-root", default="conference-papers", help="Workspace root for manifests, downloads, unresolved, and logs")
    parser.add_argument("--resolve-workers", type=int, default=6, help="Concurrent workers for detail-page enrichment and metadata supplement")
    parser.add_argument("--download-workers", type=int, default=6, help="Concurrent workers for PDF downloads")
    parser.add_argument("--http-timeout", type=int, default=20, help="HTTP timeout in seconds")
    parser.add_argument("--http-retry-attempts", type=int, default=3, help="Retry attempts for transient HTTP failures")
    parser.add_argument("--http-retry-backoff", type=float, default=1.5, help="Base backoff in seconds for transient HTTP failures")
    parser.add_argument("--html-max-bytes", type=int, default=2_000_000, help="Maximum bytes to read when fetching HTML / JSON / XML")
    parser.add_argument("--download-max-bytes", type=int, default=150_000_000, help="Maximum bytes to allow for a single downloaded PDF")
    parser.add_argument("--http-user-agent", default=DEFAULT_USER_AGENT, help="User-Agent used for venue discovery and PDF downloads")
    parser.add_argument(
        "--disable-browser-like-http",
        action="store_true",
        help="Disable browser-style Accept-Language / cache / upgrade headers on HTTP requests",
    )
    parser.add_argument("--http-cookie-header-env", help=f"Env var containing a raw Cookie header, default: {DEFAULT_COOKIE_HEADER_ENV}")
    parser.add_argument("--http-cookie-file", help=f"Path to a raw Cookie header file or Netscape cookie jar, default env: {DEFAULT_COOKIE_FILE_ENV}")
    parser.add_argument("--acm-cookie-header-env", help=f"Env var containing a raw ACM Cookie header, default: {DEFAULT_ACM_COOKIE_HEADER_ENV}")
    parser.add_argument("--acm-cookie-file", help=f"Path to ACM cookie header file or Netscape cookie jar, default env: {DEFAULT_ACM_COOKIE_FILE_ENV}")
    parser.add_argument(
        "--acm-browser-fallback",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=f"Enable Playwright browser fallback for ACM PDF downloads, default env: {DEFAULT_ACM_BROWSER_FALLBACK_ENV}",
    )
    parser.add_argument(
        "--playwright-cdp-url",
        help=f"Connect Playwright to an existing Chrome DevTools endpoint, default env: {DEFAULT_PLAYWRIGHT_CDP_URL_ENV}",
    )
    parser.add_argument(
        "--playwright-browser-executable",
        help=f"Chrome executable path for Playwright persistent launch, default env: {DEFAULT_PLAYWRIGHT_BROWSER_EXECUTABLE_ENV}",
    )
    parser.add_argument(
        "--playwright-user-data-dir",
        help=f"Chrome user data dir for Playwright persistent launch, default env: {DEFAULT_PLAYWRIGHT_USER_DATA_DIR_ENV}",
    )
    parser.add_argument(
        "--playwright-profile-directory",
        help=f"Chrome profile directory name such as Default or 'Profile 1', default env: {DEFAULT_PLAYWRIGHT_PROFILE_DIRECTORY_ENV}",
    )
    parser.add_argument(
        "--playwright-headless",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=f"Launch Playwright Chrome in headless mode when not using CDP, default env: {DEFAULT_PLAYWRIGHT_HEADLESS_ENV}",
    )
    parser.add_argument(
        "--playwright-launch-timeout-ms",
        type=int,
        help=f"Playwright browser launch/connect timeout in milliseconds, default env: {DEFAULT_PLAYWRIGHT_LAUNCH_TIMEOUT_MS_ENV}",
    )
    parser.add_argument(
        "--playwright-navigation-timeout-ms",
        type=int,
        help=f"Playwright navigation timeout in milliseconds, default env: {DEFAULT_PLAYWRIGHT_NAVIGATION_TIMEOUT_MS_ENV}",
    )
    parser.add_argument("--limit-per-venue", type=int, help="Only keep the first N discovered papers per venue-year")
    parser.add_argument("--skip-existing", action="store_true", help="Reuse an existing downloaded PDF if the destination path already exists")
    parser.add_argument("--dry-run", action="store_true", help="Only generate manifests and unresolved lists, without downloading PDFs")
    parser.add_argument(
        "--disable-supplemental-lookups",
        action="store_true",
        help="Disable DBLP / arXiv supplemental metadata resolution",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level, for example INFO or DEBUG")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    venues = _parse_csv_items(args.venues)
    years = _parse_years(args.years)
    supported_venues = {"osdi", "sosp", "pldi"}
    unknown_venues = [venue for venue in venues if venue not in supported_venues]
    if unknown_venues:
        parser.exit(status=1, message=f"Unsupported venues: {', '.join(unknown_venues)}\n")

    output_root = Path(args.output_root).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = output_root / "logs" / f"{timestamp}-{'-'.join(venues)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    default_cookie_header, default_cookie_env_name = _resolve_env_value(args.http_cookie_header_env, DEFAULT_COOKIE_HEADER_ENV)
    acm_cookie_header, acm_cookie_env_name = _resolve_env_value(args.acm_cookie_header_env, DEFAULT_ACM_COOKIE_HEADER_ENV)
    default_cookie_file_value, default_cookie_file_env_name = _resolve_env_value(None, DEFAULT_COOKIE_FILE_ENV)
    acm_cookie_file_value, acm_cookie_file_env_name = _resolve_env_value(None, DEFAULT_ACM_COOKIE_FILE_ENV)
    default_cookie_file = Path(args.http_cookie_file or default_cookie_file_value).expanduser().resolve() if (args.http_cookie_file or default_cookie_file_value) else None
    acm_cookie_file = Path(args.acm_cookie_file or acm_cookie_file_value).expanduser().resolve() if (args.acm_cookie_file or acm_cookie_file_value) else None
    cookie_source = CookieHeaderSource.from_inputs(
        cookie_header=default_cookie_header,
        cookie_file=default_cookie_file,
        source_label=default_cookie_env_name or (str(default_cookie_file) if default_cookie_file else None),
    )
    acm_cookie_source = CookieHeaderSource.from_inputs(
        cookie_header=acm_cookie_header,
        cookie_file=acm_cookie_file,
        source_label=acm_cookie_env_name or (str(acm_cookie_file) if acm_cookie_file else None),
    )
    playwright_env = resolve_playwright_env_config()
    playwright_enabled = playwright_env["enabled"] if args.acm_browser_fallback is None else bool(args.acm_browser_fallback)
    playwright_headless = playwright_env["headless"] if args.playwright_headless is None else bool(args.playwright_headless)
    playwright_config = build_playwright_download_config(
        enabled=playwright_enabled,
        cdp_url=args.playwright_cdp_url or playwright_env["cdp_url"],
        browser_executable_path=args.playwright_browser_executable or playwright_env["browser_executable_path"],
        user_data_dir=args.playwright_user_data_dir or playwright_env["user_data_dir"],
        profile_directory=args.playwright_profile_directory or playwright_env["profile_directory"],
        headless=playwright_headless,
        launch_timeout_ms=args.playwright_launch_timeout_ms
        if args.playwright_launch_timeout_ms is not None
        else playwright_env["launch_timeout_ms"],
        navigation_timeout_ms=args.playwright_navigation_timeout_ms
        if args.playwright_navigation_timeout_ms is not None
        else playwright_env["navigation_timeout_ms"],
    )
    browser_pdf_downloader = (
        PlaywrightPDFDownloader(
            config=playwright_config,
            download_max_bytes=args.download_max_bytes,
            user_agent=args.http_user_agent,
            accept_language=DEFAULT_ACCEPT_LANGUAGE,
        )
        if playwright_config is not None
        else None
    )

    configure_logging(level=args.log_level, run_dir=run_dir)
    log_event(
        "info",
        "Conference fetch CLI parsed",
        venues=venues,
        years=years,
        output_root=output_root,
        resolve_workers=args.resolve_workers,
        download_workers=args.download_workers,
        skip_existing=args.skip_existing,
        dry_run=args.dry_run,
        supplemental_lookups=not args.disable_supplemental_lookups,
        browser_like_http=not args.disable_browser_like_http,
        http_user_agent=args.http_user_agent,
        cookie_header_source=cookie_source.source_label if cookie_source else None,
        acm_cookie_header_source=acm_cookie_source.source_label if acm_cookie_source else None,
        acm_browser_fallback=playwright_config is not None,
        playwright_mode=playwright_config.mode_label if playwright_config else None,
        playwright_cdp_url=playwright_config.cdp_url if playwright_config else None,
        playwright_user_data_dir=playwright_config.user_data_dir if playwright_config else None,
        playwright_profile_directory=playwright_config.profile_directory if playwright_config else None,
        playwright_headless=playwright_config.headless if playwright_config else None,
    )

    service = ConferenceFetchService(
        output_root=output_root,
        run_dir=run_dir,
        timeout_seconds=args.http_timeout,
        html_max_bytes=args.html_max_bytes,
        download_max_bytes=args.download_max_bytes,
        resolve_workers=args.resolve_workers,
        download_workers=args.download_workers,
        retry_attempts=args.http_retry_attempts,
        retry_backoff_seconds=args.http_retry_backoff,
        skip_existing=args.skip_existing,
        dry_run=args.dry_run,
        enable_supplemental_lookups=not args.disable_supplemental_lookups,
        limit_per_venue=args.limit_per_venue,
        http_user_agent=args.http_user_agent,
        browser_like_http=not args.disable_browser_like_http,
        cookie_source=cookie_source,
        acm_cookie_source=acm_cookie_source,
        browser_pdf_downloader=browser_pdf_downloader,
    )

    summary = service.run(venues, years)
    print(f"Conference workspace: {output_root}")
    print(f"Run log directory: {run_dir}")
    print(f"Downloaded PDFs root: {output_root / 'downloads'}")
    print(f"Manifests root: {output_root / 'manifests'}")
    print(f"Unresolved root: {output_root / 'unresolved'}")
    print(
        "Venue-years completed: "
        f"{summary['completed_count']} | failed: {summary['failed_count']} | "
        f"papers: {summary['paper_count']} | downloaded: {summary['downloaded_count']} | "
        f"unresolved: {summary['unresolved_count']} | pending: {summary['pending_count']}"
    )
    if summary["failed_count"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
