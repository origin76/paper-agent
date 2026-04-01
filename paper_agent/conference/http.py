from __future__ import annotations

import json
import re
import threading
import time
from http.cookiejar import LoadError, MozillaCookieJar
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree

from .parsing import parse_html_document
from paper_agent.browser.playwright_download import BrowserPDFDownloader
from paper_agent.runtime import log_event


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
ACM_HOST_SUFFIX = ".acm.org"
HOST_REQUEST_MIN_INTERVALS: tuple[tuple[str, float], ...] = (
    ("api.openalex.org", 0.75),
    ("export.arxiv.org", 1.50),
    ("arxiv.org", 1.00),
    ("dblp.org", 0.35),
)
SUPPLEMENTAL_RATE_LIMIT_SHORT_CIRCUIT_HOSTS: tuple[str, ...] = (
    "api.openalex.org",
    "export.arxiv.org",
    "arxiv.org",
)
MAX_RETRY_AFTER_SECONDS = 30.0


def is_acm_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host == "acm.org" or host.endswith(ACM_HOST_SUFFIX)


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
        self._host_rate_limit_lock = threading.Lock()
        self._host_next_allowed_at: dict[str, float] = {}

    def _cookie_header_for_url(self, url: str) -> str | None:
        if is_acm_url(url) and self.acm_cookie_source is not None:
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

    @staticmethod
    def _normalized_host(url: str) -> str:
        return urlparse(url).netloc.lower().split(":", 1)[0]

    def _host_min_interval_seconds(self, url: str) -> float:
        host = self._normalized_host(url)
        for pattern, seconds in HOST_REQUEST_MIN_INTERVALS:
            if host == pattern or host.endswith(f".{pattern}"):
                return seconds
        return 0.0

    def _throttle_for_host(self, url: str, *, stage: str) -> None:
        host = self._normalized_host(url)
        min_interval_seconds = self._host_min_interval_seconds(url)
        if not host or min_interval_seconds <= 0:
            return

        with self._host_rate_limit_lock:
            now = time.monotonic()
            next_allowed_at = self._host_next_allowed_at.get(host, 0.0)
            wait_seconds = max(0.0, next_allowed_at - now)
            reserved_start = max(now, next_allowed_at)
            self._host_next_allowed_at[host] = reserved_start + min_interval_seconds

        if wait_seconds <= 0:
            return

        log_event(
            "info",
            "Conference HTTP host throttle waiting",
            url=url,
            host=host,
            stage=stage,
            wait_seconds=f"{wait_seconds:.2f}",
            min_interval_seconds=f"{min_interval_seconds:.2f}",
        )
        time.sleep(wait_seconds)

    @staticmethod
    def _retry_after_seconds(exc: Exception) -> float | None:
        if not isinstance(exc, HTTPError) or exc.headers is None:
            return None
        retry_after = str(exc.headers.get("Retry-After") or "").strip()
        if not retry_after:
            return None
        try:
            return min(MAX_RETRY_AFTER_SECONDS, max(0.0, float(retry_after)))
        except ValueError:
            return None

    def _retry_sleep_seconds(self, url: str, attempt: int, exc: Exception) -> float:
        base_sleep = max(0.0, self.retry_backoff_seconds * attempt)
        host_interval_seconds = self._host_min_interval_seconds(url)
        penalty_sleep = 0.0
        if isinstance(exc, HTTPError) and exc.code == 429:
            penalty_sleep = max(host_interval_seconds * (attempt + 1), self.retry_backoff_seconds * max(2, attempt))
        retry_after_seconds = self._retry_after_seconds(exc) or 0.0
        return max(base_sleep, penalty_sleep, retry_after_seconds)

    def _should_short_circuit_rate_limit(self, url: str, exc: Exception) -> bool:
        if not isinstance(exc, HTTPError) or exc.code != 429:
            return False
        host = self._normalized_host(url)
        return any(host == pattern or host.endswith(f".{pattern}") for pattern in SUPPLEMENTAL_RATE_LIMIT_SHORT_CIRCUIT_HOSTS)

    def fetch_text(self, url: str) -> tuple[str, str, str]:
        start_time = time.perf_counter()
        log_event("info", "Conference HTTP text request started", url=url)
        for attempt in range(1, self.retry_attempts + 1):
            try:
                self._throttle_for_host(url, stage="text")
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
                if self._should_short_circuit_rate_limit(url, exc):
                    log_event(
                        "warning",
                        "Conference HTTP retry short-circuited after rate limit",
                        url=url,
                        attempt=attempt,
                        stage="text",
                    )
                    raise
                if not should_retry:
                    raise
                sleep_seconds = self._retry_sleep_seconds(url, attempt, exc)
                if sleep_seconds > 0:
                    log_event(
                        "info",
                        "Conference HTTP retry backoff scheduled",
                        url=url,
                        attempt=attempt,
                        stage="text",
                        sleep_seconds=f"{sleep_seconds:.2f}",
                    )
                    time.sleep(sleep_seconds)

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
        if self._must_force_browser_download(url):
            if self.browser_pdf_downloader is None:
                raise RuntimeError(
                    "ACM PDF URLs require Playwright transport because direct HTTP access is expected to fail. "
                    "Configure PAPER_AGENT_PLAYWRIGHT_CDP_URL or PAPER_AGENT_PLAYWRIGHT_USER_DATA_DIR."
                )
            try:
                log_event(
                    "info",
                    "Conference PDF forcing Playwright transport for ACM URL",
                    url=url,
                    destination=destination,
                )
                return self.browser_pdf_downloader.download_pdf(
                    url,
                    destination,
                    referer=derive_pdf_download_referer(url),
                )
            except Exception as exc:
                log_event(
                    "error",
                    "Conference PDF Playwright transport failed for ACM URL",
                    url=url,
                    destination=destination,
                    error=str(exc),
                )
                raise
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

    @staticmethod
    def _must_force_browser_download(url: str) -> bool:
        return is_acm_url(url)

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
                self._throttle_for_host(url, stage="pdf")
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
                sleep_seconds = self._retry_sleep_seconds(url, attempt, exc)
                if sleep_seconds > 0:
                    log_event(
                        "info",
                        "Conference HTTP retry backoff scheduled",
                        url=url,
                        attempt=attempt,
                        stage="pdf",
                        sleep_seconds=f"{sleep_seconds:.2f}",
                    )
                    time.sleep(sleep_seconds)

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
        if not is_acm_url(url):
            return False
        if isinstance(exc, HTTPError):
            return exc.code in {401, 403, 429}
        if isinstance(exc, URLError):
            message = str(exc.reason).lower()
            if any(
                marker in message
                for marker in (
                    "nodename nor servname provided",
                    "name or service not known",
                    "temporary failure in name resolution",
                    "getaddrinfo failed",
                    "connection refused",
                    "network is unreachable",
                )
            ):
                return True
        message = str(exc).lower()
        return any(
            marker in message
            for marker in (
                "forbidden",
                "not a pdf",
                "cloudflare",
                "challenge",
                "blocked",
                "nodename nor servname provided",
                "name or service not known",
                "temporary failure in name resolution",
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
