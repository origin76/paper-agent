from __future__ import annotations

import base64
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from paper_agent.runtime import log_event


DEFAULT_ACM_BROWSER_FALLBACK_ENV = "PAPER_AGENT_ACM_BROWSER_FALLBACK"
DEFAULT_PLAYWRIGHT_CDP_URL_ENV = "PAPER_AGENT_PLAYWRIGHT_CDP_URL"
DEFAULT_PLAYWRIGHT_BROWSER_EXECUTABLE_ENV = "PAPER_AGENT_PLAYWRIGHT_BROWSER_EXECUTABLE"
DEFAULT_PLAYWRIGHT_USER_DATA_DIR_ENV = "PAPER_AGENT_PLAYWRIGHT_USER_DATA_DIR"
DEFAULT_PLAYWRIGHT_PROFILE_DIRECTORY_ENV = "PAPER_AGENT_PLAYWRIGHT_PROFILE_DIRECTORY"
DEFAULT_PLAYWRIGHT_HEADLESS_ENV = "PAPER_AGENT_PLAYWRIGHT_HEADLESS"
DEFAULT_PLAYWRIGHT_LAUNCH_TIMEOUT_MS_ENV = "PAPER_AGENT_PLAYWRIGHT_LAUNCH_TIMEOUT_MS"
DEFAULT_PLAYWRIGHT_NAVIGATION_TIMEOUT_MS_ENV = "PAPER_AGENT_PLAYWRIGHT_NAVIGATION_TIMEOUT_MS"


class BrowserPDFDownloader(Protocol):
    def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class PlaywrightDownloadConfig:
    cdp_url: str | None = None
    browser_executable_path: str | None = None
    user_data_dir: Path | None = None
    profile_directory: str | None = None
    headless: bool = False
    launch_timeout_ms: int = 30_000
    navigation_timeout_ms: int = 45_000

    @property
    def mode_label(self) -> str:
        if self.cdp_url:
            return "cdp"
        if self.user_data_dir:
            return "persistent-context"
        return "unconfigured"


def parse_bool_env(raw_value: str | None, default: bool = False) -> bool:
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "y", "on"}


def parse_int_env(raw_value: str | None, default: int) -> int:
    if raw_value is None or not raw_value.strip():
        return default
    try:
        return int(raw_value.strip())
    except ValueError:
        return default


def default_chrome_executable_path() -> str | None:
    candidates = [
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path.home() / "Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return None


class PlaywrightPDFDownloader:
    def __init__(
        self,
        *,
        config: PlaywrightDownloadConfig,
        download_max_bytes: int,
        user_agent: str,
        accept_language: str,
    ) -> None:
        self.config = config
        self.download_max_bytes = download_max_bytes
        self.user_agent = user_agent
        self.accept_language = accept_language
        self._lock = threading.Lock()

    def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, Any]:
        with self._lock:
            return self._download_pdf_locked(url, destination, referer=referer)

    def _download_pdf_locked(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, Any]:
        sync_playwright, playwright_timeout_error, playwright_error = _import_playwright_sync()

        start_time = time.perf_counter()
        log_event(
            "info",
            "Playwright PDF download started",
            url=url,
            destination=destination,
            mode=self.config.mode_label,
            cdp_url=self.config.cdp_url,
            user_data_dir=self.config.user_data_dir,
            profile_directory=self.config.profile_directory,
            headless=self.config.headless,
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        if tmp_path.exists():
            tmp_path.unlink()

        page = None
        context = None
        browser = None
        content_type = ""
        final_url = url

        try:
            with sync_playwright() as playwright:
                context, page, browser = self._open_page(playwright)
                page.set_default_timeout(self.config.navigation_timeout_ms)
                page.set_default_navigation_timeout(self.config.navigation_timeout_ms)
                page.set_extra_http_headers({"Accept-Language": self.accept_language})
                bootstrap_url = self._bootstrap_url(url, referer=referer)
                page.goto(bootstrap_url, wait_until="domcontentloaded")
                self._wait_for_bootstrap_ready(page, bootstrap_url)
                self._dismiss_cookie_banner(page)
                pdf_payload = self._fetch_pdf_payload(
                    context,
                    page,
                    url,
                    referer=referer,
                    timeout_error=playwright_timeout_error,
                )
                final_url = str(pdf_payload.get("final_url") or page.url or url)
                content_type = str(pdf_payload.get("content_type") or "")
                raw_bytes = base64.b64decode(str(pdf_payload.get("body_base64") or ""))
                expected_bytes = int(pdf_payload.get("byte_length") or len(raw_bytes))
                if len(raw_bytes) != expected_bytes:
                    raise RuntimeError(
                        f"Playwright captured PDF bytes mismatch: decoded={len(raw_bytes)} expected={expected_bytes}"
                    )
                if len(raw_bytes) > self.download_max_bytes:
                    raise RuntimeError(f"Playwright download exceeded size limit ({self.download_max_bytes} bytes)")
                if not raw_bytes.startswith(b"%PDF"):
                    raise RuntimeError(
                        f"Playwright captured non-PDF content: {final_url} ({content_type or 'unknown content-type'})"
                    )
                tmp_path.write_bytes(raw_bytes)
        except (playwright_timeout_error, playwright_error) as exc:  # type: ignore[misc]
            if tmp_path.exists():
                tmp_path.unlink()
            raise RuntimeError(f"Playwright browser download failed: {exc}") from exc
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise
        finally:
            try:
                if page is not None:
                    page.close()
            except Exception:
                pass
            try:
                if context is not None and self.config.cdp_url is None:
                    context.close()
            except Exception:
                pass
            try:
                if browser is not None and self.config.cdp_url is None:
                    browser.close()
            except Exception:
                pass

        tmp_path.replace(destination)
        byte_count = destination.stat().st_size
        log_event(
            "info",
            "Playwright PDF download finished",
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
            "transport": f"playwright:{self.config.mode_label}",
        }

    def _open_page(self, playwright: Any) -> tuple[Any, Any, Any | None]:
        if self.config.cdp_url:
            browser = playwright.chromium.connect_over_cdp(self.config.cdp_url, timeout=self.config.launch_timeout_ms)
            if not browser.contexts:
                raise RuntimeError(
                    "Connected to Chrome CDP endpoint, but no browser context is available. "
                    "Please open at least one regular tab in that Chrome session first."
                )
            context = browser.contexts[0]
            page = context.new_page()
            return context, page, browser

        user_data_dir = self.config.user_data_dir
        if user_data_dir is None:
            raise RuntimeError(
                "Playwright browser fallback is enabled, but neither PAPER_AGENT_PLAYWRIGHT_CDP_URL nor "
                "PAPER_AGENT_PLAYWRIGHT_USER_DATA_DIR is configured."
            )

        launch_args: list[str] = []
        if self.config.profile_directory:
            launch_args.append(f"--profile-directory={self.config.profile_directory}")

        context = playwright.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=self.config.headless,
            executable_path=self.config.browser_executable_path or None,
            args=launch_args,
            accept_downloads=True,
            viewport={"width": 1440, "height": 960},
            timeout=self.config.launch_timeout_ms,
        )
        page = context.new_page()
        return context, page, None

    @staticmethod
    def _bootstrap_url(url: str, *, referer: str | None) -> str:
        parsed = urlparse(url)
        if referer:
            referer_parsed = urlparse(referer)
            referer_host = referer_parsed.netloc.lower()
            target_host = parsed.netloc.lower()
            if referer_host == target_host:
                return referer
            if target_host == "dl.acm.org" and parsed.path.startswith("/doi/pdf/") and referer_host == "doi.org":
                doi_suffix = referer_parsed.path.lstrip("/")
                if doi_suffix.startswith("doi/"):
                    doi_suffix = doi_suffix[len("doi/") :]
                if doi_suffix:
                    return f"{parsed.scheme or 'https'}://{parsed.netloc}/doi/{doi_suffix}"
        return f"{parsed.scheme or 'https'}://{parsed.netloc}/"

    def _fetch_pdf_payload(
        self,
        context: Any,
        page: Any,
        url: str,
        *,
        referer: str | None,
        timeout_error: type[BaseException],
    ) -> dict[str, Any]:
        try:
            return self._fetch_pdf_via_context_cookies(context, url, referer=referer)
        except timeout_error:
            raise
        except Exception as exc:
            log_event(
                "warning",
                "Playwright cookie-backed PDF download failed, falling back to in-browser fetch",
                url=url,
                error=str(exc),
            )

        try:
            payload = page.evaluate(
                """async (targetUrl) => {
                    const response = await fetch(targetUrl, { credentials: 'include' });
                    const blob = await response.blob();
                    const bodyBase64 = await new Promise((resolve, reject) => {
                        const reader = new FileReader();
                        reader.onerror = () => reject(reader.error || new Error("FileReader failed"));
                        reader.onload = () => {
                            const result = String(reader.result || "");
                            const commaIndex = result.indexOf(",");
                            resolve(commaIndex >= 0 ? result.slice(commaIndex + 1) : "");
                        };
                        reader.readAsDataURL(blob);
                    });
                    return {
                        ok: response.ok,
                        status: response.status,
                        statusText: response.statusText || "",
                        finalUrl: response.url || targetUrl,
                        contentType: response.headers.get("content-type") || "",
                        byteLength: blob.size,
                        bodyBase64,
                    };
                }""",
                url,
            )
        except timeout_error as exc:
            raise RuntimeError("Playwright did not observe a PDF response") from exc

        status = int(payload.get("status") or 0)
        if not payload.get("ok"):
            raise RuntimeError(
                f"Playwright in-browser fetch returned HTTP {status}: {payload.get('statusText') or 'unknown error'}"
            )
        return {
            "final_url": payload.get("finalUrl") or url,
            "content_type": payload.get("contentType") or "",
            "byte_length": payload.get("byteLength") or 0,
            "body_base64": payload.get("bodyBase64") or "",
        }

    def _fetch_pdf_via_context_cookies(
        self,
        context: Any,
        url: str,
        *,
        referer: str | None,
    ) -> dict[str, Any]:
        cookies = context.cookies([url])
        cookie_header = "; ".join(
            f"{cookie['name']}={cookie['value']}"
            for cookie in cookies
            if cookie.get("name") and cookie.get("value") is not None
        )
        headers = {
            "User-Agent": self.user_agent,
            "Accept": "application/pdf,*/*;q=0.8",
            "Accept-Language": self.accept_language,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        }
        if cookie_header:
            headers["Cookie"] = cookie_header
        if referer:
            headers["Referer"] = referer

        raw_bytes = b""
        final_url = url
        content_type = ""
        with urlopen(Request(url, headers=headers), timeout=self.config.navigation_timeout_ms / 1000) as response:
            final_url = response.geturl() or url
            content_type = response.headers.get("Content-Type", "")
            raw_bytes = response.read(self.download_max_bytes + 1)

        if len(raw_bytes) > self.download_max_bytes:
            raise RuntimeError(f"Playwright cookie-backed download exceeded size limit ({self.download_max_bytes} bytes)")
        if not raw_bytes.startswith(b"%PDF"):
            raise RuntimeError(
                f"Playwright cookie-backed download captured non-PDF content: "
                f"{final_url} ({content_type or 'unknown content-type'})"
            )
        return {
            "final_url": final_url,
            "content_type": content_type,
            "byte_length": len(raw_bytes),
            "body_base64": base64.b64encode(raw_bytes).decode("ascii"),
        }

    def _wait_for_bootstrap_ready(self, page: Any, bootstrap_url: str) -> None:
        max_wait_seconds = max(20.0, min(90.0, self.config.navigation_timeout_ms / 1000))
        deadline = time.perf_counter() + max_wait_seconds
        while time.perf_counter() < deadline:
            dismissed_banner = self._dismiss_cookie_banner(page)
            if dismissed_banner:
                page.wait_for_timeout(1_000)
            try:
                title = str(page.title() or "").strip()
                current_url = str(page.url or "").strip()
            except Exception:
                page.wait_for_timeout(500)
                continue
            if not self._looks_like_browser_challenge(current_url, title):
                log_event(
                    "info",
                    "Playwright bootstrap page ready",
                    bootstrap_url=bootstrap_url,
                    current_url=current_url,
                    title=title,
                )
                return
            log_event(
                "info",
                "Playwright waiting for browser challenge",
                bootstrap_url=bootstrap_url,
                current_url=current_url,
                title=title,
            )
            page.wait_for_timeout(2_000)

        log_event(
            "warning",
            "Playwright bootstrap challenge did not clear before timeout",
            bootstrap_url=bootstrap_url,
            current_url=self._safe_page_url(page),
            title=self._safe_page_title(page),
        )

    @staticmethod
    def _looks_like_browser_challenge(current_url: str, title: str) -> bool:
        lowered_url = current_url.lower()
        lowered_title = title.lower()
        return any(
            marker in lowered_url or marker in lowered_title
            for marker in (
                "just a moment",
                "checking your browser",
                "请稍候",
                "challenge-platform",
                "cdn-cgi/challenge",
            )
        )

    @staticmethod
    def _safe_page_title(page: Any) -> str:
        try:
            return str(page.title() or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _safe_page_url(page: Any) -> str:
        try:
            return str(page.url or "").strip()
        except Exception:
            return ""

    def _dismiss_cookie_banner(self, page: Any) -> bool:
        button_ids = (
            "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "CybotCookiebotDialogBodyButtonAccept",
            "CybotCookiebotDialogBodyButtonAcceptRecommended",
        )
        selectors = (
            "#CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
            "#CybotCookiebotDialogBodyButtonAccept",
            "#CybotCookiebotDialogBodyButtonAcceptRecommended",
            "#onetrust-accept-btn-handler",
            "button:has-text('Accept all cookies')",
            "button:has-text('Accept All Cookies')",
            "button:has-text('Accept all')",
            "button:has-text('Accept All')",
            "button:has-text('I Accept')",
            "button:has-text('Accept')",
            "button:has-text('同意')",
            "button:has-text('接受')",
            "button:has-text('全部接受')",
        )
        frames = []
        try:
            frames = list(page.frames)
        except Exception:
            frames = [page]
        for frame in frames or [page]:
            try:
                clicked_id = frame.evaluate(
                    """(ids) => {
                        for (const id of ids) {
                            const element = document.getElementById(id);
                            if (element) {
                                element.click();
                                return id;
                            }
                        }
                        return "";
                    }""",
                    list(button_ids),
                )
                if clicked_id:
                    log_event(
                        "info",
                        "Playwright cookie banner accepted",
                        selector=f"#{clicked_id}",
                        current_url=self._safe_page_url(page),
                    )
                    return True
            except Exception:
                pass
            for selector in selectors:
                try:
                    locator = frame.locator(selector).first
                    if locator.is_visible(timeout=500):
                        locator.click(timeout=1_500)
                        log_event(
                            "info",
                            "Playwright cookie banner accepted",
                            selector=selector,
                            current_url=self._safe_page_url(page),
                        )
                        return True
                except Exception:
                    continue
        return False


def build_playwright_download_config(
    *,
    enabled: bool,
    cdp_url: str | None,
    browser_executable_path: str | None,
    user_data_dir: str | Path | None,
    profile_directory: str | None,
    headless: bool,
    launch_timeout_ms: int,
    navigation_timeout_ms: int,
) -> PlaywrightDownloadConfig | None:
    if not enabled:
        return None

    resolved_user_data_dir = Path(user_data_dir).expanduser().resolve() if user_data_dir else None
    resolved_browser_executable = browser_executable_path or default_chrome_executable_path()

    return PlaywrightDownloadConfig(
        cdp_url=(cdp_url or "").strip() or None,
        browser_executable_path=(resolved_browser_executable or "").strip() or None,
        user_data_dir=resolved_user_data_dir,
        profile_directory=(profile_directory or "").strip() or None,
        headless=headless,
        launch_timeout_ms=max(1_000, int(launch_timeout_ms)),
        navigation_timeout_ms=max(1_000, int(navigation_timeout_ms)),
    )


def _import_playwright_sync() -> tuple[Any, type[BaseException], type[BaseException]]:
    try:
        from playwright.sync_api import Error as PlaywrightError
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "Playwright browser fallback requires the `playwright` Python package. "
            "Install it with `python3 -m pip install playwright`."
        ) from exc
    return sync_playwright, PlaywrightTimeoutError, PlaywrightError


def resolve_playwright_env_config() -> dict[str, Any]:
    return {
        "enabled": parse_bool_env(os.getenv(DEFAULT_ACM_BROWSER_FALLBACK_ENV), default=False),
        "cdp_url": os.getenv(DEFAULT_PLAYWRIGHT_CDP_URL_ENV),
        "browser_executable_path": os.getenv(DEFAULT_PLAYWRIGHT_BROWSER_EXECUTABLE_ENV),
        "user_data_dir": os.getenv(DEFAULT_PLAYWRIGHT_USER_DATA_DIR_ENV),
        "profile_directory": os.getenv(DEFAULT_PLAYWRIGHT_PROFILE_DIRECTORY_ENV),
        "headless": parse_bool_env(os.getenv(DEFAULT_PLAYWRIGHT_HEADLESS_ENV), default=False),
        "launch_timeout_ms": parse_int_env(os.getenv(DEFAULT_PLAYWRIGHT_LAUNCH_TIMEOUT_MS_ENV), default=30_000),
        "navigation_timeout_ms": parse_int_env(os.getenv(DEFAULT_PLAYWRIGHT_NAVIGATION_TIMEOUT_MS_ENV), default=45_000),
    }
