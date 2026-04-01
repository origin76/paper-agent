from __future__ import annotations

import base64
import os
import queue
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
DEFAULT_PLAYWRIGHT_DOWNLOAD_POOL_SIZE_ENV = "PAPER_AGENT_PLAYWRIGHT_DOWNLOAD_POOL_SIZE"
DEFAULT_PLAYWRIGHT_TOTAL_TIMEOUT_MS_ENV = "PAPER_AGENT_PLAYWRIGHT_TOTAL_TIMEOUT_MS"


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
    total_timeout_ms: int = 180_000

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


def infer_playwright_browser_fallback_enabled(
    *,
    explicit_enabled: bool | None,
    env_enabled: bool,
    cdp_url: str | None,
    user_data_dir: str | Path | None,
) -> bool:
    if explicit_enabled is not None:
        return bool(explicit_enabled)
    if env_enabled:
        return True
    return bool((str(cdp_url or "").strip()) or (str(user_data_dir or "").strip()))


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
        slot_label: str | None = None,
    ) -> None:
        self.config = config
        self.download_max_bytes = download_max_bytes
        self.user_agent = user_agent
        self.accept_language = accept_language
        self.slot_label = slot_label
        self._lock = threading.Lock()

    def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, Any]:
        with self._lock:
            return self._download_pdf_locked(url, destination, referer=referer)

    def _download_pdf_locked(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, Any]:
        sync_playwright, playwright_timeout_error, playwright_error = _import_playwright_sync()

        start_time = time.perf_counter()
        deadline = start_time + (self.config.total_timeout_ms / 1000)
        current_stage = "initialize"
        log_event(
            "info",
            "Playwright PDF download started",
            url=url,
            destination=destination,
            mode=self.config.mode_label,
            slot=self.slot_label,
            cdp_url=self.config.cdp_url,
            user_data_dir=self.config.user_data_dir,
            profile_directory=self.config.profile_directory,
            headless=self.config.headless,
            total_timeout_ms=self.config.total_timeout_ms,
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
                current_stage = "open_page"
                open_timeout_ms = self._stage_timeout_ms(deadline, self.config.launch_timeout_ms, stage=current_stage)
                log_event(
                    "info",
                    "Playwright browser session opening",
                    url=url,
                    destination=destination,
                    slot=self.slot_label,
                    stage=current_stage,
                    timeout_ms=open_timeout_ms,
                )
                context, page, browser = self._open_page(playwright, timeout_ms=open_timeout_ms)
                log_event(
                    "info",
                    "Playwright browser session ready",
                    url=url,
                    destination=destination,
                    slot=self.slot_label,
                    stage=current_stage,
                )
                per_step_timeout_ms = self._stage_timeout_ms(deadline, self.config.navigation_timeout_ms, stage="bootstrap_prepare")
                page.set_default_timeout(per_step_timeout_ms)
                page.set_default_navigation_timeout(per_step_timeout_ms)
                page.set_extra_http_headers({"Accept-Language": self.accept_language})
                bootstrap_url = self._bootstrap_url(url, referer=referer)
                current_stage = "bootstrap_goto"
                bootstrap_goto_timeout_ms = self._stage_timeout_ms(deadline, self.config.navigation_timeout_ms, stage=current_stage)
                log_event(
                    "info",
                    "Playwright bootstrap navigation started",
                    url=url,
                    bootstrap_url=bootstrap_url,
                    destination=destination,
                    slot=self.slot_label,
                    stage=current_stage,
                    timeout_ms=bootstrap_goto_timeout_ms,
                )
                page.goto(bootstrap_url, wait_until="domcontentloaded", timeout=bootstrap_goto_timeout_ms)
                log_event(
                    "info",
                    "Playwright bootstrap navigation finished",
                    url=url,
                    bootstrap_url=bootstrap_url,
                    destination=destination,
                    slot=self.slot_label,
                    stage=current_stage,
                    current_url=self._safe_page_url(page),
                    title=self._safe_page_title(page),
                )
                current_stage = "bootstrap_wait"
                bootstrap_wait_timeout_ms = self._stage_timeout_ms(deadline, self.config.navigation_timeout_ms, stage=current_stage)
                log_event(
                    "info",
                    "Playwright bootstrap readiness wait started",
                    url=url,
                    bootstrap_url=bootstrap_url,
                    destination=destination,
                    slot=self.slot_label,
                    stage=current_stage,
                    timeout_ms=bootstrap_wait_timeout_ms,
                )
                self._wait_for_bootstrap_ready(page, bootstrap_url, timeout_ms=bootstrap_wait_timeout_ms)
                current_stage = "cookie_banner"
                cookie_banner_timeout_ms = self._stage_timeout_ms(deadline, 2_000, stage=current_stage)
                self._dismiss_cookie_banner(page, budget_ms=cookie_banner_timeout_ms)
                current_stage = "pdf_fetch"
                pdf_payload = self._fetch_pdf_payload(
                    context,
                    page,
                    url,
                    referer=referer,
                    timeout_error=playwright_timeout_error,
                    deadline=deadline,
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
                current_stage = "write_file"
                tmp_path.write_bytes(raw_bytes)
        except (playwright_timeout_error, playwright_error) as exc:  # type: ignore[misc]
            if tmp_path.exists():
                tmp_path.unlink()
            raise RuntimeError(f"Playwright browser download failed during stage={current_stage}: {exc}") from exc
        except Exception as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            if isinstance(exc, RuntimeError):
                raise
            raise RuntimeError(f"Playwright browser download failed during stage={current_stage}: {exc}") from exc
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
            slot=self.slot_label,
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

    @staticmethod
    def _stage_timeout_ms(deadline: float, fallback_ms: int, *, stage: str) -> int:
        remaining_ms = max(0, int((deadline - time.perf_counter()) * 1000))
        if remaining_ms <= 0:
            raise RuntimeError(f"Playwright total timeout exceeded before stage={stage}")
        return max(1, min(int(fallback_ms), remaining_ms))

    def _open_page(self, playwright: Any, *, timeout_ms: int) -> tuple[Any, Any, Any | None]:
        if self.config.cdp_url:
            browser = playwright.chromium.connect_over_cdp(self.config.cdp_url, timeout=timeout_ms)
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
            timeout=timeout_ms,
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
        deadline: float,
    ) -> dict[str, Any]:
        cookie_fetch_timeout_ms = self._stage_timeout_ms(deadline, self.config.navigation_timeout_ms, stage="cookie_fetch")
        log_event(
            "info",
            "Playwright cookie-backed PDF fetch started",
            url=url,
            slot=self.slot_label,
            stage="cookie_fetch",
            timeout_ms=cookie_fetch_timeout_ms,
        )
        try:
            payload = self._fetch_pdf_via_context_cookies(
                context,
                url,
                referer=referer,
                timeout_seconds=max(0.1, cookie_fetch_timeout_ms / 1000),
            )
            log_event(
                "info",
                "Playwright cookie-backed PDF fetch finished",
                url=url,
                slot=self.slot_label,
                stage="cookie_fetch",
                byte_length=payload.get("byte_length") or 0,
                final_url=payload.get("final_url") or url,
            )
            return payload
        except timeout_error:
            raise
        except Exception as exc:
            log_event(
                "warning",
                "Playwright cookie-backed PDF fetch failed, falling back to in-browser fetch",
                url=url,
                slot=self.slot_label,
                stage="cookie_fetch",
                error=str(exc),
            )

        browser_fetch_timeout_ms = self._stage_timeout_ms(deadline, self.config.navigation_timeout_ms, stage="in_browser_fetch")
        log_event(
            "info",
            "Playwright in-browser PDF fetch started",
            url=url,
            slot=self.slot_label,
            stage="in_browser_fetch",
            timeout_ms=browser_fetch_timeout_ms,
        )
        try:
            payload = page.evaluate(
                """async ({ targetUrl, timeoutMs }) => {
                    const controller = new AbortController();
                    const timer = setTimeout(() => controller.abort(new Error("fetch timeout")), timeoutMs);
                    try {
                        const response = await fetch(targetUrl, { credentials: 'include', signal: controller.signal });
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
                            error: "",
                        };
                    } catch (error) {
                        const message = error instanceof Error ? error.message : String(error || "unknown error");
                        return {
                            ok: false,
                            status: 0,
                            statusText: "",
                            finalUrl: targetUrl,
                            contentType: "",
                            byteLength: 0,
                            bodyBase64: "",
                            error: message,
                        };
                    } finally {
                        clearTimeout(timer);
                    }
                }""",
                {"targetUrl": url, "timeoutMs": browser_fetch_timeout_ms},
            )
        except timeout_error as exc:
            raise RuntimeError("Playwright in-browser fetch timed out while waiting for a PDF response") from exc

        status = int(payload.get("status") or 0)
        if not payload.get("ok"):
            raise RuntimeError(
                f"Playwright in-browser fetch failed: {payload.get('error') or payload.get('statusText') or f'HTTP {status}'}"
            )
        log_event(
            "info",
            "Playwright in-browser PDF fetch finished",
            url=url,
            slot=self.slot_label,
            stage="in_browser_fetch",
            byte_length=payload.get("byteLength") or 0,
            final_url=payload.get("finalUrl") or url,
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
        timeout_seconds: float,
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
        with urlopen(Request(url, headers=headers), timeout=timeout_seconds) as response:
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

    def _wait_for_bootstrap_ready(self, page: Any, bootstrap_url: str, *, timeout_ms: int) -> None:
        max_wait_seconds = max(0.1, timeout_ms / 1000)
        deadline = time.perf_counter() + max_wait_seconds
        while time.perf_counter() < deadline:
            try:
                title = str(page.title() or "").strip()
                current_url = str(page.url or "").strip()
            except Exception:
                remaining_wait_ms = max(100, min(500, int((deadline - time.perf_counter()) * 1000)))
                page.wait_for_timeout(remaining_wait_ms)
                continue
            if not self._looks_like_browser_challenge(current_url, title):
                log_event(
                    "info",
                    "Playwright bootstrap page ready",
                    bootstrap_url=bootstrap_url,
                    slot=self.slot_label,
                    current_url=current_url,
                    title=title,
                )
                return
            log_event(
                "info",
                "Playwright waiting for browser challenge",
                bootstrap_url=bootstrap_url,
                slot=self.slot_label,
                current_url=current_url,
                title=title,
            )
            remaining_wait_ms = max(100, min(2_000, int((deadline - time.perf_counter()) * 1000)))
            page.wait_for_timeout(remaining_wait_ms)

        log_event(
            "warning",
            "Playwright bootstrap challenge did not clear before timeout",
            bootstrap_url=bootstrap_url,
            slot=self.slot_label,
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

    @staticmethod
    def _safe_frame_url(frame: Any) -> str:
        try:
            return str(frame.url or "").strip()
        except Exception:
            return ""

    @staticmethod
    def _safe_frame_name(frame: Any) -> str:
        try:
            return str(frame.name or "").strip()
        except Exception:
            return ""

    def _candidate_cookie_targets(self, page: Any) -> list[tuple[str, Any, str]]:
        targets: list[tuple[str, Any, str]] = [("page", page, self._safe_page_url(page))]
        frame_hints = (
            "cookie",
            "consent",
            "cybot",
            "cookiebot",
            "onetrust",
            "trustarc",
            "privacy",
        )
        seen_keys = {("page", targets[0][2])}
        try:
            frames = list(page.frames)
        except Exception:
            frames = []
        for frame in frames:
            frame_url = self._safe_frame_url(frame)
            frame_name = self._safe_frame_name(frame)
            lowered = f"{frame_name} {frame_url}".lower()
            if not any(hint in lowered for hint in frame_hints):
                continue
            key = (frame_name or "frame", frame_url)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            label = frame_name or "frame"
            targets.append((label, frame, frame_url))
        return targets

    def _dismiss_cookie_banner(self, page: Any, *, budget_ms: int) -> bool:
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
        start_time = time.perf_counter()
        deadline = time.perf_counter() + max(0.1, budget_ms / 1000)
        targets = self._candidate_cookie_targets(page)
        log_event(
            "info",
            "Playwright cookie banner scan started",
            slot=self.slot_label,
            current_url=self._safe_page_url(page),
            budget_ms=budget_ms,
            target_count=len(targets),
        )
        for target_label, frame, frame_url in targets:
            if time.perf_counter() >= deadline:
                break
            for selector in selectors:
                remaining_ms = int((deadline - time.perf_counter()) * 1000)
                if remaining_ms <= 0:
                    break
                try:
                    locator = frame.locator(selector).first
                    visible_timeout_ms = max(50, min(200, remaining_ms))
                    if locator.is_visible(timeout=visible_timeout_ms):
                        click_timeout_ms = max(100, min(800, remaining_ms))
                        locator.click(timeout=click_timeout_ms)
                        log_event(
                            "info",
                            "Playwright cookie banner accepted",
                            selector=selector,
                            slot=self.slot_label,
                            current_url=self._safe_page_url(page),
                            target=target_label,
                            frame_url=frame_url,
                            duration_ms=int((time.perf_counter() - start_time) * 1000),
                        )
                        return True
                except Exception:
                    continue
        log_event(
            "info",
            "Playwright cookie banner scan finished",
            slot=self.slot_label,
            current_url=self._safe_page_url(page),
            accepted=False,
            target_count=len(targets),
            duration_ms=int((time.perf_counter() - start_time) * 1000),
        )
        return False


class BrowserPDFDownloaderPool:
    def __init__(self, downloaders: list[BrowserPDFDownloader]) -> None:
        if not downloaders:
            raise ValueError("BrowserPDFDownloaderPool requires at least one downloader")
        self._downloaders = list(downloaders)
        self.pool_size = len(self._downloaders)
        self._available_indexes: queue.Queue[int] = queue.Queue()
        for index in range(self.pool_size):
            self._available_indexes.put(index)

    def download_pdf(self, url: str, destination: Path, *, referer: str | None = None) -> dict[str, Any]:
        downloader_index = self._available_indexes.get()
        try:
            return self._downloaders[downloader_index].download_pdf(url, destination, referer=referer)
        finally:
            self._available_indexes.put(downloader_index)


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
    total_timeout_ms: int,
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
        total_timeout_ms=max(5_000, int(total_timeout_ms)),
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
        "download_pool_size": parse_int_env(os.getenv(DEFAULT_PLAYWRIGHT_DOWNLOAD_POOL_SIZE_ENV), default=0),
        "total_timeout_ms": parse_int_env(os.getenv(DEFAULT_PLAYWRIGHT_TOTAL_TIMEOUT_MS_ENV), default=180_000),
    }
