from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .http import (
    DEFAULT_ACCEPT_LANGUAGE,
    DEFAULT_ACM_COOKIE_FILE_ENV,
    DEFAULT_ACM_COOKIE_HEADER_ENV,
    DEFAULT_COOKIE_FILE_ENV,
    DEFAULT_COOKIE_HEADER_ENV,
    DEFAULT_USER_AGENT,
    CookieHeaderSource,
)
from paper_agent.browser.playwright_download import (
    DEFAULT_ACM_BROWSER_FALLBACK_ENV,
    DEFAULT_PLAYWRIGHT_BROWSER_EXECUTABLE_ENV,
    DEFAULT_PLAYWRIGHT_CDP_URL_ENV,
    DEFAULT_PLAYWRIGHT_DOWNLOAD_POOL_SIZE_ENV,
    DEFAULT_PLAYWRIGHT_HEADLESS_ENV,
    DEFAULT_PLAYWRIGHT_LAUNCH_TIMEOUT_MS_ENV,
    DEFAULT_PLAYWRIGHT_NAVIGATION_TIMEOUT_MS_ENV,
    DEFAULT_PLAYWRIGHT_PROFILE_DIRECTORY_ENV,
    DEFAULT_PLAYWRIGHT_TOTAL_TIMEOUT_MS_ENV,
    DEFAULT_PLAYWRIGHT_USER_DATA_DIR_ENV,
    BrowserPDFDownloader,
    BrowserPDFDownloaderPool,
    PlaywrightPDFDownloader,
    PlaywrightDownloadConfig,
    build_playwright_download_config,
    infer_playwright_browser_fallback_enabled,
    resolve_playwright_env_config,
)
from paper_agent.runtime import configure_logging, log_event


@dataclass(slots=True)
class ConferenceFetchCLIContext:
    venues: list[str]
    years: list[int]
    output_root: Path
    run_dir: Path
    cookie_source: CookieHeaderSource | None
    acm_cookie_source: CookieHeaderSource | None
    browser_pdf_downloader: BrowserPDFDownloader | None
    playwright_config: PlaywrightDownloadConfig | None
    resolved_playwright_pool_size: int


def default_recent_years() -> list[int]:
    current_year = datetime.now().year
    return [current_year - 3, current_year - 2, current_year - 1]


def parse_csv_items(raw_value: str) -> list[str]:
    return [item.strip().lower() for item in raw_value.split(",") if item.strip()]


def parse_years(raw_value: str | None) -> list[int]:
    if not raw_value:
        return default_recent_years()

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


def resolve_env_value(explicit_env_name: str | None, fallback_env_name: str) -> tuple[str | None, str | None]:
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
        description="Discover and download conference paper PDFs for OSDI / SOSP / PLDI / POPL into a local batch workspace.",
    )
    parser.add_argument("--venues", default="osdi,sosp,pldi,popl", help="Comma-separated venues. Supported: osdi,sosp,pldi,popl")
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
    parser.add_argument(
        "--playwright-download-pool-size",
        type=int,
        help=(
            "Number of concurrent Playwright downloader slots for ACM PDFs. "
            f"Default env: {DEFAULT_PLAYWRIGHT_DOWNLOAD_POOL_SIZE_ENV}; "
            "if omitted, defaults to min(download_workers, 4)."
        ),
    )
    parser.add_argument(
        "--playwright-total-timeout-ms",
        type=int,
        help=(
            "Hard upper bound for a single Playwright PDF download across all stages. "
            f"Default env: {DEFAULT_PLAYWRIGHT_TOTAL_TIMEOUT_MS_ENV}; "
            "if omitted, defaults to max(180000, 3 * navigation_timeout_ms)."
        ),
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


def parse_cli_context(args: argparse.Namespace, parser: argparse.ArgumentParser) -> ConferenceFetchCLIContext:
    venues = parse_csv_items(args.venues)
    years = parse_years(args.years)
    supported_venues = {"osdi", "sosp", "pldi", "popl"}
    unknown_venues = [venue for venue in venues if venue not in supported_venues]
    if unknown_venues:
        parser.exit(status=1, message=f"Unsupported venues: {', '.join(unknown_venues)}\n")

    output_root = Path(args.output_root).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    run_dir = output_root / "logs" / f"{timestamp}-{'-'.join(venues)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    default_cookie_header, default_cookie_env_name = resolve_env_value(args.http_cookie_header_env, DEFAULT_COOKIE_HEADER_ENV)
    acm_cookie_header, acm_cookie_env_name = resolve_env_value(args.acm_cookie_header_env, DEFAULT_ACM_COOKIE_HEADER_ENV)
    default_cookie_file_value, _ = resolve_env_value(None, DEFAULT_COOKIE_FILE_ENV)
    acm_cookie_file_value, _ = resolve_env_value(None, DEFAULT_ACM_COOKIE_FILE_ENV)
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
    resolved_playwright_cdp_url = args.playwright_cdp_url or playwright_env["cdp_url"]
    resolved_playwright_user_data_dir = args.playwright_user_data_dir or playwright_env["user_data_dir"]
    resolved_playwright_launch_timeout_ms = (
        args.playwright_launch_timeout_ms if args.playwright_launch_timeout_ms is not None else playwright_env["launch_timeout_ms"]
    )
    resolved_playwright_navigation_timeout_ms = (
        args.playwright_navigation_timeout_ms
        if args.playwright_navigation_timeout_ms is not None
        else playwright_env["navigation_timeout_ms"]
    )
    resolved_playwright_total_timeout_ms = (
        args.playwright_total_timeout_ms
        if args.playwright_total_timeout_ms is not None
        else (
            playwright_env["total_timeout_ms"]
            if playwright_env["total_timeout_ms"]
            else max(180_000, int(resolved_playwright_navigation_timeout_ms) * 3)
        )
    )
    playwright_enabled = infer_playwright_browser_fallback_enabled(
        explicit_enabled=args.acm_browser_fallback,
        env_enabled=playwright_env["enabled"],
        cdp_url=resolved_playwright_cdp_url,
        user_data_dir=resolved_playwright_user_data_dir,
    )
    playwright_headless = playwright_env["headless"] if args.playwright_headless is None else bool(args.playwright_headless)
    playwright_config = build_playwright_download_config(
        enabled=playwright_enabled,
        cdp_url=resolved_playwright_cdp_url,
        browser_executable_path=args.playwright_browser_executable or playwright_env["browser_executable_path"],
        user_data_dir=resolved_playwright_user_data_dir,
        profile_directory=args.playwright_profile_directory or playwright_env["profile_directory"],
        headless=playwright_headless,
        launch_timeout_ms=resolved_playwright_launch_timeout_ms,
        navigation_timeout_ms=resolved_playwright_navigation_timeout_ms,
        total_timeout_ms=resolved_playwright_total_timeout_ms,
    )
    requested_playwright_pool_size = args.playwright_download_pool_size
    if requested_playwright_pool_size is None:
        requested_playwright_pool_size = playwright_env["download_pool_size"] or None
    resolved_playwright_pool_size = 0
    if playwright_config is not None:
        if requested_playwright_pool_size is None:
            resolved_playwright_pool_size = max(1, min(args.download_workers, 4))
        else:
            resolved_playwright_pool_size = max(1, min(int(requested_playwright_pool_size), max(1, args.download_workers)))

    browser_pdf_downloader = build_browser_pdf_downloader(
        args=args,
        playwright_config=playwright_config,
        resolved_playwright_pool_size=resolved_playwright_pool_size,
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
        playwright_download_pool_size=resolved_playwright_pool_size if playwright_config else None,
        playwright_total_timeout_ms=playwright_config.total_timeout_ms if playwright_config else None,
    )

    return ConferenceFetchCLIContext(
        venues=venues,
        years=years,
        output_root=output_root,
        run_dir=run_dir,
        cookie_source=cookie_source,
        acm_cookie_source=acm_cookie_source,
        browser_pdf_downloader=browser_pdf_downloader,
        playwright_config=playwright_config,
        resolved_playwright_pool_size=resolved_playwright_pool_size,
    )


def build_browser_pdf_downloader(
    *,
    args: argparse.Namespace,
    playwright_config: PlaywrightDownloadConfig | None,
    resolved_playwright_pool_size: int,
) -> BrowserPDFDownloader | None:
    if playwright_config is None:
        return None
    if resolved_playwright_pool_size > 1:
        return BrowserPDFDownloaderPool(
            [
                PlaywrightPDFDownloader(
                    config=playwright_config,
                    download_max_bytes=args.download_max_bytes,
                    user_agent=args.http_user_agent,
                    accept_language=DEFAULT_ACCEPT_LANGUAGE,
                    slot_label=f"slot-{index + 1}",
                )
                for index in range(resolved_playwright_pool_size)
            ]
        )
    return PlaywrightPDFDownloader(
        config=playwright_config,
        download_max_bytes=args.download_max_bytes,
        user_agent=args.http_user_agent,
        accept_language=DEFAULT_ACCEPT_LANGUAGE,
        slot_label="slot-1",
    )
