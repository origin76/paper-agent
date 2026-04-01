from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import urlopen

from . import http as conference_http_module
from .cli import (
    build_parser,
    parse_cli_context,
    parse_years as _parse_years,
)
from .http import (
    DEFAULT_USER_AGENT,
    CookieHeaderSource,
    ConferenceHTTPClient as BaseConferenceHTTPClient,
    ReturnedPDFError,
    derive_pdf_download_referer,
)
from .paper_ops import ConferencePaperOps
from .parsing import maybe_promote_to_pdf_url, normalize_title_key
from .types import ConferenceManifest, ConferencePaper
from .venues.osdi import OSDIAdapter
from .venues.popl import POPLAdapter
from .venues.pldi import PLDIAdapter
from .venues.sosp import SOSPAdapter
from paper_agent.browser.playwright_download import (
    BrowserPDFDownloader,
)
from paper_agent.runtime import append_stage_trace, log_event
from paper_agent.utils import write_json

class ConferenceHTTPClient(BaseConferenceHTTPClient):
    """Compatibility wrapper for legacy patches against paper_agent.conference_fetch."""

    @staticmethod
    def _sync_compat_urlopen() -> None:
        conference_http_module.urlopen = urlopen

    def fetch_text(self, url: str) -> tuple[str, str, str]:
        self._sync_compat_urlopen()
        return super().fetch_text(url)

    def _download_pdf_via_http(self, url: str, destination: Path) -> dict[str, Any]:
        self._sync_compat_urlopen()
        return super()._download_pdf_via_http(url, destination)


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
            "popl": POPLAdapter(),
            "pldi": PLDIAdapter(),
            "sosp": SOSPAdapter(),
        }
        self.manifests_dir = self.output_root / "manifests"
        self.downloads_dir = self.output_root / "downloads"
        self.unresolved_dir = self.output_root / "unresolved"
        self.indexes_dir = self.output_root / "indexes"
        for path in (self.manifests_dir, self.downloads_dir, self.unresolved_dir, self.indexes_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.paper_ops = ConferencePaperOps(
            http=self.http,
            downloads_dir=self.downloads_dir,
            skip_existing=self.skip_existing,
        )

    def _paper_ops(self) -> ConferencePaperOps:
        self.paper_ops.http = self.http
        self.paper_ops.downloads_dir = self.downloads_dir
        self.paper_ops.skip_existing = self.skip_existing
        return self.paper_ops

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

            existing_papers, papers = self._prefilter_existing_papers(papers)
            if existing_papers:
                append_stage_trace(
                    self.run_dir,
                    f"prefilter.{stage_prefix}",
                    "finished",
                    reused_count=len(existing_papers),
                    pending_count=len(papers),
                )
                log_event(
                    "info",
                    "Conference prefilter reused existing PDFs",
                    venue=venue,
                    year=year,
                    reused_count=len(existing_papers),
                    pending_count=len(papers),
                )

            if papers:
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

            papers = existing_papers + papers

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

    def _prefilter_existing_papers(self, papers: list[ConferencePaper]) -> tuple[list[ConferencePaper], list[ConferencePaper]]:
        if not self.skip_existing:
            return [], papers

        existing_papers: list[ConferencePaper] = []
        pending_papers: list[ConferencePaper] = []
        for paper in papers:
            destination = self._paper_ops().destination_for_paper(paper)
            if destination.exists() and destination.stat().st_size > 0:
                paper.status = "existing"
                paper.download_path = str(destination)
                paper.download_url = paper.download_url or paper.pdf_url or paper.preprint_url or paper.doi_url
                paper.add_trace("prefilter:reused_existing_file_before_enrich")
                existing_papers.append(paper)
            else:
                pending_papers.append(paper)
        return existing_papers, pending_papers

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
        return self._paper_ops().supplement_paper(paper)

    @staticmethod
    def _looks_like_preprint_host(url: str) -> bool:
        return ConferencePaperOps.looks_like_preprint_host(url)

    @staticmethod
    def _looks_like_downloadable_source(url: str) -> bool:
        return ConferencePaperOps.looks_like_downloadable_source(url)

    def _supplement_from_dblp(self, paper: ConferencePaper) -> ConferencePaper:
        return self._paper_ops().supplement_from_dblp(paper)

    def _supplement_from_openalex(self, paper: ConferencePaper) -> ConferencePaper:
        return self._paper_ops().supplement_from_openalex(paper)

    def _supplement_from_arxiv(self, paper: ConferencePaper) -> ConferencePaper:
        return self._paper_ops().supplement_from_arxiv(paper)

    def _resolve_pdf_urls(self, paper: ConferencePaper) -> list[str]:
        return self._paper_ops().resolve_pdf_urls(paper)

    def _resolve_pdf_url(self, paper: ConferencePaper) -> str | None:
        return self._paper_ops().resolve_pdf_url(paper)

    @staticmethod
    def _looks_like_listing_page_url(url: str) -> bool:
        return ConferencePaperOps.looks_like_listing_page_url(url)

    def _resolve_pdf_candidate(self, url: str) -> str | None:
        return self._paper_ops().resolve_pdf_candidate(url)

    def _destination_for_paper(self, paper: ConferencePaper) -> Path:
        return self._paper_ops().destination_for_paper(paper)

    def _download_paper(self, paper: ConferencePaper) -> ConferencePaper:
        return self._paper_ops().download_paper(paper)

def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    context = parse_cli_context(args, parser)

    service = ConferenceFetchService(
        output_root=context.output_root,
        run_dir=context.run_dir,
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
        cookie_source=context.cookie_source,
        acm_cookie_source=context.acm_cookie_source,
        browser_pdf_downloader=context.browser_pdf_downloader,
    )

    summary = service.run(context.venues, context.years)
    print(f"Conference workspace: {context.output_root}")
    print(f"Run log directory: {context.run_dir}")
    print(f"Downloaded PDFs root: {context.output_root / 'downloads'}")
    print(f"Manifests root: {context.output_root / 'manifests'}")
    print(f"Unresolved root: {context.output_root / 'unresolved'}")
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
