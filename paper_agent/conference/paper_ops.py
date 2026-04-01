from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode

from .http import ConferenceHTTPClient, ReturnedPDFError
from .parsing import (
    extract_document_paper_metadata,
    infer_doi_pdf_candidate,
    looks_like_pdf_url,
    maybe_promote_to_pdf_url,
    titles_match,
)
from .types import ConferencePaper
from paper_agent.runtime import log_event
from paper_agent.utils import sanitize_filename


ATOM_NAMESPACE = {"atom": "http://www.w3.org/2005/Atom"}
OPENALEX_COOLDOWN_SECONDS = 600.0
ARXIV_COOLDOWN_SECONDS = 300.0


class ConferencePaperOps:
    def __init__(
        self,
        *,
        http: ConferenceHTTPClient,
        downloads_dir: Path,
        skip_existing: bool,
    ) -> None:
        self.http = http
        self.downloads_dir = downloads_dir
        self.skip_existing = skip_existing
        self._supplement_source_lock = threading.Lock()
        self._supplement_disabled_until: dict[str, float] = {"openalex": 0.0, "arxiv": 0.0}

    def is_supplement_source_disabled(self, source: str) -> bool:
        with self._supplement_source_lock:
            disabled_until = self._supplement_disabled_until.get(source, 0.0)
        return time.monotonic() < disabled_until

    def disable_supplement_source(self, source: str, *, cooldown_seconds: float, error: Exception) -> None:
        disabled_until = time.monotonic() + max(0.0, cooldown_seconds)
        with self._supplement_source_lock:
            self._supplement_disabled_until[source] = max(self._supplement_disabled_until.get(source, 0.0), disabled_until)
        log_event(
            "warning",
            "Conference supplemental source temporarily disabled",
            source=source,
            cooldown_seconds=f"{cooldown_seconds:.0f}",
            error=str(error),
        )

    def supplement_paper(self, paper: ConferencePaper) -> ConferencePaper:
        if paper.pdf_url and paper.preprint_url and paper.doi_url and paper.authors:
            paper.add_trace("supplement:skipped_already_rich")
            return paper

        paper = self.supplement_from_dblp(paper)
        if paper.doi_url and not paper.pdf_url and not paper.preprint_url:
            paper.add_trace("supplement:external_lookup_skipped_doi_already_present")
            return paper
        if not paper.pdf_url and not paper.preprint_url:
            paper = self.supplement_from_openalex(paper)
        if not paper.pdf_url and not paper.preprint_url and not paper.alternate_urls and not paper.doi_url:
            paper = self.supplement_from_arxiv(paper)
        return paper

    @staticmethod
    def looks_like_preprint_host(url: str) -> bool:
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
    def looks_like_downloadable_source(url: str) -> bool:
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

    def supplement_from_dblp(self, paper: ConferencePaper) -> ConferencePaper:
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
                elif not paper.preprint_url and self.looks_like_preprint_host(lowered):
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

    def supplement_from_openalex(self, paper: ConferencePaper) -> ConferencePaper:
        if self.is_supplement_source_disabled("openalex"):
            paper.add_trace("supplement:openalex_skipped_rate_limited")
            return paper
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
                    if self.looks_like_preprint_host(pdf_url):
                        if not paper.preprint_url:
                            paper.preprint_url = pdf_url
                        paper.add_source_url(pdf_url)
                    elif not paper.pdf_url and self.looks_like_downloadable_source(pdf_url):
                        paper.pdf_url = pdf_url
                        paper.add_source_url(pdf_url)
                    else:
                        oa_candidates.append(pdf_url)

                if landing_url:
                    if self.looks_like_preprint_host(landing_url):
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
                if self.looks_like_preprint_host(oa_url) and not paper.preprint_url:
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
            if isinstance(exc, HTTPError) and exc.code == 429:
                self.disable_supplement_source("openalex", cooldown_seconds=OPENALEX_COOLDOWN_SECONDS, error=exc)
            paper.add_trace(f"supplement:openalex_error={exc}")
            return paper

    def supplement_from_arxiv(self, paper: ConferencePaper) -> ConferencePaper:
        if self.is_supplement_source_disabled("arxiv"):
            paper.add_trace("supplement:arxiv_skipped_rate_limited")
            return paper
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
            if isinstance(exc, HTTPError) and exc.code == 429:
                self.disable_supplement_source("arxiv", cooldown_seconds=ARXIV_COOLDOWN_SECONDS, error=exc)
            paper.add_trace(f"supplement:arxiv_error={exc}")
            return paper

    def resolve_pdf_urls(self, paper: ConferencePaper) -> list[str]:
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
            if label == "landing_page_url" and self.looks_like_listing_page_url(normalized):
                continue
            if normalized not in candidates:
                candidates.append(normalized)

        resolved_candidates: list[str] = []
        for candidate in candidates:
            resolved = self.resolve_pdf_candidate(candidate)
            if resolved:
                paper.add_trace(f"resolve:pdf_candidate={candidate} -> {resolved}")
                if resolved not in resolved_candidates:
                    resolved_candidates.append(resolved)

        direct_doi_pdf = infer_doi_pdf_candidate(paper.doi_url or "")
        if direct_doi_pdf and direct_doi_pdf not in resolved_candidates:
            paper.add_trace(f"resolve:doi_pdf_guess={direct_doi_pdf}")
            resolved_candidates.append(direct_doi_pdf)

        if not resolved_candidates:
            paper.add_trace("resolve:no_pdf_url_found")
        return resolved_candidates

    def resolve_pdf_url(self, paper: ConferencePaper) -> str | None:
        resolved_candidates = self.resolve_pdf_urls(paper)
        return resolved_candidates[0] if resolved_candidates else None

    @staticmethod
    def looks_like_listing_page_url(url: str) -> bool:
        normalized = str(url or "").strip().lower()
        return any(
            marker in normalized
            for marker in (
                "/technical-sessions",
                "/track/",
                "/accepted.html",
            )
        )

    def resolve_pdf_candidate(self, url: str) -> str | None:
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

    def destination_for_paper(self, paper: ConferencePaper) -> Path:
        directory = self.downloads_dir / paper.venue / str(paper.year)
        filename = f"{sanitize_filename(paper.title, fallback=paper.paper_id(), max_length=150)} [{paper.venue.upper()} {paper.year}].pdf"
        return directory / filename

    def download_paper(self, paper: ConferencePaper) -> ConferencePaper:
        destination = self.destination_for_paper(paper)

        if self.skip_existing and destination.exists() and destination.stat().st_size > 0:
            paper.status = "existing"
            paper.download_path = str(destination)
            paper.download_url = paper.download_url or paper.pdf_url or paper.preprint_url or paper.doi_url
            paper.add_trace("download:reused_existing_file")
            return paper

        resolved_urls = self.resolve_pdf_urls(paper)
        if not resolved_urls:
            paper.status = "unresolved"
            paper.download_error = "No PDF URL could be resolved"
            paper.add_note("未能解析到可下载 PDF，已写入 unresolved 清单。")
            return paper

        attempt_failures: list[dict[str, str]] = []
        for resolved_url in resolved_urls:
            try:
                download_meta = self.http.download_pdf(resolved_url, destination)
                paper.download_url = str(download_meta["final_url"] or resolved_url)
                paper.download_path = str(destination)
                paper.status = "downloaded"
                paper.metadata["download_content_type"] = download_meta["content_type"]
                paper.metadata["download_bytes"] = download_meta["byte_count"]
                if download_meta.get("transport"):
                    paper.metadata["download_transport"] = download_meta["transport"]
                if attempt_failures:
                    paper.metadata["download_failures"] = attempt_failures
                paper.add_source_url(resolved_url)
                paper.add_source_url(paper.download_url)
                paper.add_trace(f"download:success={paper.download_url}")
                return paper
            except Exception as exc:
                failure = {"url": resolved_url, "error": str(exc)}
                attempt_failures.append(failure)
                paper.add_trace(f"download:attempt_error={resolved_url} error={exc}")

        paper.status = "unresolved"
        if attempt_failures:
            paper.metadata["download_failures"] = attempt_failures
            paper.download_error = attempt_failures[-1]["error"]
        else:
            paper.download_error = "Unknown PDF download failure"
        paper.add_note("PDF 下载失败，详情见 resolution_trace 与 run.log。")
        paper.add_trace(f"download:error={paper.download_error}")
        return paper
