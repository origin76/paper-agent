from __future__ import annotations

from typing import Any

from paper_agent.conference_parsing import (
    extract_document_paper_metadata,
    looks_like_paper_title,
    normalize_space,
    normalize_title_key,
)
from paper_agent.conference_types import ConferencePaper


class OSDIAdapter:
    venue = "osdi"

    def build_index_url(self, year: int) -> str:
        return f"https://www.usenix.org/conference/osdi{str(year)[-2:]}/technical-sessions"

    def discover_papers(self, year: int, client: Any) -> tuple[str, list[ConferencePaper]]:
        index_url = self.build_index_url(year)
        document = client.fetch_document(index_url)
        presentation_marker = f"/conference/osdi{str(year)[-2:]}/presentation/"

        papers: list[ConferencePaper] = []
        seen_titles: set[str] = set()
        for link in document.links:
            if presentation_marker not in link.url.lower():
                continue
            title = normalize_space(link.text)
            if not looks_like_paper_title(title):
                continue
            title_key = normalize_title_key(title)
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            paper = ConferencePaper(
                venue=self.venue,
                year=year,
                title=title,
                detail_url=link.url,
                landing_page_url=link.url,
                discovery_source=index_url,
            )
            paper.add_source_url(index_url)
            paper.add_source_url(link.url)
            paper.add_trace("official_index:discovered_from_usenix_technical_sessions")
            papers.append(paper)

        return index_url, papers

    def enrich_paper(self, paper: ConferencePaper, client: Any) -> ConferencePaper:
        if not paper.detail_url:
            return paper

        document = client.fetch_document(paper.detail_url)
        metadata = extract_document_paper_metadata(document)
        refined_title = str(metadata.get("title") or "").strip()
        if refined_title and looks_like_paper_title(refined_title):
            paper.title = refined_title

        if metadata.get("authors"):
            paper.authors = [str(item).strip() for item in metadata["authors"] if str(item).strip()]
        if metadata.get("pdf_url"):
            paper.pdf_url = str(metadata["pdf_url"])
            paper.add_source_url(paper.pdf_url)
        if metadata.get("preprint_url"):
            paper.preprint_url = str(metadata["preprint_url"])
            paper.add_source_url(paper.preprint_url)
        if metadata.get("doi_url"):
            paper.doi_url = str(metadata["doi_url"])
            paper.add_source_url(paper.doi_url)

        paper.landing_page_url = document.final_url
        paper.metadata["detail_page_title"] = document.title
        paper.metadata["detail_headings"] = document.headings[:4]
        paper.add_trace("official_detail:enriched_from_usenix_presentation_page")
        return paper
