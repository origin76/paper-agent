from __future__ import annotations

from typing import Any

from paper_agent.conference_parsing import extract_title_author_blocks, looks_like_paper_title, normalize_space
from paper_agent.conference_types import ConferencePaper


SOSP_ACCEPTED_URLS = {
    2021: "https://sosp2021.mpi-sws.org/accepted.html",
    2023: "https://sosp2023.mpi-sws.org/accepted.html",
    2024: "https://sigops.org/s/conferences/sosp/2024/accepted.html",
    2025: "https://sigops.org/s/conferences/sosp/2025/accepted.html",
}


class SOSPAdapter:
    venue = "sosp"

    def _clean_title(self, title: str) -> str:
        normalized = normalize_space(title)
        if normalized.lower().endswith(" by"):
            normalized = normalized[:-3].rstrip()
        return normalized

    def build_index_url(self, year: int) -> str:
        return SOSP_ACCEPTED_URLS.get(year, f"https://sigops.org/s/conferences/sosp/{year}/accepted.html")

    def discover_papers(self, year: int, client: Any) -> tuple[str, list[ConferencePaper]]:
        index_url = self.build_index_url(year)
        document = client.fetch_document(index_url)
        title_author_pairs = extract_title_author_blocks(document)

        papers: list[ConferencePaper] = []
        for title, authors in title_author_pairs:
            cleaned_title = self._clean_title(title)
            lowered = cleaned_title.lower()
            if not looks_like_paper_title(cleaned_title):
                continue
            if lowered.startswith("the following papers have been accepted"):
                continue
            if lowered.startswith("the 28th acm symposium") or lowered.startswith("the 29th acm symposium"):
                continue
            paper = ConferencePaper(
                venue=self.venue,
                year=year,
                title=cleaned_title,
                authors=authors,
                landing_page_url=index_url,
                discovery_source=index_url,
            )
            paper.add_source_url(index_url)
            paper.add_trace("official_index:discovered_from_sigops_accepted_page")
            papers.append(paper)

        return index_url, papers

    def enrich_paper(self, paper: ConferencePaper, client: Any) -> ConferencePaper:
        paper.add_trace("official_detail:no_detail_page_available_from_sigops_accepted_list")
        return paper
