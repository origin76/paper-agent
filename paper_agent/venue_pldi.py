from __future__ import annotations

import re
from typing import Any

from paper_agent.conference_parsing import (
    extract_document_paper_metadata,
    looks_like_author_line,
    looks_like_paper_title,
    normalize_space,
    normalize_title_key,
    split_authors,
)
from paper_agent.conference_types import ConferencePaper


PLDI_BRACKETED_FOREIGN_TRACK_MARKERS = (
    "oopsla",
    "popl",
    "cc ",
    "cc]",
    "icfp",
    "toplas",
    "plmw",
    "src",
)


class PLDIAdapter:
    venue = "pldi"

    def _is_track_title(self, title: str) -> bool:
        normalized = normalize_space(title)
        lowered = normalized.lower()
        if lowered.startswith("["):
            closing = lowered.find("]")
            bracket_label = lowered[1:closing] if closing != -1 else lowered[1:]
            if any(marker in bracket_label for marker in PLDI_BRACKETED_FOREIGN_TRACK_MARKERS):
                return False
        if lowered.startswith("-"):
            return False
        if "pldi 2024 -" in lowered or "pldi research papers at" in lowered:
            return False
        return True

    def build_index_url(self, year: int) -> str:
        year_suffix = str(year)[-2:]
        if year in {2022, 2023}:
            return f"https://pldi{year_suffix}.sigplan.org/track/pldi-{year}-pldi"
        return f"https://pldi{year_suffix}.sigplan.org/track/pldi-{year}-papers"

    def _extract_accepted_section_papers(self, document: Any, year: int, index_url: str) -> list[ConferencePaper]:
        lines: list[str] = []
        for block in document.text_blocks:
            lines.extend(normalize_space(line) for line in block.splitlines() if normalize_space(line))

        accepted_start = next(
            (index for index, line in enumerate(lines) if line.lower() == "accepted papers"),
            None,
        )
        if accepted_start is None:
            return []

        papers: list[ConferencePaper] = []
        seen_titles: set[str] = set()
        skip_markers = {
            "accepted papers",
            "title",
            "pldi research papers",
            "doi",
            "pre-print",
            "media attached",
            "file attached",
            "link to publication",
        }
        stop_markers = {
            "important dates",
            "submission link",
            "program display configuration",
        }

        index = accepted_start + 1
        while index < len(lines):
            line = lines[index]
            lowered = line.lower()
            if lowered in stop_markers:
                break
            if lowered in skip_markers or lowered.startswith("-"):
                index += 1
                continue
            if not looks_like_paper_title(line) or not self._is_track_title(line):
                index += 1
                continue

            next_marker = ""
            next_marker_index = index + 1
            while next_marker_index < min(index + 5, len(lines)):
                candidate_marker = lines[next_marker_index].lower()
                if candidate_marker in skip_markers:
                    next_marker = candidate_marker
                    break
                if candidate_marker:
                    next_marker = candidate_marker
                    break
                next_marker_index += 1
            if next_marker != "pldi research papers":
                index += 1
                continue

            title = line
            authors: list[str] = []
            cursor = next_marker_index + 1
            while cursor < min(index + 8, len(lines)):
                candidate = lines[cursor]
                candidate_lowered = candidate.lower()
                if candidate_lowered in stop_markers:
                    break
                if candidate_lowered in skip_markers:
                    cursor += 1
                    continue
                if looks_like_author_line(candidate):
                    authors = split_authors(candidate)
                    cursor += 1
                    break
                if looks_like_paper_title(candidate) and self._is_track_title(candidate):
                    break
                cursor += 1

            title_key = normalize_title_key(title)
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                paper = ConferencePaper(
                    venue=self.venue,
                    year=year,
                    title=title,
                    authors=authors,
                    landing_page_url=index_url,
                    discovery_source=index_url,
                )
                paper.add_source_url(index_url)
                paper.add_trace("official_index:discovered_from_sigplan_accepted_section")
                papers.append(paper)

            index = max(index + 1, cursor)

        return papers

    def discover_papers(self, year: int, client: Any) -> tuple[str, list[ConferencePaper]]:
        index_url = self.build_index_url(year)
        document = client.fetch_document(index_url)
        detail_marker = f"/details/pldi-{year}-papers/"

        detail_papers_by_key: dict[str, ConferencePaper] = {}
        for link in document.links:
            if detail_marker not in link.url.lower():
                continue
            title = normalize_space(link.text)
            if not looks_like_paper_title(title) or not self._is_track_title(title):
                continue
            title_key = normalize_title_key(title)
            if title_key in detail_papers_by_key:
                continue
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
            paper.add_trace("official_index:discovered_from_sigplan_track_page")
            detail_papers_by_key[title_key] = paper

        resource_links = [
            link
            for link in document.links
            if (
                "doi.org/" in link.url.lower()
                or "pre-print" in link.text.lower()
                or "file attached" in link.text.lower()
                or "arxiv.org/" in link.url.lower()
                or link.url.lower().endswith(".pdf")
            )
        ]

        lines: list[str] = []
        for block in document.text_blocks:
            lines.extend(normalize_space(line) for line in block.splitlines() if normalize_space(line))

        main_program_start = 0
        for index, line in enumerate(lines):
            if re.fullmatch(r"\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}", line):
                main_program_start = index
                break
        if main_program_start:
            lines = lines[main_program_start:]

        fallback_papers = self._extract_accepted_section_papers(document, year, index_url)
        papers: list[ConferencePaper] = []
        seen_titles = {normalize_title_key(paper.title) for paper in fallback_papers}
        for paper in fallback_papers:
            detail_paper = detail_papers_by_key.get(normalize_title_key(paper.title))
            if detail_paper:
                paper.detail_url = detail_paper.detail_url
                paper.landing_page_url = detail_paper.landing_page_url
                for url in detail_paper.source_urls:
                    paper.add_source_url(url)
                for trace in detail_paper.resolution_trace:
                    paper.add_trace(trace)
            papers.append(paper)

        for index, line in enumerate(lines):
            if line.lower() != "talk":
                continue

            title = ""
            authors: list[str] = []
            for cursor in range(index + 1, min(index + 6, len(lines))):
                candidate = lines[cursor]
                if not title and looks_like_paper_title(candidate) and self._is_track_title(candidate):
                    if "pldi 2024" in candidate.lower():
                        continue
                    title = candidate
                    continue
                if title and looks_like_author_line(candidate):
                    authors = split_authors(candidate)
                    break

            if not title or not authors:
                continue
            title_key = normalize_title_key(title)
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            paper = ConferencePaper(
                venue=self.venue,
                year=year,
                title=title,
                authors=authors,
                landing_page_url=index_url,
                discovery_source=index_url,
            )
            paper.add_source_url(index_url)
            paper.add_trace("official_index:discovered_from_sigplan_talk_sequence")
            papers.append(paper)

        skip_markers = {"talk", "pldi research papers", "doi", "pre-print", "doipre-print", "file attached"}
        for index, line in enumerate(lines):
            lowered = line.lower()
            if lowered in skip_markers or lowered.startswith("-") or "pldi 2024" in lowered:
                continue
            if not looks_like_paper_title(line) or looks_like_author_line(line) or not self._is_track_title(line):
                continue
            if re.fullmatch(r"\d{1,2}:\d{2}", line) or re.fullmatch(r"\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}", line):
                continue
            if any(marker in lowered for marker in ("chair(s):", "keynote", "lunch", "displayed time zone", "program display configuration")):
                continue

            title = line
            authors: list[str] = []
            previous_line = lines[index - 1] if index > 0 else ""
            if not (
                previous_line.lower() == "talk"
                or re.fullmatch(r"\d{1,2}:\d{2}", previous_line)
                or previous_line.lower().startswith("chair(s):")
            ):
                continue
            for cursor in range(index + 1, min(index + 5, len(lines))):
                candidate = lines[cursor]
                if title and looks_like_author_line(candidate):
                    authors = split_authors(candidate)
                    break

            if not authors:
                continue
            title_key = normalize_title_key(title)
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            paper = ConferencePaper(
                venue=self.venue,
                year=year,
                title=title,
                authors=authors,
                landing_page_url=index_url,
                discovery_source=index_url,
            )
            paper.add_source_url(index_url)
            paper.add_trace("official_index:discovered_from_sigplan_program_blocks")
            papers.append(paper)

        for title_key, detail_paper in detail_papers_by_key.items():
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            papers.append(detail_paper)

        link_index = 0
        for paper in papers:
            while link_index < len(resource_links) and "doi.org/" not in resource_links[link_index].url.lower():
                link_index += 1
            if link_index >= len(resource_links):
                break

            doi_link = resource_links[link_index]
            paper.doi_url = doi_link.url
            paper.add_source_url(doi_link.url)
            link_index += 1

            while link_index < len(resource_links) and "doi.org/" not in resource_links[link_index].url.lower():
                extra_link = resource_links[link_index]
                extra_url = extra_link.url
                extra_text = extra_link.text.lower()
                if not paper.preprint_url and (
                    "pre-print" in extra_text
                    or "file attached" in extra_text
                    or "arxiv.org/" in extra_url.lower()
                ):
                    paper.preprint_url = extra_url
                    paper.add_source_url(extra_url)
                if not paper.pdf_url and extra_url.lower().endswith(".pdf"):
                    paper.pdf_url = extra_url
                    paper.add_source_url(extra_url)
                link_index += 1

        return index_url, papers

    def enrich_paper(self, paper: ConferencePaper, client: Any) -> ConferencePaper:
        if not paper.detail_url:
            paper.add_trace("official_detail:no_sigplan_detail_page_used")
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
        paper.add_trace("official_detail:enriched_from_sigplan_detail_page")
        return paper
