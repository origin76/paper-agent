from __future__ import annotations

import re
from typing import Any

from paper_agent.conference_parsing import (
    extract_document_paper_metadata,
    looks_like_author_line,
    normalize_space,
    normalize_title_key,
    split_authors,
)
from paper_agent.conference_types import ConferencePaper


POPL_FOREIGN_TRACK_MARKERS = (
    "toplas",
    "src",
    "student research competition",
    "cpp",
    "icfp",
    "oopsla",
    "pldi",
)
POPL_NOISE_TITLE_MARKERS = (
    "welcome from the general chair",
    "welcome from the pc chair",
    "welcome from the program chair",
    "program display configuration",
    "popl call for papers",
    "invited talk",
    "tutorial",
    "panel",
)
POPL_EVENT_TITLE_MARKERS = (
    "catering at",
    "networking reception",
    "women @ popl",
    "mentoring lunch",
    "steering committee lunch",
    "urm lunch",
    "social event",
    "business meeting",
    "student research competition",
    "poster session",
    "sigplan ec meeting",
)
POPL_TITLE_DECORATION_MARKERS = (
    "distinguished paper",
    "inperson",
    "remote",
    "virtual",
    "recorded",
)
POPL_RESOURCE_MARKERS = (
    "link to publication",
    "doi",
    "pre-print",
    "file attached",
    "media attached",
)
POPL_AFFILIATION_MARKERS = (
    "university",
    "institute",
    "college",
    "laboratory",
    "laboratories",
    "research",
    "school",
    "department",
)


class POPLAdapter:
    venue = "popl"

    def build_index_url(self, year: int) -> str:
        return f"https://popl{str(year)[-2:]}.sigplan.org/track/POPL-{year}-popl-research-papers"

    def _looks_like_title(self, title: str) -> bool:
        normalized = normalize_space(title)
        lowered = normalized.lower()
        if len(normalized) < 12 or len(normalized) > 260:
            return False
        if normalized.endswith(":"):
            return False
        if len(normalized.split()) < 3:
            return False
        if lowered in {"accepted papers", "title"}:
            return False
        if any(marker in lowered for marker in POPL_NOISE_TITLE_MARKERS):
            return False
        if self._looks_like_event_title(lowered):
            return False
        if self._looks_like_affiliation_line(normalized):
            return False
        return True

    def _is_track_title(self, title: str) -> bool:
        normalized = normalize_space(title)
        lowered = normalized.lower()
        bracketed_labels = re.findall(r"[\[\(]([^\]\)]+)[\]\)]", lowered)
        if any(any(marker in label for marker in POPL_FOREIGN_TRACK_MARKERS) for label in bracketed_labels):
            return False
        if any(marker in lowered for marker in ("(toplas)", "[toplas]", "(src)", "[src]")):
            return False
        if any(marker in lowered for marker in POPL_NOISE_TITLE_MARKERS):
            return False
        if self._looks_like_event_title(lowered):
            return False
        return True

    @staticmethod
    def _looks_like_event_title(lowered_title: str) -> bool:
        if any(marker in lowered_title for marker in POPL_EVENT_TITLE_MARKERS):
            return True
        if lowered_title.startswith(("lunchcatering", "dinnercatering", "receptioncatering")):
            return True
        if lowered_title.startswith(("src poster", "poster session", "sigplan ec")):
            return True
        if re.search(r"\b(?:lunch|dinner|reception|breakfast|banquet)\b.*\bcatering at\b", lowered_title):
            return True
        return False

    def _apply_resource_link(self, paper: ConferencePaper, url: str) -> None:
        lowered = url.lower()
        if "doi.org/" in lowered:
            if not paper.doi_url:
                paper.doi_url = url
                paper.add_source_url(url)
            return
        if any(marker in lowered for marker in ("arxiv.org/", "zenodo.org")):
            if not paper.preprint_url:
                paper.preprint_url = url
                paper.add_source_url(url)
            if not paper.pdf_url and lowered.endswith(".pdf"):
                paper.pdf_url = url
                paper.add_source_url(url)
            return
        if lowered.endswith(".pdf"):
            if not paper.pdf_url:
                paper.pdf_url = url
                paper.add_source_url(url)
            elif not paper.preprint_url:
                paper.preprint_url = url
                paper.add_source_url(url)
            return
        paper.add_alternate_url(url)

    def _consume_resource_links(self, paper: ConferencePaper, marker_line: str, resource_urls: list[str], index: int) -> int:
        lowered = marker_line.lower()
        expected_count = 0
        if "link to publication" in lowered:
            expected_count += 1
        if "doi" in lowered:
            expected_count += 1
        if any(marker in lowered for marker in ("pre-print", "file attached", "media attached")):
            expected_count += 1

        for _ in range(expected_count):
            if index >= len(resource_urls):
                break
            self._apply_resource_link(paper, resource_urls[index])
            index += 1
        return index

    def _is_program_session_header(self, line: str) -> bool:
        return "POPL at " in line

    def _extract_session_title(self, line: str) -> str:
        return normalize_space(line.split("POPL at ", 1)[0])

    def _is_research_session(self, session_title: str) -> bool:
        lowered = session_title.lower()
        return not any(
            marker in lowered
            for marker in (
                "welcome",
                "keynote",
                "break",
                "lunch",
                "reception",
                "social event",
                "business meeting",
                "poster",
            )
        )

    @staticmethod
    def _is_time_range(line: str) -> bool:
        return re.fullmatch(r"\d{1,2}:\d{2}\s*-\s*\d{1,2}:\d{2}", line) is not None

    @staticmethod
    def _is_clock_time(line: str) -> bool:
        return re.fullmatch(r"\d{1,2}:\d{2}", line) is not None

    @staticmethod
    def _is_duration(line: str) -> bool:
        return re.fullmatch(r"(\d+h)?\d+m", line) is not None or re.fullmatch(r"\+\d+min", line) is not None

    def _is_resource_marker_line(self, line: str) -> bool:
        lowered = line.lower()
        return any(marker in lowered for marker in POPL_RESOURCE_MARKERS)

    @staticmethod
    def _is_session_metadata_line(line: str) -> bool:
        lowered = line.lower()
        return lowered.startswith("chair(s):")

    def _looks_like_affiliation_line(self, text: str) -> bool:
        normalized = normalize_space(text)
        lowered = normalized.lower()
        if not lowered:
            return False
        has_author_separator = "," in lowered or " and " in lowered or ";" in lowered
        has_affiliation_marker = any(marker in lowered for marker in POPL_AFFILIATION_MARKERS)
        if has_author_separator and has_affiliation_marker:
            return True
        if has_affiliation_marker and re.match(
            r"^[A-Z][\w'.-]+(?: [A-Z][\w'.-]+){1,6} (?:University|Institute|College|Laboratory|Laboratories|School|Department)\b",
            normalized,
        ):
            return True
        return False

    @staticmethod
    def _is_program_section_break(line: str) -> bool:
        lowered = normalize_space(line).lower()
        return lowered in {"accepted papers", "title", "accepted papersaccepted papers"}

    def _extract_program_papers(self, document: Any, year: int, index_url: str, resource_urls: list[str]) -> list[ConferencePaper]:
        lines: list[str] = []
        for block in document.text_blocks:
            lines.extend(normalize_space(line) for line in block.splitlines() if normalize_space(line))

        papers: list[ConferencePaper] = []
        seen_titles: set[str] = set()
        current_session_title = ""
        current_session_is_research = False
        pending_title: str | None = None
        resource_index = 0

        for line in lines:
            if self._is_time_range(line):
                continue
            if self._is_program_section_break(line):
                current_session_is_research = False
                pending_title = None
                continue
            if self._is_program_session_header(line):
                current_session_title = self._extract_session_title(line)
                current_session_is_research = self._is_research_session(current_session_title)
                pending_title = None
                continue
            if not current_session_is_research:
                continue
            if self._is_clock_time(line) or self._is_duration(line):
                continue
            if self._is_session_metadata_line(line):
                continue

            if papers and self._is_resource_marker_line(line):
                resource_index = self._consume_resource_links(papers[-1], line, resource_urls, resource_index)
                continue

            if pending_title is None:
                candidate_title = self._clean_title(line)
                if not self._looks_like_title(candidate_title) or not self._is_track_title(candidate_title):
                    continue
                pending_title = candidate_title
                continue

            authors_line = self._clean_authors(line)
            if not authors_line:
                continue
            authors = split_authors(authors_line) if looks_like_author_line(authors_line) else [authors_line]
            title_key = normalize_title_key(pending_title)
            if title_key not in seen_titles:
                seen_titles.add(title_key)
                paper = ConferencePaper(
                    venue=self.venue,
                    year=year,
                    title=pending_title,
                    authors=authors,
                    session=current_session_title,
                    landing_page_url=index_url,
                    discovery_source=index_url,
                )
                paper.add_source_url(index_url)
                paper.add_trace("official_index:discovered_from_sigplan_program_blocks")
                papers.append(paper)
            pending_title = None

        return papers

    def _assign_accepted_section_resource_links(self, papers: list[ConferencePaper], resource_urls: list[str]) -> None:
        link_index = 0
        for paper in papers:
            while link_index < len(resource_urls) and "doi.org/" not in resource_urls[link_index].lower():
                link_index += 1
            if link_index >= len(resource_urls):
                break

            if not paper.doi_url:
                self._apply_resource_link(paper, resource_urls[link_index])
            link_index += 1

            while link_index < len(resource_urls) and "doi.org/" not in resource_urls[link_index].lower():
                self._apply_resource_link(paper, resource_urls[link_index])
                link_index += 1

    def _strip_suffix_markers(self, text: str, markers: tuple[str, ...]) -> str:
        normalized = normalize_space(text)
        changed = True
        while changed and normalized:
            changed = False
            lowered = normalized.lower()
            for marker in markers:
                if lowered.endswith(marker):
                    normalized = normalize_space(normalized[: -len(marker)])
                    changed = True
                    break
        return normalized

    def _clean_title(self, text: str) -> str:
        normalized = self._strip_suffix_markers(text, POPL_TITLE_DECORATION_MARKERS)
        return normalize_space(normalized)

    def _clean_authors(self, text: str) -> str:
        normalized = self._strip_suffix_markers(text, POPL_RESOURCE_MARKERS)
        return normalize_space(normalized)

    def _parse_inline_accepted_paper(self, line: str) -> tuple[str, list[str]] | None:
        normalized = normalize_space(line)
        split_index = normalized.rfind("POPL")
        if split_index <= 0:
            return None

        title_part = self._clean_title(normalized[:split_index])
        authors_part = self._clean_authors(normalized[split_index + 4 :])
        if not self._looks_like_title(title_part) or not self._is_track_title(title_part):
            return None
        authors = split_authors(authors_part) if looks_like_author_line(authors_part) else []
        return title_part, authors

    def _extract_accepted_section_papers(self, document: Any, year: int, index_url: str) -> list[ConferencePaper]:
        lines: list[str] = []
        for block in document.text_blocks:
            lines.extend(normalize_space(line) for line in block.splitlines() if normalize_space(line))

        accepted_start = next(
            (
                index
                for index, line in enumerate(lines)
                if "accepted papers" in line.lower()
            ),
            None,
        )
        if accepted_start is None:
            return []

        papers: list[ConferencePaper] = []
        seen_titles: set[str] = set()
        stop_markers = {
            f"popl {year} call for papers",
            "scope",
            "evaluation criteria",
            "double-blind reviewing",
            "submission guidelines",
            "important dates aoe (utc-12h)",
            "program committee",
        }
        skip_markers = {"title", "---"}

        for line in lines[accepted_start + 1 :]:
            lowered = line.lower()
            if lowered in stop_markers:
                break
            if lowered in skip_markers:
                continue
            parsed = self._parse_inline_accepted_paper(line)
            if parsed is None:
                continue
            title, authors = parsed
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
            paper.add_trace("official_index:discovered_from_sigplan_accepted_section")
            papers.append(paper)

        return papers

    def discover_papers(self, year: int, client: Any) -> tuple[str, list[ConferencePaper]]:
        index_url = self.build_index_url(year)
        document = client.fetch_document(index_url)
        detail_marker = f"/details/popl-{year}-popl-research-papers/"

        detail_papers_by_key: dict[str, ConferencePaper] = {}
        for link in document.links:
            if detail_marker not in link.url.lower():
                continue
            title = self._clean_title(link.text)
            if not self._looks_like_title(title) or not self._is_track_title(title):
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
                or "media attached" in link.text.lower()
                or "link to publication" in link.text.lower()
                or "arxiv.org/" in link.url.lower()
                or link.url.lower().endswith(".pdf")
            )
        ]
        resource_urls = [link.url for link in resource_links]

        papers: list[ConferencePaper] = []
        seen_titles = set()
        program_papers = self._extract_program_papers(document, year, index_url, resource_urls)
        for paper in program_papers:
            detail_paper = detail_papers_by_key.get(normalize_title_key(paper.title))
            if detail_paper:
                paper.detail_url = detail_paper.detail_url
                paper.landing_page_url = detail_paper.landing_page_url
                for url in detail_paper.source_urls:
                    paper.add_source_url(url)
                for trace in detail_paper.resolution_trace:
                    paper.add_trace(trace)
            seen_titles.add(normalize_title_key(paper.title))
            papers.append(paper)

        accepted_section_papers = self._extract_accepted_section_papers(document, year, index_url)
        if accepted_section_papers and not program_papers:
            self._assign_accepted_section_resource_links(accepted_section_papers, resource_urls)
        for paper in accepted_section_papers:
            title_key = normalize_title_key(paper.title)
            if title_key in seen_titles:
                continue
            detail_paper = detail_papers_by_key.get(normalize_title_key(paper.title))
            if detail_paper:
                paper.detail_url = detail_paper.detail_url
                paper.landing_page_url = detail_paper.landing_page_url
                for url in detail_paper.source_urls:
                    paper.add_source_url(url)
                for trace in detail_paper.resolution_trace:
                    paper.add_trace(trace)
            seen_titles.add(title_key)
            papers.append(paper)

        for title_key, detail_paper in detail_papers_by_key.items():
            if title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            papers.append(detail_paper)

        return index_url, papers

    def enrich_paper(self, paper: ConferencePaper, client: Any) -> ConferencePaper:
        if not paper.detail_url:
            paper.add_trace("official_detail:no_sigplan_detail_page_used")
            return paper

        document = client.fetch_document(paper.detail_url)
        metadata = extract_document_paper_metadata(document)
        refined_title = str(metadata.get("title") or "").strip()
        if refined_title and self._looks_like_title(refined_title) and self._is_track_title(refined_title):
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
