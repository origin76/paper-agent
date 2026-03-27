from __future__ import annotations

from typing import Any


def _author_query_fragment(authors: list[str]) -> str:
    surnames: list[str] = []
    for author in authors[:3]:
        parts = str(author).strip().split()
        if parts:
            surnames.append(parts[-1])
    return " ".join(surnames)


def build_search_queries(overview: dict[str, Any], paper_web_signals: dict[str, Any] | None = None) -> list[str]:
    title = str(overview.get("paper_title", "")).strip()
    authors = [str(item).strip() for item in (overview.get("authors") or []) if str(item).strip()]
    venue = str(overview.get("venue", "")).strip()
    publication_year = str(overview.get("publication_year", "")).strip()
    key_terms = [str(item).strip() for item in (overview.get("key_terms") or []) if str(item).strip()]
    claims = overview.get("core_claims") or []
    problem = str(overview.get("problem_statement", "")).strip()
    takeaway = str(overview.get("one_sentence_takeaway", "")).strip()
    paper_web_signals = paper_web_signals or {}
    official_urls = [str(item).strip() for item in (paper_web_signals.get("official_urls") or []) if str(item).strip()]
    github_urls = [str(item).strip() for item in (paper_web_signals.get("github_urls") or []) if str(item).strip()]

    author_fragment = _author_query_fragment(authors)
    venue_fragment = " ".join(item for item in [venue, publication_year] if item)

    query_candidates = [
        f'"{title}" {venue_fragment}'.strip(),
        f'"{title}" {author_fragment}'.strip(),
        f'site:usenix.org "{title}"',
        f'"{title}" github code implementation',
        f'"{title}" artifact evaluation reproducibility',
        f'"{title}" blog review reading note',
    ]

    if key_terms:
        query_candidates.append(f'{title} {" ".join(key_terms[:3])}')
    if claims:
        query_candidates.append(f'{title} {" ".join(str(item) for item in claims[:2])}')
    elif problem:
        query_candidates.append(f"{title} {problem}")
    elif takeaway:
        query_candidates.append(f"{title} {takeaway}")

    for url in official_urls[:3]:
        query_candidates.append(url)
    for url in github_urls[:2]:
        query_candidates.append(url)

    deduped: list[str] = []
    for query in query_candidates:
        normalized = " ".join(query.split())
        if normalized and normalized not in deduped:
            deduped.append(normalized)

    return deduped[:8]
