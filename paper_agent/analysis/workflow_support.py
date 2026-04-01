from __future__ import annotations

import json
import re
from typing import Any

from paper_agent.config import RuntimeConfig
from paper_agent.reporting.sections import clean_section_title
from paper_agent.runtime import append_stage_trace, log_event

from .kimi_client import KimiClient


def empty_web_research_payload() -> dict[str, Any]:
    return {
        "related_work_signals": [],
        "code_resources": [],
        "reading_notes": [],
        "reviewer_signals": [],
        "external_risks_or_confusions": [],
        "source_shortlist": [],
    }


def empty_resource_discovery_payload() -> dict[str, Any]:
    return {
        "official_pages": [],
        "code_repositories": [],
        "datasets_and_benchmarks": [],
        "reproducibility_materials": [],
        "implementation_signals": [],
        "missing_resource_gaps": [],
    }


def dedupe_and_limit_items(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    seen_urls: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url", "")).strip()
        key = url or json.dumps(item, ensure_ascii=False, sort_keys=True)
        if key in seen_urls:
            continue
        seen_urls.add(key)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


def merge_web_research_with_paper_signals(
    web_research: dict[str, Any],
    paper_web_signals: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(web_research)
    source_shortlist = list(merged.get("source_shortlist") or [])
    code_resources = list(merged.get("code_resources") or [])
    reading_notes = list(merged.get("reading_notes") or [])

    existing_source_urls = {str(item.get("url", "")).strip() for item in source_shortlist if isinstance(item, dict)}
    existing_code_urls = {str(item.get("url", "")).strip() for item in code_resources if isinstance(item, dict)}
    existing_note_urls = {str(item.get("url", "")).strip() for item in reading_notes if isinstance(item, dict)}

    for url in paper_web_signals.get("official_urls") or []:
        if url in existing_source_urls:
            continue
        source_shortlist.append(
            {
                "title": "论文内提到的官方链接",
                "url": url,
                "type": "paper_embedded_official_link",
            }
        )
        existing_source_urls.add(url)
        if url not in existing_note_urls:
            reading_notes.append(
                {
                    "title": "论文正文中的官方页面",
                    "url": url,
                    "insight": "该链接直接出现在论文正文中，可作为最可信的一手入口，用于核验会议页面、项目页或 artifact 页面。",
                }
            )
            existing_note_urls.add(url)

    for url in paper_web_signals.get("github_urls") or []:
        if url not in existing_source_urls:
            source_shortlist.append(
                {
                    "title": "论文内提到的代码仓库",
                    "url": url,
                    "type": "paper_embedded_repository",
                }
            )
            existing_source_urls.add(url)
        if url not in existing_code_urls:
            code_resources.append(
                {
                    "title": "论文内提到的 GitHub 仓库",
                    "url": url,
                    "why_relevant": "该仓库链接直接出现在论文正文中，优先级高于二手网页检索结果。",
                }
            )
            existing_code_urls.add(url)

    merged["source_shortlist"] = dedupe_and_limit_items(source_shortlist, limit=12)
    merged["code_resources"] = dedupe_and_limit_items(code_resources, limit=8)
    merged["reading_notes"] = dedupe_and_limit_items(reading_notes, limit=10)
    return merged


def merge_resource_discovery_with_paper_signals(
    resource_discovery: dict[str, Any],
    paper_web_signals: dict[str, Any],
) -> dict[str, Any]:
    merged = dict(resource_discovery)
    official_pages = list(merged.get("official_pages") or [])
    code_repositories = list(merged.get("code_repositories") or [])
    reproducibility_materials = list(merged.get("reproducibility_materials") or [])

    existing_official_urls = {str(item.get("url", "")).strip() for item in official_pages if isinstance(item, dict)}
    existing_repo_urls = {str(item.get("url", "")).strip() for item in code_repositories if isinstance(item, dict)}
    existing_repro_urls = {
        str(item.get("url", "")).strip() for item in reproducibility_materials if isinstance(item, dict)
    }

    for url in paper_web_signals.get("official_urls") or []:
        if url in existing_official_urls:
            continue
        official_pages.append(
            {
                "title": "论文内提到的官方页面",
                "url": url,
                "page_type": "paper_embedded_official_link",
                "why_relevant": "该页面直接出现在论文正文中，适合作为核验会议页面、项目主页或 artifact 入口的一手来源。",
            }
        )
        existing_official_urls.add(url)

    for url in paper_web_signals.get("github_urls") or []:
        if url in existing_repo_urls:
            continue
        code_repositories.append(
            {
                "title": "论文内提到的 GitHub 仓库",
                "url": url,
                "repo_kind": "paper_embedded_repository",
                "why_relevant": "代码仓库链接由论文正文直接给出，可信度高，适合优先作为复现入口。",
            }
        )
        existing_repo_urls.add(url)

    for url in paper_web_signals.get("artifact_urls") or []:
        if url in existing_repro_urls:
            continue
        reproducibility_materials.append(
            {
                "title": "论文内提到的 artifact / DOI 页面",
                "url": url,
                "material_type": "paper_embedded_artifact",
                "why_relevant": "该链接直接来自论文正文，适合作为 artifact、归档材料或复现实验入口。",
            }
        )
        existing_repro_urls.add(url)

    merged["official_pages"] = dedupe_and_limit_items(official_pages, limit=10)
    merged["code_repositories"] = dedupe_and_limit_items(code_repositories, limit=8)
    merged["reproducibility_materials"] = dedupe_and_limit_items(reproducibility_materials, limit=10)
    return merged


def normalize_section_name(title: str) -> str:
    cleaned_title = clean_section_title(title) or title
    normalized = re.sub(r"^\d+(\.\d+)*\s*", "", cleaned_title.strip().lower())
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def match_extracted_section(
    desired_title: str,
    extracted_sections: list[dict[str, Any]],
    used_indexes: set[int],
) -> dict[str, Any] | None:
    desired = normalize_section_name(desired_title)
    if not desired:
        return None

    desired_tokens = {
        token
        for token in desired.split()
        if token not in {"section", "the", "and", "of", "to"} and not token.isdigit()
    }
    exact_match = None
    fuzzy_match = None
    token_match = None
    for index, section in enumerate(extracted_sections):
        if index in used_indexes:
            continue
        candidate = normalize_section_name(str(section.get("title", "")))
        candidate_tokens = {
            token
            for token in candidate.split()
            if token not in {"section", "the", "and", "of", "to"} and not token.isdigit()
        }
        if candidate == desired:
            exact_match = (index, section)
            break
        if desired in candidate or candidate in desired:
            fuzzy_match = fuzzy_match or (index, section)
        if desired_tokens and candidate_tokens and desired_tokens & candidate_tokens:
            token_match = token_match or (index, section)

    if exact_match is not None:
        used_indexes.add(exact_match[0])
        return exact_match[1]
    if fuzzy_match is not None:
        used_indexes.add(fuzzy_match[0])
        return fuzzy_match[1]
    if token_match is not None:
        used_indexes.add(token_match[0])
        return token_match[1]
    return None


def section_selection_score(section: dict[str, Any]) -> tuple[int, int]:
    title = str(section.get("title", "")).strip()
    lowered = title.lower()
    score = 0
    title_quality = int(section.get("title_quality", 0))

    if "front matter" in lowered:
        score -= 10
    if "." in title:
        score -= 2
    if "+" in title:
        score -= 1
    score += title_quality
    if title_quality < 0:
        score -= 4
    elif title_quality >= 5:
        score += 2

    high_value_keywords = (
        "abstract",
        "introduction",
        "background",
        "motivation",
        "approach",
        "design",
        "implementation",
        "evaluation",
        "related work",
        "discussion",
        "future work",
        "conclusion",
    )
    for keyword in high_value_keywords:
        if keyword in lowered:
            score += 4

    word_count = len(title.split())
    if 1 <= word_count <= 8:
        score += 2
    elif word_count > 14:
        score -= 2

    return score, int(section.get("char_count", 0))


def pick_section_targets(
    structure: dict[str, Any],
    extracted_sections: list[dict[str, Any]],
    *,
    max_sections: int,
) -> list[dict[str, Any]]:
    if not extracted_sections:
        return []

    prioritized_titles: list[str] = []
    for priority in ("high", "medium", "low"):
        for item in structure.get("section_map") or []:
            if not isinstance(item, dict):
                continue
            title = str(item.get("section_title", "")).strip()
            if not title or title in prioritized_titles:
                continue
            if str(item.get("priority", "")).lower() == priority:
                prioritized_titles.append(title)
            if len(prioritized_titles) >= max_sections:
                break
        if len(prioritized_titles) >= max_sections:
            break

    used_indexes: set[int] = set()
    selected_sections: list[dict[str, Any]] = []
    for title in prioritized_titles:
        matched = match_extracted_section(title, extracted_sections, used_indexes)
        if matched is not None:
            selected_sections.append(matched)
        if len(selected_sections) >= max_sections:
            break

    if len(selected_sections) < max_sections:
        ranked_remaining_sections = sorted(
            [
                (index, section)
                for index, section in enumerate(extracted_sections)
                if index not in used_indexes
            ],
            key=lambda item: section_selection_score(item[1]),
            reverse=True,
        )
        for index, section in ranked_remaining_sections:
            if index in used_indexes:
                continue
            selected_sections.append(section)
            used_indexes.add(index)
            if len(selected_sections) >= max_sections:
                break

    if not selected_sections:
        selected_sections = extracted_sections[:max_sections]

    return selected_sections[:max_sections]


def chat_analysis_text_with_fallback(
    *,
    client: KimiClient,
    config: RuntimeConfig,
    run_dir: str,
    web_search_enabled: bool,
    stage: str,
    messages: list[dict[str, Any]],
) -> tuple[str, dict[str, Any]]:
    requested_model = config.analysis_model
    try:
        content, meta = client.chat_text(
            messages,
            model=requested_model,
            enable_thinking=config.analysis_enable_thinking,
            enable_search=web_search_enabled,
            stage=stage,
        )
        meta["requested_model"] = requested_model
        meta["fallback_used"] = False
        return content, meta
    except Exception as exc:
        fallback_model = config.analysis_fallback_model
        if not fallback_model or fallback_model == requested_model or not client.is_model_availability_error(exc):
            raise

        append_stage_trace(
            run_dir,
            stage,
            "fallback",
            requested_model=requested_model,
            fallback_model=fallback_model,
            reason=str(exc),
        )
        log_event(
            "warning",
            "Analysis model unavailable, retrying with fallback model",
            stage=stage,
            requested_model=requested_model,
            fallback_model=fallback_model,
            reason=str(exc),
        )
        content, meta = client.chat_text(
            messages,
            model=fallback_model,
            enable_thinking=config.analysis_enable_thinking,
            enable_search=web_search_enabled,
            stage=f"{stage}.fallback",
        )
        meta["requested_model"] = requested_model
        meta["fallback_model"] = fallback_model
        meta["fallback_used"] = True
        meta["fallback_reason"] = str(exc)
        return content, meta
