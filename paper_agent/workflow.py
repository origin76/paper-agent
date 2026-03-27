from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, TypedDict

from langgraph.graph import END, START, StateGraph

from paper_agent.config import RuntimeConfig
from paper_agent.kimi_client import KimiClient
from paper_agent.pdf_extract import extract_pdf_text
from paper_agent.prompts import (
    BASE_SYSTEM_PROMPT,
    build_critique_prompt,
    build_experiment_prompt,
    build_extensions_prompt,
    build_overview_prompt,
    build_resource_discovery_prompt,
    build_section_prompt,
    build_structure_prompt,
    build_web_research_summary_prompt,
)
from paper_agent.report import render_report
from paper_agent.runtime import append_stage_trace, configure_logging, log_event
from paper_agent.sections import detect_sections, select_experiment_sections
from paper_agent.utils import extract_paper_web_signals, trim_balanced_text, write_json, write_text
from paper_agent.web_search import build_search_queries


class PaperState(TypedDict, total=False):
    pdf_path: str
    source_name: str
    run_dir: str
    paper_text: str
    paper_text_meta: dict[str, Any]
    paper_web_signals: dict[str, Any]
    extracted_sections: list[dict[str, Any]]
    overview: dict[str, Any]
    overview_meta: dict[str, Any]
    web_search_enabled: bool
    web_search_queries: list[str]
    web_search_results: list[dict[str, Any]]
    web_research: dict[str, Any]
    web_research_meta: dict[str, Any]
    resource_discovery: dict[str, Any]
    resource_discovery_meta: dict[str, Any]
    structure: dict[str, Any]
    structure_meta: dict[str, Any]
    section_targets: list[str]
    selected_sections: list[dict[str, Any]]
    section_analyses: list[dict[str, Any]]
    section_analyses_meta: list[dict[str, Any]]
    experiment_review: dict[str, Any]
    experiment_review_meta: dict[str, Any]
    critique: str
    critique_meta: dict[str, Any]
    extensions: str
    extensions_meta: dict[str, Any]
    report_markdown: str
    cleanup_result: dict[str, Any]


class PaperAnalysisWorkflow:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.client = KimiClient(config)
        self.graph = self._build_graph()

    def _build_graph(self):
        graph = StateGraph(PaperState)
        graph.add_node("ingest_pdf", self.ingest_pdf)
        graph.add_node("global_overview", self.global_overview)
        graph.add_node("web_research", self.web_research)
        graph.add_node("resource_discovery", self.resource_discovery)
        graph.add_node("structure_breakdown", self.structure_breakdown)
        graph.add_node("section_deep_dive", self.section_deep_dive)
        graph.add_node("experiment_review", self.experiment_review)
        graph.add_node("critique", self.critique)
        graph.add_node("extensions", self.extensions)
        graph.add_node("render_report", self.render_report_node)
        graph.add_node("cleanup_remote_file", self.cleanup_remote_file)

        graph.add_edge(START, "ingest_pdf")
        graph.add_edge("ingest_pdf", "global_overview")
        graph.add_edge("global_overview", "web_research")
        graph.add_edge("web_research", "resource_discovery")
        graph.add_edge("resource_discovery", "structure_breakdown")
        graph.add_edge("structure_breakdown", "section_deep_dive")
        graph.add_edge("section_deep_dive", "experiment_review")
        graph.add_edge("experiment_review", "critique")
        graph.add_edge("critique", "extensions")
        graph.add_edge("extensions", "render_report")
        graph.add_edge("render_report", "cleanup_remote_file")
        graph.add_edge("cleanup_remote_file", END)
        return graph.compile()

    def _empty_web_research_payload(self) -> dict[str, Any]:
        return {
            "related_work_signals": [],
            "code_resources": [],
            "reading_notes": [],
            "reviewer_signals": [],
            "external_risks_or_confusions": [],
            "source_shortlist": [],
        }

    def _empty_resource_discovery_payload(self) -> dict[str, Any]:
        return {
            "official_pages": [],
            "code_repositories": [],
            "datasets_and_benchmarks": [],
            "reproducibility_materials": [],
            "implementation_signals": [],
            "missing_resource_gaps": [],
        }

    def _merge_web_research_with_paper_signals(
        self,
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

        merged["source_shortlist"] = self._dedupe_and_limit_items(source_shortlist, limit=12)
        merged["code_resources"] = self._dedupe_and_limit_items(code_resources, limit=8)
        merged["reading_notes"] = self._dedupe_and_limit_items(reading_notes, limit=10)
        return merged

    def _merge_resource_discovery_with_paper_signals(
        self,
        resource_discovery: dict[str, Any],
        paper_web_signals: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(resource_discovery)
        official_pages = list(merged.get("official_pages") or [])
        code_repositories = list(merged.get("code_repositories") or [])
        reproducibility_materials = list(merged.get("reproducibility_materials") or [])

        existing_official_urls = {str(item.get("url", "")).strip() for item in official_pages if isinstance(item, dict)}
        existing_repo_urls = {str(item.get("url", "")).strip() for item in code_repositories if isinstance(item, dict)}
        existing_repro_urls = {str(item.get("url", "")).strip() for item in reproducibility_materials if isinstance(item, dict)}

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

        merged["official_pages"] = self._dedupe_and_limit_items(official_pages, limit=10)
        merged["code_repositories"] = self._dedupe_and_limit_items(code_repositories, limit=8)
        merged["reproducibility_materials"] = self._dedupe_and_limit_items(reproducibility_materials, limit=10)
        return merged

    def _dedupe_and_limit_items(self, items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
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

    def _stage_start(self, state: PaperState, stage: str, **fields: Any) -> None:
        append_stage_trace(state["run_dir"], stage, "started", **fields)
        log_event("info", "Stage started", stage=stage, **fields)

    def _stage_finish(self, state: PaperState, stage: str, **fields: Any) -> None:
        append_stage_trace(state["run_dir"], stage, "finished", **fields)
        log_event("info", "Stage finished", stage=stage, **fields)

    def _stage_error(self, state: PaperState, stage: str, error: Exception, **fields: Any) -> None:
        append_stage_trace(state["run_dir"], stage, "error", error=str(error), **fields)
        log_event("error", "Stage failed", stage=stage, error=str(error), **fields)

    def _paper_context(self, state: PaperState) -> str:
        return trim_balanced_text(state["paper_text"], self.config.paper_context_max_chars)

    def _normalize_section_name(self, title: str) -> str:
        normalized = re.sub(r"^\d+(\.\d+)*\s*", "", title.strip().lower())
        normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    def _match_extracted_section(
        self,
        desired_title: str,
        extracted_sections: list[dict[str, Any]],
        used_indexes: set[int],
    ) -> dict[str, Any] | None:
        desired = self._normalize_section_name(desired_title)
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
            candidate = self._normalize_section_name(str(section.get("title", "")))
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

    def _pick_section_targets(
        self,
        structure: dict[str, Any],
        extracted_sections: list[dict[str, Any]],
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
                if len(prioritized_titles) >= self.config.max_sections:
                    break
            if len(prioritized_titles) >= self.config.max_sections:
                break

        used_indexes: set[int] = set()
        selected_sections: list[dict[str, Any]] = []
        for title in prioritized_titles:
            matched = self._match_extracted_section(title, extracted_sections, used_indexes)
            if matched is not None:
                selected_sections.append(matched)
            if len(selected_sections) >= self.config.max_sections:
                break

        if len(selected_sections) < self.config.max_sections:
            ranked_remaining_sections = sorted(
                [
                    (index, section)
                    for index, section in enumerate(extracted_sections)
                    if index not in used_indexes
                ],
                key=lambda item: self._section_selection_score(item[1]),
                reverse=True,
            )
            for index, section in ranked_remaining_sections:
                if index in used_indexes:
                    continue
                selected_sections.append(section)
                used_indexes.add(index)
                if len(selected_sections) >= self.config.max_sections:
                    break

        if not selected_sections:
            selected_sections = extracted_sections[: self.config.max_sections]

        return selected_sections[: self.config.max_sections]

    def _section_selection_score(self, section: dict[str, Any]) -> tuple[int, int]:
        title = str(section.get("title", "")).strip()
        lowered = title.lower()
        score = 0

        if "front matter" in lowered:
            score -= 10
        if "." in title:
            score -= 2
        if "+" in title:
            score -= 1

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

    def _chat_analysis_text_with_fallback(
        self,
        state: PaperState,
        stage: str,
        messages: list[dict[str, Any]],
    ) -> tuple[str, dict[str, Any]]:
        requested_model = self.config.analysis_model
        try:
            content, meta = self.client.chat_text(
                messages,
                model=requested_model,
                enable_thinking=self.config.analysis_enable_thinking,
                enable_search=state.get("web_search_enabled", False),
                stage=stage,
            )
            meta["requested_model"] = requested_model
            meta["fallback_used"] = False
            return content, meta
        except Exception as exc:
            fallback_model = self.config.analysis_fallback_model
            if (
                not fallback_model
                or fallback_model == requested_model
                or not self.client.is_model_availability_error(exc)
            ):
                raise

            append_stage_trace(
                state["run_dir"],
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
            content, meta = self.client.chat_text(
                messages,
                model=fallback_model,
                enable_thinking=self.config.analysis_enable_thinking,
                enable_search=state.get("web_search_enabled", False),
                stage=f"{stage}.fallback",
            )
            meta["requested_model"] = requested_model
            meta["fallback_model"] = fallback_model
            meta["fallback_used"] = True
            meta["fallback_reason"] = str(exc)
            return content, meta

    def run(self, pdf_path: str, output_dir: str | None = None) -> PaperState:
        absolute_pdf = str(Path(pdf_path).expanduser().resolve())
        run_dir = self.config.create_run_dir(absolute_pdf, explicit_output_dir=output_dir)
        (run_dir / "stage_trace.jsonl").unlink(missing_ok=True)
        configure_logging(level=self.config.log_level, run_dir=run_dir)
        append_stage_trace(run_dir, "workflow", "started", pdf_path=absolute_pdf, output_dir=str(run_dir))
        log_event(
            "info",
            "Paper analysis run started",
            pdf_path=absolute_pdf,
            run_dir=run_dir,
            document_model=self.config.document_model,
            analysis_model=self.config.analysis_model,
            web_search_enabled=self.config.web_search_enabled,
        )
        state: PaperState = {
            "pdf_path": absolute_pdf,
            "source_name": Path(absolute_pdf).name,
            "run_dir": str(run_dir),
        }
        try:
            result = self.graph.invoke(state)
        except Exception as exc:
            append_stage_trace(run_dir, "workflow", "error", error=str(exc))
            log_event("error", "Paper analysis run failed", error=str(exc))
            raise
        append_stage_trace(run_dir, "workflow", "finished", output_dir=str(run_dir))
        log_event("info", "Paper analysis run finished", run_dir=run_dir)
        return result

    def ingest_pdf(self, state: PaperState) -> PaperState:
        stage = "ingest_pdf"
        run_dir = Path(state["run_dir"])
        self._stage_start(state, stage, pdf_path=state["pdf_path"])
        try:
            paper_text, paper_text_meta = extract_pdf_text(
                state["pdf_path"],
                timeout_seconds=self.config.pdf_extract_timeout_seconds,
                stage=stage,
            )
            extracted_sections = detect_sections(
                paper_text,
                max_sections=max(self.config.max_sections + 4, self.config.max_sections),
                target_chars=self.config.section_target_chars,
            )
            paper_web_signals = extract_paper_web_signals(paper_text)
            write_text(run_dir / "paper_text.txt", paper_text)
            write_json(run_dir / "paper_text_meta.json", paper_text_meta)
            write_json(run_dir / "paper_web_signals.json", paper_web_signals)
            write_json(run_dir / "extracted_sections.json", extracted_sections)
            result = {
                "paper_text": paper_text,
                "paper_text_meta": paper_text_meta,
                "paper_web_signals": paper_web_signals,
                "extracted_sections": extracted_sections,
                "web_search_enabled": self.config.web_search_enabled,
            }
            self._stage_finish(
                state,
                stage,
                char_count=paper_text_meta.get("char_count"),
                url_count=len(paper_web_signals.get("all_urls") or []),
                section_count=len(extracted_sections),
                web_search_enabled=self.config.web_search_enabled,
            )
            return result
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def global_overview(self, state: PaperState) -> PaperState:
        stage = "global_overview"
        run_dir = Path(state["run_dir"])
        paper_context = self._paper_context(state)
        self._stage_start(state, stage, paper_chars=len(paper_context))
        try:
            overview, meta = self.client.chat_json_with_text(
                paper_context,
                build_overview_prompt(),
                model=self.config.document_model,
                enable_search=state.get("web_search_enabled", False),
                stage=stage,
                source_label="Paper text",
            )
            write_json(run_dir / "overview.json", overview)
            write_json(run_dir / "overview_meta.json", meta)
            self._stage_finish(
                state,
                stage,
                paper_title=overview.get("paper_title"),
                claim_count=len(overview.get("core_claims") or []),
            )
            return {"overview": overview, "overview_meta": meta}
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def web_research(self, state: PaperState) -> PaperState:
        stage = "web_research"
        run_dir = Path(state["run_dir"])
        empty_payload = self._empty_web_research_payload()
        self._stage_start(state, stage, enabled=state.get("web_search_enabled", False))

        if not state.get("web_search_enabled", False):
            write_json(run_dir / "web_search_queries.json", [])
            write_json(run_dir / "web_search_results.json", [])
            write_json(run_dir / "web_research.json", empty_payload)
            write_json(run_dir / "web_research_meta.json", {"enabled": False, "reason": "disabled"})
            self._stage_finish(state, stage, enabled=False, reason="disabled")
            return {
                "web_search_queries": [],
                "web_search_results": [],
                "web_research": empty_payload,
                "web_research_meta": {"enabled": False, "reason": "disabled"},
            }

        queries = build_search_queries(state["overview"], state.get("paper_web_signals"))
        try:
            summary, meta = self.client.chat_json(
                messages=[
                    {"role": "system", "content": BASE_SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": build_web_research_summary_prompt(
                            state["overview"],
                            queries,
                            state.get("paper_web_signals") or {},
                        ),
                    },
                ],
                model=self.config.document_model,
                enable_thinking=False,
                enable_search=True,
                stage=stage,
            )
            summary = self._merge_web_research_with_paper_signals(summary, state.get("paper_web_signals") or {})
            meta["enabled"] = True
            write_json(run_dir / "web_search_queries.json", queries)
            write_json(run_dir / "web_search_results.json", [])
            write_json(run_dir / "web_research.json", summary)
            write_json(run_dir / "web_research_meta.json", meta)
            self._stage_finish(
                state,
                stage,
                query_count=len(queries),
                source_count=len(summary.get("source_shortlist") or []),
            )
            return {
                "web_search_queries": queries,
                "web_search_results": [],
                "web_research": summary,
                "web_research_meta": meta,
            }
        except Exception as exc:
            self._stage_error(state, stage, exc, query_count=len(queries))
            error_meta = {"enabled": False, "error": str(exc)}
            write_json(run_dir / "web_search_queries.json", queries)
            write_json(run_dir / "web_search_results.json", [])
            write_json(run_dir / "web_research.json", empty_payload)
            write_json(run_dir / "web_research_meta.json", error_meta)
            return {
                "web_search_queries": queries,
                "web_search_results": [],
                "web_research": empty_payload,
                "web_research_meta": error_meta,
            }

    def resource_discovery(self, state: PaperState) -> PaperState:
        stage = "resource_discovery"
        run_dir = Path(state["run_dir"])
        empty_payload = self._empty_resource_discovery_payload()
        web_sources = len((state.get("web_research") or {}).get("source_shortlist", []))
        self._stage_start(state, stage, web_sources=web_sources)

        if not state.get("web_search_enabled", False):
            write_json(run_dir / "resource_discovery.json", empty_payload)
            write_json(run_dir / "resource_discovery_meta.json", {"enabled": False, "reason": "disabled"})
            self._stage_finish(state, stage, enabled=False, reason="disabled")
            return {
                "resource_discovery": empty_payload,
                "resource_discovery_meta": {"enabled": False, "reason": "disabled"},
            }

        try:
            resource_discovery, meta = self.client.chat_json(
                messages=[
                    {"role": "system", "content": BASE_SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": build_resource_discovery_prompt(
                            state["overview"],
                            state.get("web_research") or {},
                            state.get("paper_web_signals") or {},
                        ),
                    },
                ],
                model=self.config.document_model,
                enable_thinking=False,
                enable_search=True,
                stage=stage,
            )
            resource_discovery = self._merge_resource_discovery_with_paper_signals(
                resource_discovery,
                state.get("paper_web_signals") or {},
            )
            write_json(run_dir / "resource_discovery.json", resource_discovery)
            write_json(run_dir / "resource_discovery_meta.json", meta)
            self._stage_finish(
                state,
                stage,
                code_repo_count=len(resource_discovery.get("code_repositories") or []),
                reproducibility_material_count=len(resource_discovery.get("reproducibility_materials") or []),
            )
            return {
                "resource_discovery": resource_discovery,
                "resource_discovery_meta": meta,
            }
        except Exception as exc:
            self._stage_error(state, stage, exc)
            write_json(run_dir / "resource_discovery.json", empty_payload)
            write_json(run_dir / "resource_discovery_meta.json", {"enabled": False, "error": str(exc)})
            return {
                "resource_discovery": empty_payload,
                "resource_discovery_meta": {"enabled": False, "error": str(exc)},
            }

    def structure_breakdown(self, state: PaperState) -> PaperState:
        stage = "structure_breakdown"
        run_dir = Path(state["run_dir"])
        paper_context = self._paper_context(state)
        self._stage_start(state, stage, paper_chars=len(paper_context))
        try:
            structure, meta = self.client.chat_json_with_text(
                paper_context,
                build_structure_prompt(state["overview"]),
                model=self.config.document_model,
                enable_search=state.get("web_search_enabled", False),
                stage=stage,
                source_label="Paper text",
            )
            selected_sections = self._pick_section_targets(structure, state["extracted_sections"])
            section_targets = [str(item.get("title", "")).strip() for item in selected_sections]
            write_json(run_dir / "structure.json", structure)
            write_json(run_dir / "structure_meta.json", meta)
            write_json(run_dir / "section_targets.json", section_targets)
            write_json(run_dir / "selected_sections.json", selected_sections)
            self._stage_finish(state, stage, section_target_count=len(section_targets))
            return {
                "structure": structure,
                "structure_meta": meta,
                "selected_sections": selected_sections,
                "section_targets": section_targets,
            }
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def _analyze_single_section(
        self,
        section: dict[str, Any],
        overview: dict[str, Any],
        structure: dict[str, Any],
        web_research: dict[str, Any] | None,
        resource_discovery: dict[str, Any] | None,
        enable_search: bool,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        client = KimiClient(self.config)
        section_title = str(section.get("title", "Unnamed Section")).strip()
        payload, meta = client.chat_json_with_text(
            str(section.get("content", "")),
            build_section_prompt(section_title, overview, structure, web_research, resource_discovery),
            model=self.config.document_model,
            enable_search=enable_search,
            stage=f"section_deep_dive.{section_title}",
            source_label=f"Paper section: {section_title}",
        )
        meta["section_start_line"] = section.get("start_line")
        meta["section_end_line"] = section.get("end_line")
        return payload, meta

    def section_deep_dive(self, state: PaperState) -> PaperState:
        stage = "section_deep_dive"
        run_dir = Path(state["run_dir"])
        section_analyses: list[dict[str, Any]] = []
        section_meta: list[dict[str, Any]] = []
        selected_sections = state.get("selected_sections") or []
        self._stage_start(state, stage, section_count=len(selected_sections))

        try:
            if self.config.section_max_workers > 1 and len(selected_sections) > 1:
                indexed_results: dict[int, tuple[dict[str, Any], dict[str, Any]]] = {}
                with ThreadPoolExecutor(max_workers=self.config.section_max_workers) as executor:
                    future_map = {
                        executor.submit(
                            self._analyze_single_section,
                            section,
                            state["overview"],
                            state["structure"],
                            state.get("web_research"),
                            state.get("resource_discovery"),
                            state.get("web_search_enabled", False),
                        ): index
                        for index, section in enumerate(selected_sections)
                    }
                    for future in as_completed(future_map):
                        indexed_results[future_map[future]] = future.result()

                for index in range(len(selected_sections)):
                    payload, meta = indexed_results[index]
                    section_analyses.append(payload)
                    section_meta.append(meta)
            else:
                for section in selected_sections:
                    payload, meta = self._analyze_single_section(
                        section,
                        state["overview"],
                        state["structure"],
                        state.get("web_research"),
                        state.get("resource_discovery"),
                        state.get("web_search_enabled", False),
                    )
                    section_analyses.append(payload)
                    section_meta.append(meta)

            write_json(run_dir / "section_analyses.json", section_analyses)
            write_json(run_dir / "section_analyses_meta.json", section_meta)
            self._stage_finish(state, stage, analyzed_sections=len(section_analyses))
            return {
                "section_analyses": section_analyses,
                "section_analyses_meta": section_meta,
            }
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def experiment_review(self, state: PaperState) -> PaperState:
        stage = "experiment_review"
        run_dir = Path(state["run_dir"])
        experiment_sections = select_experiment_sections(state.get("extracted_sections") or state.get("selected_sections") or [])
        if experiment_sections:
            experiment_context = "\n\n".join(
                f"## {section.get('title', 'Unnamed Section')}\n{section.get('content', '')}" for section in experiment_sections
            )
        else:
            experiment_context = state["paper_text"]
        experiment_context = trim_balanced_text(experiment_context, self.config.paper_context_max_chars)
        self._stage_start(state, stage, context_chars=len(experiment_context))
        try:
            experiment_review, meta = self.client.chat_json_with_text(
                experiment_context,
                build_experiment_prompt(state["overview"], state["structure"], state.get("resource_discovery")),
                model=self.config.document_model,
                enable_search=state.get("web_search_enabled", False),
                stage=stage,
                source_label="Evaluation-related paper text",
            )
            write_json(run_dir / "experiment_review.json", experiment_review)
            write_json(run_dir / "experiment_review_meta.json", meta)
            self._stage_finish(state, stage, experiment_count=len(experiment_review.get("experiments") or []))
            return {
                "experiment_review": experiment_review,
                "experiment_review_meta": meta,
            }
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def critique(self, state: PaperState) -> PaperState:
        stage = "critique"
        run_dir = Path(state["run_dir"])
        self._stage_start(state, stage)
        try:
            messages = [
                {"role": "system", "content": BASE_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": build_critique_prompt(
                        state["overview"],
                        state["structure"],
                        state["section_analyses"],
                        state["experiment_review"],
                        state.get("web_research"),
                        state.get("resource_discovery"),
                    ),
                },
            ]
            critique_markdown, meta = self._chat_analysis_text_with_fallback(state, stage, messages)
            write_text(run_dir / "critique.md", critique_markdown)
            write_json(run_dir / "critique_meta.json", meta)
            self._stage_finish(state, stage, critique_chars=len(critique_markdown))
            return {"critique": critique_markdown, "critique_meta": meta}
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def extensions(self, state: PaperState) -> PaperState:
        stage = "extensions"
        run_dir = Path(state["run_dir"])
        self._stage_start(state, stage)
        try:
            messages = [
                {"role": "system", "content": BASE_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": build_extensions_prompt(
                        state["overview"],
                        state["structure"],
                        state["critique"],
                        state.get("web_research"),
                        state.get("resource_discovery"),
                    ),
                },
            ]
            extensions_markdown, meta = self._chat_analysis_text_with_fallback(state, stage, messages)
            write_text(run_dir / "extensions.md", extensions_markdown)
            write_json(run_dir / "extensions_meta.json", meta)
            self._stage_finish(state, stage, extensions_chars=len(extensions_markdown))
            return {"extensions": extensions_markdown, "extensions_meta": meta}
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def render_report_node(self, state: PaperState) -> PaperState:
        stage = "render_report"
        run_dir = Path(state["run_dir"])
        self._stage_start(state, stage)
        try:
            report_markdown = render_report(state)
            write_text(run_dir / "final_report.md", report_markdown)
            write_json(
                run_dir / "run_summary.json",
                {
                    "pdf_path": state["pdf_path"],
                    "run_dir": state["run_dir"],
                    "document_model": state["overview_meta"].get("model"),
                    "analysis_model": state["critique_meta"].get("model"),
                    "requested_analysis_model": state["critique_meta"].get("requested_model"),
                    "analysis_fallback_used": state["critique_meta"].get("fallback_used", False),
                    "sections": len(state.get("section_targets") or []),
                    "paper_char_count": state["paper_text_meta"].get("char_count"),
                    "web_search_enabled": state.get("web_search_enabled", False),
                    "web_sources": len((state.get("web_research") or {}).get("source_shortlist", [])),
                    "resource_repositories": len((state.get("resource_discovery") or {}).get("code_repositories", [])),
                },
            )
            self._stage_finish(state, stage, report_path=run_dir / "final_report.md")
            return {"report_markdown": report_markdown}
        except Exception as exc:
            self._stage_error(state, stage, exc)
            raise

    def cleanup_remote_file(self, state: PaperState) -> PaperState:
        stage = "cleanup_remote_file"
        run_dir = Path(state["run_dir"])
        self._stage_start(state, stage)
        cleanup_result = {
            "mode": "local_pdf_text_extraction",
            "deleted": False,
            "kept": False,
        }
        write_json(run_dir / "cleanup_result.json", cleanup_result)
        self._stage_finish(state, stage, mode=cleanup_result["mode"])
        return {"cleanup_result": cleanup_result}


def run_analysis(pdf_path: str, output_dir: str | None = None, config: RuntimeConfig | None = None) -> PaperState:
    workflow = PaperAnalysisWorkflow(config or RuntimeConfig.from_env())
    return workflow.run(pdf_path=pdf_path, output_dir=output_dir)
