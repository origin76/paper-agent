from __future__ import annotations

import argparse
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.config import RuntimeConfig
from paper_agent.reporting.exporters import build_report_document, export_html_report, export_pdf_report
from paper_agent.analysis.kimi_client import KimiClient
from .narrative import PaperProfile, ReadingStep, StoryArc, TurningPoint, YearMoment
from .detail_export import (
    ArcReportArtifact,
    export_single_arc_reports as _export_single_arc_reports,
    render_single_arc_markdown as _render_single_arc_markdown,
)
from paper_agent.analysis.prompts import (
    BASE_SYSTEM_PROMPT,
    build_narrative_arc_section_detail_prompt,
    build_narrative_arc_supporting_detail_prompt,
)
from paper_agent.runtime import append_stage_trace, configure_logging, log_event
from paper_agent.utils import slugify, write_json, write_text


MULTISPACE_PATTERN = re.compile(r"\s+")
LIST_ITEM_PATTERN = re.compile(r"^\s*(?:[-*]|\d+\.)\s+(.*)$")
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.*)$")

SECTION_SPECS: dict[str, dict[str, str]] = {
    "setup": {
        "title": "起：问题最初是如何被定义的",
        "focus": "解释这个方向最初试图解决什么、为什么当时这是一个真问题、早期论文默认接受了哪些前提。",
    },
    "build_up": {
        "title": "承：主流技术路线如何逐渐成形",
        "focus": "解释中段论文怎样把松散的问题意识压成稳定方法，哪些模块、证明套路或实验范式开始反复出现。",
    },
    "turn": {
        "title": "转：研究共同体为什么改变路线",
        "focus": "抓住真正的转折点，说明之前的路线卡在什么地方、转向后的新方法替换了什么假设或优化目标。",
    },
    "synthesis": {
        "title": "合：今天的收束、边界与新张力",
        "focus": "总结这条线在最近阶段达成了什么共识，同时指出仍未闭合的矛盾、代价与下一步可能的突破口。",
    },
}


@dataclass(slots=True)
class PaperEvidence:
    paper_id: str
    paper_label: str
    year: int | None
    run_dir: str
    relevance_tags: list[str]
    takeaway: str
    problem_statement: str
    prior_work_positioning: str
    core_claims: list[str]
    method_modules: list[str]
    core_pipeline: list[str]
    evaluation_goal: str
    experiment_names: list[str]
    missing_ablations: list[str]
    reproducibility_risks: list[str]
    section_highlights: list[str]
    reviewer_highlights: list[str]
    extension_highlights: list[str]
    verification_questions: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ArcEvidenceBundle:
    theme_id: str
    title: str
    synopsis: str
    paper_count: int
    year_range: str
    keywords: list[str]
    venues: list[str]
    selected_paper_ids: list[str]
    selected_papers: list[PaperEvidence]
    turning_points: list[TurningPoint]
    reading_path: list[ReadingStep]
    year_moments: list[YearMoment]
    tensions: list[str]
    year_distribution: dict[str, int]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["selected_papers"] = [item.to_dict() for item in self.selected_papers]
        payload["turning_points"] = [item.to_dict() for item in self.turning_points]
        payload["reading_path"] = [item.to_dict() for item in self.reading_path]
        payload["year_moments"] = [item.to_dict() for item in self.year_moments]
        return payload


@dataclass(slots=True)
class EvidencePoint:
    paper_id: str
    paper_label: str
    note: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DetailedSection:
    section_key: str
    section_title: str
    section_summary: str
    paragraphs: list[str]
    evidence_points: list[EvidencePoint] = field(default_factory=list)
    anchor_papers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence_points"] = [item.to_dict() for item in self.evidence_points]
        return payload


@dataclass(slots=True)
class DetailedTurningPoint:
    paper_id: str
    paper_label: str
    year: int | None
    what_changed: str
    why_it_mattered: str
    reading_question: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DetailedReadingStep:
    paper_id: str
    paper_label: str
    year: int | None
    stage_label: str
    why_read_now: str
    focus_question: str
    next_connection: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DetailedYearProgression:
    year: int | None
    narrative: str
    representative_papers: list[str]
    shift: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DetailedTension:
    tension: str
    why_it_persists: str
    what_to_watch: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DetailedStoryArc:
    theme_id: str
    title: str
    synopsis: str
    paper_count: int
    year_range: str
    keywords: list[str]
    venues: list[str]
    arc_overview: str
    setup: DetailedSection
    build_up: DetailedSection
    turn: DetailedSection
    synthesis: DetailedSection
    turning_points_detailed: list[DetailedTurningPoint]
    reading_path_detailed: list[DetailedReadingStep]
    year_progression_detailed: list[DetailedYearProgression]
    open_tensions_detailed: list[DetailedTension]
    source_paper_ids: list[str]
    selected_paper_ids: list[str]
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["setup"] = self.setup.to_dict()
        payload["build_up"] = self.build_up.to_dict()
        payload["turn"] = self.turn.to_dict()
        payload["synthesis"] = self.synthesis.to_dict()
        payload["turning_points_detailed"] = [item.to_dict() for item in self.turning_points_detailed]
        payload["reading_path_detailed"] = [item.to_dict() for item in self.reading_path_detailed]
        payload["year_progression_detailed"] = [item.to_dict() for item in self.year_progression_detailed]
        payload["open_tensions_detailed"] = [item.to_dict() for item in self.open_tensions_detailed]
        return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refine existing narrative story arcs into advisor-style detailed sub-sections.",
    )
    parser.add_argument(
        "narrative_root",
        help="Directory containing paper_profiles.jsonl, story_arcs.json, and narrative_summary.json.",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory where detailed narrative artifacts will be written. Defaults to <narrative_root>/detailed.",
    )
    parser.add_argument(
        "--report-title",
        help="Optional title for the detailed report. Defaults to the original report title suffixed with （细化版）.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum concurrent LLM detail requests.",
    )
    parser.add_argument(
        "--max-papers-per-arc",
        type=int,
        default=10,
        help="Maximum representative papers loaded into each story-arc evidence bundle.",
    )
    parser.add_argument(
        "--arc-limit",
        type=int,
        default=0,
        help="If set, only refine the first N arcs.",
    )
    parser.add_argument(
        "--enable-search",
        action="store_true",
        help="Enable model-side web search during detail synthesis.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Reuse existing per-arc JSON outputs when present.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level, for example INFO or DEBUG.",
    )
    return parser


def _collapse_whitespace(text: str | None) -> str:
    if not text:
        return ""
    return MULTISPACE_PATTERN.sub(" ", str(text)).strip()


def _trim_text(text: str | None, max_chars: int = 220) -> str:
    cleaned = _collapse_whitespace(text)
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip(" ,.;，。；：:") + "…"


def _coerce_string_list(value: Any, max_items: int | None = None, max_chars: int = 200) -> list[str]:
    items: list[str] = []
    if isinstance(value, str):
        cleaned = _trim_text(value, max_chars=max_chars)
        if cleaned:
            items.append(cleaned)
    elif isinstance(value, list):
        for raw_item in value:
            if not isinstance(raw_item, str):
                continue
            cleaned = _trim_text(raw_item, max_chars=max_chars)
            if cleaned:
                items.append(cleaned)
    if max_items is not None:
        return items[:max_items]
    return items


def _safe_load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _safe_load_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _paper_label(profile: PaperProfile | PaperEvidence) -> str:
    title = getattr(profile, "display_title", "") or getattr(profile, "paper_label", "") or getattr(profile, "source_title", "")
    year = getattr(profile, "publication_year", None)
    if year is None:
        year = getattr(profile, "year", None)
    venue_short = getattr(profile, "venue_short", "") or "PAPER"
    if title.startswith("《") and title.endswith("》"):
        body = title
    else:
        body = f"《{title or '未命名论文'}》"
    if year is not None:
        return f"{body} ({venue_short} {year})"
    return body


def _split_markdown_sections(markdown_text: str) -> list[tuple[str, list[str]]]:
    sections: list[tuple[str, list[str]]] = []
    current_title = ""
    current_lines: list[str] = []
    has_heading = False

    for raw_line in markdown_text.splitlines():
        match = HEADING_PATTERN.match(raw_line.strip())
        if match:
            if has_heading:
                sections.append((current_title, current_lines))
            current_title = _collapse_whitespace(match.group(2))
            current_lines = []
            has_heading = True
            continue
        if has_heading:
            current_lines.append(raw_line)

    if has_heading:
        sections.append((current_title, current_lines))
    return sections


def _extract_markdown_highlights(markdown_text: str, preferred_headings: set[str], max_items: int = 4) -> list[str]:
    if not markdown_text:
        return []
    normalized_target = {_collapse_whitespace(item).lower() for item in preferred_headings}
    highlights: list[str] = []
    for heading, lines in _split_markdown_sections(markdown_text):
        if _collapse_whitespace(heading).lower() not in normalized_target:
            continue
        buffer: list[str] = []
        for raw_line in lines:
            stripped = raw_line.strip()
            if not stripped:
                if buffer:
                    text = _trim_text(" ".join(buffer), max_chars=240)
                    if text:
                        highlights.append(text)
                    buffer = []
                continue
            list_match = LIST_ITEM_PATTERN.match(stripped)
            if list_match:
                text = _trim_text(list_match.group(1), max_chars=240)
                if text:
                    highlights.append(text)
                continue
            buffer.append(stripped)
        if buffer:
            text = _trim_text(" ".join(buffer), max_chars=240)
            if text:
                highlights.append(text)
        if len(highlights) >= max_items:
            break
    return highlights[:max_items]


def _profile_from_dict(payload: dict[str, Any]) -> PaperProfile:
    return PaperProfile(
        paper_id=payload["paper_id"],
        identity_key=payload["identity_key"],
        run_dir=payload["run_dir"],
        pdf_path=payload["pdf_path"],
        display_title=payload["display_title"],
        source_title=payload["source_title"],
        venue=payload["venue"],
        venue_short=payload["venue_short"],
        publication_year=payload.get("publication_year"),
        authors=payload.get("authors", []),
        paper_type=payload.get("paper_type", ""),
        takeaway=payload.get("takeaway", ""),
        problem_statement=payload.get("problem_statement", ""),
        prior_work_positioning=payload.get("prior_work_positioning", ""),
        core_claims=payload.get("core_claims", []),
        method_modules=payload.get("method_modules", []),
        core_pipeline=payload.get("core_pipeline", []),
        evaluation_goal=payload.get("evaluation_goal", ""),
        experiment_names=payload.get("experiment_names", []),
        missing_ablations=payload.get("missing_ablations", []),
        reproducibility_risks=payload.get("reproducibility_risks", []),
        code_resources=payload.get("code_resources", []),
        official_resources=payload.get("official_resources", []),
        keywords=payload.get("keywords", []),
        theme_scores=payload.get("theme_scores", {}),
        primary_theme=payload.get("primary_theme", ""),
        secondary_themes=payload.get("secondary_themes", []),
        turning_markers=payload.get("turning_markers", []),
        profile_quality=payload.get("profile_quality", 0.0),
    )


def _turning_point_from_dict(payload: dict[str, Any]) -> TurningPoint:
    return TurningPoint(
        paper_id=payload.get("paper_id", ""),
        paper_label=payload.get("paper_label", ""),
        year=payload.get("year"),
        score=float(payload.get("score", 0.0) or 0.0),
        reason=payload.get("reason", ""),
    )


def _reading_step_from_dict(payload: dict[str, Any]) -> ReadingStep:
    return ReadingStep(
        paper_id=payload.get("paper_id", ""),
        paper_label=payload.get("paper_label", ""),
        year=payload.get("year"),
        reason=payload.get("reason", ""),
    )


def _year_moment_from_dict(payload: dict[str, Any]) -> YearMoment:
    return YearMoment(
        year=payload.get("year"),
        paper_count=int(payload.get("paper_count", 0) or 0),
        summary=payload.get("summary", ""),
        representative_papers=payload.get("representative_papers", []),
    )


def _story_arc_from_dict(payload: dict[str, Any]) -> StoryArc:
    return StoryArc(
        theme_id=payload.get("theme_id", ""),
        title=payload.get("title", ""),
        synopsis=payload.get("synopsis", ""),
        paper_count=int(payload.get("paper_count", 0) or 0),
        year_range=payload.get("year_range", ""),
        venues=payload.get("venues", []),
        keywords=payload.get("keywords", []),
        paper_ids=payload.get("paper_ids", []),
        turning_points=[_turning_point_from_dict(item) for item in payload.get("turning_points", []) if isinstance(item, dict)],
        reading_path=[_reading_step_from_dict(item) for item in payload.get("reading_path", []) if isinstance(item, dict)],
        year_moments=[_year_moment_from_dict(item) for item in payload.get("year_moments", []) if isinstance(item, dict)],
        tensions=payload.get("tensions", []),
        setup_text=payload.get("setup_text", ""),
        build_up_text=payload.get("build_up_text", ""),
        turn_text=payload.get("turn_text", ""),
        synthesis_text=payload.get("synthesis_text", ""),
    )


def load_narrative_inputs(narrative_root: Path) -> tuple[dict[str, PaperProfile], list[StoryArc], dict[str, Any]]:
    profiles_path = narrative_root / "paper_profiles.jsonl"
    arcs_path = narrative_root / "story_arcs.json"
    summary_path = narrative_root / "narrative_summary.json"

    profiles_by_id: dict[str, PaperProfile] = {}
    if profiles_path.exists():
        for raw_line in profiles_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            profile = _profile_from_dict(payload)
            profiles_by_id[profile.paper_id] = profile

    arcs_payload = _safe_load_json(arcs_path)
    arcs = []
    if isinstance(arcs_payload, list):
        arcs = [_story_arc_from_dict(item) for item in arcs_payload if isinstance(item, dict)]

    summary_payload = _safe_load_json(summary_path)
    if not isinstance(summary_payload, dict):
        summary_payload = {}
    return profiles_by_id, arcs, summary_payload


def _assign_relevance_tags(
    arc: StoryArc,
    paper_ids: list[str],
    profiles_by_id: dict[str, PaperProfile],
) -> dict[str, list[str]]:
    tags: dict[str, list[str]] = {paper_id: [] for paper_id in paper_ids}

    if paper_ids:
        tags[paper_ids[0]].append("opening")
        tags[paper_ids[-1]].append("frontier")
    if len(paper_ids) >= 2:
        tags[paper_ids[1]].append("early_anchor")
        tags[paper_ids[-2]].append("recent_anchor")

    for item in arc.turning_points:
        if item.paper_id in tags:
            tags[item.paper_id].append("turning_point")

    for index, item in enumerate(arc.reading_path, start=1):
        if item.paper_id in tags:
            tags[item.paper_id].append(f"reading_step_{index}")

    sorted_by_quality = sorted(
        (profiles_by_id[paper_id] for paper_id in paper_ids if paper_id in profiles_by_id),
        key=lambda profile: (-profile.profile_quality, profile.publication_year or 9999, profile.paper_id),
    )
    for profile in sorted_by_quality[:2]:
        tags.setdefault(profile.paper_id, []).append("high_signal")
    return tags


def _select_representative_paper_ids(
    arc: StoryArc,
    profiles_by_id: dict[str, PaperProfile],
    max_papers: int,
) -> list[str]:
    ordered_ids = [paper_id for paper_id in arc.paper_ids if paper_id in profiles_by_id]
    if len(ordered_ids) <= max_papers:
        return ordered_ids

    selected: list[str] = []

    def add(paper_id: str) -> None:
        if paper_id not in ordered_ids or paper_id in selected or len(selected) >= max_papers:
            return
        selected.append(paper_id)

    for paper_id in ordered_ids[:2]:
        add(paper_id)
    for item in arc.turning_points:
        add(item.paper_id)
    for item in arc.reading_path:
        add(item.paper_id)
    if ordered_ids:
        add(ordered_ids[len(ordered_ids) // 2])
    for paper_id in ordered_ids[-2:]:
        add(paper_id)

    remaining = sorted(
        (profiles_by_id[paper_id] for paper_id in ordered_ids if paper_id not in selected),
        key=lambda profile: (-profile.profile_quality, profile.publication_year or 9999, profile.paper_id),
    )
    for profile in remaining:
        add(profile.paper_id)
        if len(selected) >= max_papers:
            break
    return selected[:max_papers]


def _build_section_highlights(section_analyses: Any) -> tuple[list[str], list[str]]:
    highlights: list[str] = []
    verification_questions: list[str] = []
    if not isinstance(section_analyses, list):
        return highlights, verification_questions

    for item in section_analyses[:4]:
        if not isinstance(item, dict):
            continue
        title = _collapse_whitespace(item.get("section_title")) or "未命名章节"
        role = _trim_text(item.get("section_role_in_paper"), max_chars=180)
        reviewer = _trim_text(item.get("reviewer_view"), max_chars=200)
        engineer = _trim_text(item.get("engineer_view"), max_chars=200)
        if role:
            highlights.append(f"{title}：{role}")
        if reviewer:
            highlights.append(f"{title}（审稿人视角）：{reviewer}")
        elif engineer:
            highlights.append(f"{title}（实现视角）：{engineer}")
        verification_questions.extend(_coerce_string_list(item.get("verification_questions"), max_items=2, max_chars=180))
        if len(highlights) >= 4 and len(verification_questions) >= 4:
            break
    return highlights[:4], verification_questions[:4]


def build_paper_evidence(profile: PaperProfile, relevance_tags: list[str]) -> PaperEvidence:
    run_dir = Path(profile.run_dir)
    structure = _safe_load_json(run_dir / "structure.json")
    if not isinstance(structure, dict):
        structure = {}
    experiment_review = _safe_load_json(run_dir / "experiment_review.json")
    if not isinstance(experiment_review, dict):
        experiment_review = {}
    section_analyses = _safe_load_json(run_dir / "section_analyses.json")

    critique_text = _safe_load_text(run_dir / "critique.md")
    extensions_text = _safe_load_text(run_dir / "extensions.md")

    section_highlights, verification_questions = _build_section_highlights(section_analyses)
    reviewer_highlights = _extract_markdown_highlights(
        critique_text,
        preferred_headings={"最薄弱环节", "隐含假设", "阻碍接收的关键问题"},
        max_items=4,
    )
    extension_highlights = _extract_markdown_highlights(
        extensions_text,
        preferred_headings={"如果我们继续做这条线", "三个快速跟进实验", "三个更有野心的研究方向", "仍然开放的问题"},
        max_items=4,
    )

    method_modules = _coerce_string_list(profile.method_modules, max_items=5, max_chars=120)
    core_pipeline = _coerce_string_list(structure.get("core_pipeline") or profile.core_pipeline, max_items=5, max_chars=120)
    experiment_names = _coerce_string_list(profile.experiment_names, max_items=4, max_chars=120)
    missing_ablations = _coerce_string_list(
        experiment_review.get("missing_ablations") or profile.missing_ablations,
        max_items=4,
        max_chars=160,
    )
    reproducibility_risks = _coerce_string_list(
        experiment_review.get("reproducibility_risks") or profile.reproducibility_risks,
        max_items=4,
        max_chars=160,
    )

    return PaperEvidence(
        paper_id=profile.paper_id,
        paper_label=_paper_label(profile),
        year=profile.publication_year,
        run_dir=profile.run_dir,
        relevance_tags=sorted(set(relevance_tags)),
        takeaway=_trim_text(profile.takeaway, max_chars=220),
        problem_statement=_trim_text(profile.problem_statement, max_chars=220),
        prior_work_positioning=_trim_text(profile.prior_work_positioning, max_chars=220),
        core_claims=_coerce_string_list(profile.core_claims, max_items=4, max_chars=180),
        method_modules=method_modules,
        core_pipeline=core_pipeline,
        evaluation_goal=_trim_text(profile.evaluation_goal, max_chars=200),
        experiment_names=experiment_names,
        missing_ablations=missing_ablations,
        reproducibility_risks=reproducibility_risks,
        section_highlights=section_highlights,
        reviewer_highlights=reviewer_highlights,
        extension_highlights=extension_highlights,
        verification_questions=verification_questions,
    )


def build_arc_evidence_bundle(
    arc: StoryArc,
    profiles_by_id: dict[str, PaperProfile],
    max_papers_per_arc: int = 10,
) -> ArcEvidenceBundle:
    selected_paper_ids = _select_representative_paper_ids(arc, profiles_by_id, max_papers=max(3, max_papers_per_arc))
    tag_map = _assign_relevance_tags(arc, selected_paper_ids, profiles_by_id)
    selected_papers = [
        build_paper_evidence(profiles_by_id[paper_id], tag_map.get(paper_id, []))
        for paper_id in selected_paper_ids
        if paper_id in profiles_by_id
    ]

    year_distribution: dict[str, int] = {}
    for paper_id in arc.paper_ids:
        profile = profiles_by_id.get(paper_id)
        if profile is None:
            continue
        year_label = str(profile.publication_year) if profile.publication_year is not None else "未知"
        year_distribution[year_label] = year_distribution.get(year_label, 0) + 1

    return ArcEvidenceBundle(
        theme_id=arc.theme_id,
        title=arc.title,
        synopsis=arc.synopsis,
        paper_count=arc.paper_count,
        year_range=arc.year_range,
        keywords=arc.keywords,
        venues=arc.venues,
        selected_paper_ids=selected_paper_ids,
        selected_papers=selected_papers,
        turning_points=arc.turning_points,
        reading_path=arc.reading_path,
        year_moments=arc.year_moments,
        tensions=arc.tensions,
        year_distribution=year_distribution,
    )


def _section_output_path(output_dir: Path, arc: StoryArc, section_key: str) -> Path:
    arc_slug = slugify(f"{arc.title}-{arc.theme_id}", fallback=arc.theme_id or "arc")
    return output_dir / "section_details" / f"{arc_slug}-{section_key}.json"


def _supporting_output_path(output_dir: Path, arc: StoryArc) -> Path:
    arc_slug = slugify(f"{arc.title}-{arc.theme_id}", fallback=arc.theme_id or "arc")
    return output_dir / "section_details" / f"{arc_slug}-supporting.json"


def _debug_snapshot_path(output_dir: Path, stage_name: str, arc: StoryArc, suffix: str) -> Path:
    arc_slug = slugify(f"{arc.title}-{arc.theme_id}", fallback=arc.theme_id or "arc")
    return output_dir / "debug" / stage_name / f"{arc_slug}-{suffix}.json"


def _fallback_section(arc: StoryArc, bundle: ArcEvidenceBundle, section_key: str) -> DetailedSection:
    section_title = SECTION_SPECS[section_key]["title"]
    heuristic_text = {
        "setup": arc.setup_text,
        "build_up": arc.build_up_text,
        "turn": arc.turn_text,
        "synthesis": arc.synthesis_text,
    }.get(section_key, "")
    anchor_papers = [item.paper_label for item in bundle.selected_papers[:3]]
    paragraphs = []
    if heuristic_text:
        paragraphs.append(_trim_text(heuristic_text, max_chars=320))
    if bundle.selected_papers:
        lead_papers = "、".join(item.paper_label for item in bundle.selected_papers[:3])
        paragraphs.append(f"这一段最适合拿 {lead_papers} 作为锚点来读，因为它们分别覆盖了问题提出、路线成形与当前边界。")
    if not paragraphs:
        paragraphs.append(f"围绕“{arc.title}”这条线，现有自动化骨架还不足以细化出完整叙事，需要回到代表论文进一步补证。")
    evidence_points = [
        EvidencePoint(paper_id=item.paper_id, paper_label=item.paper_label, note="自动回退时保留的代表论文锚点。")
        for item in bundle.selected_papers[:3]
    ]
    return DetailedSection(
        section_key=section_key,
        section_title=section_title,
        section_summary=_trim_text(paragraphs[0], max_chars=120),
        paragraphs=paragraphs[:3],
        evidence_points=evidence_points,
        anchor_papers=anchor_papers[:4],
    )


def _normalize_evidence_points(raw_items: Any) -> list[EvidencePoint]:
    result: list[EvidencePoint] = []
    if not isinstance(raw_items, list):
        return result
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        note = _trim_text(item.get("note"), max_chars=180)
        paper_label = _trim_text(item.get("paper_label"), max_chars=120)
        paper_id = _collapse_whitespace(item.get("paper_id"))
        if not note and not paper_label:
            continue
        result.append(
            EvidencePoint(
                paper_id=paper_id,
                paper_label=paper_label or paper_id or "未命名论文",
                note=note or "这是该段落中被引用的关键证据锚点。",
            )
        )
    return result[:5]


def _normalize_section_payload(
    payload: dict[str, Any] | None,
    arc: StoryArc,
    bundle: ArcEvidenceBundle,
    section_key: str,
) -> DetailedSection:
    if not isinstance(payload, dict):
        return _fallback_section(arc, bundle, section_key)

    title = _collapse_whitespace(payload.get("section_title")) or SECTION_SPECS[section_key]["title"]
    section_summary = _trim_text(payload.get("section_summary"), max_chars=180)
    paragraphs = _coerce_string_list(payload.get("paragraphs"), max_items=4, max_chars=420)
    evidence_points = _normalize_evidence_points(payload.get("evidence_points"))
    anchor_papers = _coerce_string_list(payload.get("anchor_papers"), max_items=5, max_chars=120)

    if not paragraphs:
        return _fallback_section(arc, bundle, section_key)
    if not section_summary:
        section_summary = _trim_text(paragraphs[0], max_chars=140)
    if not anchor_papers and evidence_points:
        anchor_papers = [item.paper_label for item in evidence_points[:4]]

    return DetailedSection(
        section_key=section_key,
        section_title=title,
        section_summary=section_summary,
        paragraphs=paragraphs,
        evidence_points=evidence_points,
        anchor_papers=anchor_papers,
    )


def _fallback_supporting_payload(arc: StoryArc, bundle: ArcEvidenceBundle) -> dict[str, Any]:
    arc_overview = (
        f"{arc.title} 这条线覆盖 {arc.paper_count} 篇论文，时间跨度 {arc.year_range}，"
        f"可以把它理解为“{arc.synopsis}”这一判断在不同年份、不同技术条件下不断被改写和细化的过程。"
    )
    turning_points = [
        {
            "paper_id": item.paper_id,
            "paper_label": item.paper_label,
            "year": item.year,
            "what_changed": item.reason or "它把原有路线重新组织成了一个新的主流判断。",
            "why_it_mattered": "它改变了这条线理解问题或组织方法的方式。",
            "reading_question": "读这篇时重点追问：它到底替换了旧路线中的哪一个默认前提？",
        }
        for item in arc.turning_points[:3]
    ]
    reading_path = [
        {
            "paper_id": item.paper_id,
            "paper_label": item.paper_label,
            "year": item.year,
            "stage_label": f"第 {index} 站",
            "why_read_now": item.reason or "它对应这条线的关键阶段。",
            "focus_question": "读这篇时要抓它对问题定义、方法结构或评价标准的改写。",
            "next_connection": "读完后把它和下一篇在假设、方法模块和证据强度上对照起来。",
        }
        for index, item in enumerate(arc.reading_path[:5], start=1)
    ]
    year_progression = [
        {
            "year": item.year,
            "narrative": item.summary or "这一年代表论文共同推动了这条线的边界。",
            "representative_papers": item.representative_papers,
            "shift": "这一年可以看作问题定义、方法稳定性或评价口径的一次推进。",
        }
        for item in arc.year_moments
    ]
    tensions = [
        {
            "tension": text,
            "why_it_persists": "现有论文已经意识到这个问题，但还没有拿出同时兼顾表达力、证据强度与工程可用性的解法。",
            "what_to_watch": "后续值得观察是否出现更强的 ablation、跨基线比较或真正降低复现门槛的实现工作。",
        }
        for text in bundle.tensions[:6]
    ]
    return {
        "arc_overview": arc_overview,
        "turning_points_detailed": turning_points,
        "reading_path_detailed": reading_path,
        "year_progression_detailed": year_progression,
        "open_tensions_detailed": tensions,
    }


def _normalize_supporting_payload(payload: dict[str, Any] | None, arc: StoryArc, bundle: ArcEvidenceBundle) -> dict[str, Any]:
    resolved = payload if isinstance(payload, dict) else _fallback_supporting_payload(arc, bundle)
    fallback = _fallback_supporting_payload(arc, bundle)

    arc_overview = _trim_text(resolved.get("arc_overview"), max_chars=260) or fallback["arc_overview"]

    turning_points_detailed: list[DetailedTurningPoint] = []
    for raw_item in resolved.get("turning_points_detailed", []) or fallback["turning_points_detailed"]:
        if not isinstance(raw_item, dict):
            continue
        turning_points_detailed.append(
            DetailedTurningPoint(
                paper_id=_collapse_whitespace(raw_item.get("paper_id")),
                paper_label=_trim_text(raw_item.get("paper_label"), max_chars=140) or "未命名论文",
                year=raw_item.get("year"),
                what_changed=_trim_text(raw_item.get("what_changed"), max_chars=220) or "它推动了这条线的技术重心发生变化。",
                why_it_mattered=_trim_text(raw_item.get("why_it_mattered"), max_chars=220) or "它改变了后续论文组织问题与证据的方式。",
                reading_question=_trim_text(raw_item.get("reading_question"), max_chars=180) or "读这篇时要问：它真正替换了什么旧假设？",
            )
        )

    reading_path_detailed: list[DetailedReadingStep] = []
    for raw_item in resolved.get("reading_path_detailed", []) or fallback["reading_path_detailed"]:
        if not isinstance(raw_item, dict):
            continue
        reading_path_detailed.append(
            DetailedReadingStep(
                paper_id=_collapse_whitespace(raw_item.get("paper_id")),
                paper_label=_trim_text(raw_item.get("paper_label"), max_chars=140) or "未命名论文",
                year=raw_item.get("year"),
                stage_label=_trim_text(raw_item.get("stage_label"), max_chars=40) or "阅读节点",
                why_read_now=_trim_text(raw_item.get("why_read_now"), max_chars=220) or "它对应这条线的重要阶段。",
                focus_question=_trim_text(raw_item.get("focus_question"), max_chars=180) or "重点追问：这篇在问题定义或方法结构上做了什么改写？",
                next_connection=_trim_text(raw_item.get("next_connection"), max_chars=180) or "读完后，把它与下一篇在假设和证据上并读。",
            )
        )

    year_progression_detailed: list[DetailedYearProgression] = []
    for raw_item in resolved.get("year_progression_detailed", []) or fallback["year_progression_detailed"]:
        if not isinstance(raw_item, dict):
            continue
        year_progression_detailed.append(
            DetailedYearProgression(
                year=raw_item.get("year"),
                narrative=_trim_text(raw_item.get("narrative"), max_chars=260) or "这一年是该方向推进的重要节点。",
                representative_papers=_coerce_string_list(raw_item.get("representative_papers"), max_items=4, max_chars=140),
                shift=_trim_text(raw_item.get("shift"), max_chars=200) or "这一年的推进体现为问题定义、方法结构或评价口径的变化。",
            )
        )

    open_tensions_detailed: list[DetailedTension] = []
    for raw_item in resolved.get("open_tensions_detailed", []) or fallback["open_tensions_detailed"]:
        if not isinstance(raw_item, dict):
            continue
        open_tensions_detailed.append(
            DetailedTension(
                tension=_trim_text(raw_item.get("tension"), max_chars=220) or "这条线仍存在尚未闭合的问题。",
                why_it_persists=_trim_text(raw_item.get("why_it_persists"), max_chars=220) or "现有工作还没有给出兼顾理论性与工程性的解法。",
                what_to_watch=_trim_text(raw_item.get("what_to_watch"), max_chars=180) or "后续值得关注是否出现更强证据或更低复现门槛。",
            )
        )

    return {
        "arc_overview": arc_overview,
        "turning_points_detailed": turning_points_detailed[:4],
        "reading_path_detailed": reading_path_detailed[:6],
        "year_progression_detailed": year_progression_detailed[:8],
        "open_tensions_detailed": open_tensions_detailed[:6],
    }


def _chat_json_with_search_fallback(
    client: KimiClient,
    messages: list[dict[str, Any]],
    stage: str,
    enable_search: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        return client.chat_json(
            messages,
            enable_thinking=client.config.analysis_enable_thinking,
            enable_search=enable_search,
            stage=stage,
        )
    except Exception as exc:
        if not enable_search:
            raise
        log_event(
            "warning",
            "Narrative detail request failed with search enabled, retrying without search",
            stage=stage,
            error=str(exc),
        )
        return client.chat_json(
            messages,
            enable_thinking=client.config.analysis_enable_thinking,
            enable_search=False,
            stage=f"{stage}_retry_no_search",
        )


def _generate_section_detail(
    config: RuntimeConfig,
    output_dir: Path,
    arc: StoryArc,
    bundle: ArcEvidenceBundle,
    section_key: str,
    enable_search: bool,
    skip_existing: bool,
) -> dict[str, Any]:
    output_path = _section_output_path(output_dir, arc, section_key)
    if skip_existing and output_path.exists():
        payload = _safe_load_json(output_path)
        if isinstance(payload, dict):
            log_event("info", "Narrative detail section reused", arc=arc.title, section=section_key, output_path=output_path)
            return payload

    request_payload = {
        "arc": {
            "theme_id": arc.theme_id,
            "title": arc.title,
            "synopsis": arc.synopsis,
            "paper_count": arc.paper_count,
            "year_range": arc.year_range,
            "keywords": arc.keywords,
            "venues": arc.venues,
            "turning_points": [item.to_dict() for item in arc.turning_points],
            "reading_path": [item.to_dict() for item in arc.reading_path],
            "year_moments": [item.to_dict() for item in arc.year_moments],
            "tensions": arc.tensions,
        },
        "evidence_bundle": bundle.to_dict(),
        "section_key": section_key,
        "section_spec": SECTION_SPECS[section_key],
    }
    write_json(_debug_snapshot_path(output_dir, "inputs", arc, section_key), request_payload)

    append_stage_trace(
        output_dir,
        "detail_section",
        "started",
        arc_title=arc.title,
        section_key=section_key,
        selected_papers=len(bundle.selected_papers),
        enable_search=enable_search,
    )
    log_event(
        "info",
        "Narrative detail section started",
        arc=arc.title,
        section=section_key,
        selected_papers=len(bundle.selected_papers),
    )

    client = KimiClient(config)
    messages = [
        {
            "role": "system",
            "content": (
                BASE_SYSTEM_PROMPT
                + "\n你现在负责写一条研究故事线里的单个小标题。输出必须是 JSON，不要写 Markdown。"
                " 重点是把问题演化讲清楚，而不是堆砌项目符号。"
            ),
        },
        {
            "role": "user",
            "content": build_narrative_arc_section_detail_prompt(
                section_key=section_key,
                section_title=SECTION_SPECS[section_key]["title"],
                section_focus=SECTION_SPECS[section_key]["focus"],
                arc_payload=request_payload["arc"],
                evidence_bundle_payload=request_payload["evidence_bundle"],
            ),
        },
    ]
    response_payload, response_meta = _chat_json_with_search_fallback(
        client=client,
        messages=messages,
        stage=f"narrative_detail_{slugify(arc.theme_id or arc.title)}_{section_key}",
        enable_search=enable_search,
    )
    normalized = _normalize_section_payload(response_payload, arc, bundle, section_key)
    wrapped = {
        "section_key": section_key,
        "arc_title": arc.title,
        "response_meta": response_meta,
        "request_snapshot_path": str(_debug_snapshot_path(output_dir, "inputs", arc, section_key)),
        "normalized": normalized.to_dict(),
        "raw_response": response_payload,
    }
    write_json(output_path, wrapped)
    write_json(_debug_snapshot_path(output_dir, "outputs", arc, section_key), wrapped)
    append_stage_trace(
        output_dir,
        "detail_section",
        "finished",
        arc_title=arc.title,
        section_key=section_key,
        output_path=str(output_path),
    )
    log_event("info", "Narrative detail section finished", arc=arc.title, section=section_key, output_path=output_path)
    return wrapped


def _generate_supporting_detail(
    config: RuntimeConfig,
    output_dir: Path,
    arc: StoryArc,
    bundle: ArcEvidenceBundle,
    enable_search: bool,
    skip_existing: bool,
) -> dict[str, Any]:
    output_path = _supporting_output_path(output_dir, arc)
    if skip_existing and output_path.exists():
        payload = _safe_load_json(output_path)
        if isinstance(payload, dict):
            log_event("info", "Narrative detail supporting block reused", arc=arc.title, output_path=output_path)
            return payload

    request_payload = {
        "arc": {
            "theme_id": arc.theme_id,
            "title": arc.title,
            "synopsis": arc.synopsis,
            "paper_count": arc.paper_count,
            "year_range": arc.year_range,
            "keywords": arc.keywords,
            "venues": arc.venues,
            "turning_points": [item.to_dict() for item in arc.turning_points],
            "reading_path": [item.to_dict() for item in arc.reading_path],
            "year_moments": [item.to_dict() for item in arc.year_moments],
            "tensions": arc.tensions,
        },
        "evidence_bundle": bundle.to_dict(),
    }
    write_json(_debug_snapshot_path(output_dir, "inputs", arc, "supporting"), request_payload)

    append_stage_trace(
        output_dir,
        "detail_supporting",
        "started",
        arc_title=arc.title,
        selected_papers=len(bundle.selected_papers),
        enable_search=enable_search,
    )
    log_event("info", "Narrative detail supporting block started", arc=arc.title, selected_papers=len(bundle.selected_papers))

    client = KimiClient(config)
    messages = [
        {
            "role": "system",
            "content": (
                BASE_SYSTEM_PROMPT
                + "\n你现在负责把故事线的辅助小标题写扎实，包括转折点、导师带读路径、年度推进和未解问题。"
                " 输出必须是 JSON，不要写 Markdown。"
            ),
        },
        {
            "role": "user",
            "content": build_narrative_arc_supporting_detail_prompt(
                arc_payload=request_payload["arc"],
                evidence_bundle_payload=request_payload["evidence_bundle"],
            ),
        },
    ]
    response_payload, response_meta = _chat_json_with_search_fallback(
        client=client,
        messages=messages,
        stage=f"narrative_detail_{slugify(arc.theme_id or arc.title)}_supporting",
        enable_search=enable_search,
    )
    normalized = _normalize_supporting_payload(response_payload, arc, bundle)
    wrapped = {
        "arc_title": arc.title,
        "response_meta": response_meta,
        "request_snapshot_path": str(_debug_snapshot_path(output_dir, "inputs", arc, "supporting")),
        "normalized": {
            "arc_overview": normalized["arc_overview"],
            "turning_points_detailed": [item.to_dict() for item in normalized["turning_points_detailed"]],
            "reading_path_detailed": [item.to_dict() for item in normalized["reading_path_detailed"]],
            "year_progression_detailed": [item.to_dict() for item in normalized["year_progression_detailed"]],
            "open_tensions_detailed": [item.to_dict() for item in normalized["open_tensions_detailed"]],
        },
        "raw_response": response_payload,
    }
    write_json(output_path, wrapped)
    write_json(_debug_snapshot_path(output_dir, "outputs", arc, "supporting"), wrapped)
    append_stage_trace(
        output_dir,
        "detail_supporting",
        "finished",
        arc_title=arc.title,
        output_path=str(output_path),
    )
    log_event("info", "Narrative detail supporting block finished", arc=arc.title, output_path=output_path)
    return wrapped


def _load_existing_or_fallback_section(arc: StoryArc, bundle: ArcEvidenceBundle, output_dir: Path, section_key: str) -> DetailedSection:
    payload = _safe_load_json(_section_output_path(output_dir, arc, section_key))
    if isinstance(payload, dict) and isinstance(payload.get("normalized"), dict):
        return _normalize_section_payload(payload["normalized"], arc, bundle, section_key)
    return _fallback_section(arc, bundle, section_key)


def _load_existing_or_fallback_supporting(arc: StoryArc, bundle: ArcEvidenceBundle, output_dir: Path) -> dict[str, Any]:
    payload = _safe_load_json(_supporting_output_path(output_dir, arc))
    if isinstance(payload, dict) and isinstance(payload.get("normalized"), dict):
        return _normalize_supporting_payload(payload["normalized"], arc, bundle)
    return _normalize_supporting_payload(None, arc, bundle)


def _build_detailed_story_arc(
    arc: StoryArc,
    bundle: ArcEvidenceBundle,
    output_dir: Path,
) -> DetailedStoryArc:
    setup = _load_existing_or_fallback_section(arc, bundle, output_dir, "setup")
    build_up = _load_existing_or_fallback_section(arc, bundle, output_dir, "build_up")
    turn = _load_existing_or_fallback_section(arc, bundle, output_dir, "turn")
    synthesis = _load_existing_or_fallback_section(arc, bundle, output_dir, "synthesis")
    supporting = _load_existing_or_fallback_supporting(arc, bundle, output_dir)

    return DetailedStoryArc(
        theme_id=arc.theme_id,
        title=arc.title,
        synopsis=arc.synopsis,
        paper_count=arc.paper_count,
        year_range=arc.year_range,
        keywords=arc.keywords,
        venues=arc.venues,
        arc_overview=supporting["arc_overview"],
        setup=setup,
        build_up=build_up,
        turn=turn,
        synthesis=synthesis,
        turning_points_detailed=supporting["turning_points_detailed"],
        reading_path_detailed=supporting["reading_path_detailed"],
        year_progression_detailed=supporting["year_progression_detailed"],
        open_tensions_detailed=supporting["open_tensions_detailed"],
        source_paper_ids=arc.paper_ids,
        selected_paper_ids=bundle.selected_paper_ids,
        generated_at=datetime.now().isoformat(),
    )


def render_detailed_narrative_markdown(
    report_title: str,
    detailed_arcs: list[DetailedStoryArc],
    source_summary: dict[str, Any],
) -> str:
    summary_block = source_summary.get("summary", {}) if isinstance(source_summary.get("summary"), dict) else {}
    lines: list[str] = [f"# {report_title}", ""]

    lines.append("## 阅读说明")
    lines.append("")
    paper_count = summary_block.get("paper_count")
    source_arc_count = summary_block.get("arc_count")
    refined_arc_count = len(detailed_arcs)
    year_range = summary_block.get("year_range")
    if paper_count is not None:
        lines.append(f"- 语料规模：{paper_count} 篇")
    lines.append(f"- 本次细化故事线：{refined_arc_count} 条")
    if source_arc_count is not None and source_arc_count != refined_arc_count:
        lines.append(f"- 原始 narrative 故事线总数：{source_arc_count} 条")
    if year_range:
        lines.append(f"- 时间跨度：{year_range}")
    lines.append("")
    lines.append(
        "这份报告不是重新解析 PDF，而是在已有单篇解析产物和第一版 narrative 聚合之上，继续把每条故事线的“起承转合、转折点、年度推进、带读路径”向下压实。"
    )
    lines.append("")

    lines.append("## 故事线地图")
    lines.append("")
    for arc in detailed_arcs:
        keyword_text = "、".join(arc.keywords[:5]) if arc.keywords else "暂无稳定关键词"
        venue_text = " / ".join(arc.venues[:4]) if arc.venues else "未知 venue"
        lines.append(f"- **{arc.title}**：{arc.paper_count} 篇，跨度 {arc.year_range}，主要来自 {venue_text}。关键词：{keyword_text}。")
    lines.append("")

    for arc in detailed_arcs:
        lines.append(f"## {arc.title}")
        lines.append("")
        lines.append(arc.arc_overview or arc.synopsis)
        lines.append("")
        lines.append(f"- 覆盖论文：{arc.paper_count}")
        lines.append(f"- 时间跨度：{arc.year_range}")
        if arc.venues:
            lines.append(f"- 主要 venue：{' / '.join(arc.venues[:6])}")
        if arc.keywords:
            lines.append(f"- 关键词：{'、'.join(arc.keywords[:8])}")
        lines.append("")

        for section in (arc.setup, arc.build_up, arc.turn, arc.synthesis):
            lines.append(f"### {section.section_title}")
            lines.append("")
            if section.section_summary:
                lines.append(section.section_summary)
                lines.append("")
            for paragraph in section.paragraphs:
                lines.append(paragraph)
                lines.append("")
            if section.evidence_points:
                evidence_text = "；".join(
                    f"{item.paper_label}：{item.note}"
                    for item in section.evidence_points[:4]
                )
                lines.append(f"证据锚点：{evidence_text}")
                lines.append("")
            elif section.anchor_papers:
                lines.append(f"锚点论文：{'、'.join(section.anchor_papers[:5])}")
                lines.append("")

        if arc.turning_points_detailed:
            lines.append("### 代表转折点")
            lines.append("")
            for index, item in enumerate(arc.turning_points_detailed, start=1):
                year_label = f"{item.year} 年" if item.year is not None else "未知年份"
                lines.append(
                    f"{index}. {item.paper_label}（{year_label}）：{item.what_changed} 它重要的原因是：{item.why_it_mattered} 读它时可以追问：{item.reading_question}"
                )
            lines.append("")

        if arc.reading_path_detailed:
            lines.append("### 导师带读路径")
            lines.append("")
            for index, item in enumerate(arc.reading_path_detailed, start=1):
                lines.append(
                    f"{index}. {item.stage_label}，先读 {item.paper_label}：{item.why_read_now} 带着这个问题去看：{item.focus_question} 读完后接到下一步：{item.next_connection}"
                )
            lines.append("")

        if arc.year_progression_detailed:
            lines.append("### 年度推进")
            lines.append("")
            for item in arc.year_progression_detailed:
                year_label = str(item.year) if item.year is not None else "未知年份"
                lines.append(f"#### {year_label}")
                lines.append("")
                lines.append(item.narrative)
                lines.append("")
                if item.shift:
                    lines.append(f"这一年的推进重点：{item.shift}")
                    lines.append("")
                if item.representative_papers:
                    lines.append(f"代表论文：{'、'.join(item.representative_papers[:4])}")
                    lines.append("")

        if arc.open_tensions_detailed:
            lines.append("### 仍然悬而未决的问题")
            lines.append("")
            for index, item in enumerate(arc.open_tensions_detailed, start=1):
                lines.append(f"{index}. {item.tension} 它之所以还没被解决，是因为：{item.why_it_persists} 接下来最值得观察的信号是：{item.what_to_watch}")
            lines.append("")

    lines.append("## 方法说明")
    lines.append("")
    lines.append(
        "这一版细化报告使用已有的 `paper_profiles.jsonl`、`story_arcs.json` 和对应 run 目录中的 overview / structure / experiments / critique / extensions 等中间产物进行二次合成。"
    )
    lines.append("")
    lines.append(
        "它适合做“导师带读式”的领域梳理；如果下一步要做更强的跨故事线对照，可以继续在这层之上加“共同问题、互相借鉴、范式迁移”的横向章节。"
    )
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _resolve_output_dir(narrative_root: Path, explicit_output_dir: str | None) -> Path:
    if explicit_output_dir:
        return Path(explicit_output_dir).expanduser().resolve()
    return (narrative_root / "detailed").resolve()


def _resolve_report_title(summary_payload: dict[str, Any], explicit_title: str | None) -> str:
    if explicit_title:
        return explicit_title
    base_title = _collapse_whitespace(summary_payload.get("report_title"))
    if base_title:
        return f"{base_title}（细化版）"
    return "论文领域发展与转折叙事报告（细化版）"


def build_detailed_narrative_report(
    narrative_root: Path,
    output_dir: Path,
    report_title: str,
    max_workers: int = 4,
    max_papers_per_arc: int = 10,
    arc_limit: int = 0,
    enable_search: bool = False,
    skip_existing: bool = False,
    log_level: str = "INFO",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(level=log_level, run_dir=output_dir)

    append_stage_trace(output_dir, "load_narrative_inputs", "started", narrative_root=str(narrative_root))
    profiles_by_id, arcs, summary_payload = load_narrative_inputs(narrative_root)
    if arc_limit > 0:
        arcs = arcs[:arc_limit]
    append_stage_trace(
        output_dir,
        "load_narrative_inputs",
        "finished",
        profile_count=len(profiles_by_id),
        arc_count=len(arcs),
    )
    log_event(
        "info",
        "Detailed narrative input loaded",
        narrative_root=narrative_root,
        profile_count=len(profiles_by_id),
        arc_count=len(arcs),
    )

    append_stage_trace(output_dir, "build_evidence_bundles", "started", max_papers_per_arc=max_papers_per_arc)
    bundles: dict[str, ArcEvidenceBundle] = {}
    for arc in arcs:
        bundle = build_arc_evidence_bundle(arc, profiles_by_id, max_papers_per_arc=max_papers_per_arc)
        bundles[arc.theme_id] = bundle
    evidence_path = output_dir / "evidence_bundles.json"
    write_json(evidence_path, [bundle.to_dict() for bundle in bundles.values()])
    append_stage_trace(output_dir, "build_evidence_bundles", "finished", bundle_count=len(bundles), output_path=str(evidence_path))
    log_event("info", "Detailed narrative evidence bundles built", bundle_count=len(bundles), output_path=evidence_path)

    config = RuntimeConfig.from_env()
    append_stage_trace(
        output_dir,
        "llm_refine_story_arcs",
        "started",
        arc_count=len(arcs),
        max_workers=max_workers,
        enable_search=enable_search,
    )
    futures: dict[Any, tuple[str, str, str]] = {}
    with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
        for arc in arcs:
            bundle = bundles[arc.theme_id]
            for section_key in SECTION_SPECS:
                future = executor.submit(
                    _generate_section_detail,
                    config,
                    output_dir,
                    arc,
                    bundle,
                    section_key,
                    enable_search,
                    skip_existing,
                )
                futures[future] = ("section", arc.theme_id, section_key)
            future = executor.submit(
                _generate_supporting_detail,
                config,
                output_dir,
                arc,
                bundle,
                enable_search,
                skip_existing,
            )
            futures[future] = ("supporting", arc.theme_id, "supporting")

        failure_count = 0
        for future in as_completed(futures):
            task_kind, theme_id, detail_key = futures[future]
            arc_title = next((item.title for item in arcs if item.theme_id == theme_id), theme_id)
            try:
                future.result()
            except Exception as exc:
                failure_count += 1
                append_stage_trace(
                    output_dir,
                    "llm_refine_story_arcs",
                    "failed_item",
                    arc_title=arc_title,
                    task_kind=task_kind,
                    detail_key=detail_key,
                    error=str(exc),
                )
                log_event(
                    "error",
                    "Detailed narrative request failed",
                    arc=arc_title,
                    task_kind=task_kind,
                    detail_key=detail_key,
                    error=str(exc),
                )
    append_stage_trace(output_dir, "llm_refine_story_arcs", "finished", failure_count=failure_count)
    log_event("info", "Detailed narrative LLM refinement finished", failure_count=failure_count)

    append_stage_trace(output_dir, "assemble_outputs", "started", arc_count=len(arcs))
    detailed_arcs = [_build_detailed_story_arc(arc, bundles[arc.theme_id], output_dir) for arc in arcs]
    detailed_story_arcs_path = output_dir / "detailed_story_arcs.json"
    markdown_path = output_dir / "detailed_narrative_report.md"
    html_path = output_dir / "detailed_narrative_report.html"
    pdf_path = output_dir / "detailed_narrative_report.pdf"
    arc_reports_dir = output_dir / "arc_reports"

    write_json(detailed_story_arcs_path, [arc.to_dict() for arc in detailed_arcs])
    append_stage_trace(output_dir, "export_arc_reports", "started", arc_count=len(detailed_arcs))
    arc_artifacts = _export_single_arc_reports(arc_reports_dir, detailed_arcs, bundles)
    append_stage_trace(
        output_dir,
        "export_arc_reports",
        "finished",
        arc_count=len(arc_artifacts),
        output_path=str(arc_reports_dir / "index.json"),
    )

    markdown = render_detailed_narrative_markdown(
        report_title=report_title,
        detailed_arcs=detailed_arcs,
        source_summary=summary_payload,
    )
    write_text(markdown_path, markdown)
    document = build_report_document(markdown, title=report_title)
    html_meta = export_html_report(document, html_path, metadata={"arc_count": len(detailed_arcs), "paper_count": len(profiles_by_id)})
    pdf_meta = export_pdf_report(document, pdf_path, metadata={"arc_count": len(detailed_arcs), "paper_count": len(profiles_by_id)})

    result = {
        "narrative_root": str(narrative_root),
        "output_dir": str(output_dir),
        "profiles": len(profiles_by_id),
        "arcs": len(arcs),
        "report_title": report_title,
        "evidence_bundles_json": str(evidence_path),
        "detailed_story_arcs_json": str(detailed_story_arcs_path),
        "markdown": str(markdown_path),
        "html": html_meta,
        "pdf": pdf_meta,
        "arc_reports_dir": str(arc_reports_dir),
        "arc_reports_index_json": str(arc_reports_dir / "index.json"),
        "arc_reports": [item.to_dict() for item in arc_artifacts],
    }
    write_json(output_dir / "run_summary.json", result)
    append_stage_trace(output_dir, "assemble_outputs", "finished", output_path=str(output_dir), arc_count=len(detailed_arcs))
    log_event("info", "Detailed narrative report finished", output_dir=output_dir, arcs=len(detailed_arcs), profiles=len(profiles_by_id))
    return result


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    narrative_root = Path(args.narrative_root).expanduser().resolve()
    output_dir = _resolve_output_dir(narrative_root, args.output_dir)
    _, _, summary_payload = load_narrative_inputs(narrative_root)
    report_title = _resolve_report_title(summary_payload, args.report_title)

    build_detailed_narrative_report(
        narrative_root=narrative_root,
        output_dir=output_dir,
        report_title=report_title,
        max_workers=max(1, args.max_workers),
        max_papers_per_arc=max(3, args.max_papers_per_arc),
        arc_limit=max(0, args.arc_limit),
        enable_search=bool(args.enable_search),
        skip_existing=bool(args.skip_existing),
        log_level=args.log_level,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
