from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from paper_agent.reporting.exporters import build_report_document, export_html_report, export_pdf_report
from paper_agent.runtime import log_event
from paper_agent.utils import sanitize_filename, write_json, write_text

if TYPE_CHECKING:
    from .detail import ArcEvidenceBundle, DetailedStoryArc


MULTISPACE_PATTERN = re.compile(r"\s+")


@dataclass(slots=True)
class ArcReportArtifact:
    theme_id: str
    title: str
    basename: str
    markdown_path: str
    html_path: str
    pdf_path: str
    paper_count: int
    year_range: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _collapse_whitespace(text: str | None) -> str:
    if not text:
        return ""
    return MULTISPACE_PATTERN.sub(" ", str(text)).strip()


def _trim_text(text: str | None, max_chars: int = 220) -> str:
    cleaned = _collapse_whitespace(text)
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip(" ,.;，。；：:") + "…"


def _humanize_relevance_tag(tag: str) -> str:
    mapping = {
        "opening": "开场问题",
        "frontier": "最近前沿",
        "early_anchor": "早期锚点",
        "recent_anchor": "近年前沿",
        "turning_point": "关键转折",
        "high_signal": "高信号论文",
    }
    if tag in mapping:
        return mapping[tag]
    if tag.startswith("reading_step_"):
        suffix = tag.split("_")[-1]
        if suffix.isdigit():
            return f"带读路径第 {suffix} 站"
    return tag.replace("_", " ")


def _join_sentence_parts(parts: list[str], fallback: str = "") -> str:
    cleaned = [_collapse_whitespace(part) for part in parts if _collapse_whitespace(part)]
    if not cleaned:
        return fallback
    return "；".join(cleaned)


def _build_arc_core_questions(arc: DetailedStoryArc) -> list[str]:
    questions: list[str] = []

    for item in arc.reading_path_detailed:
        text = _trim_text(item.focus_question, max_chars=120)
        if text and text not in questions:
            questions.append(text)
        if len(questions) >= 3:
            break

    if len(questions) < 3:
        for item in arc.open_tensions_detailed:
            text = _trim_text(item.tension, max_chars=120)
            if text and text not in questions:
                questions.append(text)
            if len(questions) >= 3:
                break

    if len(questions) < 3:
        for section in (arc.setup, arc.build_up, arc.turn):
            text = _trim_text(section.section_summary, max_chars=120)
            if text and text not in questions:
                questions.append(text)
            if len(questions) >= 3:
                break

    return questions[:3]


def _append_metadata_block(lines: list[str], arc: DetailedStoryArc, bundle: ArcEvidenceBundle) -> None:
    lines.append(arc.arc_overview or arc.synopsis)
    lines.append("")
    lines.append(f"- 覆盖论文：{arc.paper_count}")
    lines.append(f"- 时间跨度：{arc.year_range}")
    if arc.venues:
        lines.append(f"- 主要 venue：{' / '.join(arc.venues[:6])}")
    if arc.keywords:
        lines.append(f"- 关键词：{'、'.join(arc.keywords[:8])}")
    lines.append(f"- 代表锚点论文：{len(bundle.selected_papers)} 篇")
    lines.append("")


def _append_reading_intro(lines: list[str], arc: DetailedStoryArc, bundle: ArcEvidenceBundle) -> None:
    lines.append("## 这本小册子怎么读")
    lines.append("")
    lines.append(
        "这不是故事线总报告中的节选，而是一份可以单独阅读的带读讲义。推荐先抓住主轴，再顺着“起承转合”理解问题如何被重写，最后回到代表论文卡片里补证据细节。"
    )
    lines.append("")
    if arc.reading_path_detailed:
        lead = arc.reading_path_detailed[0]
        lines.append(
            f"如果你只想先读一篇，建议从 {lead.paper_label} 开始，因为它最适合作为进入这条线的第一扇门：{lead.why_read_now}"
        )
        lines.append("")

    core_questions = _build_arc_core_questions(arc)
    if core_questions:
        lines.append("这条线最值得带着去读的三个问题是：")
        lines.append("")
        for index, question in enumerate(core_questions, start=1):
            lines.append(f"{index}. {question}")
        lines.append("")

    if bundle.tensions:
        lines.append(f"一句话抓住这条线的张力：{_trim_text(bundle.tensions[0], max_chars=180)}")
        lines.append("")


def _append_story_axis(lines: list[str], arc: DetailedStoryArc) -> None:
    lines.append("## 故事线主轴")
    lines.append("")
    lines.append(
        "把这条线放在一起看，它并不是一串并列论文，而是研究共同体不断改写“什么才算一个好解法”的过程。下面的四段适合按顺序读：先看问题如何被定义，再看主流方法怎样稳定，接着找真正的转向，最后判断今天的共识和边界。"
    )
    lines.append("")

    for section in (arc.setup, arc.build_up, arc.turn, arc.synthesis):
        lines.append(f"## {section.section_title}")
        lines.append("")
        if section.section_summary:
            lines.append(section.section_summary)
            lines.append("")
        for paragraph in section.paragraphs:
            lines.append(paragraph)
            lines.append("")
        if section.evidence_points:
            lines.append("这一段最值得回看的证据锚点：")
            lines.append("")
            for item in section.evidence_points[:5]:
                lines.append(f"- {item.paper_label}：{item.note}")
            lines.append("")
        elif section.anchor_papers:
            lines.append(f"可回看的锚点论文：{'、'.join(section.anchor_papers[:5])}")
            lines.append("")


def _append_turning_points(lines: list[str], arc: DetailedStoryArc) -> None:
    if not arc.turning_points_detailed:
        return
    lines.append("<!--PAGE_BREAK-->")
    lines.append("")
    lines.append("## 关键转折点")
    lines.append("")
    lines.append("下面几篇不是简单的“代表作”，而是能看出研究共同体为什么改口、改目标、改评价标准的节点。")
    lines.append("")
    for index, item in enumerate(arc.turning_points_detailed, start=1):
        year_label = f"{item.year} 年" if item.year is not None else "未知年份"
        lines.append(f"### 转折点 {index}：{item.paper_label}")
        lines.append("")
        lines.append(f"{year_label}，它改变的是：{item.what_changed}")
        lines.append("")
        lines.append(f"为什么这一步重要：{item.why_it_mattered}")
        lines.append("")
        lines.append(f"带着这个问题去读：{item.reading_question}")
        lines.append("")


def _append_reading_path(lines: list[str], arc: DetailedStoryArc) -> None:
    if not arc.reading_path_detailed:
        return
    lines.append("## 导师带读路径")
    lines.append("")
    lines.append("这部分不是论文目录，而是一条更接近“导师会怎么安排阅读顺序”的路径。")
    lines.append("")
    for index, item in enumerate(arc.reading_path_detailed, start=1):
        lines.append(f"### 第 {index} 站：{item.paper_label}")
        lines.append("")
        lines.append(f"{item.stage_label}。{item.why_read_now}")
        lines.append("")
        lines.append(f"读这篇时先盯住：{item.focus_question}")
        lines.append("")
        lines.append(f"读完之后怎么接下一篇：{item.next_connection}")
        lines.append("")


def _append_year_progression(lines: list[str], arc: DetailedStoryArc) -> None:
    if not arc.year_progression_detailed:
        return
    lines.append("<!--PAGE_BREAK-->")
    lines.append("")
    lines.append("## 年度推进")
    lines.append("")
    lines.append("如果你想快速把握领域节奏，可以按年份看问题重心、证据口径和方法模块是怎样一层层推进的。")
    lines.append("")
    for item in arc.year_progression_detailed:
        year_label = str(item.year) if item.year is not None else "未知年份"
        lines.append(f"### {year_label}")
        lines.append("")
        lines.append(item.narrative)
        lines.append("")
        if item.shift:
            lines.append(f"这一年的推进重点是：{item.shift}")
            lines.append("")
        if item.representative_papers:
            lines.append(f"建议对照的代表论文：{'、'.join(item.representative_papers[:4])}")
            lines.append("")


def _append_selected_paper_cards(lines: list[str], bundle: ArcEvidenceBundle) -> None:
    if not bundle.selected_papers:
        return
    lines.append("<!--PAGE_BREAK-->")
    lines.append("")
    lines.append("## 代表论文速读卡")
    lines.append("")
    lines.append("下面这些卡片不是替代原论文，而是告诉你每篇在这条故事线里扮演什么角色、该抓什么证据、又该怀疑什么地方。")
    lines.append("")

    for paper in bundle.selected_papers:
        lines.append(f"### {paper.paper_label}")
        lines.append("")
        role_text = "、".join(_humanize_relevance_tag(tag) for tag in paper.relevance_tags[:4])
        if role_text:
            lines.append(f"它在这条线里的位置：{role_text}")
            lines.append("")

        problem_text = _join_sentence_parts(
            [paper.problem_statement or paper.takeaway, paper.prior_work_positioning],
            fallback="这篇论文在现有故事线里提供了一个可定位的关键证据点。",
        )
        if problem_text:
            lines.append(f"它试图回答的问题：{problem_text}")
            lines.append("")

        method_bits: list[str] = []
        if paper.method_modules:
            method_bits.append(f"方法模块：{'、'.join(paper.method_modules[:5])}")
        if paper.core_pipeline:
            method_bits.append(f"核心流程：{' → '.join(paper.core_pipeline[:5])}")
        if paper.core_claims:
            method_bits.append(f"核心主张：{'；'.join(paper.core_claims[:3])}")
        if method_bits:
            lines.append("方法骨架可以抓这几个点：")
            lines.append("")
            for bit in method_bits:
                lines.append(f"- {bit}")
            lines.append("")

        evidence_bits: list[str] = []
        if paper.evaluation_goal:
            evidence_bits.append(f"实验目标：{paper.evaluation_goal}")
        if paper.experiment_names:
            evidence_bits.append(f"关键实验：{'、'.join(paper.experiment_names[:4])}")
        if paper.section_highlights:
            evidence_bits.extend(paper.section_highlights[:2])
        if evidence_bits:
            lines.append("最该补看的证据位置：")
            lines.append("")
            for bit in evidence_bits[:4]:
                lines.append(f"- {bit}")
            lines.append("")

        caution_bits: list[str] = []
        caution_bits.extend(paper.reviewer_highlights[:2])
        caution_bits.extend(paper.missing_ablations[:2])
        caution_bits.extend(paper.reproducibility_risks[:2])
        if caution_bits:
            lines.append("读这篇时要保留的怀疑：")
            lines.append("")
            for bit in caution_bits[:4]:
                lines.append(f"- {bit}")
            lines.append("")

        next_bits: list[str] = []
        next_bits.extend(paper.extension_highlights[:2])
        next_bits.extend(paper.verification_questions[:2])
        if next_bits:
            lines.append("继续往下追时最自然的方向：")
            lines.append("")
            for bit in next_bits[:4]:
                lines.append(f"- {bit}")
            lines.append("")


def _append_open_tensions(lines: list[str], arc: DetailedStoryArc) -> None:
    if not arc.open_tensions_detailed:
        return
    lines.append("## 仍未解决的问题")
    lines.append("")
    lines.append("这一节不是重复 critique，而是把整条故事线压缩成几个真正还没闭环的矛盾。")
    lines.append("")
    for index, item in enumerate(arc.open_tensions_detailed, start=1):
        lines.append(f"### 问题 {index}：{item.tension}")
        lines.append("")
        lines.append(f"为什么它一直没被彻底解决：{item.why_it_persists}")
        lines.append("")
        lines.append(f"接下来最值得盯住的信号：{item.what_to_watch}")
        lines.append("")


def _append_evidence_scope(lines: list[str], arc: DetailedStoryArc, bundle: ArcEvidenceBundle) -> None:
    lines.append("## 附：这条线的证据范围")
    lines.append("")
    lines.append(f"这份单独报告覆盖原始故事线中的 {arc.paper_count} 篇论文，并额外挑出 {len(bundle.selected_papers)} 篇代表论文作为锚点卡片。")
    lines.append("")
    if bundle.selected_papers:
        lines.append("本次重点锚点包括：")
        lines.append("")
        for paper in bundle.selected_papers:
            lines.append(f"- {paper.paper_label}")
        lines.append("")

    lines.append("## 方法说明")
    lines.append("")
    lines.append("这份单独故事线 PDF 复用了已有单篇解析产物与 narrative detail 中间结果，没有重新解析 PDF 原文；它的目标是把分散证据重新组织成适合连续阅读的一条研究脉络。")
    lines.append("")


def render_single_arc_markdown(arc: DetailedStoryArc, bundle: ArcEvidenceBundle) -> str:
    lines: list[str] = [f"# {arc.title}", ""]
    _append_metadata_block(lines, arc, bundle)
    _append_reading_intro(lines, arc, bundle)
    lines.append("<!--PAGE_BREAK-->")
    lines.append("")
    _append_story_axis(lines, arc)
    _append_turning_points(lines, arc)
    _append_reading_path(lines, arc)
    _append_year_progression(lines, arc)
    _append_selected_paper_cards(lines, bundle)
    _append_open_tensions(lines, arc)
    _append_evidence_scope(lines, arc, bundle)
    return "\n".join(lines).strip() + "\n"


def export_single_arc_reports(
    arc_reports_dir: Path,
    detailed_arcs: list[DetailedStoryArc],
    bundles_by_theme_id: dict[str, ArcEvidenceBundle],
) -> list[ArcReportArtifact]:
    arc_reports_dir.mkdir(parents=True, exist_ok=True)
    artifacts: list[ArcReportArtifact] = []
    used_basenames: set[str] = set()

    for arc in detailed_arcs:
        bundle = bundles_by_theme_id[arc.theme_id]
        base_name = sanitize_filename(arc.title, fallback=arc.theme_id or "story-arc")
        if base_name in used_basenames:
            base_name = sanitize_filename(f"{arc.title} - {arc.theme_id}", fallback=arc.theme_id or "story-arc")
        used_basenames.add(base_name)

        markdown_path = arc_reports_dir / f"{base_name}.md"
        html_path = arc_reports_dir / f"{base_name}.html"
        pdf_path = arc_reports_dir / f"{base_name}.pdf"
        arc_markdown = render_single_arc_markdown(arc, bundle)
        write_text(markdown_path, arc_markdown)
        document = build_report_document(arc_markdown, title=arc.title)
        export_html_report(
            document,
            html_path,
            metadata={"theme_id": arc.theme_id, "paper_count": arc.paper_count, "year_range": arc.year_range},
        )
        export_pdf_report(
            document,
            pdf_path,
            metadata={"theme_id": arc.theme_id, "paper_count": arc.paper_count, "year_range": arc.year_range},
        )
        log_event(
            "info",
            "Detailed narrative single-arc report exported",
            arc=arc.title,
            markdown_path=markdown_path,
            html_path=html_path,
            pdf_path=pdf_path,
        )
        artifacts.append(
            ArcReportArtifact(
                theme_id=arc.theme_id,
                title=arc.title,
                basename=base_name,
                markdown_path=str(markdown_path),
                html_path=str(html_path),
                pdf_path=str(pdf_path),
                paper_count=arc.paper_count,
                year_range=arc.year_range,
            )
        )

    write_json(arc_reports_dir / "index.json", [item.to_dict() for item in artifacts])
    return artifacts
