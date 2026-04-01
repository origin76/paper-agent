from __future__ import annotations

from typing import Any

from .reference_appendix import has_meaningful_content, top_reference_links
from .text_utils import (
    clean_list_texts,
    compact_alternative_summary,
    compact_claim_summary,
    compact_decision_summary,
    compact_design_choice_summary,
    compact_module_names,
    compact_module_readout,
    compact_module_summary,
    compact_pipeline_steps,
    count_phrase,
    display_section_title,
    ensure_terminal_punctuation,
    format_authors,
    inline_list,
    inline_ordinal_points,
    join_sentences,
    render_question_prompt,
    shorten_text,
    strip_terminal_punctuation,
    clean_pipeline_step,
)


def render_overview_section(overview: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 1. 一页读懂这篇论文"]
    takeaway = shorten_text(overview.get("one_sentence_takeaway"), max_chars=180, sentence_limit=2)
    authors = format_authors(overview.get("authors"))
    venue = shorten_text(overview.get("venue"), max_chars=80, sentence_limit=1)
    paper_type = shorten_text(overview.get("paper_type"), max_chars=40, sentence_limit=1)

    meta_bits = [
        f"作者：{authors}" if authors else "",
        f"来源：{venue}" if venue else "",
        f"年份：{overview.get('publication_year')}" if overview.get("publication_year") else "",
        f"类型：{paper_type}" if paper_type else "",
    ]
    meta_line = " ｜ ".join(bit for bit in meta_bits if bit)
    if meta_line:
        parts.extend([f"> {meta_line}", ""])
    if takeaway:
        parts.extend([f"> {takeaway}", ""])

    problem = shorten_text(overview.get("problem_statement"), max_chars=260, sentence_limit=2)
    importance = shorten_text(overview.get("why_this_problem_matters"), max_chars=240, sentence_limit=2)
    if problem or importance:
        paragraph = []
        if problem:
            paragraph.append(f"这篇论文抓住的问题是：{strip_terminal_punctuation(problem)}")
        if importance:
            paragraph.append(f"作者认为它重要，是因为{strip_terminal_punctuation(importance)}")
        parts.extend([join_sentences(paragraph), ""])

    prior_work = shorten_text(overview.get("prior_work_positioning"), max_chars=240, sentence_limit=2)
    claim_summary = inline_ordinal_points(overview.get("core_claims") or [], limit=3, max_chars=100)
    if prior_work or claim_summary:
        paragraph = []
        if prior_work:
            paragraph.append(f"和已有工作的关系上，作者的定位是：{strip_terminal_punctuation(prior_work)}")
        if claim_summary:
            paragraph.append(f"如果只记住论文最核心的几个判断，可以压缩成：{claim_summary}")
        parts.extend([join_sentences(paragraph), ""])

    read_order = clean_list_texts(overview.get("read_order"), limit=3, max_chars=120)
    if read_order:
        parts.append("### 快读顺序")
        for index, item in enumerate(read_order, start=1):
            parts.append(f"{index}. {item}")
        parts.append("")

    questions = clean_list_texts(overview.get("must_clarify_questions"), limit=3, max_chars=100)
    if questions:
        parts.append("### 读前先带着这几个问题")
        parts.extend(
            [
                ensure_terminal_punctuation(
                    f"建议先带着{count_phrase(len(questions))}进入正文：{inline_ordinal_points(questions, limit=3, max_chars=100)}"
                ),
                "",
            ]
        )

    return parts


def render_structure_section(structure: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 2. 问题、方法与贡献主线"]

    problem = shorten_text(structure.get("problem"), max_chars=260, sentence_limit=2)
    assumptions = clean_list_texts(structure.get("assumptions"), limit=3, max_chars=90)
    if problem or assumptions:
        parts.append("### 论文真正卡住的问题")
        paragraph = []
        if problem:
            paragraph.append(f"作者要解决的核心困难是：{strip_terminal_punctuation(problem)}")
        if assumptions:
            paragraph.append(f"整套方法成立依赖几个前提：{inline_list([strip_terminal_punctuation(item) for item in assumptions])}")
        parts.extend([join_sentences(paragraph), ""])

    parts.append("### 方法主线")
    module_names = compact_module_names(structure.get("method_modules"))
    module_summary = compact_module_readout(module_names) or compact_module_summary(structure.get("method_modules"), limit=1)
    pipeline = compact_pipeline_steps(structure.get("core_pipeline"), limit=5)
    if module_names:
        parts.append(ensure_terminal_punctuation(f"整条方法可以先看成几个咬合在一起的齿轮：{'、'.join(module_names)}"))
        parts.append("")
    if module_summary:
        parts.append(ensure_terminal_punctuation(f"其中真正决定它为何既快又省显存的，是：{strip_terminal_punctuation(module_summary)}"))
        parts.append("")
    if pipeline:
        parts.append("如果把论文的方法压成一条可执行流程，最值得记住的是：")
        for index, step in enumerate(pipeline, start=1):
            parts.append(f"{index}. {clean_pipeline_step(step)}")
        parts.append("")

    decision_summary = compact_decision_summary(structure.get("decision_points"))
    claim_summary = compact_claim_summary(structure.get("claim_to_evidence_map"))
    if decision_summary:
        parts.append("### 作者做了哪些关键取舍")
        parts.extend([join_sentences(decision_summary), ""])
    if claim_summary:
        parts.append("### 证据地图")
        parts.extend([join_sentences(claim_summary), ""])

    return parts


def render_external_context_section(
    state: dict[str, Any],
    web_research: dict[str, Any],
    resource_discovery: dict[str, Any],
) -> list[str]:
    parts: list[str] = ["## 3. 外部视角补充"]
    if not (has_meaningful_content(web_research) or has_meaningful_content(resource_discovery)):
        parts.extend(["本次没有拿到足够稳定的外部补充信息，因此正文判断仍以论文本身为准。", ""])
        return parts

    reviewer_points = clean_list_texts(web_research.get("reviewer_signals"), limit=2, max_chars=120)
    related_points = clean_list_texts(web_research.get("related_work_signals"), limit=2, max_chars=120)
    if reviewer_points or related_points:
        paragraph = []
        if reviewer_points:
            paragraph.append(f"从社区与审稿视角看，这篇论文最被认可的地方主要有：{inline_list([strip_terminal_punctuation(item) for item in reviewer_points])}")
        if related_points:
            paragraph.append(f"把它放回文献脉络里，最重要的对照关系是：{inline_list([strip_terminal_punctuation(item) for item in related_points])}")
        parts.extend([join_sentences(paragraph), ""])

    implementation_points = clean_list_texts(resource_discovery.get("implementation_signals"), limit=2, max_chars=120)
    risk_points = clean_list_texts(web_research.get("external_risks_or_confusions"), limit=2, max_chars=120)
    gap_points = clean_list_texts(resource_discovery.get("missing_resource_gaps"), limit=2, max_chars=120)
    if implementation_points or risk_points or gap_points:
        paragraph = []
        if implementation_points:
            paragraph.append(f"如果把它当成工程对象，最有价值的实现线索是：{inline_list([strip_terminal_punctuation(item) for item in implementation_points])}")
        if risk_points:
            paragraph.append(f"外部资料里反复提醒的边界包括：{inline_list([strip_terminal_punctuation(item) for item in risk_points])}")
        if gap_points:
            paragraph.append(f"公开材料目前最大的空白则是：{inline_list([strip_terminal_punctuation(item) for item in gap_points])}")
        parts.extend([join_sentences(paragraph), ""])

    links = top_reference_links(state)
    if links:
        parts.extend([f"> 如果准备动手复现，建议先打开：{inline_list(links)}", ""])

    return parts


def render_deep_read_section(section_analyses: list[dict[str, Any]]) -> list[str]:
    parts: list[str] = ["## 4. 逐节带读"]
    for item in section_analyses:
        cleaned_title = display_section_title(item.get("section_title"))
        parts.append(f"### {cleaned_title or '未命名章节'}")

        role = shorten_text(item.get("section_role_in_paper"), max_chars=180, sentence_limit=2)
        author_view = shorten_text(item.get("author_view"), max_chars=220, sentence_limit=2)
        if role or author_view:
            paragraph = []
            if role:
                paragraph.append(f"这一节在全文里的任务是：{strip_terminal_punctuation(role)}")
            if author_view:
                paragraph.append(f"从作者叙事看，他真正想让你接受的是：{strip_terminal_punctuation(author_view)}")
            parts.extend([join_sentences(paragraph), ""])

        math_points = clean_list_texts(item.get("math_or_algorithm"), limit=2, max_chars=100)
        design_summary = compact_design_choice_summary(item.get("design_choices"), limit=1)
        alternative_summary = compact_alternative_summary(item.get("alternatives"))
        if math_points or design_summary or alternative_summary:
            paragraph = []
            if math_points:
                paragraph.append(f"从机制上看，这一节最值得抓住的是：{inline_list([strip_terminal_punctuation(point) for point in math_points], joiner='、')}")
            if design_summary:
                paragraph.append(f"作者在这里的关键取舍是：{design_summary}")
            if alternative_summary:
                paragraph.append(strip_terminal_punctuation(alternative_summary))
            parts.extend([join_sentences(paragraph), ""])

        reviewer_view = shorten_text(item.get("reviewer_view"), max_chars=180, sentence_limit=2)
        engineer_view = shorten_text(item.get("engineer_view"), max_chars=200, sentence_limit=2)
        if reviewer_view or engineer_view:
            paragraph = []
            if reviewer_view:
                paragraph.append(f"站在审稿人角度，最该盯住的是：{strip_terminal_punctuation(reviewer_view)}")
            if engineer_view:
                paragraph.append(f"如果你准备复现，这一节最实用的提醒是：{strip_terminal_punctuation(engineer_view)}")
            parts.extend([join_sentences(paragraph), ""])

        question_prompt = render_question_prompt(item.get("verification_questions"), limit=2, max_chars=110)
        if question_prompt:
            parts.extend([question_prompt, ""])

    return parts


def render_experiment_section(experiment_review: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 5. 实验到底支持了什么"]
    goal = shorten_text(experiment_review.get("evaluation_goal"), max_chars=220, sentence_limit=2)
    support = shorten_text(experiment_review.get("overall_support_for_claims"), max_chars=240, sentence_limit=2)
    if goal or support:
        paragraph = []
        if goal:
            paragraph.append(f"实验部分的目标是：{strip_terminal_punctuation(goal)}")
        if support:
            paragraph.append(f"整体看下来，我对证据强度的判断是：{strip_terminal_punctuation(support)}")
        parts.extend([join_sentences(paragraph), ""])

    experiments = experiment_review.get("experiments") if isinstance(experiment_review.get("experiments"), list) else []
    strong = [item for item in experiments if str(item.get("evidence_strength") or "").lower() == "strong"]
    medium_or_weak = [item for item in experiments if str(item.get("evidence_strength") or "").lower() != "strong"]

    if strong:
        parts.append("### 证据最强的部分")
        parts.extend([join_sentences([_experiment_sentence(item) for item in strong[:2]]), ""])

    if medium_or_weak:
        parts.append("### 仍然没被完全回答的问题")
        concerns: list[str] = []
        for item in medium_or_weak[:3]:
            sentence = strip_terminal_punctuation(_experiment_sentence(item))
            biases = clean_list_texts(item.get("possible_bias"), limit=1, max_chars=90)
            if biases:
                sentence += f"，但要留意{strip_terminal_punctuation(biases[0])}"
            concerns.append(sentence)
        parts.extend([join_sentences(concerns), ""])

    missing_ablations = clean_list_texts(experiment_review.get("missing_ablations"), limit=3, max_chars=100)
    reproducibility_risks = clean_list_texts(experiment_review.get("reproducibility_risks"), limit=3, max_chars=100)
    if missing_ablations or reproducibility_risks:
        parts.append("### 如果你要复现，先补这几件事")
        fixes: list[str] = []
        if missing_ablations:
            fixes.append(f"先补实验：{inline_ordinal_points(missing_ablations, limit=2, max_chars=90)}")
        if reproducibility_risks:
            fixes.append(f"先防风险：{inline_ordinal_points(reproducibility_risks[:2], limit=2, max_chars=90)}")
        parts.extend([join_sentences(fixes), ""])

    return parts


def _experiment_sentence(item: dict[str, Any]) -> str:
    name = strip_terminal_punctuation(str(item.get("name") or ""))
    claim = shorten_text(item.get("claim_tested"), max_chars=82, sentence_limit=1)
    note = shorten_text(item.get("reviewer_notes"), max_chars=92, sentence_limit=1)
    pieces = [name] if name else []
    if claim:
        pieces.append(f"主要验证“{strip_terminal_punctuation(claim)}”")
    if note:
        pieces.append(f"说服力在于{strip_terminal_punctuation(note)}")
    return ensure_terminal_punctuation("，".join(piece for piece in pieces if piece))
