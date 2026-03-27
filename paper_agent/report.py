from __future__ import annotations

import re
from typing import Any

from paper_agent.url_enrichment import normalize_reference_descriptor, normalize_resource_payload


KEY_LABELS = {
    "paper_title": "论文标题",
    "paper_type": "论文类型",
    "authors": "作者",
    "venue": "会议或来源",
    "publication_year": "发表年份",
    "one_sentence_takeaway": "一句话结论",
    "problem_statement": "问题定义",
    "why_this_problem_matters": "问题重要性",
    "prior_work_positioning": "与已有工作的关系",
    "core_claims": "核心主张",
    "key_terms": "关键词",
    "read_order": "建议阅读顺序",
    "must_clarify_questions": "必须澄清的问题",
    "problem": "核心问题",
    "assumptions": "关键假设",
    "inputs_and_outputs": "输入与输出",
    "inputs": "输入",
    "outputs": "输出",
    "method_modules": "方法模块",
    "name": "名称",
    "role": "作用",
    "depends_on": "依赖项",
    "core_pipeline": "核心流程",
    "decision_points": "关键设计抉择",
    "choice": "设计选择",
    "reason": "原因",
    "tradeoff": "权衡",
    "claim_to_evidence_map": "主张与证据映射",
    "claim": "主张",
    "evidence_sections": "对应证据章节",
    "section_map": "章节地图",
    "section_title": "章节标题",
    "purpose": "章节作用",
    "priority": "优先级",
    "related_work_signals": "相关工作信号",
    "code_resources": "代码资源",
    "reading_notes": "外部阅读笔记",
    "reviewer_signals": "审稿视角信号",
    "external_risks_or_confusions": "外部风险与歧义",
    "source_shortlist": "高价值来源清单",
    "title": "标题",
    "url": "链接",
    "type": "类型",
    "why_relevant": "相关性说明",
    "insight": "关键洞见",
    "official_pages": "官方页面",
    "page_type": "页面类型",
    "code_repositories": "代码仓库",
    "repo_kind": "仓库类型",
    "datasets_and_benchmarks": "数据集与基准",
    "reproducibility_materials": "复现材料",
    "material_type": "材料类型",
    "implementation_signals": "实现线索",
    "missing_resource_gaps": "资源缺口",
    "section_role_in_paper": "该章节在全文中的作用",
    "author_view": "作者视角",
    "reviewer_view": "审稿人视角",
    "engineer_view": "工程视角",
    "math_or_algorithm": "数学或算法要点",
    "design_choices": "设计选择",
    "risk": "风险",
    "alternatives": "可替代方案",
    "alternative": "替代方案",
    "why_not_chosen": "为何未采用",
    "verification_questions": "验证问题",
    "evaluation_goal": "评测目标",
    "experiments": "实验项",
    "variable": "变量",
    "controls": "控制项",
    "claim_tested": "验证的主张",
    "evidence_strength": "证据强度",
    "possible_bias": "可能偏差",
    "reviewer_notes": "审稿备注",
    "overall_support_for_claims": "整体支持力度",
    "missing_ablations": "缺失的消融实验",
    "reproducibility_risks": "复现风险",
}

NOTE_STYLE_LABELS = {
    "why_relevant": "批注",
    "insight": "批注",
    "reviewer_notes": "审稿批注",
}

ORDERED_LIST_KEYS = {
    "read_order",
    "must_clarify_questions",
    "core_pipeline",
    "experiments",
    "verification_questions",
    "design_choices",
    "alternatives",
    "decision_points",
    "claim_to_evidence_map",
    "section_map",
}

PRIMARY_RECORD_KEYS = (
    "title",
    "name",
    "section_title",
    "choice",
    "alternative",
    "claim",
)

INLINE_DESCRIPTOR_KEYS = (
    "type",
    "page_type",
    "repo_kind",
    "material_type",
    "role",
    "priority",
    "evidence_strength",
)

VALUE_LABELS = {
    "strong": "强",
    "moderate": "中",
    "weak": "弱",
    "true": "是",
    "false": "否",
}

REFERENCE_SOURCE_LABELS = {
    "source_shortlist": "高价值来源清单",
    "official_pages": "官方页面",
    "code_resources": "代码资源",
    "code_repositories": "代码仓库",
    "datasets_and_benchmarks": "数据集与基准",
    "reproducibility_materials": "复现材料",
    "reading_notes": "外部阅读笔记",
}

REFERENCE_BUCKET_PRIORITY = {
    "核心入口": 0,
    "代码与实现": 1,
    "数据与复现": 2,
    "延伸阅读": 3,
}

REFERENCE_DESCRIPTOR_VALUE_LABELS = {
    "conference_page": "会议页面",
    "conference_index": "会议总页",
    "conference_presentation": "会议演讲页",
    "technical_reference": "技术参考",
    "documentation": "文档页",
    "github_repository": "GitHub 仓库",
    "github_organization": "GitHub 组织",
    "artifact_page": "Artifact 页面",
    "artifact_archive": "Artifact 归档",
    "dependency": "依赖工具",
    "background_reference": "背景参考",
}

REFERENCE_DESCRIPTOR_FAMILIES = {
    "conference_presentation": "conference",
    "conference_page": "conference",
    "conference_index": "conference",
    "github_repository": "github",
    "github_organization": "github",
    "artifact_page": "artifact",
    "artifact_archive": "artifact",
    "technical_reference": "reference",
    "documentation": "reference",
    "dependency": "reference",
    "background_reference": "reference",
}

REFERENCE_DESCRIPTOR_PRIORITY = {
    "conference_presentation": 0,
    "conference_page": 1,
    "conference_index": 2,
    "github_organization": 0,
    "github_repository": 1,
    "artifact_page": 0,
    "artifact_archive": 1,
    "technical_reference": 0,
    "documentation": 1,
    "dependency": 2,
    "background_reference": 3,
}

REFERENCE_PAGE_BREAK_MARKER = "<!--PAGE_BREAK-->"


def _has_meaningful_content(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return False
    for value in payload.values():
        if _is_non_empty(value):
            return True
    return False


def _is_non_empty(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, dict, tuple, set)):
        return bool(value)
    return True


def _label_key(key: str) -> str:
    return KEY_LABELS.get(key, key)


def _render_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "是" if value else "否"
    if isinstance(value, (int, float)):
        return str(value)
    if value is None:
        return ""

    text = str(value).strip()
    normalized = VALUE_LABELS.get(text.lower())
    if normalized:
        return normalized
    descriptor_label = REFERENCE_DESCRIPTOR_VALUE_LABELS.get(normalize_reference_descriptor(text))
    if descriptor_label:
        return descriptor_label
    if _looks_like_url(text):
        return _markdown_link(text, text)
    return text


def _markdown_link(label: str, url: str) -> str:
    safe_label = label.replace("[", "\\[").replace("]", "\\]")
    safe_url = url.replace(" ", "%20")
    return f"[{safe_label}]({safe_url})"


def _format_key_value_dict(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    for key, value in payload.items():
        lines.extend(_render_field(key, value, indent=0))
    return "\n".join(lines)


def _render_field(key: str, value: Any, indent: int) -> list[str]:
    if not _is_non_empty(value):
        return []

    prefix = f"{'  ' * indent}- "
    label = NOTE_STYLE_LABELS.get(key, _label_key(key))

    if isinstance(value, dict):
        lines = [f"{prefix}**{label}**"]
        lines.extend(_render_mapping(value, indent + 1))
        return lines

    if isinstance(value, list):
        lines = [f"{prefix}**{label}**"]
        lines.extend(_render_list_items(key, value, indent + 1))
        return lines

    return [f"{prefix}**{label}**：{_render_scalar(value)}"]


def _render_mapping(payload: dict[str, Any], indent: int) -> list[str]:
    lines: list[str] = []
    for key, value in payload.items():
        if key == "url" and "title" in payload:
            continue
        lines.extend(_render_field(key, value, indent))
    return lines


def _render_list_items(parent_key: str, items: list[Any], indent: int) -> list[str]:
    lines: list[str] = []
    ordered = parent_key in ORDERED_LIST_KEYS
    for index, item in enumerate(items, start=1):
        marker = f"{index}." if ordered else "-"
        if isinstance(item, dict):
            lines.extend(_render_record_item(item, indent, marker))
            continue
        if isinstance(item, list):
            lines.append(f"{'  ' * indent}{marker}")
            lines.extend(_render_list_items(parent_key, item, indent + 1))
            continue
        lines.append(f"{'  ' * indent}{marker} {_normalize_list_text(_render_scalar(item), ordered=ordered)}")
    return lines


def _render_record_item(payload: dict[str, Any], indent: int, marker: str) -> list[str]:
    header, consumed_keys = _render_record_header(payload)
    prefix = f"{'  ' * indent}{marker} "
    lines = [f"{prefix}{header}"]

    for key, value in payload.items():
        if key in consumed_keys or not _is_non_empty(value):
            continue
        lines.extend(_render_field(key, value, indent + 1))
    return lines


def _render_record_header(payload: dict[str, Any]) -> tuple[str, set[str]]:
    consumed_keys: set[str] = set()
    primary_key = next((key for key in PRIMARY_RECORD_KEYS if _is_non_empty(payload.get(key))), None)
    url = payload.get("url")
    descriptors: list[str] = []
    for key in INLINE_DESCRIPTOR_KEYS:
        value = payload.get(key)
        if key == primary_key or not _is_non_empty(value):
            continue
        descriptors.append(f"{_label_key(key)}：{_render_scalar(value)}")
        consumed_keys.add(key)

    if primary_key is not None:
        primary_value = str(payload[primary_key]).strip()
        consumed_keys.add(primary_key)
        if _is_non_empty(url):
            header_text = f"**{_markdown_link(primary_value, str(url))}**"
            consumed_keys.add("url")
        else:
            header_text = f"**{primary_value}**"
    elif _is_non_empty(url):
        header_text = f"**{_markdown_link('打开链接', str(url))}**"
        consumed_keys.add("url")
    else:
        header_text = "**条目**"

    if descriptors:
        header_text = f"{header_text}（{'；'.join(descriptors)}）"

    return header_text, consumed_keys


def _looks_like_url(text: str) -> bool:
    return text.startswith("http://") or text.startswith("https://")


def _normalize_list_text(text: str, ordered: bool) -> str:
    normalized = text.strip()
    if ordered:
        normalized = re.sub(r"^\d+[\.\)、]\s*", "", normalized)
    return normalized


def _append_unique(values: list[str], candidate: str | None) -> None:
    if not candidate:
        return
    normalized = candidate.strip()
    if not normalized or normalized in values:
        return
    values.append(normalized)


def _choose_reference_bucket(source_key: str, payload: dict[str, Any], url: str) -> str:
    lowered_url = url.lower()
    lowered_title = str(payload.get("title") or payload.get("name") or "").lower()
    lowered_type = str(
        payload.get("type")
        or payload.get("page_type")
        or payload.get("repo_kind")
        or payload.get("material_type")
        or payload.get("role")
        or ""
    ).lower()

    if (
        source_key in {"datasets_and_benchmarks", "reproducibility_materials"}
        or "doi.org" in lowered_url
        or "zenodo" in lowered_url
        or "artifact" in lowered_title
        or "artifact" in lowered_type
        or "benchmark" in lowered_type
    ):
        return "数据与复现"
    if source_key in {"code_resources", "code_repositories"} or "github.com" in lowered_url:
        return "代码与实现"
    if source_key == "reading_notes":
        return "延伸阅读"
    return "核心入口"


def _collect_reference_entries(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {
        "核心入口": [],
        "代码与实现": [],
        "数据与复现": [],
        "延伸阅读": [],
    }
    by_url: dict[str, dict[str, Any]] = {}

    for payload_key in ("web_research", "resource_discovery"):
        payload = normalize_resource_payload(state.get(payload_key) or {})
        if not isinstance(payload, dict):
            continue
        for source_name in payload.keys():
            items = payload.get(source_name)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url") or "").strip()
                if not url:
                    continue

                entry = by_url.get(url)
                if entry is None:
                    title = str(item.get("title") or item.get("name") or url).strip()
                    bucket = _choose_reference_bucket(source_name, item, url)
                    entry = {
                        "title": title,
                        "url": url,
                        "bucket": bucket,
                        "source_labels": [],
                        "descriptor_labels": [],
                        "notes": [],
                    }
                    by_url[url] = entry
                    grouped[bucket].append(entry)
                else:
                    candidate_title = str(item.get("title") or item.get("name") or "").strip()
                    if candidate_title and len(candidate_title) > len(str(entry["title"])):
                        entry["title"] = candidate_title
                    candidate_bucket = _choose_reference_bucket(source_name, item, url)
                    current_bucket = str(entry["bucket"])
                    if REFERENCE_BUCKET_PRIORITY[candidate_bucket] < REFERENCE_BUCKET_PRIORITY[current_bucket]:
                        grouped[current_bucket] = [existing for existing in grouped[current_bucket] if existing is not entry]
                        grouped[candidate_bucket].append(entry)
                        entry["bucket"] = candidate_bucket

                _append_unique(entry["source_labels"], REFERENCE_SOURCE_LABELS.get(source_name, _label_key(source_name)))

                for descriptor_key in ("type", "page_type", "repo_kind", "material_type", "role"):
                    descriptor_value = item.get(descriptor_key)
                    if _is_non_empty(descriptor_value):
                        _append_unique(
                            entry["descriptor_labels"],
                            f"{_label_key(descriptor_key)}：{_render_scalar(descriptor_value)}",
                        )

                for note_key in ("why_relevant", "insight", "reviewer_notes"):
                    note_value = item.get(note_key)
                    if _is_non_empty(note_value):
                        label = NOTE_STYLE_LABELS.get(note_key, _label_key(note_key))
                        _append_unique(entry["notes"], f"{label}：{_render_scalar(note_value)}")

    return {bucket: entries for bucket, entries in grouped.items() if entries}


def _render_reference_appendix(state: dict[str, Any], section_index: int) -> list[str]:
    reference_groups = _collect_reference_entries(state)
    if not reference_groups:
        return []

    parts: list[str] = [
        REFERENCE_PAGE_BREAK_MARKER,
        "",
        f"## {section_index}. 参考链接页",
        "> 本附录仅保留与论文核验、实现追踪和复现实验直接相关的外部入口。",
        "",
    ]

    for group_title, entries in reference_groups.items():
        parts.append(f"### {group_title}")
        for index, entry in enumerate(entries, start=1):
            parts.append(f"{index}. **{_markdown_link(str(entry['title']), str(entry['url']))}**")
            parts.append(f"   - 链接：`{entry['url']}`")
            locator = _build_reference_locator(entry)
            if locator:
                parts.append(f"   - 定位：{locator}")
            summary = _build_reference_summary(entry)
            if summary:
                parts.append(f"   - 用途：{summary}")
        parts.append("")

    return parts


def _build_reference_locator(entry: dict[str, Any]) -> str:
    descriptor_labels = [str(item).strip() for item in entry.get("descriptor_labels") or [] if str(item).strip()]
    best_by_family: dict[str, str] = {}
    for label in descriptor_labels:
        value = label.split("：", 1)[1].strip() if "：" in label else label
        normalized_value = normalize_reference_descriptor(value)
        family = REFERENCE_DESCRIPTOR_FAMILIES.get(normalized_value, normalized_value)
        previous = best_by_family.get(family)
        if previous is None or REFERENCE_DESCRIPTOR_PRIORITY.get(normalized_value, 99) < REFERENCE_DESCRIPTOR_PRIORITY.get(previous, 99):
            best_by_family[family] = normalized_value
    pretty_descriptors = [
        REFERENCE_DESCRIPTOR_VALUE_LABELS.get(value, value)
        for value in sorted(best_by_family.values(), key=lambda item: (REFERENCE_DESCRIPTOR_PRIORITY.get(item, 99), item))
        if value
    ]
    if pretty_descriptors:
        return "；".join(pretty_descriptors[:2])
    return ""


def _build_reference_summary(entry: dict[str, Any]) -> str:
    note_texts = [
        re.sub(r"^(批注|审稿批注)：", "", str(item).strip()).strip()
        for item in entry.get("notes") or []
        if str(item).strip()
    ]
    deduped: list[str] = []
    for note in note_texts:
        if note and note not in deduped:
            deduped.append(note)
    if not deduped:
        return ""
    return max(deduped, key=len)


def render_report(state: dict[str, Any]) -> str:
    overview = state["overview"]
    web_research = normalize_resource_payload(state.get("web_research"))
    resource_discovery = normalize_resource_payload(state.get("resource_discovery"))
    structure = state["structure"]
    section_analyses = state["section_analyses"]
    experiment_review = state["experiment_review"]
    critique = state["critique"]
    extensions = state["extensions"]
    section_targets = state["section_targets"]

    parts: list[str] = [
        f"# {overview.get('paper_title') or state['source_name']}",
        "",
        "## 运行信息",
        f"- 源 PDF：`{state['pdf_path']}`",
        "- PDF 读取方式：`local pdftotext extraction`",
        f"- 提取文本字符数：{state['paper_text_meta'].get('char_count')}",
        f"- 文档分析模型：`{state['overview_meta'].get('model')}`",
        f"- 高层分析模型：`{state['critique_meta'].get('model')}`",
        f"- 深读章节数：{len(section_targets)}",
        f"- 是否启用联网搜索：{_render_scalar(state.get('web_search_enabled', False))}",
        "",
    ]

    section_index = 1
    parts.extend([f"## {section_index}. 全局理解", _format_key_value_dict(overview), ""])
    section_index += 1

    if _has_meaningful_content(web_research):
        parts.extend([f"## {section_index}. 外部联网研究", _format_key_value_dict(web_research), ""])
        section_index += 1

    if _has_meaningful_content(resource_discovery):
        parts.extend([f"## {section_index}. 资源发现", _format_key_value_dict(resource_discovery), ""])
        section_index += 1

    parts.extend([f"## {section_index}. 可执行结构", _format_key_value_dict(structure), ""])
    section_index += 1

    parts.append(f"## {section_index}. 逐节精读")
    for item in section_analyses:
        parts.extend(
            [
                f"### {item.get('section_title', '未命名章节')}",
                _format_key_value_dict(item),
                "",
            ]
        )
    section_index += 1

    parts.extend([f"## {section_index}. 实验审查", _format_key_value_dict(experiment_review), ""])
    section_index += 1

    parts.extend([f"## {section_index}. 批判性评审", critique.strip(), ""])
    section_index += 1

    parts.extend([f"## {section_index}. 延伸与研究方向", extensions.strip(), ""])
    section_index += 1

    normalized_state = dict(state)
    normalized_state["web_research"] = web_research
    normalized_state["resource_discovery"] = resource_discovery
    parts.extend([""])
    parts.extend(_render_reference_appendix(normalized_state, section_index))

    return "\n".join(parts).strip() + "\n"
