from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from paper_agent.sections import clean_section_title
from paper_agent.url_enrichment import normalize_reference_descriptor, normalize_resource_payload
from paper_agent.utils import normalize_text


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
    "technical_article": "技术文章",
    "documentation": "文档页",
    "github_repository": "GitHub 仓库",
    "github_organization": "GitHub 组织",
    "artifact_page": "Artifact 页面",
    "artifact_archive": "Artifact 归档",
    "dependency": "依赖工具",
    "background_reference": "背景参考",
    "official_preprint": "预印本",
    "official_paper": "正式论文页",
    "publication": "正式出版页",
    "preprint_server": "预印本",
    "source_code": "源码",
    "implementation": "实现入口",
    "implementation_source": "实现源码",
    "experiment_config": "实验配置",
    "evaluation_script": "评测脚本",
    "formal_proof": "形式化证明",
    "config_guide": "配置指南",
    "training_script": "训练脚本",
    "benchmark_tool": "基准工具",
    "benchmark_suite": "基准套件",
    "benchmark_framework": "基准框架",
    "security_baseline": "安全基线",
    "vulnerability_record": "漏洞记录",
    "reference_code": "参考实现",
    "tool_dependency": "工具依赖",
    "dependency_tool": "依赖工具",
    "research_prototype": "研究原型",
    "technical_appendix": "技术附录",
    "project_page": "项目页",
    "hardware_spec": "硬件规格",
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

PLACEHOLDER_LINK_MARKERS = (
    "n/a",
    "n/a ",
    "无公开链接",
    "暂无公开链接",
    "硬件依赖",
)

LOW_VALUE_REFERENCE_DESCRIPTORS = {
    "author_page",
    "github_organization",
    "conference_index",
    "background_reference",
    "cited_reference",
    "reference_book",
    "educational_resource",
    "community_discussion",
    "blog_analysis",
    "community_review",
    "venue_info",
    "background_context",
    "context_resource",
    "publication_index",
}

DIRECT_REFERENCE_DOMAINS = {
    "arxiv.org",
    "doi.org",
    "zenodo.org",
    "github.com",
    "openreview.net",
    "usenix.org",
    "proceedings.neurips.cc",
    "dl.acm.org",
    "aclanthology.org",
    "papers.nips.cc",
}

REFERENCE_TERM_STOPWORDS = {
    "paper",
    "towards",
    "using",
    "based",
    "study",
    "analysis",
    "system",
    "efficient",
    "exact",
    "fast",
    "memory",
    "research",
    "method",
    "methods",
}

REFERENCE_VALIDATION_LABELS = {
    "weak": "搜索补全",
}

RESOURCE_LIST_FIELDS = tuple(REFERENCE_SOURCE_LABELS.keys())


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

    if key == "section_title" and isinstance(value, str):
        value = clean_section_title(value) or value

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
        if primary_key == "section_title":
            primary_value = clean_section_title(primary_value) or primary_value
        consumed_keys.add(primary_key)
        if _looks_like_url(str(url or "")):
            header_text = f"**{_markdown_link(primary_value, str(url))}**"
            consumed_keys.add("url")
        else:
            header_text = f"**{primary_value}**"
    elif _looks_like_url(str(url or "")):
        header_text = f"**{_markdown_link('打开链接', str(url))}**"
        consumed_keys.add("url")
    else:
        header_text = "**条目**"

    if descriptors:
        header_text = f"{header_text}（{'；'.join(descriptors)}）"

    return header_text, consumed_keys


def _looks_like_url(text: str) -> bool:
    candidate = normalize_text(str(text or "")).strip()
    if not candidate or any(char.isspace() for char in candidate):
        return False
    lowered = candidate.lower()
    if any(lowered == marker or lowered.startswith(marker) for marker in PLACEHOLDER_LINK_MARKERS):
        return False
    parsed = urlparse(candidate)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


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


def _extract_descriptor_keys(payload: dict[str, Any]) -> list[str]:
    descriptors: list[str] = []
    for descriptor_key in ("type", "page_type", "repo_kind", "material_type", "role"):
        descriptor_value = payload.get(descriptor_key)
        if not _is_non_empty(descriptor_value):
            continue
        normalized_value = normalize_reference_descriptor(str(descriptor_value))
        if normalized_value and normalized_value not in descriptors:
            descriptors.append(normalized_value)
    return descriptors


def _normalize_match_text(text: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", normalize_text(text).lower())


def _collect_paper_terms(state: dict[str, Any]) -> set[str]:
    overview = state.get("overview") or {}
    candidates: list[str] = [
        str(overview.get("paper_title") or ""),
        str(state.get("source_name") or ""),
    ]
    for item in overview.get("key_terms") or []:
        candidates.append(str(item))

    terms: set[str] = set()
    for raw_text in candidates:
        normalized = normalize_text(raw_text)
        for token in re.findall(r"[A-Za-z][A-Za-z0-9_-]{2,}|[\u4e00-\u9fff]{2,}", normalized):
            compact = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", token.lower())
            if compact and compact not in REFERENCE_TERM_STOPWORDS:
                terms.add(compact)
    return terms


def _entry_matches_paper_terms(entry: dict[str, Any], paper_terms: set[str]) -> bool:
    if not paper_terms:
        return False
    haystack = _normalize_match_text(" ".join([str(entry.get("title") or ""), str(entry.get("url") or "")]))
    return any(term in haystack for term in paper_terms if len(term) >= 4)


def _looks_like_error_context(context: dict[str, Any]) -> bool:
    combined = normalize_text(
        " ".join(
            [
                str(context.get("html_title") or ""),
                str(context.get("og_title") or ""),
                str(context.get("meta_description") or ""),
                " ".join(str(item) for item in context.get("headings") or []),
                str(context.get("text_snippet") or ""),
            ]
        )
    ).lower()
    if not combined:
        return False
    return any(
        marker in combined
        for marker in (
            "404",
            "not found",
            "page not found",
            "access denied",
            "forbidden",
            "error",
        )
    )


def _build_reference_validation_map(state: dict[str, Any]) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}

    for context in state.get("url_resource_contexts") or []:
        if not isinstance(context, dict):
            continue
        url = str(context.get("url") or "").strip()
        if not url:
            continue
        result[url] = {
            "status": "failed" if _looks_like_error_context(context) else "validated",
            "final_url": str(context.get("final_url") or url).strip(),
        }

    enrichment = state.get("url_resource_enrichment") or {}
    for page in enrichment.get("pages") or []:
        if not isinstance(page, dict):
            continue
        url = str(page.get("url") or "").strip()
        if not url:
            continue
        result.setdefault(url, {"status": "validated", "final_url": url})

    for page in enrichment.get("search_fallback_pages") or []:
        if not isinstance(page, dict):
            continue
        url = str(page.get("url") or "").strip()
        if not url:
            continue
        if result.get(url, {}).get("status") != "validated":
            result[url] = {"status": "weak", "final_url": url}

    fetch_failures = (state.get("url_resource_enrichment_meta") or {}).get("fetch_failures") or []
    for failure in fetch_failures:
        if not isinstance(failure, dict):
            continue
        url = str(failure.get("url") or "").strip()
        if not url or result.get(url, {}).get("status") in {"validated", "weak"}:
            continue
        result[url] = {"status": "failed", "final_url": url}

    return result


def _url_path_parts(url: str) -> list[str]:
    return [part for part in urlparse(url).path.split("/") if part]


def _has_specific_resource_path(url: str) -> bool:
    path_parts = _url_path_parts(url)
    if len(path_parts) >= 2:
        return True
    if path_parts and "." in path_parts[-1]:
        return True
    return False


def _is_generic_portal_entry(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    path_parts = _url_path_parts(url)

    if host == "github.com" and len(path_parts) <= 1:
        return True
    if host == "huggingface.co" and len(path_parts) <= 1:
        return True
    if host in {"gluebenchmark.com", "mlperf.org"} and len(path_parts) <= 1:
        return True
    if host in {"docs.nvidia.com", "developer.huawei.com"} and len(path_parts) <= 1:
        return True
    if host == "redis.io" and path_parts[:1] == ["docs"] and len(path_parts) <= 1:
        return True
    return False


def _title_matches_url_identity(title: str, url: str) -> bool:
    lowered_url = url.lower()

    cve_matches = re.findall(r"\bCVE-\d{4}-\d{4,7}\b", title, flags=re.IGNORECASE)
    if cve_matches and not all(match.lower() in lowered_url for match in cve_matches):
        return False

    arxiv_matches = re.findall(r"\b\d{4}\.\d{4,5}(?:v\d+)?\b", title, flags=re.IGNORECASE)
    if arxiv_matches and not all(match.lower() in lowered_url for match in arxiv_matches):
        return False

    return True


def _has_direct_reference_signal(entry: dict[str, Any], paper_terms: set[str]) -> bool:
    descriptor_keys = set(str(item) for item in entry.get("descriptor_keys") or [])
    parsed = urlparse(str(entry.get("url") or ""))
    host = parsed.netloc.lower().removeprefix("www.")

    if descriptor_keys & {
        "github_repository",
        "artifact_page",
        "artifact_archive",
        "conference_page",
        "conference_presentation",
        "official_paper",
        "official_preprint",
        "publication",
        "source_code",
        "implementation",
        "implementation_source",
        "evaluation_script",
        "formal_proof",
        "config_guide",
        "training_script",
        "research_prototype",
        "technical_appendix",
        "vulnerability_record",
    }:
        return True

    if host in DIRECT_REFERENCE_DOMAINS:
        return True

    return _entry_matches_paper_terms(entry, paper_terms)


def _is_allowed_reference_entry(
    entry: dict[str, Any],
    paper_terms: set[str],
    validation_map: dict[str, dict[str, str]],
) -> bool:
    url = str(entry.get("url") or "").strip()
    title = normalize_text(str(entry.get("title") or "").strip())
    source_keys = set(str(item) for item in entry.get("source_keys") or [])
    descriptor_keys = set(str(item) for item in entry.get("descriptor_keys") or [])
    validation = validation_map.get(url, {})
    validation_status = str(validation.get("status") or "unverified")

    entry["validation_status"] = validation_status

    if not _looks_like_url(url):
        return False
    if validation_status not in {"validated", "weak"}:
        return False
    if not title or not _title_matches_url_identity(title, url):
        return False
    if "reading_notes" in source_keys:
        return False
    if descriptor_keys & LOW_VALUE_REFERENCE_DESCRIPTORS:
        return False
    if _is_generic_portal_entry(url) and not _entry_matches_paper_terms(entry, paper_terms):
        return False
    if "source_shortlist" in source_keys and not _has_direct_reference_signal(entry, paper_terms):
        return False
    if "datasets_and_benchmarks" in source_keys and not (_has_specific_resource_path(url) or _entry_matches_paper_terms(entry, paper_terms)):
        return False
    if "official_pages" in source_keys and not (_has_direct_reference_signal(entry, paper_terms) or _entry_matches_paper_terms(entry, paper_terms)):
        return False

    lowered_title = title.lower()
    if any(marker in lowered_title for marker in ("author page", "作者主页", "publication list", "publications")):
        return False

    return True


def _prettify_reference_descriptor(value: str) -> str:
    pretty = REFERENCE_DESCRIPTOR_VALUE_LABELS.get(value)
    if pretty:
        return pretty
    return value.replace("_", " ")


def _build_entry_notes(payload: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    for note_key in ("why_relevant", "insight", "reviewer_notes"):
        note_value = payload.get(note_key)
        if not _is_non_empty(note_value):
            continue
        label = NOTE_STYLE_LABELS.get(note_key, _label_key(note_key))
        _append_unique(notes, f"{label}：{_render_scalar(note_value)}")
    return notes


def _filter_resource_payload_for_display(payload: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    normalized_payload = normalize_resource_payload(payload)
    paper_terms = _collect_paper_terms(state)
    validation_map = _build_reference_validation_map(state)
    filtered_payload = dict(normalized_payload)

    for source_name in RESOURCE_LIST_FIELDS:
        items = filtered_payload.get(source_name)
        if not isinstance(items, list):
            continue

        filtered_items: list[Any] = []
        for item in items:
            if not isinstance(item, dict):
                filtered_items.append(item)
                continue

            url = str(item.get("url") or "").strip()
            if not url:
                filtered_items.append(item)
                continue

            entry = {
                "title": str(item.get("title") or item.get("name") or url).strip(),
                "url": url,
                "source_keys": [source_name],
                "descriptor_keys": _extract_descriptor_keys(item),
                "notes": _build_entry_notes(item),
            }
            if _looks_like_url(url) and _is_allowed_reference_entry(entry, paper_terms, validation_map):
                filtered_items.append(item)

        filtered_payload[source_name] = filtered_items

    return filtered_payload


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
    paper_terms = _collect_paper_terms(state)
    validation_map = _build_reference_validation_map(state)

    for payload_key in ("web_research", "resource_discovery"):
        payload = normalize_resource_payload(state.get(payload_key) or {})
        if not isinstance(payload, dict):
            continue
        for source_name in payload.keys():
            if source_name == "reading_notes":
                continue
            items = payload.get(source_name)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url") or "").strip()
                if not _looks_like_url(url):
                    continue

                entry = by_url.get(url)
                if entry is None:
                    title = str(item.get("title") or item.get("name") or url).strip()
                    bucket = _choose_reference_bucket(source_name, item, url)
                    entry = {
                        "title": title,
                        "url": url,
                        "bucket": bucket,
                        "source_keys": [],
                        "source_labels": [],
                        "descriptor_keys": [],
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

                _append_unique(entry["source_keys"], source_name)
                _append_unique(entry["source_labels"], REFERENCE_SOURCE_LABELS.get(source_name, _label_key(source_name)))

                for descriptor_value in _extract_descriptor_keys(item):
                    _append_unique(entry["descriptor_keys"], descriptor_value)
                    _append_unique(entry["descriptor_labels"], _prettify_reference_descriptor(descriptor_value))

                for note_key in ("why_relevant", "insight", "reviewer_notes"):
                    note_value = item.get(note_key)
                    if _is_non_empty(note_value):
                        label = NOTE_STYLE_LABELS.get(note_key, _label_key(note_key))
                        _append_unique(entry["notes"], f"{label}：{_render_scalar(note_value)}")

    filtered_groups: dict[str, list[dict[str, Any]]] = {}
    for bucket, entries in grouped.items():
        kept_entries = [
            entry
            for entry in entries
            if _is_allowed_reference_entry(entry, paper_terms, validation_map)
        ]
        if kept_entries:
            filtered_groups[bucket] = kept_entries

    return filtered_groups


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
            line = f"{index}. **{_markdown_link(str(entry['title']), str(entry['url']))}**"
            locator = _build_reference_locator(entry)
            if locator:
                line = f"{line}  *{locator}*"
            parts.append(line)
            summary = _build_reference_summary(entry)
            if summary:
                parts.append(f"   - 用途：{summary}")
        parts.append("")

    return parts


def _build_reference_locator(entry: dict[str, Any]) -> str:
    descriptor_values = [str(item).strip() for item in entry.get("descriptor_keys") or [] if str(item).strip()]
    best_by_family: dict[str, str] = {}
    for normalized_value in descriptor_values:
        family = REFERENCE_DESCRIPTOR_FAMILIES.get(normalized_value, normalized_value)
        previous = best_by_family.get(family)
        if previous is None or REFERENCE_DESCRIPTOR_PRIORITY.get(normalized_value, 99) < REFERENCE_DESCRIPTOR_PRIORITY.get(previous, 99):
            best_by_family[family] = normalized_value
    meta_bits = [
        _prettify_reference_descriptor(value)
        for value in sorted(best_by_family.values(), key=lambda item: (REFERENCE_DESCRIPTOR_PRIORITY.get(item, 99), item))
        if value
    ]
    validation_label = REFERENCE_VALIDATION_LABELS.get(str(entry.get("validation_status") or ""))
    if validation_label:
        meta_bits.append(validation_label)
    if meta_bits:
        return "；".join(meta_bits[:3])
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


def _clean_text(value: Any) -> str:
    return normalize_text(str(value or "").replace("\x00", "")).strip()


def _trim_to_boundary(text: str, max_chars: int, min_ratio: float = 0.65) -> str:
    if len(text) <= max_chars:
        return text

    lower_bound = max(20, int(max_chars * min_ratio))
    boundary_chars = "。！？；;，、,:：)]）】》」』> \n\t"
    for index in range(max_chars, lower_bound - 1, -1):
        if text[index - 1] in boundary_chars:
            candidate = text[:index].rstrip("，、,:： \n\t")
            if candidate:
                return candidate

    return text[:max_chars].rstrip()


def _strip_terminal_punctuation(text: str) -> str:
    return re.sub(r"[。！？；;：:,，、\s]+$", "", _clean_text(text))


def _ensure_terminal_punctuation(text: str) -> str:
    cleaned = _clean_text(text)
    if not cleaned:
        return ""
    if cleaned.endswith(("。", "！", "？", ".", "!", "?")):
        return cleaned
    return f"{cleaned}。"


def _join_sentences(items: list[str]) -> str:
    sentences = [
        _ensure_terminal_punctuation(_strip_terminal_punctuation(item))
        for item in items
        if _clean_text(item)
    ]
    return "".join(sentence for sentence in sentences if sentence)


def _join_clauses(items: list[str], joiner: str = "；") -> str:
    clauses = [_strip_terminal_punctuation(item) for item in items if _clean_text(item)]
    if not clauses:
        return ""
    return _ensure_terminal_punctuation(joiner.join(clauses))


def _split_sentences(text: str) -> list[str]:
    normalized = _clean_text(text)
    if not normalized:
        return []
    parts = re.findall(r".*?(?:[。！？!?；;]|$)", normalized)
    return [part.strip() for part in parts if part.strip()]


def _shorten_text(text: Any, max_chars: int = 220, sentence_limit: int = 2) -> str:
    normalized = _clean_text(text)
    if not normalized:
        return ""

    chosen: list[str] = []
    current_len = 0
    for sentence in _split_sentences(normalized):
        projected = current_len + len(sentence)
        if chosen and (len(chosen) >= sentence_limit or projected > max_chars):
            break
        chosen.append(sentence)
        current_len = projected

    if chosen:
        candidate = "".join(chosen)
        if len(candidate) <= max_chars:
            return candidate

    if len(normalized) <= max_chars:
        return normalized

    clipped = _trim_to_boundary(normalized, max_chars)
    return clipped + ("……" if not clipped.endswith(("。", "！", "？", ".", "!", "?")) else "")


def _clean_list_texts(items: Any, limit: int | None = None, max_chars: int = 160) -> list[str]:
    if not isinstance(items, list):
        return []
    results: list[str] = []
    for item in items:
        text = _shorten_text(item, max_chars=max_chars, sentence_limit=2)
        if not text:
            continue
        text = re.sub(r"^\d+[\.\)、]\s*", "", text).strip()
        if text in results:
            continue
        results.append(text)
        if limit is not None and len(results) >= limit:
            break
    return results


def _format_authors(authors: Any, limit: int = 4) -> str:
    names = _clean_list_texts(authors, limit=limit, max_chars=60)
    if not names:
        return ""
    if isinstance(authors, list) and len(authors) > len(names):
        return "、".join(names) + " 等"
    return "、".join(names)


def _inline_list(items: list[str], joiner: str = "；") -> str:
    cleaned = [item for item in items if item]
    return joiner.join(cleaned)


def _ordinal_label(index: int) -> str:
    labels = ["第一", "第二", "第三", "第四", "第五"]
    return labels[index] if 0 <= index < len(labels) else f"第{index + 1}"


def _inline_ordinal_points(items: list[str], limit: int = 3, max_chars: int = 110) -> str:
    points = _clean_list_texts(items, limit=limit, max_chars=max_chars)
    rendered = [f"{_ordinal_label(index)}，{_strip_terminal_punctuation(item)}" for index, item in enumerate(points)]
    return "；".join(rendered)


def _count_phrase(count: int) -> str:
    return {
        1: "一点",
        2: "两点",
        3: "三点",
        4: "四点",
        5: "五点",
    }.get(count, f"{count} 点")


def _clean_module_name(name: Any) -> str:
    normalized = _clean_text(name)
    if not normalized:
        return ""
    normalized = re.sub(r"\s*\(([A-Za-z0-9 _/,&+.-]+)\)\s*$", "", normalized).strip()
    return normalized


def _compact_module_names(modules: Any, limit: int = 4) -> list[str]:
    if not isinstance(modules, list):
        return []
    names: list[str] = []
    for module in modules:
        if not isinstance(module, dict):
            continue
        name = _clean_module_name(module.get("name"))
        if not name or name in names:
            continue
        names.append(name)
        if len(names) >= limit:
            break
    return names


def _compact_module_readout(module_names: list[str]) -> str:
    clues: list[str] = []
    if any("分块" in name for name in module_names):
        clues.append("分块负责把大矩阵切进 SRAM")
    if any("softmax" in name.lower() for name in module_names):
        clues.append("在线 Softmax 负责把逐块结果合成全局正确的归一化")
    if any("重计算" in name for name in module_names):
        clues.append("重计算负责把显存占用压到线性级")
    if any("融合" in name for name in module_names):
        clues.append("算子融合负责把这些思路真正兑现成速度收益")
    return _join_sentences(clues[:3])


def _compact_module_summary(modules: Any, limit: int = 4) -> str:
    if not isinstance(modules, list):
        return ""
    parts: list[str] = []
    for module in modules:
        if not isinstance(module, dict):
            continue
        name = _clean_module_name(module.get("name"))
        role = _shorten_text(module.get("role"), max_chars=48, sentence_limit=1)
        if not name and not role:
            continue
        if name and role:
            parts.append(f"{name}负责{_strip_terminal_punctuation(role)}")
        else:
            parts.append(name or role)
        if len(parts) >= limit:
            break
    return "；".join(parts)


def _compact_decision_summary(decisions: Any, limit: int = 3) -> list[str]:
    if not isinstance(decisions, list):
        return []
    rendered: list[str] = []
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        choice = _strip_terminal_punctuation(str(decision.get("choice") or ""))
        reason = _shorten_text(decision.get("reason"), max_chars=90, sentence_limit=1)
        tradeoff = _shorten_text(decision.get("tradeoff"), max_chars=90, sentence_limit=1)
        if not choice:
            continue
        sentence = choice
        if reason:
            sentence += f"，因为{_strip_terminal_punctuation(reason)}"
        if tradeoff:
            sentence += f"，代价是{_strip_terminal_punctuation(tradeoff)}"
        rendered.append(_ensure_terminal_punctuation(sentence))
        if len(rendered) >= limit:
            break
    return rendered


def _compact_design_choice_summary(design_choices: Any, limit: int = 2) -> str:
    if not isinstance(design_choices, list):
        return ""
    fragments: list[str] = []
    for item in design_choices:
        if not isinstance(item, dict):
            continue
        choice = _strip_terminal_punctuation(str(item.get("choice") or ""))
        why = _shorten_text(item.get("why"), max_chars=80, sentence_limit=1)
        risk = _shorten_text(item.get("risk"), max_chars=80, sentence_limit=1)
        if not choice:
            continue
        fragment = choice
        if why:
            fragment += f"，因为{_strip_terminal_punctuation(why)}"
        if risk:
            fragment += f"，但风险在于{_strip_terminal_punctuation(risk)}"
        fragments.append(fragment)
        if len(fragments) >= limit:
            break
    return "；".join(fragments)


def _compact_alternative_summary(alternatives: Any, limit: int = 1) -> str:
    if not isinstance(alternatives, list):
        return ""
    for item in alternatives:
        if not isinstance(item, dict):
            continue
        alternative = _strip_terminal_punctuation(str(item.get("alternative") or ""))
        why_not = _shorten_text(item.get("why_not_chosen"), max_chars=90, sentence_limit=1)
        if alternative and why_not:
            return f"可替代路线其实是{alternative}，但作者没有采用，因为{_strip_terminal_punctuation(why_not)}。"
    return ""


def _clean_pipeline_step(step: str) -> str:
    normalized = _clean_text(step)
    normalized = re.sub(r"^\d+[\.\)]\s*", "", normalized)
    normalized = re.sub(r"^[a-zA-Z][\.\)]\s*", "", normalized)
    return normalized


def _summarize_pipeline_group(steps: list[str]) -> str:
    merged = " ".join(step for step in steps if step).strip()
    if not merged:
        return ""

    lowered = merged.lower()
    if "初始化" in merged or "initialize" in lowered:
        return "先根据片上 SRAM 容量确定块大小，并初始化输出与归一化统计量"
    if "返回最终输出" in merged or "最终输出" in merged:
        return "汇总各块结果，得到前向输出，并保留后向阶段所需的统计量"
    if "外层循环" in merged or "遍历 K, V 的块" in merged or lowered.startswith("outer loop"):
        return "按 K/V 块把数据搬入 SRAM，为后续块内计算准备局部上下文"
    if any(keyword in merged for keyword in ("反向传播", "dQ", "dK", "dV", "梯度")) and not merged.startswith("返回最终输出"):
        return "反向阶段不读取完整注意力矩阵，而是依靠保存的统计量在 SRAM 中重算并求梯度"
    if any(keyword in merged for keyword in ("内层循环", "softmax", "局部注意力", "Sij", "Qi", "掩码", "缩放")):
        return "对每个 Q 块在 SRAM 中完成局部打分、掩码与在线 Softmax 合并，同时更新输出和统计量"
    return _strip_terminal_punctuation(_shorten_text(merged, max_chars=90, sentence_limit=1))


def _compact_pipeline_steps(steps: Any, limit: int = 5) -> list[str]:
    if not isinstance(steps, list):
        return []

    groups: list[list[str]] = []
    for raw_step in steps:
        raw_text = str(raw_step or "")
        cleaned = _clean_pipeline_step(raw_text)
        if not cleaned:
            continue

        is_substep = bool(re.match(r"^\s*[a-zA-Z][\.\)]\s*", raw_text))
        if is_substep and groups:
            groups[-1].append(cleaned)
            continue
        groups.append([cleaned])

    results: list[str] = []
    for group in groups:
        summary = _summarize_pipeline_group(group)
        if not summary:
            continue
        cleaned_summary = _strip_terminal_punctuation(summary)
        if cleaned_summary in results:
            continue
        results.append(cleaned_summary)
        if len(results) >= limit:
            break
    return results


def _clean_evidence_label(value: Any, max_chars: int = 44) -> str:
    raw = _clean_text(value)
    if not raw:
        return ""

    cleaned = clean_section_title(raw) or raw
    appendix_match = re.search(r"(Appendix\s+[A-Z](?:\.\d+)?)", cleaned, flags=re.IGNORECASE)
    if appendix_match:
        cleaned = appendix_match.group(1)

    cleaned = re.sub(r"\s*\([^)]*\)", "", cleaned).strip()
    return _shorten_text(cleaned, max_chars=max_chars, sentence_limit=1)


def _display_section_title(title: Any) -> str:
    raw = _clean_text(title)
    raw_lower = raw.lower()
    cleaned = clean_section_title(raw) or raw
    lowered = cleaned.lower()

    if "flashattention" in raw_lower and "implementation details" in raw_lower:
        return "FlashAttention 实现细节"
    if "flashattention" in raw_lower and "algorithm" in raw_lower:
        return "FlashAttention 核心算法"
    if "efficient attention algorithm" in raw_lower:
        return "高效注意力算法"
    if "hardware performance" in raw_lower:
        return "硬件性能背景"
    if "front matter" in lowered and "abstract" in lowered:
        return "摘要"
    if "introduction" in lowered:
        return "引言"
    if "standard attention implementation" in lowered:
        return "标准注意力实现"
    if "experiment" in lowered:
        return "实验"
    if "block-sparse" in lowered:
        return "块稀疏扩展"

    chinese_match = re.search(r"\(([^()]*[\u4e00-\u9fff][^()]*)\)", cleaned)
    if chinese_match:
        candidate = chinese_match.group(1).strip()
        if candidate:
            return candidate

    fragments = [
        fragment.strip()
        for fragment in re.split(r"\s+\+\s+|\s+\|\s+", cleaned)
        if fragment.strip()
    ]
    filtered_fragments = [
        fragment
        for fragment in fragments
        if not re.search(r"[=∈×→^$]|R\^{|N×N|QK\^T", fragment)
    ]
    if filtered_fragments:
        cleaned = filtered_fragments[0]

    return _shorten_text(cleaned, max_chars=36, sentence_limit=1) or "未命名章节"


def _render_question_prompt(items: Any, limit: int = 2, max_chars: int = 110) -> str:
    prompts = _clean_list_texts(items, limit=limit, max_chars=max_chars)
    if not prompts:
        return ""
    return _ensure_terminal_punctuation(
        f"继续追问最好围绕{_count_phrase(len(prompts))}展开：{_inline_ordinal_points(prompts, limit=limit, max_chars=max_chars)}"
    )


def _compact_claim_summary(claim_map: Any, limit: int = 3) -> list[str]:
    if not isinstance(claim_map, list):
        return []
    rendered: list[str] = []
    for item in claim_map:
        if not isinstance(item, dict):
            continue
        claim = _shorten_text(item.get("claim"), max_chars=120, sentence_limit=1)
        evidence = [_clean_evidence_label(section) for section in item.get("evidence_sections") or []]
        evidence = [section for section in evidence if section][:2]
        if not claim:
            continue
        if evidence:
            rendered.append(f"{_strip_terminal_punctuation(claim)}。优先回看 { '、'.join(evidence) }。")
        else:
            rendered.append(_ensure_terminal_punctuation(claim))
        if len(rendered) >= limit:
            break
    return rendered


def _top_reference_links(state: dict[str, Any], max_count: int = 3) -> list[str]:
    groups = _collect_reference_entries(state)
    links: list[str] = []
    for bucket in ("核心入口", "代码与实现", "数据与复现"):
        for entry in groups.get(bucket) or []:
            title = _clean_text(entry.get("title"))
            url = _clean_text(entry.get("url"))
            if not title or not _looks_like_url(url):
                continue
            links.append(_markdown_link(title, url))
            if len(links) >= max_count:
                return links
    return links


def _demote_markdown_headings(markdown_text: str, shift: int = 1) -> str:
    lines: list[str] = []
    for raw_line in markdown_text.splitlines():
        match = re.match(r"^(#{1,6})(\s+.*)$", raw_line)
        if not match:
            lines.append(raw_line)
            continue
        level = min(6, len(match.group(1)) + shift)
        lines.append("#" * level + match.group(2))
    return "\n".join(lines).strip()


def _render_overview_section(overview: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 1. 一页读懂这篇论文"]
    takeaway = _clean_text(overview.get("one_sentence_takeaway"))
    meta_bits = [
        f"作者：{_format_authors(overview.get('authors'))}" if _format_authors(overview.get("authors")) else "",
        f"来源：{_clean_text(overview.get('venue'))}" if _clean_text(overview.get("venue")) else "",
        f"年份：{_render_scalar(overview.get('publication_year'))}" if _is_non_empty(overview.get("publication_year")) else "",
        f"类型：{_clean_text(overview.get('paper_type'))}" if _clean_text(overview.get("paper_type")) else "",
    ]
    meta_line = " ｜ ".join(bit for bit in meta_bits if bit)
    if meta_line:
        parts.extend([f"> {meta_line}", ""])
    if takeaway:
        parts.extend([f"> {takeaway}", ""])

    problem = _shorten_text(overview.get("problem_statement"), max_chars=260, sentence_limit=2)
    importance = _shorten_text(overview.get("why_this_problem_matters"), max_chars=240, sentence_limit=2)
    if problem or importance:
        paragraph = []
        if problem:
            paragraph.append(f"这篇论文抓住的问题是：{_strip_terminal_punctuation(problem)}")
        if importance:
            paragraph.append(f"作者认为它重要，是因为{_strip_terminal_punctuation(importance)}")
        parts.extend([_join_sentences(paragraph), ""])

    prior_work = _shorten_text(overview.get("prior_work_positioning"), max_chars=240, sentence_limit=2)
    claim_summary = _inline_ordinal_points(overview.get("core_claims") or [], limit=3, max_chars=100)
    if prior_work or claim_summary:
        paragraph = []
        if prior_work:
            paragraph.append(f"和已有工作的关系上，作者的定位是：{_strip_terminal_punctuation(prior_work)}")
        if claim_summary:
            paragraph.append(f"如果只记住论文最核心的几个判断，可以压缩成：{claim_summary}")
        parts.extend([_join_sentences(paragraph), ""])

    read_order = _clean_list_texts(overview.get("read_order"), limit=3, max_chars=120)
    if read_order:
        parts.append("### 快读顺序")
        for index, item in enumerate(read_order, start=1):
            parts.append(f"{index}. {item}")
        parts.append("")

    questions = _clean_list_texts(overview.get("must_clarify_questions"), limit=3, max_chars=100)
    if questions:
        parts.append("### 读前先带着这几个问题")
        parts.extend([
            _ensure_terminal_punctuation(
                f"建议先带着{_count_phrase(len(questions))}进入正文：{_inline_ordinal_points(questions, limit=3, max_chars=100)}"
            ),
            "",
        ])

    return parts


def _render_structure_section(structure: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 2. 问题、方法与贡献主线"]

    problem = _shorten_text(structure.get("problem"), max_chars=260, sentence_limit=2)
    assumptions = _clean_list_texts(structure.get("assumptions"), limit=3, max_chars=90)
    if problem or assumptions:
        parts.append("### 论文真正卡住的问题")
        paragraph = []
        if problem:
            paragraph.append(f"作者要解决的核心困难是：{_strip_terminal_punctuation(problem)}")
        if assumptions:
            paragraph.append(f"整套方法成立依赖几个前提：{_inline_list([_strip_terminal_punctuation(item) for item in assumptions])}")
        parts.extend([_join_sentences(paragraph), ""])

    parts.append("### 方法主线")
    module_names = _compact_module_names(structure.get("method_modules"))
    module_summary = _compact_module_readout(module_names) or _compact_module_summary(structure.get("method_modules"), limit=1)
    pipeline = _compact_pipeline_steps(structure.get("core_pipeline"), limit=5)
    if module_names:
        parts.append(_ensure_terminal_punctuation(f"整条方法可以先看成几个咬合在一起的齿轮：{'、'.join(module_names)}"))
        parts.append("")
    if module_summary:
        parts.append(_ensure_terminal_punctuation(f"其中真正决定它为何既快又省显存的，是：{_strip_terminal_punctuation(module_summary)}"))
        parts.append("")
    if pipeline:
        parts.append("如果把论文的方法压成一条可执行流程，最值得记住的是：")
        for index, step in enumerate(pipeline, start=1):
            parts.append(f"{index}. {_clean_pipeline_step(step)}")
        parts.append("")

    decision_summary = _compact_decision_summary(structure.get("decision_points"))
    claim_summary = _compact_claim_summary(structure.get("claim_to_evidence_map"))
    if decision_summary:
        parts.append("### 作者做了哪些关键取舍")
        parts.extend([_join_sentences(decision_summary), ""])
    if claim_summary:
        parts.append("### 证据地图")
        parts.extend([_join_sentences(claim_summary), ""])

    return parts


def _render_external_context_section(state: dict[str, Any], web_research: dict[str, Any], resource_discovery: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 3. 外部视角补充"]
    if not (_has_meaningful_content(web_research) or _has_meaningful_content(resource_discovery)):
        parts.extend(["本次没有拿到足够稳定的外部补充信息，因此正文判断仍以论文本身为准。", ""])
        return parts

    reviewer_points = _clean_list_texts(web_research.get("reviewer_signals"), limit=2, max_chars=120)
    related_points = _clean_list_texts(web_research.get("related_work_signals"), limit=2, max_chars=120)
    if reviewer_points or related_points:
        paragraph = []
        if reviewer_points:
            paragraph.append(f"从社区与审稿视角看，这篇论文最被认可的地方主要有：{_inline_list([_strip_terminal_punctuation(item) for item in reviewer_points])}")
        if related_points:
            paragraph.append(f"把它放回文献脉络里，最重要的对照关系是：{_inline_list([_strip_terminal_punctuation(item) for item in related_points])}")
        parts.extend([_join_sentences(paragraph), ""])

    implementation_points = _clean_list_texts(resource_discovery.get("implementation_signals"), limit=2, max_chars=120)
    risk_points = _clean_list_texts(web_research.get("external_risks_or_confusions"), limit=2, max_chars=120)
    gap_points = _clean_list_texts(resource_discovery.get("missing_resource_gaps"), limit=2, max_chars=120)
    if implementation_points or risk_points or gap_points:
        paragraph = []
        if implementation_points:
            paragraph.append(f"如果把它当成工程对象，最有价值的实现线索是：{_inline_list([_strip_terminal_punctuation(item) for item in implementation_points])}")
        if risk_points:
            paragraph.append(f"外部资料里反复提醒的边界包括：{_inline_list([_strip_terminal_punctuation(item) for item in risk_points])}")
        if gap_points:
            paragraph.append(f"公开材料目前最大的空白则是：{_inline_list([_strip_terminal_punctuation(item) for item in gap_points])}")
        parts.extend([_join_sentences(paragraph), ""])

    top_links = _top_reference_links(state)
    if top_links:
        parts.extend([f"> 如果准备动手复现，建议先打开：{_inline_list(top_links)}", ""])

    return parts


def _render_deep_read_section(section_analyses: list[dict[str, Any]]) -> list[str]:
    parts: list[str] = ["## 4. 逐节带读"]
    for item in section_analyses:
        cleaned_title = _display_section_title(item.get("section_title"))
        parts.append(f"### {cleaned_title or '未命名章节'}")

        role = _shorten_text(item.get("section_role_in_paper"), max_chars=180, sentence_limit=2)
        author_view = _shorten_text(item.get("author_view"), max_chars=220, sentence_limit=2)
        if role or author_view:
            paragraph = []
            if role:
                paragraph.append(f"这一节在全文里的任务是：{_strip_terminal_punctuation(role)}")
            if author_view:
                paragraph.append(f"从作者叙事看，他真正想让你接受的是：{_strip_terminal_punctuation(author_view)}")
            parts.extend([_join_sentences(paragraph), ""])

        math_points = _clean_list_texts(item.get("math_or_algorithm"), limit=2, max_chars=100)
        design_summary = _compact_design_choice_summary(item.get("design_choices"), limit=1)
        alternative_summary = _compact_alternative_summary(item.get("alternatives"))
        if math_points or design_summary or alternative_summary:
            paragraph = []
            if math_points:
                paragraph.append(f"从机制上看，这一节最值得抓住的是：{_inline_list([_strip_terminal_punctuation(point) for point in math_points], joiner='、')}")
            if design_summary:
                paragraph.append(f"作者在这里的关键取舍是：{design_summary}")
            if alternative_summary:
                paragraph.append(_strip_terminal_punctuation(alternative_summary))
            parts.extend([_join_sentences(paragraph), ""])

        reviewer_view = _shorten_text(item.get("reviewer_view"), max_chars=180, sentence_limit=2)
        engineer_view = _shorten_text(item.get("engineer_view"), max_chars=200, sentence_limit=2)
        if reviewer_view or engineer_view:
            paragraph = []
            if reviewer_view:
                paragraph.append(f"站在审稿人角度，最该盯住的是：{_strip_terminal_punctuation(reviewer_view)}")
            if engineer_view:
                paragraph.append(f"如果你准备复现，这一节最实用的提醒是：{_strip_terminal_punctuation(engineer_view)}")
            parts.extend([_join_sentences(paragraph), ""])

        question_prompt = _render_question_prompt(item.get("verification_questions"), limit=2, max_chars=110)
        if question_prompt:
            parts.extend([question_prompt, ""])

    return parts


def _experiment_sentence(item: dict[str, Any]) -> str:
    name = _strip_terminal_punctuation(str(item.get("name") or ""))
    claim = _shorten_text(item.get("claim_tested"), max_chars=82, sentence_limit=1)
    note = _shorten_text(item.get("reviewer_notes"), max_chars=92, sentence_limit=1)
    pieces = [name] if name else []
    if claim:
        pieces.append(f"主要验证“{_strip_terminal_punctuation(claim)}”")
    if note:
        pieces.append(f"说服力在于{_strip_terminal_punctuation(note)}")
    return _ensure_terminal_punctuation("，".join(piece for piece in pieces if piece))


def _render_experiment_section(experiment_review: dict[str, Any]) -> list[str]:
    parts: list[str] = ["## 5. 实验到底支持了什么"]
    goal = _shorten_text(experiment_review.get("evaluation_goal"), max_chars=220, sentence_limit=2)
    support = _shorten_text(experiment_review.get("overall_support_for_claims"), max_chars=240, sentence_limit=2)
    if goal or support:
        paragraph = []
        if goal:
            paragraph.append(f"实验部分的目标是：{_strip_terminal_punctuation(goal)}")
        if support:
            paragraph.append(f"整体看下来，我对证据强度的判断是：{_strip_terminal_punctuation(support)}")
        parts.extend([_join_sentences(paragraph), ""])

    experiments = experiment_review.get("experiments") if isinstance(experiment_review.get("experiments"), list) else []
    strong = [item for item in experiments if str(item.get("evidence_strength") or "").lower() == "strong"]
    medium_or_weak = [item for item in experiments if str(item.get("evidence_strength") or "").lower() != "strong"]

    if strong:
        parts.append("### 证据最强的部分")
        parts.extend([_join_sentences([_experiment_sentence(item) for item in strong[:2]]), ""])

    if medium_or_weak:
        parts.append("### 仍然没被完全回答的问题")
        concerns: list[str] = []
        for item in medium_or_weak[:3]:
            sentence = _strip_terminal_punctuation(_experiment_sentence(item))
            biases = _clean_list_texts(item.get("possible_bias"), limit=1, max_chars=90)
            if biases:
                sentence += f"，但要留意{_strip_terminal_punctuation(biases[0])}"
            concerns.append(sentence)
        parts.extend([_join_sentences(concerns), ""])

    missing_ablations = _clean_list_texts(experiment_review.get("missing_ablations"), limit=3, max_chars=100)
    reproducibility_risks = _clean_list_texts(experiment_review.get("reproducibility_risks"), limit=3, max_chars=100)
    if missing_ablations or reproducibility_risks:
        parts.append("### 如果你要复现，先补这几件事")
        fixes: list[str] = []
        if missing_ablations:
            fixes.append(f"先补实验：{_inline_ordinal_points(missing_ablations, limit=2, max_chars=90)}")
        if reproducibility_risks:
            fixes.append(f"先防风险：{_inline_ordinal_points(reproducibility_risks[:2], limit=2, max_chars=90)}")
        parts.extend([_join_sentences(fixes), ""])

    return parts


def _strip_markdown_formatting(text: str) -> str:
    normalized = _clean_text(text)
    if not normalized:
        return ""
    normalized = re.sub(r"\[(.*?)\]\((.*?)\)", r"\1", normalized)
    normalized = normalized.replace("**", "").replace("__", "").replace("`", "")
    normalized = re.sub(r"(?<![\w\u4e00-\u9fff])\*(?=\S)(.+?)(?<=\S)\*(?![\w\u4e00-\u9fff])", r"\1", normalized)
    return normalize_text(normalized)


def _promote_standalone_bold_labels(markdown_text: str) -> str:
    promoted_lines: list[str] = []
    for raw_line in markdown_text.splitlines():
        stripped = raw_line.strip()
        match = re.match(r"^\*\*([^*]+)\*\*\s*[：:]?$", stripped)
        if match:
            label = _strip_terminal_punctuation(_strip_markdown_formatting(match.group(1)))
            if label:
                promoted_lines.append(f"### {label}")
                continue
        promoted_lines.append(raw_line)
    return "\n".join(promoted_lines)


def _clean_narrative_heading(title: str) -> str:
    cleaned = _strip_markdown_formatting(title)
    cleaned = re.sub(r"^\d+\.\s*", "", cleaned).strip()
    return cleaned.strip("：: ") or "未命名小节"


def _split_markdown_sections(markdown_text: str) -> list[tuple[int, str, list[str]]]:
    text = _promote_standalone_bold_labels(markdown_text)
    sections: list[tuple[int, str, list[str]]] = []
    prelude: list[str] = []
    current_level: int | None = None
    current_title = ""
    current_lines: list[str] = []

    for raw_line in text.splitlines():
        match = re.match(r"^(#{1,6})\s+(.*)$", raw_line.strip())
        if match:
            if current_level is None:
                if any(line.strip() for line in prelude):
                    sections.append((0, "", prelude))
            else:
                sections.append((current_level, _clean_narrative_heading(current_title), current_lines))
            current_level = len(match.group(1))
            current_title = match.group(2).strip()
            current_lines = []
            continue

        if current_level is None:
            prelude.append(raw_line)
        else:
            current_lines.append(raw_line)

    if current_level is None:
        if any(line.strip() for line in prelude):
            sections.append((0, "", prelude))
    else:
        sections.append((current_level, _clean_narrative_heading(current_title), current_lines))

    return sections


def _split_title_and_inline_detail(text: str) -> tuple[str, str]:
    cleaned = _strip_markdown_formatting(text).strip()
    if not cleaned:
        return "", ""
    match = re.match(r"^(.{1,40}?)[：:]\s*(.*)$", cleaned)
    if not match:
        return cleaned, ""
    return match.group(1).strip(), match.group(2).strip()


def _parse_markdown_label_value(text: str) -> tuple[str, str]:
    cleaned = _strip_markdown_formatting(text).strip()
    if not cleaned:
        return "", ""
    match = re.match(r"^([\u4e00-\u9fffA-Za-z0-9 _/\-()]+?)\s*[：:]\s*(.+)$", cleaned)
    if not match:
        return "", cleaned
    return match.group(1).strip(), match.group(2).strip()


def _collapse_plain_markdown_paragraphs(lines: list[str]) -> list[str]:
    paragraphs: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        if not buffer:
            return
        text = _strip_markdown_formatting(" ".join(buffer))
        if text:
            paragraphs.append(_ensure_terminal_punctuation(_strip_terminal_punctuation(text)))
        buffer.clear()

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped == "---":
            flush()
            continue
        if re.match(r"^\s*(?:[-*]|\d+\.)\s+", raw_line):
            flush()
            continue
        buffer.append(stripped)

    flush()
    return paragraphs


def _parse_numbered_markdown_items(lines: list[str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None

    def flush() -> None:
        nonlocal current
        if current is None:
            return
        if current.get("title") or current.get("details"):
            items.append(current)
        current = None

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped == "---":
            continue

        number_match = re.match(r"^\s*(\d+)\.\s+(.*)$", raw_line)
        if number_match:
            flush()
            title_text, inline_detail = _split_title_and_inline_detail(number_match.group(2))
            current = {
                "title": title_text,
                "details": [inline_detail] if inline_detail else [],
            }
            continue

        if current is None:
            continue

        bullet_match = re.match(r"^\s*[-*]\s+(.*)$", raw_line)
        detail_text = _strip_markdown_formatting((bullet_match.group(1) if bullet_match else raw_line).strip())
        if not detail_text:
            continue
        if bullet_match:
            current["details"].append(detail_text)
            continue
        if current["details"]:
            current["details"][-1] = f"{current['details'][-1]} {detail_text}".strip()
        else:
            current["details"].append(detail_text)

    flush()
    return items


def _detail_sentence(label: str, content: str, section_title: str) -> str:
    text = _strip_terminal_punctuation(_shorten_text(content, max_chars=260, sentence_limit=3))
    if not text:
        return ""

    normalized_label = label.strip().lower()
    title_text = _clean_text(section_title)
    experiment_context = any(keyword in title_text for keyword in ("实验", "测定", "分析", "对比", "验证"))
    if label == "关键数据":
        text = re.sub(r"^(记录|需记录|需要记录)\s*", "", text).strip()

    mapping = {
        "缺陷": "缺口在于",
        "证据": "支撑这一判断的线索是",
        "风险": "更需要警惕的是",
        "技巧": "表面上的写法是",
        "实质": "更实质地看",
        "目的": "这项验证首先要回答的是" if experiment_context else "这一步首先要回答的是",
        "方法": "做法上可以",
        "预期验证": "理想情况下，希望确认",
        "潜在风险": "但也要提前想到",
        "关键数据": "最关键要记录的是",
        "动机": "之所以值得推进，是因为",
        "方案": "一个可执行的方案是",
        "挑战": "真正的难点在于",
        "场景": "适合落地的场景是",
        "可行性": "从可行性看",
        "验证点": "真正需要补的验证是",
    }
    prefix = mapping.get(label, mapping.get(normalized_label))
    if prefix:
        return _ensure_terminal_punctuation(f"{prefix}{text}")
    return _ensure_terminal_punctuation(text)


def _render_numbered_item_paragraphs(section_title: str, items: list[dict[str, Any]]) -> list[str]:
    paragraphs: list[str] = []
    for index, item in enumerate(items):
        title = _strip_terminal_punctuation(_clean_text(item.get("title")))
        details = item.get("details") or []
        sentences: list[str] = []
        if title:
            sentences.append(_ensure_terminal_punctuation(f"{_ordinal_label(index)}，{title}"))
        for detail in details:
            label, content = _parse_markdown_label_value(str(detail))
            sentence = _detail_sentence(label, content, section_title)
            if sentence:
                sentences.append(sentence)
        paragraph = "".join(sentence for sentence in sentences if sentence)
        if paragraph:
            paragraphs.append(paragraph)
    return paragraphs


def _render_labeled_detail_paragraph(lines: list[str], section_title: str) -> str:
    known_labels = {
        "缺陷",
        "证据",
        "风险",
        "技巧",
        "实质",
        "目的",
        "方法",
        "预期验证",
        "潜在风险",
        "关键数据",
        "动机",
        "方案",
        "挑战",
        "场景",
        "可行性",
        "验证点",
    }
    groups: list[tuple[str, str]] = []
    current_label = ""
    saw_bullet = False

    def append_group(label: str, content: str) -> None:
        cleaned_content = _strip_terminal_punctuation(_strip_markdown_formatting(content))
        if not cleaned_content:
            return
        if groups and groups[-1][0] == label:
            groups[-1] = (label, f"{groups[-1][1]}；{cleaned_content}")
            return
        groups.append((label, cleaned_content))

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped == "---":
            continue
        bullet_match = re.match(r"^\s*[-*]\s+(.*)$", raw_line)
        if not bullet_match:
            continue

        saw_bullet = True
        content = bullet_match.group(1)
        cleaned = _strip_markdown_formatting(content)
        if not cleaned:
            continue

        label, text = _parse_markdown_label_value(cleaned)
        if label in known_labels and text:
            current_label = label
            append_group(label, text)
            continue

        title_text, inline_detail = _split_title_and_inline_detail(cleaned)
        if title_text in known_labels and not inline_detail:
            current_label = title_text
            continue

        if current_label:
            append_group(current_label, cleaned)
            continue

        if label and text:
            append_group("", cleaned)
        else:
            append_group("", cleaned)

    if not saw_bullet:
        return ""

    sentences = [_detail_sentence(label, content, section_title) for label, content in groups]
    return "".join(sentence for sentence in sentences if sentence)


def _rewrite_markdown_body_as_narrative(body: str) -> str:
    lines_out: list[str] = []
    for level, heading, lines in _split_markdown_sections(body):
        if level > 0:
            heading_level = min(6, max(3, level + 1))
            lines_out.extend([f"{'#' * heading_level} {heading}", ""])

        plain_paragraphs = _collapse_plain_markdown_paragraphs(lines)
        numbered_items = _parse_numbered_markdown_items(lines)
        labeled_paragraph = ""
        if not numbered_items:
            labeled_paragraph = _render_labeled_detail_paragraph(lines, heading)

        for paragraph in plain_paragraphs:
            lines_out.extend([paragraph, ""])

        if numbered_items:
            for paragraph in _render_numbered_item_paragraphs(heading, numbered_items):
                lines_out.extend([paragraph, ""])
        elif labeled_paragraph:
            lines_out.extend([labeled_paragraph, ""])

    return "\n".join(lines_out).strip()


def _render_markdown_section(title: str, body: str, intro: str | None = None, narrative: bool = False) -> list[str]:
    parts = [title]
    if intro:
        parts.extend([intro, ""])
    if narrative:
        rewritten = _rewrite_markdown_body_as_narrative(body.strip())
        if rewritten:
            parts.extend([rewritten, ""])
    else:
        demoted = _demote_markdown_headings(body.strip(), shift=1)
        if demoted:
            parts.extend([demoted, ""])
    return parts


def render_report(state: dict[str, Any]) -> str:
    overview = state["overview"]
    web_research = _filter_resource_payload_for_display(state.get("web_research") or {}, state)
    resource_discovery = _filter_resource_payload_for_display(state.get("resource_discovery") or {}, state)
    structure = state["structure"]
    section_analyses = state["section_analyses"]
    experiment_review = state["experiment_review"]
    critique = state["critique"]
    extensions = state["extensions"]
    section_targets = state["section_targets"]
    normalized_state = dict(state)
    normalized_state["web_research"] = web_research
    normalized_state["resource_discovery"] = resource_discovery

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

    parts.extend(_render_overview_section(overview))
    parts.append("")
    parts.extend(_render_structure_section(structure))
    parts.append("")

    external_section = _render_external_context_section(normalized_state, web_research, resource_discovery)
    if external_section:
        parts.extend(external_section)
        parts.append("")

    parts.extend(_render_deep_read_section(section_analyses))
    parts.append("")
    parts.extend(_render_experiment_section(experiment_review))
    parts.append("")
    parts.extend(
        _render_markdown_section(
            "## 6. 批判性评审",
            critique,
            intro="这一节保留更接近审稿短评的写法，重点不是复述摘要，而是暴露这篇论文最容易被忽略的边界与代价。",
            narrative=True,
        )
    )
    parts.extend(
        _render_markdown_section(
            "## 7. 如果继续做这条线",
            extensions,
            intro="下面不把它当作“论文总结”，而是当作研究备忘录：哪些验证最值得先补，哪些方向更像下一篇论文。",
            narrative=True,
        )
    )
    parts.extend([""])
    parts.extend(_render_reference_appendix(normalized_state, 8))

    return "\n".join(parts).strip() + "\n"
