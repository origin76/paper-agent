from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from paper_agent.analysis.url_enrichment import normalize_reference_descriptor, normalize_resource_payload
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


def has_meaningful_content(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return False
    for value in payload.values():
        if _is_non_empty(value):
            return True
    return False


def filter_resource_payload_for_display(payload: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
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


def render_reference_appendix(state: dict[str, Any], section_index: int) -> list[str]:
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


def top_reference_links(state: dict[str, Any], max_count: int = 3) -> list[str]:
    groups = _collect_reference_entries(state)
    links: list[str] = []
    for bucket in ("核心入口", "代码与实现", "数据与复现"):
        for entry in groups.get(bucket) or []:
            title = str(entry.get("title") or "").strip()
            url = str(entry.get("url") or "").strip()
            if not title or not _looks_like_url(url):
                continue
            links.append(_markdown_link(title, url))
            if len(links) >= max_count:
                return links
    return links


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


def _append_unique(values: list[str], candidate: str | None) -> None:
    if not candidate:
        return
    normalized = candidate.strip()
    if not normalized or normalized in values:
        return
    values.append(normalized)


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
    if "datasets_and_benchmarks" in source_keys and not (
        _has_specific_resource_path(url) or _entry_matches_paper_terms(entry, paper_terms)
    ):
        return False
    if "official_pages" in source_keys and not (
        _has_direct_reference_signal(entry, paper_terms) or _entry_matches_paper_terms(entry, paper_terms)
    ):
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
        kept_entries = [entry for entry in entries if _is_allowed_reference_entry(entry, paper_terms, validation_map)]
        if kept_entries:
            filtered_groups[bucket] = kept_entries

    return filtered_groups


def _build_reference_locator(entry: dict[str, Any]) -> str:
    descriptor_values = [str(item).strip() for item in entry.get("descriptor_keys") or [] if str(item).strip()]
    best_by_family: dict[str, str] = {}
    for normalized_value in descriptor_values:
        family = REFERENCE_DESCRIPTOR_FAMILIES.get(normalized_value, normalized_value)
        previous = best_by_family.get(family)
        if previous is None or REFERENCE_DESCRIPTOR_PRIORITY.get(normalized_value, 99) < REFERENCE_DESCRIPTOR_PRIORITY.get(
            previous,
            99,
        ):
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


def _looks_like_url(text: str) -> bool:
    candidate = normalize_text(str(text or "")).strip()
    if not candidate or any(char.isspace() for char in candidate):
        return False
    lowered = candidate.lower()
    if any(lowered == marker or lowered.startswith(marker) for marker in PLACEHOLDER_LINK_MARKERS):
        return False
    parsed = urlparse(candidate)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
