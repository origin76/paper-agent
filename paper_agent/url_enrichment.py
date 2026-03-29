from __future__ import annotations

import re
from html import unescape
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from paper_agent.utils import normalize_text


GENERIC_TITLE_MARKERS = (
    "论文内提到的官方页面",
    "论文内提到的代码仓库",
    "论文内提到的 github 仓库",
    "论文内提到的 artifact / doi 页面",
    "官方页面",
    "代码仓库",
    "打开链接",
    "条目",
)

GENERIC_NOTE_MARKERS = (
    "该页面直接出现在论文正文中",
    "该仓库链接直接出现在论文正文中",
    "代码仓库链接由论文正文直接给出",
    "该链接直接来自论文正文",
    "一手来源",
    "优先级高于二手网页检索结果",
    "适合作为核验会议页面",
    "适合作为复现入口",
    "可信度高，适合优先作为复现入口",
)

RESOURCE_LIST_FIELDS = (
    "source_shortlist",
    "code_resources",
    "reading_notes",
    "official_pages",
    "code_repositories",
    "datasets_and_benchmarks",
    "reproducibility_materials",
)

RESOURCE_NOTE_FIELDS = {
    "source_shortlist": ("why_relevant", "insight", "reviewer_notes"),
    "code_resources": ("why_relevant", "insight"),
    "reading_notes": ("insight", "why_relevant", "reviewer_notes"),
    "official_pages": ("why_relevant",),
    "code_repositories": ("why_relevant",),
    "datasets_and_benchmarks": ("why_relevant",),
    "reproducibility_materials": ("why_relevant",),
}

RESOURCE_DESCRIPTOR_FIELDS = ("type", "page_type", "repo_kind", "material_type", "role")

REFERENCE_DESCRIPTOR_NORMALIZATION = {
    "github_repo": "github_repository",
    "github_repo_page": "github_repository",
    "github_org": "github_organization",
    "github_org_page": "github_organization",
    "conference_presentation_page": "conference_presentation",
    "conference_talk": "conference_presentation",
    "artifact_archive": "artifact_page",
    "artifact_doi": "artifact_page",
    "doi_page": "artifact_page",
    "会议页面": "conference_page",
    "会议总页": "conference_index",
    "会议演讲页": "conference_presentation",
    "技术参考": "technical_reference",
    "文档页": "documentation",
    "github_仓库": "github_repository",
    "github_组织": "github_organization",
    "artifact_页面": "artifact_page",
    "artifact_归档": "artifact_page",
    "依赖工具": "dependency",
    "背景参考": "background_reference",
}


class _HTMLSnippetParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self._capture_tag: str | None = None
        self.title_parts: list[str] = []
        self.heading_parts: list[str] = []
        self.text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if lowered == "title":
            self._capture_tag = "title"
        elif lowered in {"h1", "h2"}:
            self._capture_tag = lowered

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"} and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if self._capture_tag == lowered or (lowered == "title" and self._capture_tag == "title"):
            self._capture_tag = None

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = normalize_text(unescape(data))
        if not text:
            return
        if self._capture_tag == "title":
            self.title_parts.append(text)
        elif self._capture_tag in {"h1", "h2"}:
            self.heading_parts.append(text)
        self.text_parts.append(text)


def collect_resource_url_candidates(
    web_research: dict[str, Any] | None,
    resource_discovery: dict[str, Any] | None,
    limit: int,
) -> list[dict[str, Any]]:
    by_url: dict[str, dict[str, Any]] = {}
    for payload_key, payload in (
        ("web_research", normalize_resource_payload(web_research or {})),
        ("resource_discovery", normalize_resource_payload(resource_discovery or {})),
    ):
        if not isinstance(payload, dict):
            continue
        for field_name in RESOURCE_LIST_FIELDS:
            items = payload.get(field_name)
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                url = str(item.get("url") or "").strip()
                if not url or not _looks_like_web_url(url):
                    continue
                candidate = by_url.setdefault(
                    url,
                    {
                        "url": url,
                        "existing_titles": [],
                        "locations": [],
                    },
                )
                title = str(item.get("title") or item.get("name") or "").strip()
                if title and title not in candidate["existing_titles"]:
                    candidate["existing_titles"].append(title)
                location = f"{payload_key}.{field_name}"
                if location not in candidate["locations"]:
                    candidate["locations"].append(location)

    return list(by_url.values())[:limit]


def _looks_like_web_url(value: str) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized.startswith("http://") or normalized.startswith("https://")


def fetch_url_context(url: str, timeout_seconds: int = 12, max_bytes: int = 600_000, max_text_chars: int = 6000) -> dict[str, Any]:
    request = Request(
        url,
        headers={
            "User-Agent": "paper-agent/0.1 (+https://dashscope.aliyuncs.com/compatible-mode/v1)",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        },
    )

    with urlopen(request, timeout=timeout_seconds) as response:
        final_url = response.geturl()
        content_type = response.headers.get("Content-Type", "")
        raw_bytes = response.read(max_bytes)

    charset = _extract_charset(content_type, raw_bytes[:4096])
    html_text = raw_bytes.decode(charset, errors="replace")

    parser = _HTMLSnippetParser()
    parser.feed(html_text)

    final_url = final_url or url
    context = {
        "url": url,
        "final_url": final_url,
        "domain": urlparse(final_url).netloc.lower().removeprefix("www."),
        "content_type": content_type,
        "html_title": _extract_tag_text(html_text, "title") or _first_non_empty(parser.title_parts),
        "og_title": _extract_meta_content(html_text, "og:title"),
        "meta_description": _extract_meta_content(html_text, "description")
        or _extract_meta_content(html_text, "og:description"),
        "headings": _unique_non_empty(parser.heading_parts)[:4],
        "text_snippet": normalize_text(" ".join(parser.text_parts))[:max_text_chars],
        "fallback_title": infer_title_from_url(final_url),
    }
    context.update(_infer_domain_specific_identity(final_url, context))
    return context


def infer_title_from_url(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    path_parts = [part for part in parsed.path.split("/") if part]

    if "github.com" in host and len(path_parts) >= 2:
        owner = path_parts[0]
        repo = path_parts[1].removesuffix(".git")
        return f"{owner}/{repo} GitHub 仓库"
    if "github.com" in host and len(path_parts) == 1:
        return f"{path_parts[0]} GitHub 组织主页"
    if "doi.org" in host or "zenodo" in host:
        record_hint = _extract_zenodo_record_hint(urlparse(url))
        return f"Zenodo Artifact 页面（{record_hint}）" if record_hint else "Zenodo Artifact 页面"
    if "usenix.org" in host:
        conference_name = _format_usenix_conference_name(path_parts)
        if "presentation" in parsed.path:
            return f"USENIX {conference_name} 论文演讲页" if conference_name else "USENIX 论文演讲页"
        if conference_name:
            return f"USENIX {conference_name} 会议页面"
        return "USENIX 论文页面"
    if host:
        if path_parts:
            return f"{host} / {path_parts[-1]}"
        return host
    return url


def is_generic_title(title: str) -> bool:
    normalized = title.strip().lower()
    if not normalized:
        return True
    return any(marker in normalized for marker in GENERIC_TITLE_MARKERS)


def is_generic_note(text: str) -> bool:
    normalized = normalize_text(text).lower()
    if not normalized:
        return True
    return any(marker in normalized for marker in GENERIC_NOTE_MARKERS)


def build_enrichment_contexts_for_prompt(
    candidates: list[dict[str, Any]],
    fetched_contexts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_url = {item["url"]: item for item in candidates}
    prompt_contexts: list[dict[str, Any]] = []
    for context in fetched_contexts:
        url = str(context.get("url") or "").strip()
        candidate = by_url.get(url, {})
        prompt_contexts.append(
            {
                "url": url,
                "existing_titles": candidate.get("existing_titles", []),
                "locations": candidate.get("locations", []),
                "final_url": context.get("final_url"),
                "domain": context.get("domain"),
                "fallback_title": context.get("fallback_title"),
                "html_title": context.get("html_title"),
                "og_title": context.get("og_title"),
                "meta_description": context.get("meta_description"),
                "headings": context.get("headings", []),
                "text_snippet": context.get("text_snippet"),
                "rule_based_title": context.get("rule_based_title"),
                "rule_based_page_kind": context.get("rule_based_page_kind"),
                "rule_based_summary": context.get("rule_based_summary"),
            }
        )
    return prompt_contexts


def build_failed_page_contexts_for_prompt(
    candidates: list[dict[str, Any]],
    fetch_failures: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_url = {str(item.get("url") or ""): item for item in candidates}
    prompt_contexts: list[dict[str, Any]] = []
    for failure in fetch_failures:
        url = str(failure.get("url") or "").strip()
        if not url:
            continue
        candidate = by_url.get(url, {})
        rule_based = _infer_domain_specific_identity(url, {"url": url})
        prompt_contexts.append(
            {
                "url": url,
                "existing_titles": candidate.get("existing_titles", []),
                "locations": candidate.get("locations", []),
                "fetch_error": failure.get("error"),
                "fallback_title": infer_title_from_url(url),
                "rule_based_title": rule_based.get("rule_based_title"),
                "rule_based_page_kind": rule_based.get("rule_based_page_kind"),
                "rule_based_summary": rule_based.get("rule_based_summary"),
            }
        )
    return prompt_contexts


def build_analysis_map(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for key in ("pages", "search_fallback_pages"):
        pages = (payload or {}).get(key) or []
        if not isinstance(pages, list):
            continue
        for item in pages:
            if not isinstance(item, dict):
                continue
            url = str(item.get("url") or "").strip()
            if not url:
                continue
            result[url] = item
    return result


def normalize_reference_descriptor(value: str) -> str:
    normalized = re.sub(r"[\s-]+", "_", str(value or "").strip().lower())
    return REFERENCE_DESCRIPTOR_NORMALIZATION.get(normalized, normalized)


def normalize_resource_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}

    normalized = dict(payload)
    for field_name in RESOURCE_LIST_FIELDS:
        items = normalized.get(field_name)
        if not isinstance(items, list):
            continue
        normalized[field_name] = [
            _normalize_resource_item(item, field_name) if isinstance(item, dict) else item
            for item in items
        ]
    return normalized


def _normalize_resource_item(item: dict[str, Any], field_name: str) -> dict[str, Any]:
    updated = dict(item)

    for key, value in list(updated.items()):
        if isinstance(value, str):
            updated[key] = normalize_text(value)
        elif key in {"tags", "notes"} and isinstance(value, list):
            updated[key] = [normalize_text(str(entry)) for entry in value if str(entry).strip()]

    for descriptor_key in RESOURCE_DESCRIPTOR_FIELDS:
        descriptor_value = updated.get(descriptor_key)
        if isinstance(descriptor_value, str) and descriptor_value.strip():
            updated[descriptor_key] = normalize_reference_descriptor(descriptor_value)

    stray_fragments: list[str] = []
    for key in list(updated.keys()):
        value = updated[key]
        if not _looks_like_split_text_fragment(key, value):
            continue
        stray_fragments.append(normalize_text(str(key)))
        updated.pop(key, None)

    if stray_fragments:
        note_key = _select_note_field(updated, field_name)
        merged_note = str(updated.get(note_key) or "").strip()
        for fragment in stray_fragments:
            merged_note = _merge_note_fragments(merged_note, fragment)
        if merged_note:
            updated[note_key] = merged_note

    return updated


def _looks_like_split_text_fragment(key: Any, value: Any) -> bool:
    if not isinstance(key, str):
        return False
    if value not in (None, "", [], {}):
        return False

    stripped = key.strip()
    if not stripped:
        return False
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,48}", stripped):
        return False
    if key != stripped:
        return True
    if any("\u4e00" <= ch <= "\u9fff" for ch in stripped):
        return True
    return any(ch in stripped for ch in "“”‘’（）()，。；：！？、《》【】")


def _select_note_field(item: dict[str, Any], field_name: str) -> str:
    candidates = RESOURCE_NOTE_FIELDS.get(field_name, ("why_relevant",))
    for key in candidates:
        value = str(item.get(key) or "").strip()
        if value:
            return key
    return candidates[0]


def _merge_note_fragments(base: str, fragment: str) -> str:
    normalized_base = normalize_text(base).replace("\n", " ").strip()
    normalized_fragment = normalize_text(fragment).replace("\n", " ").strip()
    if not normalized_base:
        return normalized_fragment
    if not normalized_fragment:
        return normalized_base

    if (
        normalized_base.count("“") > normalized_base.count("”")
        and "”" in normalized_fragment
        and not normalized_fragment.startswith(("“", "”"))
    ):
        return f"{normalized_base.rstrip()}”、“{normalized_fragment.lstrip()}"

    if normalized_fragment[0] in "，。；：！？、）】》」』’”.,;:!?)]}>":
        return f"{normalized_base.rstrip()}{normalized_fragment}"

    if _needs_ascii_spacing(normalized_base, normalized_fragment):
        return f"{normalized_base.rstrip()} {normalized_fragment.lstrip()}"

    return f"{normalized_base.rstrip()}{normalized_fragment.lstrip()}"


def _needs_ascii_spacing(base: str, fragment: str) -> bool:
    return bool(base and fragment and base[-1].isascii() and fragment[0].isascii() and base[-1].isalnum() and fragment[0].isalnum())


def apply_resource_url_enrichment(
    web_research: dict[str, Any] | None,
    resource_discovery: dict[str, Any] | None,
    fetched_contexts: list[dict[str, Any]],
    analyzed_pages: dict[str, dict[str, Any]] | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    normalized_web_research = normalize_resource_payload(web_research or {})
    normalized_resource_discovery = normalize_resource_payload(resource_discovery or {})
    context_map = {
        str(item.get("url") or ""): item
        for item in fetched_contexts
        if isinstance(item, dict) and str(item.get("url") or "").strip()
    }
    analyzed_pages = analyzed_pages or {}

    return (
        _apply_payload_enrichment(normalized_web_research, context_map, analyzed_pages),
        _apply_payload_enrichment(normalized_resource_discovery, context_map, analyzed_pages),
    )


def _apply_payload_enrichment(
    payload: dict[str, Any],
    context_map: dict[str, dict[str, Any]],
    analyzed_pages: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    enriched = dict(payload)
    for field_name in RESOURCE_LIST_FIELDS:
        items = enriched.get(field_name)
        if not isinstance(items, list):
            continue
        enriched[field_name] = [
            _apply_item_enrichment(item, field_name, context_map, analyzed_pages)
            if isinstance(item, dict)
            else item
            for item in items
        ]
    return enriched


def _apply_item_enrichment(
    item: dict[str, Any],
    field_name: str,
    context_map: dict[str, dict[str, Any]],
    analyzed_pages: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    updated = dict(item)
    url = str(updated.get("url") or "").strip()
    if not url:
        return updated

    context = context_map.get(url, {})
    analysis = analyzed_pages.get(url, {})

    better_title = _pick_best_title(
        url=url,
        existing_title=str(updated.get("title") or "").strip(),
        context=context,
        analysis=analysis,
    )
    if better_title:
        updated["title"] = better_title

    page_kind = str(analysis.get("page_kind") or "").strip()
    if field_name == "source_shortlist" and page_kind and str(updated.get("type") or "").startswith("paper_embedded"):
        updated["type"] = page_kind
    if field_name == "official_pages" and page_kind and str(updated.get("page_type") or "").startswith("paper_embedded"):
        updated["page_type"] = page_kind
    if field_name == "code_repositories" and page_kind and str(updated.get("repo_kind") or "").startswith("paper_embedded"):
        updated["repo_kind"] = page_kind
    if field_name == "reproducibility_materials" and page_kind and str(updated.get("material_type") or "").startswith("paper_embedded"):
        updated["material_type"] = page_kind

    better_summary = str(analysis.get("summary") or "").strip()
    if not better_summary:
        better_summary = str(context.get("rule_based_summary") or "").strip()
    if better_summary:
        if field_name in {"code_resources", "official_pages", "code_repositories", "reproducibility_materials"}:
            if is_generic_note(str(updated.get("why_relevant") or "")):
                updated["why_relevant"] = better_summary
        elif field_name == "reading_notes":
            if is_generic_note(str(updated.get("insight") or "")):
                updated["insight"] = better_summary

    return updated


def _pick_best_title(
    url: str,
    existing_title: str,
    context: dict[str, Any],
    analysis: dict[str, Any],
) -> str:
    analyzed_title = str(analysis.get("clean_title") or "").strip()
    if analyzed_title:
        return analyzed_title

    if existing_title and not is_generic_title(existing_title):
        return existing_title

    for candidate in (
        str(context.get("rule_based_title") or "").strip(),
        str(context.get("og_title") or "").strip(),
        str(context.get("html_title") or "").strip(),
        _first_non_empty(context.get("headings") or []),
        str(context.get("fallback_title") or "").strip(),
        infer_title_from_url(url),
    ):
        if candidate:
            return candidate

    return existing_title or infer_title_from_url(url)


def _extract_charset(content_type: str, prefix_bytes: bytes) -> str:
    charset_match = re.search(r"charset=([A-Za-z0-9._-]+)", content_type or "", flags=re.IGNORECASE)
    if charset_match:
        return charset_match.group(1)
    meta_match = re.search(br"charset=['\"]?([A-Za-z0-9._-]+)", prefix_bytes, flags=re.IGNORECASE)
    if meta_match:
        try:
            return meta_match.group(1).decode("ascii", errors="ignore") or "utf-8"
        except Exception:
            return "utf-8"
    return "utf-8"


def _extract_tag_text(html_text: str, tag_name: str) -> str:
    match = re.search(
        rf"<{tag_name}\b[^>]*>(.*?)</{tag_name}>",
        html_text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    text = re.sub(r"<[^>]+>", " ", match.group(1))
    return normalize_text(unescape(text))


def _extract_meta_content(html_text: str, meta_key: str) -> str:
    patterns = [
        rf'<meta[^>]+property=["\']{re.escape(meta_key)}["\'][^>]+content=["\'](.*?)["\']',
        rf'<meta[^>]+name=["\']{re.escape(meta_key)}["\'][^>]+content=["\'](.*?)["\']',
        rf'<meta[^>]+content=["\'](.*?)["\'][^>]+property=["\']{re.escape(meta_key)}["\']',
        rf'<meta[^>]+content=["\'](.*?)["\'][^>]+name=["\']{re.escape(meta_key)}["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return normalize_text(unescape(match.group(1)))
    return ""


def _first_non_empty(items: list[str]) -> str:
    for item in items:
        normalized = str(item).strip()
        if normalized:
            return normalized
    return ""


def _unique_non_empty(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def _infer_domain_specific_identity(url: str, context: dict[str, Any]) -> dict[str, str]:
    parsed = urlparse(url)
    host = parsed.netloc.lower().removeprefix("www.")
    path_parts = [part for part in parsed.path.split("/") if part]

    if "github.com" in host:
        return _infer_github_identity(path_parts, context)
    if "doi.org" in host or "zenodo.org" in host:
        return _infer_zenodo_identity(url, context)
    if "usenix.org" in host:
        return _infer_usenix_identity(path_parts, context)
    if "rust-lang.github.io" in host and "unsafe-codeguidelines" in parsed.path:
        return {
            "rule_based_title": "Rust Unsafe Code Guidelines 术语页",
            "rule_based_page_kind": "technical_reference",
            "rule_based_summary": "这是 Rust 不安全代码指南中的术语页，可用于核对别名、初始化与未定义行为等形式化定义。",
        }
    return {}


def _infer_github_identity(path_parts: list[str], context: dict[str, Any]) -> dict[str, str]:
    og_title = str(context.get("og_title") or "").strip()
    meta_description = str(context.get("meta_description") or "").strip()

    if len(path_parts) >= 2:
        owner = path_parts[0]
        repo = path_parts[1].removesuffix(".git")
        description = _extract_github_repo_description(og_title, meta_description)
        summary = f"这是 {owner}/{repo} 的 GitHub 仓库，可直接查看 README、源码、issue 和发布记录。"
        if description:
            summary = f"这是 {owner}/{repo} 的 GitHub 仓库，主要内容是：{description}。"
        return {
            "rule_based_title": f"{owner}/{repo} GitHub 仓库",
            "rule_based_page_kind": "github_repository",
            "rule_based_summary": summary,
        }

    if len(path_parts) == 1:
        owner = path_parts[0]
        org_name = _first_non_empty([str(context.get("og_title") or "").strip(), owner])
        summary = f"这是 {owner} 的 GitHub 组织主页，可继续进入其公开仓库、fork 和相关子项目。"
        if meta_description:
            summary = f"这是 {owner} 的 GitHub 组织主页，简介为：{normalize_text(meta_description).removesuffix('.')}。"
        return {
            "rule_based_title": f"{org_name} GitHub 组织主页",
            "rule_based_page_kind": "github_organization",
            "rule_based_summary": summary,
        }

    return {}


def _infer_zenodo_identity(url: str, context: dict[str, Any]) -> dict[str, str]:
    title_source = _first_non_empty(
        [
            str(context.get("og_title") or "").strip(),
            str(context.get("html_title") or "").strip(),
            str(context.get("fallback_title") or "").strip(),
        ]
    )
    clean_title = re.sub(r"\s+", " ", title_source.replace("OSDI'25", "OSDI '25")).strip()
    clean_title = re.sub(r"\s*[|·-]\s*Zenodo.*$", "", clean_title, flags=re.IGNORECASE).strip()
    clean_title = re.sub(r"^Zenodo\s*[|:-]\s*", "", clean_title, flags=re.IGNORECASE).strip()
    if not clean_title:
        record_hint = _extract_zenodo_record_hint(urlparse(url))
        clean_title = f"Zenodo Artifact 页面（{record_hint}）" if record_hint else "Zenodo Artifact 页面"
    summary = "这是 Zenodo 上的 artifact / DOI 页面，通常包含复现材料、README、归档文件和引用信息。"
    meta_description = str(context.get("meta_description") or "").strip()
    if meta_description:
        summary = normalize_text(meta_description).removesuffix(".") + "。"
    return {
        "rule_based_title": clean_title,
        "rule_based_page_kind": "artifact_page",
        "rule_based_summary": summary,
    }


def _infer_usenix_identity(path_parts: list[str], context: dict[str, Any]) -> dict[str, str]:
    joined_path = "/".join(path_parts)
    conference_name = _format_usenix_conference_name(path_parts)
    content_title = _extract_usenix_content_title(context)
    if "presentation" in joined_path:
        title = "USENIX 论文演讲页面"
        if conference_name and content_title:
            title = f"USENIX {conference_name} 演讲页：{content_title}"
        elif conference_name:
            title = f"USENIX {conference_name} 论文演讲页"
        return {
            "rule_based_title": title,
            "rule_based_page_kind": "conference_presentation",
            "rule_based_summary": "这是 USENIX 的论文演讲页面，通常可继续跳转到论文 PDF、摘要、作者信息和会场材料。",
        }
    if path_parts[:2] == ["conference", "osdi25"]:
        return {
            "rule_based_title": f"USENIX {conference_name} 会务总页" if conference_name else "USENIX 会务总页",
            "rule_based_page_kind": "conference_index",
            "rule_based_summary": "这是 USENIX OSDI '25 的会务总页，可用于确认会议议程、收录论文和相关导航入口。",
        }
    return {
        "rule_based_title": f"USENIX {conference_name} 会议页面" if conference_name else "USENIX 会议页面",
        "rule_based_page_kind": "conference_page",
        "rule_based_summary": "这是 USENIX 站点上的会议相关页面，可用于继续定位论文主页、会程安排或演讲材料。",
    }


def _format_usenix_conference_name(path_parts: list[str]) -> str:
    for part in path_parts:
        match = re.fullmatch(r"([a-z]+)(\d{2})", part.lower())
        if not match:
            continue
        return f"{match.group(1).upper()} '{match.group(2)}"
    return ""


def _extract_usenix_content_title(context: dict[str, Any]) -> str:
    title_source = _first_non_empty(
        [
            str(context.get("og_title") or "").strip(),
            str(context.get("html_title") or "").strip(),
            _first_non_empty(context.get("headings") or []),
        ]
    )
    if not title_source:
        return ""
    clean_title = re.sub(r"\s*[|:-]\s*USENIX.*$", "", title_source, flags=re.IGNORECASE).strip()
    clean_title = re.sub(r"^Presentation[:：]\s*", "", clean_title, flags=re.IGNORECASE).strip()
    clean_title = re.sub(r"^\s*(OSDI|ATC|NSDI)\s*'?\d{2}\s*[:-]\s*", "", clean_title, flags=re.IGNORECASE).strip()
    return clean_title


def _extract_zenodo_record_hint(parsed) -> str:
    path_parts = [part for part in parsed.path.split("/") if part]
    for index, part in enumerate(path_parts):
        if part in {"record", "records"} and index + 1 < len(path_parts):
            return path_parts[index + 1]
    for part in reversed(path_parts):
        if any(char.isdigit() for char in part):
            return part
    return ""


def _extract_github_repo_description(og_title: str, meta_description: str) -> str:
    if meta_description:
        match = re.match(r"(.+?)\s*-\s*[^-]+/[^-]+$", meta_description)
        if match:
            return normalize_text(match.group(1))
        return normalize_text(meta_description)

    if ": " in og_title:
        return normalize_text(og_title.split(": ", 1)[1].replace(" · GitHub", ""))
    return ""
