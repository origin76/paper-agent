from __future__ import annotations

import re
from typing import Any

from paper_agent.utils import normalize_text

from .text_utils import (
    clean_text,
    ensure_terminal_punctuation,
    ordinal_label,
    shorten_text,
    strip_terminal_punctuation,
)


def demote_markdown_headings(markdown_text: str, shift: int = 1) -> str:
    lines: list[str] = []
    for raw_line in markdown_text.splitlines():
        match = re.match(r"^(#{1,6})(\s+.*)$", raw_line)
        if not match:
            lines.append(raw_line)
            continue
        level = min(6, len(match.group(1)) + shift)
        lines.append("#" * level + match.group(2))
    return "\n".join(lines).strip()


def strip_markdown_formatting(text: str) -> str:
    normalized = clean_text(text)
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
            label = strip_terminal_punctuation(strip_markdown_formatting(match.group(1)))
            if label:
                promoted_lines.append(f"### {label}")
                continue
        promoted_lines.append(raw_line)
    return "\n".join(promoted_lines)


def _clean_narrative_heading(title: str) -> str:
    cleaned = strip_markdown_formatting(title)
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
    cleaned = strip_markdown_formatting(text).strip()
    if not cleaned:
        return "", ""
    match = re.match(r"^(.{1,40}?)[：:]\s*(.*)$", cleaned)
    if not match:
        return cleaned, ""
    return match.group(1).strip(), match.group(2).strip()


def _parse_markdown_label_value(text: str) -> tuple[str, str]:
    cleaned = strip_markdown_formatting(text).strip()
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
        text = strip_markdown_formatting(" ".join(buffer))
        if text:
            paragraphs.append(ensure_terminal_punctuation(strip_terminal_punctuation(text)))
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
        detail_text = strip_markdown_formatting((bullet_match.group(1) if bullet_match else raw_line).strip())
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
    text = strip_terminal_punctuation(shorten_text(content, max_chars=260, sentence_limit=3))
    if not text:
        return ""

    normalized_label = label.strip().lower()
    title_text = clean_text(section_title)
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
        return ensure_terminal_punctuation(f"{prefix}{text}")
    return ensure_terminal_punctuation(text)


def _render_numbered_item_paragraphs(section_title: str, items: list[dict[str, Any]]) -> list[str]:
    paragraphs: list[str] = []
    for index, item in enumerate(items):
        title = strip_terminal_punctuation(clean_text(item.get("title")))
        details = item.get("details") or []
        sentences: list[str] = []
        if title:
            sentences.append(ensure_terminal_punctuation(f"{ordinal_label(index)}，{title}"))
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
        cleaned_content = strip_terminal_punctuation(strip_markdown_formatting(content))
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
        cleaned = strip_markdown_formatting(content)
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

        append_group("", cleaned)

    if not saw_bullet:
        return ""

    sentences = [_detail_sentence(label, content, section_title) for label, content in groups]
    return "".join(sentence for sentence in sentences if sentence)


def rewrite_markdown_body_as_narrative(body: str) -> str:
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


def render_markdown_section(title: str, body: str, intro: str | None = None, narrative: bool = False) -> list[str]:
    parts = [title]
    if intro:
        parts.extend([intro, ""])
    if narrative:
        rewritten = rewrite_markdown_body_as_narrative(body.strip())
        if rewritten:
            parts.extend([rewritten, ""])
    else:
        demoted = demote_markdown_headings(body.strip(), shift=1)
        if demoted:
            parts.extend([demoted, ""])
    return parts
