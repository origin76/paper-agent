from __future__ import annotations

import re
from typing import Any

from paper_agent.utils import estimate_tokens, normalize_text, trim_balanced_text

SECTION_KEYWORDS = (
    "abstract",
    "introduction",
    "background",
    "preliminaries",
    "related work",
    "design",
    "implementation",
    "method",
    "methods",
    "approach",
    "framework",
    "algorithm",
    "training",
    "setup",
    "experiment",
    "evaluation",
    "result",
    "analysis",
    "discussion",
    "future work",
    "limitation",
    "conclusion",
    "appendix",
)


def _canonicalize_heading(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip())


def _looks_like_title_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped or len(stripped) > 80:
        return False
    if "." in stripped:
        return False

    lowered = stripped.lower()
    if any(lowered.startswith(keyword) for keyword in SECTION_KEYWORDS):
        return True

    words = [word for word in re.split(r"\s+", stripped) if word]
    alpha_words = [word for word in words if any(char.isalpha() for char in word)]
    if not alpha_words or len(alpha_words) > 8:
        return False
    if any(len(word) > 25 for word in alpha_words):
        return False

    capitalized_words = sum(1 for word in alpha_words if word[0].isupper())
    return capitalized_words >= max(1, len(alpha_words) - 1)


def _looks_like_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if len(stripped) > 120:
        return False
    numbered_match = re.match(r"^(\d+(\.\d+)*)\s+(.+)$", stripped)
    if numbered_match:
        return _looks_like_title_heading(numbered_match.group(3))
    lowered = stripped.lower()
    if any(lowered.startswith(keyword) for keyword in SECTION_KEYWORDS):
        return True
    if stripped.isupper() and 2 <= len(stripped.split()) <= 10:
        return True
    return False


def _is_numeric_heading_marker(line: str) -> bool:
    return bool(re.match(r"^\d+(\.\d+)*$", line.strip()))


def _build_fallback_chunks(text: str, target_chars: int, max_sections: int) -> list[dict[str, Any]]:
    paragraphs = [paragraph.strip() for paragraph in text.split("\n\n") if paragraph.strip()]
    chunks: list[dict[str, Any]] = []
    current_parts: list[str] = []

    for paragraph in paragraphs:
        projected = len("\n\n".join(current_parts + [paragraph]))
        if current_parts and projected > target_chars:
            content = "\n\n".join(current_parts).strip()
            chunks.append(
                {
                    "title": f"Chunk {len(chunks) + 1}",
                    "content": content,
                    "char_count": len(content),
                    "estimated_tokens": estimate_tokens(content),
                }
            )
            current_parts = [paragraph]
        else:
            current_parts.append(paragraph)

    if current_parts:
        content = "\n\n".join(current_parts).strip()
        chunks.append(
            {
                "title": f"Chunk {len(chunks) + 1}",
                "content": content,
                "char_count": len(content),
                "estimated_tokens": estimate_tokens(content),
            }
        )

    return chunks[:max_sections]


def _merge_short_sections(sections: list[dict[str, Any]], min_chars: int = 500) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    pending: dict[str, Any] | None = None

    for section in sections:
        if pending is None:
            pending = dict(section)
            continue

        if pending["char_count"] < min_chars:
            pending["title"] = f'{pending["title"]} + {section["title"]}'
            pending["content"] = f'{pending["content"]}\n\n{section["content"]}'.strip()
            pending["char_count"] = len(pending["content"])
            pending["estimated_tokens"] = estimate_tokens(pending["content"])
            pending["end_line"] = section.get("end_line")
        else:
            merged.append(pending)
            pending = dict(section)

    if pending is not None:
        merged.append(pending)

    return merged


def _prioritize_sections(sections: list[dict[str, Any]], max_sections: int) -> list[dict[str, Any]]:
    if len(sections) <= max_sections:
        return sections

    chosen_indexes: list[int] = []
    keyword_indexes = [
        index
        for index, section in enumerate(sections)
        if any(
            keyword in section["title"].lower()
            for keyword in ("abstract", "introduction", "method", "experiment", "evaluation", "result", "discussion", "conclusion")
        )
    ]

    for index in [0, 1, *keyword_indexes]:
        if 0 <= index < len(sections) and index not in chosen_indexes:
            chosen_indexes.append(index)
        if len(chosen_indexes) >= max_sections:
            break

    if len(chosen_indexes) < max_sections:
        for index in range(len(sections)):
            if index not in chosen_indexes:
                chosen_indexes.append(index)
            if len(chosen_indexes) >= max_sections:
                break

    return [sections[index] for index in sorted(chosen_indexes)]


def detect_sections(text: str, max_sections: int, target_chars: int) -> list[dict[str, Any]]:
    normalized = normalize_text(text)
    lines = normalized.splitlines()
    sections: list[dict[str, Any]] = []
    current_title = "Front Matter"
    current_lines: list[str] = []
    content_start_line = 1

    def flush(end_line: int) -> None:
        content = "\n".join(current_lines).strip()
        if not content:
            return
        trimmed = trim_balanced_text(content, target_chars)
        sections.append(
            {
                "title": current_title,
                "content": trimmed,
                "start_line": content_start_line,
                "end_line": end_line,
                "char_count": len(trimmed),
                "estimated_tokens": estimate_tokens(trimmed),
            }
        )

    line_number = 1
    index = 0
    while index < len(lines):
        line = lines[index]
        if _looks_like_heading(line):
            flush(line_number - 1)
            current_title = _canonicalize_heading(line)
            current_lines = []
            content_start_line = line_number + 1
            index += 1
            line_number += 1
            continue
        if _is_numeric_heading_marker(line):
            lookahead_index = index + 1
            lookahead_line_number = line_number + 1
            while lookahead_index < len(lines) and not lines[lookahead_index].strip() and lookahead_index <= index + 2:
                lookahead_index += 1
                lookahead_line_number += 1
            if lookahead_index < len(lines) and _looks_like_title_heading(lines[lookahead_index]):
                combined_heading = f"{line.strip()} {lines[lookahead_index].strip()}"
                flush(line_number - 1)
                current_title = _canonicalize_heading(combined_heading)
                current_lines = []
                content_start_line = lookahead_line_number + 1
                index = lookahead_index + 1
                line_number = lookahead_line_number + 1
                continue
        current_lines.append(line)
        index += 1
        line_number += 1

    flush(len(lines))

    filtered_sections = [section for section in sections if section["content"].strip()]
    merged_sections = _merge_short_sections(filtered_sections)

    if len(merged_sections) < 3:
        return _build_fallback_chunks(normalized, target_chars=target_chars, max_sections=max_sections)

    return _prioritize_sections(merged_sections, max_sections=max_sections)


def select_experiment_sections(sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected = [
        section
        for section in sections
        if any(
            keyword in section["title"].lower()
            for keyword in ("experiment", "evaluation", "result", "analysis", "discussion")
        )
    ]
    return selected or sections[-2:]
