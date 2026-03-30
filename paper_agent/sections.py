from __future__ import annotations

import re
from typing import Any

from paper_agent.utils import estimate_tokens, normalize_text, trim_balanced_text

SECTION_KEYWORDS = (
    "abstract",
    "introduction",
    "overview",
    "motivation",
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
    "results",
    "evaluation",
    "result",
    "analysis",
    "discussion",
    "future work",
    "limitation",
    "limitations",
    "conclusion",
    "appendix",
)

NON_CONTENT_HEADINGS = {
    "references",
    "reference",
    "acknowledgements",
    "acknowledgments",
    "acknowledgement",
    "acknowledgment",
    "keywords",
    "ccs concepts",
    "acm reference format",
}

HEADING_NOISE_PREFIXES = (
    "doi:",
    "acm isbn",
    "figure ",
    "table ",
    "copyright",
    "publication rights",
    "december ",
    "june ",
)

HEADING_NOISE_SUBSTRINGS = (
    "communications of the acm",
    "vol.",
    "licensed to acm",
    "http://",
    "https://",
    "@",
)

KEYWORD_CANONICAL_TITLES = {
    "abstract": "Abstract",
    "introduction": "Introduction",
    "background": "Background",
    "preliminaries": "Preliminaries",
    "related work": "Related Work",
    "method": "Method",
    "methods": "Methods",
    "approach": "Approach",
    "design": "Design",
    "implementation": "Implementation",
    "evaluation": "Evaluation",
    "experiment": "Experiments",
    "result": "Results",
    "analysis": "Analysis",
    "discussion": "Discussion",
    "future work": "Future Work",
    "limitation": "Limitations",
    "conclusion": "Conclusion",
    "appendix": "Appendix",
}


def _canonicalize_heading(line: str) -> str:
    normalized = re.sub(r"\s+", " ", line.strip()).strip(" -|,;:.")
    while normalized.endswith(")") and normalized.count("(") < normalized.count(")"):
        normalized = normalized[:-1].rstrip()
    while normalized.endswith("]") and normalized.count("[") < normalized.count("]"):
        normalized = normalized[:-1].rstrip()
    return normalized


def _strip_leading_heading_number(text: str) -> str:
    return re.sub(r"^\d+(\.\d+)*\s+", "", text.strip())


def _contains_cjk(text: str) -> bool:
    return any("\u4e00" <= char <= "\u9fff" for char in text)


def _heading_word_count(text: str) -> int:
    return len([token for token in re.split(r"\s+", text.strip()) if token])


def _is_obvious_noise_heading(line: str) -> bool:
    stripped = _canonicalize_heading(line)
    if not stripped:
        return True

    lowered = stripped.lower()
    alpha_words = [word for word in re.split(r"\s+", stripped) if any(char.isalpha() for char in word)]
    if lowered in NON_CONTENT_HEADINGS:
        return True
    if any(lowered.startswith(prefix) for prefix in HEADING_NOISE_PREFIXES):
        return True
    if any(marker in lowered for marker in HEADING_NOISE_SUBSTRINGS):
        return True
    if re.search(r"\b(fig(?:ure)?|table)\s*\d+[:.]?", lowered):
        return True
    if re.fullmatch(r"[\W_0-9]+", stripped):
        return True
    if re.search(r"\b(copyright|licensed|proceedings of|journal of|conference on)\b", lowered) and _heading_word_count(stripped) > 8:
        return True
    if re.search(r"[=<>×→∈∀∃⊢⊣¬∧∨≈≠±÷∑∏]", stripped) and not any(keyword in lowered for keyword in SECTION_KEYWORDS):
        return True
    if (stripped.count("(") + stripped.count(")") >= 2 or "/" in stripped) and not any(keyword in lowered for keyword in SECTION_KEYWORDS):
        return True
    if alpha_words and all(len(re.sub(r"[^A-Za-z]+", "", word)) <= 2 for word in alpha_words) and lowered not in SECTION_KEYWORDS:
        return True
    if len(alpha_words) <= 1 and re.search(r"[^A-Za-z0-9\s]", stripped):
        return True
    if "," in stripped and (_heading_word_count(stripped) > 5 or re.search(r",\s*(and|or|but)$", lowered)):
        return True
    if stripped.endswith((".", ";")):
        return True
    if _heading_word_count(stripped) > 16:
        return True
    if len(re.findall(r"[@:/]", stripped)) >= 2:
        return True
    if stripped.isupper() and len(re.sub(r"[^A-Z]", "", stripped)) >= 12 and "abstract" not in lowered and "introduction" not in lowered:
        return True
    if stripped[:1].islower() and _heading_word_count(stripped) > 3:
        return True
    return False


def _heading_quality_score(line: str) -> int:
    stripped = _canonicalize_heading(line)
    if not stripped or _is_obvious_noise_heading(stripped):
        return -10

    lowered = stripped.lower()
    score = 0
    if re.match(r"^\d+(\.\d+)*\s+", stripped):
        score += 5
    if stripped.isupper():
        score += 2
    if stripped[:1].isupper() or _contains_cjk(stripped):
        score += 2

    for keyword in SECTION_KEYWORDS:
        if lowered == keyword:
            score += 5
            break
        if lowered.startswith(f"{keyword} "):
            score += 3
            break

    word_count = _heading_word_count(stripped)
    if 1 <= word_count <= 8:
        score += 3
    elif word_count <= 12:
        score += 1
    else:
        score -= 2

    if "+" in stripped:
        score -= 1
    if any(char in stripped for char in "=<>@"):
        score -= 3
    return score


def _canonical_title_for_keyword(keyword: str) -> str | None:
    lowered = _strip_leading_heading_number(keyword.strip().lower())
    for key, title in KEYWORD_CANONICAL_TITLES.items():
        if lowered == key:
            return title
    return None


def _leading_section_keyword(text: str) -> str:
    lowered = _strip_leading_heading_number(_canonicalize_heading(text)).lower()
    for keyword in sorted(SECTION_KEYWORDS, key=len, reverse=True):
        if lowered == keyword or lowered.startswith(f"{keyword} "):
            return keyword
    return ""


def _choose_best_heading_fragment(fragments: list[str]) -> str:
    if not fragments:
        return ""
    if len(fragments) == 1:
        return fragments[0]

    left = fragments[0]
    right = fragments[1]
    if left.lower().startswith("front matter") and right.lower().startswith("abstract"):
        return "Front Matter + Abstract"

    left_keyword = _leading_section_keyword(left)
    right_keyword = _leading_section_keyword(right)
    if left_keyword and right_keyword and left_keyword == right_keyword:
        return right if _heading_word_count(right) >= _heading_word_count(left) else left

    return max(
        fragments,
        key=lambda fragment: (
            _heading_quality_score(fragment),
            -min(abs(_heading_word_count(fragment) - 3), 10),
            -len(fragment),
        ),
    )


def clean_section_title(title: str, content: str = "") -> str:
    cleaned = _canonicalize_heading(title)
    if not cleaned:
        return ""

    fragments = [_canonicalize_heading(fragment) for fragment in re.split(r"\s+\+\s+|\s+\|\s+", cleaned) if fragment.strip()]
    good_fragments = [
        fragment
        for fragment in fragments
        if _heading_quality_score(fragment) >= 2
    ]
    if good_fragments:
        cleaned = _choose_best_heading_fragment(good_fragments)

    cleaned = _canonicalize_heading(_strip_leading_heading_number(cleaned))
    if cleaned and cleaned[:1].islower() and not _contains_cjk(cleaned) and _heading_word_count(cleaned) <= 6:
        cleaned = cleaned[:1].upper() + cleaned[1:]

    keyword_title = _canonical_title_for_keyword(cleaned)
    if keyword_title and _looks_like_reference_heavy_content(content):
        return ""
    if keyword_title:
        return keyword_title if keyword_title != "Abstract" or not cleaned.lower().startswith("front matter") else "Front Matter + Abstract"

    if _is_obvious_noise_heading(cleaned):
        derived = _derive_section_title_from_content(content, fallback_title=cleaned)
        return derived

    return cleaned


def _derive_section_title_from_content(content: str, fallback_title: str = "") -> str:
    normalized = normalize_text(content or "")
    if not normalized:
        return ""

    preview = normalized[:2000]
    lowered_preview = preview.lower()

    if "abstract" in lowered_preview[:400]:
        return "Front Matter + Abstract" if "front matter" in fallback_title.lower() else "Abstract"
    if re.search(r"\bintroduction\b", lowered_preview[:600]):
        return "Introduction"
    if re.search(r"\bbackground\b", lowered_preview[:800]):
        return "Background"
    if re.search(r"\brelated work\b", lowered_preview[:1200]):
        return "Related Work"
    if re.search(r"\bevaluation\b", lowered_preview[:1200]):
        return "Evaluation"
    if re.search(r"\bexperiment", lowered_preview[:1200]):
        return "Experiments"
    if re.search(r"\bdiscussion\b", lowered_preview[:1200]):
        return "Discussion"
    if re.search(r"\bconclusion\b", lowered_preview[:1200]):
        return "Conclusion"

    for raw_line in normalized.splitlines()[:40]:
        candidate = _canonicalize_heading(raw_line)
        if _heading_quality_score(candidate) >= 5:
            cleaned_candidate = clean_section_title(candidate)
            if cleaned_candidate:
                return cleaned_candidate

    if fallback_title.lower().startswith("front matter"):
        return "Front Matter"
    if fallback_title and not _is_obvious_noise_heading(fallback_title):
        return fallback_title
    return ""


def resolve_section_title(title: str, content: str, index: int | None = None) -> str:
    cleaned = clean_section_title(title, content)
    if cleaned and not _is_obvious_noise_heading(cleaned):
        return cleaned

    derived = _derive_section_title_from_content(content, fallback_title=cleaned or title)
    if derived and not _is_obvious_noise_heading(derived) and (
        _leading_section_keyword(derived) or _heading_quality_score(derived) >= 5
    ):
        return derived

    if index is not None:
        return f"Section {index}"
    if derived and not _is_obvious_noise_heading(derived):
        return derived
    return ""


def _looks_like_reference_heavy_content(content: str) -> bool:
    lines = [line.strip() for line in normalize_text(content or "").splitlines() if line.strip()]
    if not lines:
        return False
    window = lines[:24]
    if any(line.lower() == "references" for line in window[:4]):
        return True
    reference_like = 0
    for line in window:
        if re.match(r"^\[?\d+\]?[.)]?\s+[A-Z][a-z]+", line):
            reference_like += 1
            continue
        if re.search(r"\b(19|20)\d{2}\b", line) and re.search(r"\b(journal|proceedings|press|university|conference)\b", line.lower()):
            reference_like += 1
    return reference_like >= 6


def _looks_like_title_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped or len(stripped) > 90:
        return False
    if _is_obvious_noise_heading(stripped):
        return False
    if "." in stripped:
        return False
    if stripped.endswith((".", ";", ":")):
        return False

    lowered = stripped.lower()
    if lowered in SECTION_KEYWORDS:
        return True
    if any(lowered.startswith(f"{keyword} ") for keyword in SECTION_KEYWORDS):
        if stripped[:1].isupper() or _contains_cjk(stripped):
            return _heading_word_count(stripped) <= 10
        return _heading_word_count(stripped) <= 3 and not re.search(r"[,:;]", stripped)

    words = [word for word in re.split(r"\s+", stripped) if word]
    alpha_words = [word for word in words if any(char.isalpha() for char in word)]
    if not alpha_words or len(alpha_words) > 8:
        return False
    if len(alpha_words) == 1:
        return alpha_words[0].lower() in SECTION_KEYWORDS
    if any(len(word) > 25 for word in alpha_words):
        return False

    capitalized_words = sum(1 for word in alpha_words if word[0].isupper())
    return capitalized_words >= max(1, len(alpha_words) - 1)


def _looks_like_heading(line: str) -> bool:
    stripped = line.strip()
    if not stripped:
        return False
    if len(stripped) > 100:
        return False
    if _is_obvious_noise_heading(stripped):
        return False
    numbered_match = re.match(r"^(\d+(\.\d+)*)\s+(.+)$", stripped)
    if numbered_match:
        return _looks_like_title_heading(numbered_match.group(3))
    lowered = stripped.lower()
    if lowered in SECTION_KEYWORDS:
        return True
    if any(lowered.startswith(f"{keyword} ") for keyword in SECTION_KEYWORDS):
        return _looks_like_title_heading(stripped)
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
                    "title": resolve_section_title("", content, index=len(chunks) + 1),
                    "content": content,
                    "char_count": len(content),
                    "estimated_tokens": estimate_tokens(content),
                    "title_quality": _heading_quality_score(resolve_section_title("", content, index=len(chunks) + 1)),
                }
            )
            current_parts = [paragraph]
        else:
            current_parts.append(paragraph)

    if current_parts:
        content = "\n\n".join(current_parts).strip()
        chunks.append(
            {
                "title": resolve_section_title("", content, index=len(chunks) + 1),
                "content": content,
                "char_count": len(content),
                "estimated_tokens": estimate_tokens(content),
                "title_quality": _heading_quality_score(resolve_section_title("", content, index=len(chunks) + 1)),
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
            pending["title"] = _merge_section_titles(pending, section)
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


def _merge_section_titles(left: dict[str, Any], right: dict[str, Any]) -> str:
    left_title = clean_section_title(str(left.get("title", "")), str(left.get("content", "")))
    right_title = clean_section_title(str(right.get("title", "")), str(right.get("content", "")))

    left_score = _heading_quality_score(left_title)
    right_score = _heading_quality_score(right_title)

    if left_title.lower().startswith("front matter") and right_title.lower().startswith("abstract"):
        return "Front Matter + Abstract"
    if right_score >= left_score and right_title:
        return right_title
    if left_score > right_score and left_title:
        return left_title

    return left_title or right_title


def _should_use_fallback_sections(sections: list[dict[str, Any]]) -> bool:
    if len(sections) < 3:
        return True

    meaningful_count = sum(
        1
        for section in sections
        if int(section.get("title_quality", 0)) >= 3 and not str(section.get("title", "")).lower().startswith("front matter")
    )
    noisy_count = sum(
        1
        for section in sections
        if int(section.get("title_quality", 0)) < 0 or re.fullmatch(r"Section \d+", str(section.get("title", "")))
    )

    required_meaningful = 2 if len(sections) <= 5 else 3
    return meaningful_count < required_meaningful or noisy_count >= max(2, len(sections) // 2)


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

    filtered_sections = []
    for index, section in enumerate(sections, start=1):
        if not section["content"].strip():
            continue
        normalized_section = dict(section)
        normalized_section["title"] = resolve_section_title(
            str(section.get("title", "")),
            str(section.get("content", "")),
            index=index,
        )
        normalized_section["title_quality"] = _heading_quality_score(normalized_section["title"])
        filtered_sections.append(normalized_section)

    filtered_sections = [
        section
        for section in filtered_sections
        if not _looks_like_reference_heavy_content(str(section.get("content", ""))) or str(section.get("title", "")).lower().startswith("appendix")
    ]
    merged_sections = _merge_short_sections(filtered_sections)

    for index, section in enumerate(merged_sections, start=1):
        section["title"] = resolve_section_title(
            str(section.get("title", "")),
            str(section.get("content", "")),
            index=index,
        )
        section["title_quality"] = _heading_quality_score(section["title"])

    if _should_use_fallback_sections(merged_sections):
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
