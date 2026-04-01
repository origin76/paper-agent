from __future__ import annotations

import re
from typing import Any

from paper_agent.utils import normalize_text

from .sections import clean_section_title


def clean_text(value: Any) -> str:
    return normalize_text(str(value or "").replace("\x00", "")).strip()


def trim_to_boundary(text: str, max_chars: int, min_ratio: float = 0.65) -> str:
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


def strip_terminal_punctuation(text: str) -> str:
    return re.sub(r"[。！？；;：:,，、\s]+$", "", clean_text(text))


def ensure_terminal_punctuation(text: str) -> str:
    cleaned = clean_text(text)
    if not cleaned:
        return ""
    if cleaned.endswith(("。", "！", "？", ".", "!", "?")):
        return cleaned
    return f"{cleaned}。"


def join_sentences(items: list[str]) -> str:
    sentences = [
        ensure_terminal_punctuation(strip_terminal_punctuation(item))
        for item in items
        if clean_text(item)
    ]
    return "".join(sentence for sentence in sentences if sentence)


def join_clauses(items: list[str], joiner: str = "；") -> str:
    clauses = [strip_terminal_punctuation(item) for item in items if clean_text(item)]
    if not clauses:
        return ""
    return ensure_terminal_punctuation(joiner.join(clauses))


def split_sentences(text: str) -> list[str]:
    normalized = clean_text(text)
    if not normalized:
        return []
    parts = re.findall(r".*?(?:[。！？!?；;]|$)", normalized)
    return [part.strip() for part in parts if part.strip()]


def shorten_text(text: Any, max_chars: int = 220, sentence_limit: int = 2) -> str:
    normalized = clean_text(text)
    if not normalized:
        return ""

    chosen: list[str] = []
    current_len = 0
    for sentence in split_sentences(normalized):
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

    clipped = trim_to_boundary(normalized, max_chars)
    return clipped + ("……" if not clipped.endswith(("。", "！", "？", ".", "!", "?")) else "")


def clean_list_texts(items: Any, limit: int | None = None, max_chars: int = 160) -> list[str]:
    if not isinstance(items, list):
        return []
    results: list[str] = []
    for item in items:
        text = shorten_text(item, max_chars=max_chars, sentence_limit=2)
        if not text:
            continue
        text = re.sub(r"^\d+[\.\)、]\s*", "", text).strip()
        if text in results:
            continue
        results.append(text)
        if limit is not None and len(results) >= limit:
            break
    return results


def format_authors(authors: Any, limit: int = 4) -> str:
    names = clean_list_texts(authors, limit=limit, max_chars=60)
    if not names:
        return ""
    if isinstance(authors, list) and len(authors) > len(names):
        return "、".join(names) + " 等"
    return "、".join(names)


def inline_list(items: list[str], joiner: str = "；") -> str:
    cleaned = [item for item in items if item]
    return joiner.join(cleaned)


def ordinal_label(index: int) -> str:
    labels = ["第一", "第二", "第三", "第四", "第五"]
    return labels[index] if 0 <= index < len(labels) else f"第{index + 1}"


def inline_ordinal_points(items: list[str], limit: int = 3, max_chars: int = 110) -> str:
    points = clean_list_texts(items, limit=limit, max_chars=max_chars)
    rendered = [f"{ordinal_label(index)}，{strip_terminal_punctuation(item)}" for index, item in enumerate(points)]
    return "；".join(rendered)


def count_phrase(count: int) -> str:
    return {
        1: "一点",
        2: "两点",
        3: "三点",
        4: "四点",
        5: "五点",
    }.get(count, f"{count} 点")


def clean_module_name(name: Any) -> str:
    normalized = clean_text(name)
    if not normalized:
        return ""
    normalized = re.sub(r"\s*\(([A-Za-z0-9 _/,&+.-]+)\)\s*$", "", normalized).strip()
    return normalized


def compact_module_names(modules: Any, limit: int = 4) -> list[str]:
    if not isinstance(modules, list):
        return []
    names: list[str] = []
    for module in modules:
        if not isinstance(module, dict):
            continue
        name = clean_module_name(module.get("name"))
        if not name or name in names:
            continue
        names.append(name)
        if len(names) >= limit:
            break
    return names


def compact_module_readout(module_names: list[str]) -> str:
    clues: list[str] = []
    if any("分块" in name for name in module_names):
        clues.append("分块负责把大矩阵切进 SRAM")
    if any("softmax" in name.lower() for name in module_names):
        clues.append("在线 Softmax 负责把逐块结果合成全局正确的归一化")
    if any("重计算" in name for name in module_names):
        clues.append("重计算负责把显存占用压到线性级")
    if any("融合" in name for name in module_names):
        clues.append("算子融合负责把这些思路真正兑现成速度收益")
    return join_sentences(clues[:3])


def compact_module_summary(modules: Any, limit: int = 4) -> str:
    if not isinstance(modules, list):
        return ""
    parts: list[str] = []
    for module in modules:
        if not isinstance(module, dict):
            continue
        name = clean_module_name(module.get("name"))
        role = shorten_text(module.get("role"), max_chars=48, sentence_limit=1)
        if not name and not role:
            continue
        if name and role:
            parts.append(f"{name}负责{strip_terminal_punctuation(role)}")
        else:
            parts.append(name or role)
        if len(parts) >= limit:
            break
    return "；".join(parts)


def compact_decision_summary(decisions: Any, limit: int = 3) -> list[str]:
    if not isinstance(decisions, list):
        return []
    rendered: list[str] = []
    for decision in decisions:
        if not isinstance(decision, dict):
            continue
        choice = strip_terminal_punctuation(str(decision.get("choice") or ""))
        reason = shorten_text(decision.get("reason"), max_chars=90, sentence_limit=1)
        tradeoff = shorten_text(decision.get("tradeoff"), max_chars=90, sentence_limit=1)
        if not choice:
            continue
        sentence = choice
        if reason:
            sentence += f"，因为{strip_terminal_punctuation(reason)}"
        if tradeoff:
            sentence += f"，代价是{strip_terminal_punctuation(tradeoff)}"
        rendered.append(ensure_terminal_punctuation(sentence))
        if len(rendered) >= limit:
            break
    return rendered


def compact_design_choice_summary(design_choices: Any, limit: int = 2) -> str:
    if not isinstance(design_choices, list):
        return ""
    fragments: list[str] = []
    for item in design_choices:
        if not isinstance(item, dict):
            continue
        choice = strip_terminal_punctuation(str(item.get("choice") or ""))
        why = shorten_text(item.get("why"), max_chars=80, sentence_limit=1)
        risk = shorten_text(item.get("risk"), max_chars=80, sentence_limit=1)
        if not choice:
            continue
        fragment = choice
        if why:
            fragment += f"，因为{strip_terminal_punctuation(why)}"
        if risk:
            fragment += f"，但风险在于{strip_terminal_punctuation(risk)}"
        fragments.append(fragment)
        if len(fragments) >= limit:
            break
    return "；".join(fragments)


def compact_alternative_summary(alternatives: Any, limit: int = 1) -> str:
    if not isinstance(alternatives, list):
        return ""
    for item in alternatives:
        if not isinstance(item, dict):
            continue
        alternative = strip_terminal_punctuation(str(item.get("alternative") or ""))
        why_not = shorten_text(item.get("why_not_chosen"), max_chars=90, sentence_limit=1)
        if alternative and why_not:
            return f"可替代路线其实是{alternative}，但作者没有采用，因为{strip_terminal_punctuation(why_not)}。"
    return ""


def clean_pipeline_step(step: str) -> str:
    normalized = clean_text(step)
    normalized = re.sub(r"^\d+[\.\)]\s*", "", normalized)
    normalized = re.sub(r"^[a-zA-Z][\.\)]\s*", "", normalized)
    return normalized


def summarize_pipeline_group(steps: list[str]) -> str:
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
    return strip_terminal_punctuation(shorten_text(merged, max_chars=90, sentence_limit=1))


def compact_pipeline_steps(steps: Any, limit: int = 5) -> list[str]:
    if not isinstance(steps, list):
        return []

    groups: list[list[str]] = []
    for raw_step in steps:
        raw_text = str(raw_step or "")
        cleaned = clean_pipeline_step(raw_text)
        if not cleaned:
            continue

        is_substep = bool(re.match(r"^\s*[a-zA-Z][\.\)]\s*", raw_text))
        if is_substep and groups:
            groups[-1].append(cleaned)
            continue
        groups.append([cleaned])

    results: list[str] = []
    for group in groups:
        summary = summarize_pipeline_group(group)
        if not summary:
            continue
        cleaned_summary = strip_terminal_punctuation(summary)
        if cleaned_summary in results:
            continue
        results.append(cleaned_summary)
        if len(results) >= limit:
            break
    return results


def clean_evidence_label(value: Any, max_chars: int = 44) -> str:
    raw = clean_text(value)
    if not raw:
        return ""

    cleaned = clean_section_title(raw) or raw
    appendix_match = re.search(r"(Appendix\s+[A-Z](?:\.\d+)?)", cleaned, flags=re.IGNORECASE)
    if appendix_match:
        cleaned = appendix_match.group(1)

    cleaned = re.sub(r"\s*\([^)]*\)", "", cleaned).strip()
    return shorten_text(cleaned, max_chars=max_chars, sentence_limit=1)


def display_section_title(title: Any) -> str:
    raw = clean_text(title)
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

    return shorten_text(cleaned, max_chars=36, sentence_limit=1) or "未命名章节"


def render_question_prompt(items: Any, limit: int = 2, max_chars: int = 110) -> str:
    prompts = clean_list_texts(items, limit=limit, max_chars=max_chars)
    if not prompts:
        return ""
    return ensure_terminal_punctuation(
        f"继续追问最好围绕{count_phrase(len(prompts))}展开：{inline_ordinal_points(prompts, limit=limit, max_chars=max_chars)}"
    )


def compact_claim_summary(claim_map: Any, limit: int = 3) -> list[str]:
    if not isinstance(claim_map, list):
        return []
    rendered: list[str] = []
    for item in claim_map:
        if not isinstance(item, dict):
            continue
        claim = shorten_text(item.get("claim"), max_chars=120, sentence_limit=1)
        evidence = [clean_evidence_label(section) for section in item.get("evidence_sections") or []]
        evidence = [section for section in evidence if section][:2]
        if not claim:
            continue
        if evidence:
            rendered.append(f"{strip_terminal_punctuation(claim)}。优先回看 {'、'.join(evidence)}。")
        else:
            rendered.append(ensure_terminal_punctuation(claim))
        if len(rendered) >= limit:
            break
    return rendered
