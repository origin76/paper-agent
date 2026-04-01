from __future__ import annotations

from typing import Any

from .narrative_markdown import render_markdown_section as _render_markdown_section
from .reference_appendix import (
    filter_resource_payload_for_display as _filter_resource_payload_for_display,
    render_reference_appendix as _render_reference_appendix,
)
from .section_renderers import (
    render_deep_read_section as _render_deep_read_section,
    render_experiment_section as _render_experiment_section,
    render_external_context_section as _render_external_context_section,
    render_overview_section as _render_overview_section,
    render_structure_section as _render_structure_section,
)


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
        f"- 是否启用联网搜索：{'是' if state.get('web_search_enabled', False) else '否'}",
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
