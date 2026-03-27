from __future__ import annotations

import json
from typing import Any


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


def _has_meaningful_content(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return False
    for value in payload.values():
        if isinstance(value, list) and value:
            return True
        if isinstance(value, dict) and value:
            return True
        if isinstance(value, str) and value.strip():
            return True
    return False


def _label_key(key: str) -> str:
    return KEY_LABELS.get(key, key)


def _localize_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {_label_key(str(key)): _localize_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_localize_payload(item) for item in value]
    return value


def _format_key_value_dict(payload: dict[str, Any]) -> str:
    localized_payload = _localize_payload(payload)
    lines: list[str] = []
    for key, value in localized_payload.items():
        if isinstance(value, dict):
            lines.append(f"- **{key}**")
            lines.append("```json")
            lines.append(json.dumps(value, indent=2, ensure_ascii=False))
            lines.append("```")
        elif isinstance(value, list):
            lines.append(f"- **{key}**")
            if value and all(isinstance(item, str) for item in value):
                lines.extend(f"  - {item}" for item in value)
            else:
                lines.append("```json")
                lines.append(json.dumps(value, indent=2, ensure_ascii=False))
                lines.append("```")
        else:
            lines.append(f"- **{key}**: {value}")
    return "\n".join(lines)


def render_report(state: dict[str, Any]) -> str:
    overview = state["overview"]
    web_research = state.get("web_research")
    resource_discovery = state.get("resource_discovery")
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
        f"- 是否启用联网搜索：{state.get('web_search_enabled', False)}",
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

    return "\n".join(parts).strip() + "\n"
