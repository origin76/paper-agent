from __future__ import annotations

import json
from typing import Any


BASE_SYSTEM_PROMPT = """You are a research mentor who reads papers with the depth of an advisor doing a guided reading session.

Rules:
- Do not write fluffy summaries.
- Always separate what the authors claim from what the evidence actually supports.
- Prefer mechanism, assumptions, and tradeoffs over restating prose.
- If the paper text is ambiguous, say what needs verification instead of hallucinating.
- Unless explicitly told otherwise, write all explanations in Simplified Chinese.
"""


def build_overview_prompt() -> str:
    return """阅读论文并返回一个 JSON 对象，必须包含这些键：
- paper_title: string
- paper_type: string
- authors: string[]
- venue: string
- publication_year: string
- one_sentence_takeaway: string
- problem_statement: string
- why_this_problem_matters: string
- prior_work_positioning: string
- core_claims: string[]
- key_terms: string[]
- read_order: string[]
- must_clarify_questions: string[]

要求：
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文。
- 内容要高密度、高信息量，避免空话。
- 不要输出 schema 之外的键。
"""


def build_structure_prompt(overview: dict[str, Any]) -> str:
    return f"""你要把论文转换成“可执行的研究结构”。

已知 overview：
{json.dumps(overview, indent=2, ensure_ascii=False)}

返回一个 JSON 对象，必须包含这些键：
- problem: string
- assumptions: string[]
- inputs_and_outputs: {{"inputs": string[], "outputs": string[]}}
- method_modules: [{{"name": string, "role": string, "depends_on": string[]}}]
- core_pipeline: string[]
- decision_points: [{{"choice": string, "reason": string, "tradeoff": string}}]
- claim_to_evidence_map: [{{"claim": string, "evidence_sections": string[]}}]
- section_map: [{{"section_title": string, "purpose": string, "priority": "high" | "medium" | "low"}}]

要求：
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文。
- 不要输出额外的键。
"""


def build_resource_discovery_prompt(
    overview: dict[str, Any],
    web_research: dict[str, Any],
    paper_web_signals: dict[str, Any],
) -> str:
    return f"""你要为这篇论文提取“实现 / 复现 / 扩展”真正有用的资源。
必要时请联网搜索，但优先核验论文里已经明确提到的链接。

论文 overview：
{json.dumps(overview, indent=2, ensure_ascii=False)}

外部研究摘要：
{json.dumps(web_research, indent=2, ensure_ascii=False)}

论文中已出现的网页锚点：
{json.dumps(paper_web_signals, indent=2, ensure_ascii=False)}

返回一个 JSON 对象，必须包含这些键：
- official_pages: [{{"title": string, "url": string, "page_type": string, "why_relevant": string}}]
- code_repositories: [{{"title": string, "url": string, "repo_kind": string, "why_relevant": string}}]
- datasets_and_benchmarks: [{{"title": string, "url": string, "role": string}}]
- reproducibility_materials: [{{"title": string, "url": string, "material_type": string, "why_relevant": string}}]
- implementation_signals: string[]
- missing_resource_gaps: string[]

规则：
- 优先保留真正有助于复现或扩展论文的资源。
- 优先使用论文正文中已经给出的官方页面、代码仓库、artifact、DOI、项目页。
- 如果公开网页覆盖不足，要把它描述为“公开可见性不足 / 索引不足”，不要据此否定论文本身。
- 忽略低可信度网页噪音。
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文。
- 不要输出额外的键。
"""


def build_url_resource_enrichment_prompt(page_contexts: list[dict[str, Any]]) -> str:
    return f"""你在为论文精读 agent 清洗外部资源链接名称。
当前一些标题是非常差的占位词，例如“论文内提到的官方页面”“论文内提到的 GitHub 仓库”。这类标题一律不能保留。

下面是从真实 URL 抓取到的 HTML 上下文：
{json.dumps(page_contexts, indent=2, ensure_ascii=False)}

返回一个 JSON 对象，必须包含：
- pages: [{{"url": string, "clean_title": string, "page_kind": string, "summary": string}}]

规则：
- clean_title 必须具体、可读、像人会给网页起的名字。
- 禁止输出泛化标题，例如“官方页面”“资源页面”“论文内提到的页面”“代码仓库”。
- 如果是 GitHub 仓库，标题优先具体到项目或 owner/repo。
- 如果是会议页面，标题要体现会议或演讲页面身份。
- 如果是 Zenodo / DOI / artifact 页面，标题要明确写出 artifact、Zenodo 或 DOI。
- page_kind 用简短 snake_case 英文标识，例如 conference_page / github_repository / artifact_page / technical_reference。
- summary 用 1 句简体中文，说明为什么这个页面值得点开。
- 只根据给出的 URL 上下文命名，不要凭空编造页面没有的信息。
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文，page_kind 除外。
- 不要输出额外的键。
"""


def build_url_resource_search_fallback_prompt(
    overview: dict[str, Any],
    failed_page_contexts: list[dict[str, Any]],
) -> str:
    return f"""你在为论文精读 agent 做一个“弱补全”阶段。
这些 URL 在直接抓取 HTML 时失败了（例如 403 / 404），所以你需要使用联网搜索结果页摘要、搜索索引标题和公开网页线索，尽可能补全它们的页面名称与用途。

论文 overview：
{json.dumps(overview, indent=2, ensure_ascii=False)}

抓取失败的 URL 上下文：
{json.dumps(failed_page_contexts, indent=2, ensure_ascii=False)}

返回一个 JSON 对象，必须包含：
- pages: [{{"url": string, "clean_title": string, "page_kind": string, "summary": string}}]

规则：
- 这是弱补全，不要伪造你无法确认的细节。
- 优先根据 URL、搜索结果标题、站点已知结构来命名。
- clean_title 必须具体、可读，不能是“官方页面”“资源页面”“论文内提到的页面”这类占位词。
- page_kind 用简短 snake_case 英文标识，例如 conference_page / conference_index / technical_reference / github_repository / artifact_page。
- summary 用 1 句简体中文，说明用户为什么值得点开。
- 如果只是弱确认，也要给出尽量稳妥、不过度承诺的描述。
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文，page_kind 除外。
- 不要输出额外的键。
"""


def build_section_prompt(
    section_title: str,
    overview: dict[str, Any],
    structure: dict[str, Any],
    web_research: dict[str, Any] | None = None,
    resource_discovery: dict[str, Any] | None = None,
) -> str:
    return f"""像在给一位很强的研究生带读一样，精读这个 section。

论文 overview：
{json.dumps(overview, indent=2, ensure_ascii=False)}

论文结构：
{json.dumps(structure, indent=2, ensure_ascii=False)}

外部研究：
{json.dumps(web_research or {}, indent=2, ensure_ascii=False)}

资源发现：
{json.dumps(resource_discovery or {}, indent=2, ensure_ascii=False)}

目标 section：{section_title}

返回一个 JSON 对象，必须包含这些键：
- section_title: string
- section_role_in_paper: string
- author_view: string
- reviewer_view: string
- engineer_view: string
- math_or_algorithm: string[]
- design_choices: [{{"choice": string, "why": string, "risk": string}}]
- alternatives: [{{"alternative": string, "why_not_chosen": string}}]
- verification_questions: string[]

规则：
- 外部网页信息只能作为辅助证据，不能替代论文正文。
- engineer_view 里要尽量结合实现 / 复现线索。
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文。
- 不要输出额外的键。
"""


def build_experiment_prompt(
    overview: dict[str, Any],
    structure: dict[str, Any],
    resource_discovery: dict[str, Any] | None = None,
) -> str:
    return f"""像一个认真审稿人那样阅读实验部分。

论文 overview：
{json.dumps(overview, indent=2, ensure_ascii=False)}

论文结构：
{json.dumps(structure, indent=2, ensure_ascii=False)}

资源发现：
{json.dumps(resource_discovery or {}, indent=2, ensure_ascii=False)}

返回一个 JSON 对象，必须包含这些键：
- evaluation_goal: string
- experiments: [{{"name": string, "variable": string, "controls": string, "claim_tested": string, "evidence_strength": "strong" | "medium" | "weak", "possible_bias": string[], "reviewer_notes": string}}]
- overall_support_for_claims: string
- missing_ablations: string[]
- reproducibility_risks: string[]

规则：
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文。
- 不要输出额外的键。
"""


def build_web_research_summary_prompt(
    overview: dict[str, Any],
    search_queries: list[str],
    paper_web_signals: dict[str, Any],
) -> str:
    return f"""你正在为论文精读 agent 做外部联网研究。
请使用联网搜索收集支持材料，但优先核验论文正文里已经出现的官方链接。

论文 overview：
{json.dumps(overview, indent=2, ensure_ascii=False)}

建议搜索方向：
{json.dumps(search_queries, indent=2, ensure_ascii=False)}

论文中已出现的网页锚点：
{json.dumps(paper_web_signals, indent=2, ensure_ascii=False)}

返回一个 JSON 对象，必须包含这些键：
- related_work_signals: string[]
- code_resources: [{{"title": string, "url": string, "why_relevant": string}}]
- reading_notes: [{{"title": string, "url": string, "insight": string}}]
- reviewer_signals: string[]
- external_risks_or_confusions: string[]
- source_shortlist: [{{"title": string, "url": string, "type": string}}]

规则：
- 先验证或优先使用论文里已经明确出现的 venue 页面、DOI、artifact 页面、GitHub 仓库。
- 公开网页覆盖不足时，要明确说“公开讨论较少 / 尚未被充分索引”，不要说论文是虚构的、假的或不存在。
- 如果联网结果与论文正文冲突，核心事实以论文正文为准，并把网页侧的冲突描述成索引缺失、信息滞后或命名歧义。
- 如果论文自己已经给出官方 URL，而你没有在 source_shortlist 里返回这些 URL，视为失败。
- 优先官方项目页、代码仓库、会议页面、认真写的技术解读。
- 忽略明显无关或低可信度结果。
- JSON 键名必须与上面完全一致。
- 所有 value 必须使用简体中文。
- 不要输出额外的键。
"""


def build_critique_prompt(
    overview: dict[str, Any],
    structure: dict[str, Any],
    section_analyses: list[dict[str, Any]],
    experiment_review: dict[str, Any],
    web_research: dict[str, Any] | None = None,
    resource_discovery: dict[str, Any] | None = None,
) -> str:
    compact_sections = [
        {
            "section_title": item.get("section_title"),
            "reviewer_view": item.get("reviewer_view"),
            "verification_questions": item.get("verification_questions"),
        }
        for item in section_analyses
    ]

    return f"""假设你是一个非常苛刻的顶会审稿人。
找缺陷，不要复述摘要。

Overview:
{json.dumps(overview, indent=2, ensure_ascii=False)}

Structure:
{json.dumps(structure, indent=2, ensure_ascii=False)}

Section review notes:
{json.dumps(compact_sections, indent=2, ensure_ascii=False)}

Experiment review:
{json.dumps(experiment_review, indent=2, ensure_ascii=False)}

External web research:
{json.dumps(web_research or {}, indent=2, ensure_ascii=False)}

Resource discovery:
{json.dumps(resource_discovery or {}, indent=2, ensure_ascii=False)}

请用简体中文写 Markdown，并使用这些标题：
## 评审结论
## 最薄弱环节
## 隐含假设
## 可能的论文技巧
## 阻碍接收的关键问题

要求：
- 观点要具体、有证据。
- 外部网页信息只能作为支持材料，不能替代论文正文。
"""


def build_extensions_prompt(
    overview: dict[str, Any],
    structure: dict[str, Any],
    critique_markdown: str,
    web_research: dict[str, Any] | None = None,
    resource_discovery: dict[str, Any] | None = None,
) -> str:
    return f"""你现在是带学生做 follow-up 的导师，要基于这篇论文继续推进研究。

Overview:
{json.dumps(overview, indent=2, ensure_ascii=False)}

Structure:
{json.dumps(structure, indent=2, ensure_ascii=False)}

Critique:
{critique_markdown}

External web research:
{json.dumps(web_research or {}, indent=2, ensure_ascii=False)}

Resource discovery:
{json.dumps(resource_discovery or {}, indent=2, ensure_ascii=False)}

请用简体中文写 Markdown，并使用这些标题：
## 如果我们继续做这条线
## 三个快速跟进实验
## 三个更有野心的研究方向
## 迁移到其他任务的可能性
## 仍然开放的问题

要求：
- 建议必须可执行，不能空泛。
- 要像真实研究规划，而不是泛泛 brainstorm。
"""
