from __future__ import annotations

import argparse
import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.reporting.exporters import build_report_document, export_html_report, export_pdf_report
from paper_agent.runtime import append_stage_trace, configure_logging, log_event
from paper_agent.utils import sanitize_filename, slugify, unique_preserving_order, write_json, write_text


GENERAL_THEME_ID = "general"
GENERIC_KEYWORD_STOPWORDS = {
    "paper",
    "papers",
    "approach",
    "approaches",
    "method",
    "methods",
    "system",
    "systems",
    "model",
    "models",
    "problem",
    "problems",
    "language",
    "languages",
    "program",
    "programs",
    "programming",
    "analysis",
    "using",
    "towards",
    "study",
    "studies",
    "based",
    "theory",
    "theories",
    "practical",
    "sound",
    "complete",
    "efficient",
    "new",
    "formal",
    "logic",
    "proof",
    "proofs",
    "research",
    "work",
    "works",
    "results",
    "data",
    "framework",
    "frameworks",
    "design",
    "designed",
    "evaluation",
    "toward",
    "from",
    "with",
    "without",
    "through",
    "into",
    "between",
    "beyond",
    "under",
    "over",
    "their",
    "this",
    "that",
    "these",
    "those",
    "一个",
    "一种",
    "方法",
    "系统",
    "问题",
    "论文",
    "研究",
    "工作",
    "结果",
    "框架",
    "语言",
    "程序",
    "分析",
    "证明",
    "形式化",
    "模型",
    "理论",
}
TURNING_POINT_MARKERS = (
    "首次",
    "first",
    "the first",
    "首次将",
    "首次为",
    "重新思考",
    "rethinking",
    "rethink",
    "revisited",
    "revisiting",
    "unifies",
    "unified",
    "统一",
    "bridge",
    "bridging",
    "co-design",
    "production",
    "practical",
    "scalable",
    "改变了",
    "转向",
    "新范式",
    "new perspective",
    "new way",
    "instead of",
    "rather than",
    "moves from",
    "shifts from",
)
ENGLISH_TOKEN_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9+\-]{3,}")
CHINESE_TOKEN_PATTERN = re.compile(r"[\u4e00-\u9fff]{2,8}")
BRACKETED_VENUE_PATTERN = re.compile(r"\s*\[[A-Za-z]+(?:\s+\d{4})?\]\s*$")
SOURCE_TITLE_SUFFIX_PATTERN = re.compile(r"\s*\[[A-Za-z]+ \d{4}\]\s*$")
MULTISPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class ThemeDefinition:
    id: str
    label: str
    synopsis: str
    keywords: tuple[str, ...]


@dataclass(slots=True)
class PaperProfile:
    paper_id: str
    identity_key: str
    run_dir: str
    pdf_path: str
    display_title: str
    source_title: str
    venue: str
    venue_short: str
    publication_year: int | None
    authors: list[str]
    paper_type: str
    takeaway: str
    problem_statement: str
    prior_work_positioning: str
    core_claims: list[str]
    method_modules: list[str]
    core_pipeline: list[str]
    evaluation_goal: str
    experiment_names: list[str]
    missing_ablations: list[str]
    reproducibility_risks: list[str]
    code_resources: list[str]
    official_resources: list[str]
    keywords: list[str]
    theme_scores: dict[str, int]
    primary_theme: str
    secondary_themes: list[str]
    turning_markers: list[str]
    profile_quality: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class TurningPoint:
    paper_id: str
    paper_label: str
    year: int | None
    score: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ReadingStep:
    paper_id: str
    paper_label: str
    year: int | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class YearMoment:
    year: int | None
    paper_count: int
    summary: str
    representative_papers: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class StoryArc:
    theme_id: str
    title: str
    synopsis: str
    paper_count: int
    year_range: str
    venues: list[str]
    keywords: list[str]
    paper_ids: list[str]
    turning_points: list[TurningPoint] = field(default_factory=list)
    reading_path: list[ReadingStep] = field(default_factory=list)
    year_moments: list[YearMoment] = field(default_factory=list)
    tensions: list[str] = field(default_factory=list)
    setup_text: str = ""
    build_up_text: str = ""
    turn_text: str = ""
    synthesis_text: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["turning_points"] = [item.to_dict() for item in self.turning_points]
        payload["reading_path"] = [item.to_dict() for item in self.reading_path]
        payload["year_moments"] = [item.to_dict() for item in self.year_moments]
        return payload


THEME_DEFINITIONS: tuple[ThemeDefinition, ...] = (
    ThemeDefinition(
        id="verification_logic",
        label="程序验证与形式化推理",
        synopsis="从逻辑、公理系统到可机检正确性的工作，通常围绕可证明性、语义健全性和验证成本展开。",
        keywords=(
            "verification",
            "verified",
            "verifying",
            "proof-guided",
            "separation logic",
            "linearizability",
            "model checking",
            "theorem proving",
            "coq",
            "agda",
            "isabelle",
            "f*",
            "验证",
            "可证明",
            "线性化",
            "分离逻辑",
        ),
    ),
    ThemeDefinition(
        id="type_systems",
        label="类型系统与程序语义",
        synopsis="聚焦类型、安全边界与语义表达力的工作，常见转折是从表达能力走向可用性与工程可落地性。",
        keywords=(
            "type system",
            "type systems",
            "typing",
            "typed",
            "subtyping",
            "polymorphism",
            "dependent type",
            "dependent types",
            "gradual typing",
            "refinement type",
            "session type",
            "effect system",
            "effects",
            "coeffects",
            "type inference",
            "type error",
            "类型",
            "子类型",
            "多态",
            "依赖类型",
            "渐进类型",
            "精化类型",
            "会话类型",
            "类型推断",
        ),
    ),
    ThemeDefinition(
        id="program_analysis",
        label="程序分析与自动推理",
        synopsis="围绕静态分析、抽象解释、可达性与自动推理的主线，常见节奏是先找对抽象，再逼近可扩展性。",
        keywords=(
            "static analysis",
            "program analysis",
            "abstract interpretation",
            "incorrectness logic",
            "horn clauses",
            "datalog",
            "reachability",
            "symbolic execution",
            "oracle",
            "fuzzing",
            "dataflow",
            "alias analysis",
            "可达性",
            "抽象解释",
            "静态分析",
            "程序分析",
            "符号执行",
            "数据流",
            "错误定位",
        ),
    ),
    ThemeDefinition(
        id="compilers_synthesis",
        label="编译、优化与程序合成",
        synopsis="关注编译链路、重写系统和程序合成的工作，领域转折通常发生在优化目标、约束模型或搜索空间被重写的时候。",
        keywords=(
            "compiler",
            "compilation",
            "optimization",
            "optimizing",
            "optimizer",
            "superoptimization",
            "rewriting",
            "equality saturation",
            "synthesis",
            "program synthesis",
            "enumeration",
            "scheduling",
            "template haskell",
            "编译",
            "优化",
            "程序合成",
            "重写",
            "调度",
            "等式饱和",
        ),
    ),
    ThemeDefinition(
        id="systems_runtime",
        label="系统、操作系统与运行时",
        synopsis="面向 OS、内核、运行时与云基础设施的工作，主线往往从单机机制演进到跨硬件、跨集群和生产环境的系统化方案。",
        keywords=(
            "operating system",
            "os ",
            "kernel",
            "runtime",
            "cloud",
            "serverless",
            "scheduling",
            "process",
            "thread",
            "container",
            "checkpoint",
            "restore",
            "fork",
            "memory management",
            "系统",
            "操作系统",
            "内核",
            "运行时",
            "云",
            "调度",
            "线程",
            "进程",
            "容器",
            "检查点",
        ),
    ),
    ThemeDefinition(
        id="distributed_data",
        label="分布式系统、事务与数据基础设施",
        synopsis="处理分布式一致性、事务、远程资源和数据平面的工作，典型起承转合是从 correctness 到 elasticity 再到 heterogeneity。",
        keywords=(
            "distributed",
            "replication",
            "consensus",
            "transaction",
            "geo-distributed",
            "remote memory",
            "hybrid cloud",
            "storage access",
            "bft",
            "analytics",
            "database",
            "query",
            "分布式",
            "一致性",
            "事务",
            "远程内存",
            "混合云",
            "数据库",
            "查询",
            "分析",
        ),
    ),
    ThemeDefinition(
        id="networking_io",
        label="网络、I/O 与数据路径",
        synopsis="围绕 NIC、packet path、I/O 与通信接口的工作，重点通常从单点加速转向端到端数据路径重构。",
        keywords=(
            "network",
            "packet",
            "nic",
            "rdma",
            "transport",
            "tcp",
            "latency",
            "io",
            "i/o",
            "datapath",
            "data plane",
            "socket",
            "网络",
            "数据路径",
            "延迟",
            "通信",
            "收发",
            "协议",
        ),
    ),
    ThemeDefinition(
        id="security_privacy",
        label="安全、隔离与隐私",
        synopsis="这条线关注安全边界如何定义、强化和验证，经常在性能与安全保证之间反复拉扯。",
        keywords=(
            "security",
            "privacy",
            "secure",
            "isolation",
            "sandbox",
            "vulnerability",
            "taint",
            "private data",
            "oracle",
            "secure compilation",
            "安全",
            "隐私",
            "隔离",
            "沙箱",
            "漏洞",
            "私有数据",
        ),
    ),
    ThemeDefinition(
        id="ml_ai_systems",
        label="机器学习与大模型系统",
        synopsis="从训练到 serving 的系统工作，最常见的转折是 workload 变化、硬件约束变化以及 retrieval / generation 边界的重划。",
        keywords=(
            "machine learning",
            "deep learning",
            "llm",
            "large language model",
            "model serving",
            "training",
            "inference",
            "rag",
            "in-context",
            "transformer",
            "gpu serving",
            "机器学习",
            "深度学习",
            "大语言模型",
            "模型服务",
            "训练",
            "推理",
        ),
    ),
    ThemeDefinition(
        id="accelerators_gpu",
        label="GPU、加速器与异构计算",
        synopsis="关注 GPU、异构硬件和算子优化的工作，往往体现为从局部 kernel 优化走向系统级调度与资源协同。",
        keywords=(
            "gpu",
            "cuda",
            "accelerator",
            "heterogeneous",
            "tensor",
            "operator optimization",
            "asynchronous copy",
            "remote memory scheduling",
            "device",
            "gpu checkpoint",
            "加速器",
            "异构",
            "算子",
            "张量",
            "图形处理器",
        ),
    ),
    ThemeDefinition(
        id="quantum_reversible",
        label="量子计算与可逆程序",
        synopsis="量子与可逆计算的工作通常在语义、资源模型和可组合性之间寻找新的平衡点。",
        keywords=(
            "quantum",
            "reversible",
            "qubit",
            "zx-calculus",
            "quantum program",
            "quantum computing",
            "量子",
            "可逆",
            "量子程序",
        ),
    ),
)
THEME_BY_ID = {theme.id: theme for theme in THEME_DEFINITIONS}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Aggregate existing paper-agent runs into readable domain-development story arcs.",
    )
    parser.add_argument(
        "input_roots",
        nargs="+",
        help="One or more directories containing paper-agent run directories or batch roots.",
    )
    parser.add_argument(
        "--output-dir",
        help="Directory where the narrative artifacts will be written.",
    )
    parser.add_argument(
        "--report-title",
        default="论文领域发展与转折叙事报告",
        help="Title used for the generated markdown/html/pdf report.",
    )
    parser.add_argument(
        "--min-papers-per-arc",
        type=int,
        default=6,
        help="Minimum papers required before a theme is promoted into a story arc.",
    )
    parser.add_argument(
        "--max-arcs",
        type=int,
        default=10,
        help="Maximum number of story arcs to include in the readable report.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level, for example INFO or DEBUG.",
    )
    return parser


def _collapse_whitespace(text: str | None) -> str:
    if not text:
        return ""
    return MULTISPACE_PATTERN.sub(" ", str(text)).strip()


def _safe_load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, str):
        cleaned = _collapse_whitespace(value)
        return [cleaned] if cleaned else []
    if not isinstance(value, list):
        return []
    result: list[str] = []
    for item in value:
        if isinstance(item, str):
            cleaned = _collapse_whitespace(item)
            if cleaned:
                result.append(cleaned)
    return result


def _extract_method_modules(raw_modules: Any) -> list[str]:
    if isinstance(raw_modules, list):
        result: list[str] = []
        for item in raw_modules:
            if isinstance(item, str):
                cleaned = _collapse_whitespace(item)
                if cleaned:
                    result.append(cleaned)
            elif isinstance(item, dict):
                cleaned = _collapse_whitespace(item.get("name") or item.get("role"))
                if cleaned:
                    result.append(cleaned)
        return unique_preserving_order(result)
    return []


def _extract_experiment_names(experiment_review: dict[str, Any]) -> list[str]:
    result: list[str] = []
    experiments = experiment_review.get("experiments")
    if not isinstance(experiments, list):
        return result
    for item in experiments:
        if not isinstance(item, dict):
            continue
        name = _collapse_whitespace(item.get("name"))
        if name:
            result.append(name)
    return unique_preserving_order(result)


def _extract_url_list(resource_block: Any, *keys: str) -> list[str]:
    result: list[str] = []
    if not isinstance(resource_block, list):
        return result
    for item in resource_block:
        if not isinstance(item, dict):
            continue
        for key in keys:
            value = _collapse_whitespace(item.get(key))
            if value:
                result.append(value)
    return unique_preserving_order(result)


def _extract_source_title(pdf_path: str, fallback: str) -> str:
    if not pdf_path:
        return fallback
    stem = Path(pdf_path).stem
    stem = SOURCE_TITLE_SUFFIX_PATTERN.sub("", stem).strip()
    stem = BRACKETED_VENUE_PATTERN.sub("", stem).strip()
    return stem or fallback


def _infer_venue_short(pdf_path: str, venue_text: str) -> str:
    from_path = ""
    parts = Path(pdf_path).parts
    for part in parts:
        lowered = part.lower()
        if lowered in {"osdi", "sosp", "pldi", "popl"}:
            from_path = lowered.upper()
            break
    if from_path:
        return from_path

    lowered_text = venue_text.lower()
    for token in ("osdi", "sosp", "pldi", "popl"):
        if token in lowered_text:
            return token.upper()
    return "PAPER"


def _infer_year(overview: dict[str, Any], pdf_path: str) -> int | None:
    raw_year = overview.get("publication_year")
    if raw_year is not None:
        text = _collapse_whitespace(str(raw_year))
        if text.isdigit():
            return int(text)
    for part in Path(pdf_path).parts:
        if len(part) == 4 and part.isdigit():
            return int(part)
    return None


def _profile_identity_key(pdf_path: str, display_title: str, year: int | None, venue_short: str) -> str:
    resolved_pdf = str(Path(pdf_path).expanduser())
    if resolved_pdf:
        return resolved_pdf
    return f"{slugify(display_title or 'paper')}::{venue_short}::{year or 'unknown'}"


def _keyword_in_text(keyword: str, lowered_text: str) -> bool:
    if re.search(r"[\u4e00-\u9fff]", keyword):
        return keyword in lowered_text
    escaped = re.escape(keyword.lower())
    if " " in keyword or "-" in keyword:
        return escaped in lowered_text
    return re.search(rf"\b{escaped}\b", lowered_text) is not None


def _score_themes(text_fragments: list[str]) -> dict[str, int]:
    lowered_text = " ".join(fragment.lower() for fragment in text_fragments if fragment).strip()
    scores: dict[str, int] = {}
    for theme in THEME_DEFINITIONS:
        score = 0
        for keyword in theme.keywords:
            if _keyword_in_text(keyword, lowered_text):
                score += 2 if (" " in keyword or "-" in keyword or re.search(r"[\u4e00-\u9fff]", keyword)) else 1
        if score >= 2:
            scores[theme.id] = score
    if not scores:
        scores[GENERAL_THEME_ID] = 1
    return scores


def _select_theme_ids(theme_scores: dict[str, int]) -> tuple[str, list[str]]:
    ranked = sorted(theme_scores.items(), key=lambda item: (-item[1], item[0]))
    primary = ranked[0][0]
    secondaries: list[str] = []
    primary_score = ranked[0][1]
    for theme_id, score in ranked[1:]:
        if len(secondaries) >= 2:
            break
        if score >= max(1, primary_score - 2):
            secondaries.append(theme_id)
    return primary, secondaries


def _extract_keywords(text_fragments: list[str], favored_phrases: list[str]) -> list[str]:
    counter: Counter[str] = Counter()

    for phrase in favored_phrases:
        cleaned = _collapse_whitespace(phrase)
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered in GENERIC_KEYWORD_STOPWORDS:
            continue
        if len(cleaned) >= 6:
            counter[cleaned] += 3

    for fragment in text_fragments:
        if not fragment:
            continue
        for token in ENGLISH_TOKEN_PATTERN.findall(fragment):
            lowered = token.lower()
            if lowered in GENERIC_KEYWORD_STOPWORDS:
                continue
            counter[lowered] += 1
        for token in CHINESE_TOKEN_PATTERN.findall(fragment):
            if token in GENERIC_KEYWORD_STOPWORDS:
                continue
            counter[token] += 1

    keywords = [term for term, _ in counter.most_common(12)]
    return keywords


def _extract_turning_markers(text_fragments: list[str]) -> list[str]:
    lowered = " ".join(fragment.lower() for fragment in text_fragments if fragment)
    markers = [marker for marker in TURNING_POINT_MARKERS if marker.lower() in lowered]
    return unique_preserving_order(markers)


def _compute_profile_quality(
    takeaway: str,
    core_claims: list[str],
    method_modules: list[str],
    experiment_names: list[str],
    missing_ablations: list[str],
    reproducibility_risks: list[str],
) -> float:
    score = 0.0
    if takeaway:
        score += min(len(takeaway), 180) / 60.0
    score += min(len(core_claims), 5) * 1.5
    score += min(len(method_modules), 5) * 1.2
    score += min(len(experiment_names), 4) * 0.8
    score += min(len(missing_ablations), 4) * 0.3
    score += min(len(reproducibility_risks), 4) * 0.3
    return score


def build_paper_profile(run_dir: Path) -> PaperProfile | None:
    run_summary = _safe_load_json(run_dir / "run_summary.json")
    overview = _safe_load_json(run_dir / "overview.json")
    if not isinstance(run_summary, dict) or not isinstance(overview, dict):
        return None

    structure = _safe_load_json(run_dir / "structure.json")
    if not isinstance(structure, dict):
        structure = {}

    experiment_review = _safe_load_json(run_dir / "experiment_review.json")
    if not isinstance(experiment_review, dict):
        experiment_review = {}

    resource_discovery = _safe_load_json(run_dir / "resource_discovery.json")
    if not isinstance(resource_discovery, dict):
        resource_discovery = {}

    web_research = _safe_load_json(run_dir / "web_research.json")
    if not isinstance(web_research, dict):
        web_research = {}

    pdf_path = _collapse_whitespace(run_summary.get("pdf_path"))
    display_title = _collapse_whitespace(overview.get("paper_title") or run_summary.get("paper_title") or run_dir.name)
    source_title = _extract_source_title(pdf_path, fallback=display_title or run_dir.name)
    venue = _collapse_whitespace(overview.get("venue"))
    venue_short = _infer_venue_short(pdf_path, venue)
    publication_year = _infer_year(overview, pdf_path)
    authors = _coerce_string_list(overview.get("authors"))
    paper_type = _collapse_whitespace(overview.get("paper_type"))
    takeaway = _collapse_whitespace(overview.get("one_sentence_takeaway"))
    problem_statement = _collapse_whitespace(overview.get("problem_statement"))
    prior_work_positioning = _collapse_whitespace(overview.get("prior_work_positioning"))
    core_claims = _coerce_string_list(overview.get("core_claims"))
    method_modules = _extract_method_modules(structure.get("method_modules"))
    core_pipeline = _coerce_string_list(structure.get("core_pipeline"))
    evaluation_goal = _collapse_whitespace(experiment_review.get("evaluation_goal"))
    experiment_names = _extract_experiment_names(experiment_review)
    missing_ablations = _coerce_string_list(experiment_review.get("missing_ablations"))
    reproducibility_risks = _coerce_string_list(experiment_review.get("reproducibility_risks"))
    code_resources = _extract_url_list(resource_discovery.get("code_repositories"), "url")
    official_resources = _extract_url_list(resource_discovery.get("official_pages"), "url")
    official_resources.extend(_extract_url_list(web_research.get("source_shortlist"), "url"))
    official_resources = unique_preserving_order(official_resources)

    text_fragments = [
        display_title,
        source_title,
        takeaway,
        problem_statement,
        prior_work_positioning,
        evaluation_goal,
        *core_claims,
        *method_modules,
        *core_pipeline,
        *experiment_names,
        *missing_ablations,
        *reproducibility_risks,
    ]
    theme_scores = _score_themes(text_fragments)
    primary_theme, secondary_themes = _select_theme_ids(theme_scores)
    keywords = _extract_keywords(
        text_fragments,
        favored_phrases=method_modules + experiment_names + core_claims[:3] + [source_title],
    )
    turning_markers = _extract_turning_markers(text_fragments)
    profile_quality = _compute_profile_quality(
        takeaway,
        core_claims,
        method_modules,
        experiment_names,
        missing_ablations,
        reproducibility_risks,
    )

    paper_id = slugify(f"{source_title}-{venue_short}-{publication_year or 'unknown'}", fallback=run_dir.name)
    identity_key = _profile_identity_key(pdf_path, display_title or source_title, publication_year, venue_short)

    return PaperProfile(
        paper_id=paper_id,
        identity_key=identity_key,
        run_dir=str(run_dir.resolve()),
        pdf_path=pdf_path,
        display_title=display_title or source_title,
        source_title=source_title,
        venue=venue,
        venue_short=venue_short,
        publication_year=publication_year,
        authors=authors,
        paper_type=paper_type,
        takeaway=takeaway,
        problem_statement=problem_statement,
        prior_work_positioning=prior_work_positioning,
        core_claims=core_claims,
        method_modules=method_modules,
        core_pipeline=core_pipeline,
        evaluation_goal=evaluation_goal,
        experiment_names=experiment_names,
        missing_ablations=missing_ablations,
        reproducibility_risks=reproducibility_risks,
        code_resources=code_resources,
        official_resources=official_resources,
        keywords=keywords,
        theme_scores=theme_scores,
        primary_theme=primary_theme,
        secondary_themes=secondary_themes,
        turning_markers=turning_markers,
        profile_quality=profile_quality,
    )


def discover_analysis_run_dirs(input_roots: list[Path]) -> list[Path]:
    discovered: list[Path] = []
    for root in input_roots:
        resolved_root = root.expanduser().resolve()
        if not resolved_root.exists():
            continue
        if (resolved_root / "run_summary.json").exists():
            discovered.append(resolved_root)
            continue
        for path in resolved_root.rglob("run_summary.json"):
            discovered.append(path.parent)
    return sorted(unique_preserving_order([str(path) for path in discovered]))


def load_paper_profiles(run_dirs: list[Path]) -> tuple[list[PaperProfile], dict[str, int]]:
    profiles_by_identity: dict[str, PaperProfile] = {}
    discovered_count = 0
    skipped_count = 0
    duplicate_count = 0

    for run_dir in run_dirs:
        discovered_count += 1
        profile = build_paper_profile(Path(run_dir))
        if profile is None:
            skipped_count += 1
            continue
        existing = profiles_by_identity.get(profile.identity_key)
        if existing is None:
            profiles_by_identity[profile.identity_key] = profile
            continue
        duplicate_count += 1
        existing_mtime = Path(existing.run_dir).stat().st_mtime if Path(existing.run_dir).exists() else 0.0
        candidate_mtime = Path(profile.run_dir).stat().st_mtime if Path(profile.run_dir).exists() else 0.0
        if profile.profile_quality > existing.profile_quality or (
            profile.profile_quality == existing.profile_quality and candidate_mtime >= existing_mtime
        ):
            profiles_by_identity[profile.identity_key] = profile

    profiles = sorted(
        profiles_by_identity.values(),
        key=lambda item: (
            item.publication_year if item.publication_year is not None else 9999,
            item.venue_short,
            item.source_title.lower(),
        ),
    )
    stats = {
        "discovered_run_dirs": discovered_count,
        "skipped_run_dirs": skipped_count,
        "duplicate_run_dirs": duplicate_count,
        "unique_papers": len(profiles),
    }
    return profiles, stats


def _trim_text(text: str, max_chars: int = 72) -> str:
    cleaned = _collapse_whitespace(text)
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip(" ,.;，。；：:") + "…"


def _paper_label(profile: PaperProfile) -> str:
    title = profile.display_title or profile.source_title or "未命名论文"
    if profile.source_title and profile.display_title and profile.source_title != profile.display_title:
        title = f"{profile.display_title} / {profile.source_title}"
    if profile.publication_year is not None:
        return f"《{title}》 ({profile.venue_short} {profile.publication_year})"
    return f"《{title}》"


def _paper_short_label(profile: PaperProfile) -> str:
    title = profile.display_title or profile.source_title or "未命名论文"
    return f"《{title}》"


def _top_keywords(profiles: list[PaperProfile], limit: int = 8) -> list[str]:
    counter: Counter[str] = Counter()
    for profile in profiles:
        for keyword in profile.keywords:
            lowered = keyword.lower()
            if lowered in GENERIC_KEYWORD_STOPWORDS:
                continue
            counter[keyword] += 1
    return [term for term, _ in counter.most_common(limit)]


def _top_method_modules(profiles: list[PaperProfile], limit: int = 5) -> list[str]:
    counter: Counter[str] = Counter()
    for profile in profiles:
        for module in profile.method_modules:
            counter[module] += 1
    return [term for term, _ in counter.most_common(limit)]


def _collect_tensions(profiles: list[PaperProfile], limit: int = 6) -> list[str]:
    counter: Counter[str] = Counter()
    original_text: dict[str, str] = {}
    for profile in profiles:
        for item in profile.missing_ablations + profile.reproducibility_risks:
            cleaned = _trim_text(item, max_chars=120)
            if not cleaned:
                continue
            normalized = re.sub(r"\s+", " ", cleaned.lower())
            counter[normalized] += 1
            original_text.setdefault(normalized, cleaned)
    if not counter:
        return []
    return [original_text[key] for key, _ in counter.most_common(limit)]


def _identify_turning_points(profiles: list[PaperProfile]) -> list[TurningPoint]:
    seen_keywords: set[str] = set()
    scored: list[TurningPoint] = []
    for index, profile in enumerate(profiles):
        new_keywords = [item for item in profile.keywords if item not in seen_keywords][:4]
        score = 0.0
        reason_parts: list[str] = []

        if profile.turning_markers:
            score += len(profile.turning_markers) * 1.1
            reason_parts.append("文中带有明显的“首次 / 统一 / 转向”信号")
        if new_keywords:
            score += min(len(new_keywords), 4) * 0.7
            reason_parts.append(f"引入了新的主题词：{', '.join(new_keywords[:3])}")
        if profile.method_modules:
            score += min(len(profile.method_modules), 3) * 0.3
        if index == 0:
            score -= 0.5
        if profile.prior_work_positioning:
            lowered_prior = profile.prior_work_positioning.lower()
            if any(marker in lowered_prior for marker in ("instead of", "rather than", "different from", "突破", "区别于")):
                score += 0.9
                reason_parts.append("对已有路线给出了明确的改写或纠偏")
        if not reason_parts and profile.takeaway:
            reason_parts.append(_trim_text(profile.takeaway, max_chars=80))

        scored.append(
            TurningPoint(
                paper_id=profile.paper_id,
                paper_label=_paper_label(profile),
                year=profile.publication_year,
                score=round(score, 2),
                reason="；".join(reason_parts),
            )
        )
        seen_keywords.update(profile.keywords)

    turning_points = [item for item in sorted(scored, key=lambda item: (-item.score, item.year or 9999, item.paper_label)) if item.score >= 1.2]
    if not turning_points and profiles:
        median_index = len(profiles) // 2
        profile = profiles[median_index]
        return [
            TurningPoint(
                paper_id=profile.paper_id,
                paper_label=_paper_label(profile),
                year=profile.publication_year,
                score=1.0,
                reason="它位于这条线由早期探索走向中期成形的位置，适合作为默认转折点来理解方法成熟。",
            )
        ]
    return turning_points[:3]


def _build_reading_path(profiles: list[PaperProfile], turning_points: list[TurningPoint]) -> list[ReadingStep]:
    selected: list[ReadingStep] = []
    added_ids: set[str] = set()

    def add(profile: PaperProfile, reason: str) -> None:
        if profile.paper_id in added_ids:
            return
        selected.append(
            ReadingStep(
                paper_id=profile.paper_id,
                paper_label=_paper_label(profile),
                year=profile.publication_year,
                reason=reason,
            )
        )
        added_ids.add(profile.paper_id)

    if profiles:
        add(profiles[0], "先读它，看这条线最初如何定义问题，以及当时默认接受了哪些假设。")

    turning_by_id = {item.paper_id for item in turning_points}
    for turning_point in turning_points:
        profile = next((item for item in profiles if item.paper_id == turning_point.paper_id), None)
        if profile is not None:
            add(profile, "它是理解这条线为什么发生转向的关键节点。")

    if len(profiles) >= 3:
        middle = profiles[len(profiles) // 2]
        add(middle, "它代表主流路线如何从“能做”走向“做得系统、做得稳”。")

    if profiles:
        add(profiles[-1], "最后读它，用来把握目前的前沿边界、代价与未解决张力。")

    return selected[:5]


def _build_year_moments(profiles: list[PaperProfile]) -> list[YearMoment]:
    grouped: dict[int | None, list[PaperProfile]] = defaultdict(list)
    for profile in profiles:
        grouped[profile.publication_year].append(profile)

    moments: list[YearMoment] = []
    for year in sorted(grouped, key=lambda value: (value is None, value)):
        items = grouped[year]
        representatives = items[:2]
        summary_parts = [
            f"{_paper_short_label(profile)}：{_trim_text(profile.takeaway or profile.problem_statement or profile.evaluation_goal, 42)}"
            for profile in representatives
        ]
        moments.append(
            YearMoment(
                year=year,
                paper_count=len(items),
                summary="；".join(summary_parts),
                representative_papers=[_paper_label(profile) for profile in representatives],
            )
        )
    return moments


def _compose_setup_text(theme: ThemeDefinition, profiles: list[PaperProfile]) -> str:
    if not profiles:
        return ""
    lead_profiles = profiles[:2]
    problem_text = _trim_text(lead_profiles[0].problem_statement or lead_profiles[0].takeaway, 90)
    evidence = " ".join(
        f"{_paper_short_label(profile)} 把问题概括为“{_trim_text(profile.problem_statement or profile.takeaway, 56)}”。"
        for profile in lead_profiles
        if profile.problem_statement or profile.takeaway
    )
    return (
        f"{theme.synopsis} 这条线起步时，研究者最关心的是“{problem_text}”。"
        f" {evidence}".strip()
    )


def _compose_build_up_text(theme: ThemeDefinition, profiles: list[PaperProfile]) -> str:
    if len(profiles) <= 2:
        return "这条线还处在早期，尚未明显分化出稳定的主流工程套路。"
    middle_profiles = profiles[1:-1] if len(profiles) > 3 else profiles[1:]
    modules = _top_method_modules(middle_profiles, limit=4)
    module_text = "、".join(modules) if modules else "方法模块的系统化拆分"
    representative = middle_profiles[0]
    return (
        "随着问题定义逐渐稳定，论文的重心开始从“证明这件事值得做”转向“怎样把它做成一条可复用路线”。"
        f" 中段工作反复出现的模块包括 {module_text}。"
        f" 像 {_paper_short_label(representative)} 这样的论文，已经在把抽象想法压成更清晰的技术管线。"
    )


def _compose_turn_text(turning_points: list[TurningPoint]) -> str:
    if not turning_points:
        return "目前没有足够明确的转折点信号，这说明这条线更像是持续优化，而不是经历了强烈的范式切换。"
    lead = turning_points[0]
    if len(turning_points) == 1:
        return f"最明显的拐点出现在 {lead.paper_label}，原因是：{lead.reason}。"
    second = turning_points[1]
    return (
        f"真正的转折不是突然发生的，而是由 {lead.paper_label} 和 {second.paper_label} 这类节点连续推动的。"
        f" 前者的关键信号是：{lead.reason}；后者则把转向进一步坐实为新的主流方向。"
    )


def _compose_synthesis_text(profiles: list[PaperProfile], tensions: list[str]) -> str:
    if not profiles:
        return ""
    tail_profiles = profiles[-2:] if len(profiles) >= 2 else profiles
    tail_summary = " ".join(
        f"{_paper_short_label(profile)} 代表了最近阶段的关注点：“{_trim_text(profile.takeaway or profile.evaluation_goal or profile.problem_statement, 58)}”。"
        for profile in tail_profiles
    )
    if tensions:
        return (
            f"{tail_summary} 但这条线并没有真正收束，仍然被这些张力牵着走："
            f"{'；'.join(_trim_text(item, 48) for item in tensions[:3])}。"
        )
    return f"{tail_summary} 目前看，它已经从单点技术问题演进成了更完整的方法或系统设计问题。"


def build_story_arcs(
    profiles: list[PaperProfile],
    min_papers_per_arc: int = 6,
    max_arcs: int = 10,
) -> list[StoryArc]:
    grouped: dict[str, list[PaperProfile]] = defaultdict(list)
    for profile in profiles:
        if profile.primary_theme == GENERAL_THEME_ID:
            continue
        grouped[profile.primary_theme].append(profile)

    arcs: list[StoryArc] = []
    for theme_id, items in grouped.items():
        if len(items) < min_papers_per_arc:
            continue
        theme = THEME_BY_ID[theme_id]
        sorted_items = sorted(
            items,
            key=lambda item: (
                item.publication_year if item.publication_year is not None else 9999,
                item.venue_short,
                item.source_title.lower(),
            ),
        )
        years = [item.publication_year for item in sorted_items if item.publication_year is not None]
        year_range = f"{min(years)}-{max(years)}" if years else "未知"
        venues = [item.venue_short for item in sorted_items]
        turning_points = _identify_turning_points(sorted_items)
        tensions = _collect_tensions(sorted_items)
        arcs.append(
            StoryArc(
                theme_id=theme.id,
                title=theme.label,
                synopsis=theme.synopsis,
                paper_count=len(sorted_items),
                year_range=year_range,
                venues=[venue for venue, _ in Counter(venues).most_common()],
                keywords=_top_keywords(sorted_items),
                paper_ids=[item.paper_id for item in sorted_items],
                turning_points=turning_points,
                reading_path=_build_reading_path(sorted_items, turning_points),
                year_moments=_build_year_moments(sorted_items),
                tensions=tensions,
                setup_text=_compose_setup_text(theme, sorted_items),
                build_up_text=_compose_build_up_text(theme, sorted_items),
                turn_text=_compose_turn_text(turning_points),
                synthesis_text=_compose_synthesis_text(sorted_items, tensions),
            )
        )

    arcs.sort(key=lambda item: (-item.paper_count, item.title))
    return arcs[:max_arcs]


def _build_global_summary(profiles: list[PaperProfile], arcs: list[StoryArc]) -> dict[str, Any]:
    years = [profile.publication_year for profile in profiles if profile.publication_year is not None]
    year_range = f"{min(years)}-{max(years)}" if years else "未知"
    venue_counter = Counter(profile.venue_short for profile in profiles)
    theme_counter = Counter(profile.primary_theme for profile in profiles if profile.primary_theme != GENERAL_THEME_ID)
    return {
        "paper_count": len(profiles),
        "arc_count": len(arcs),
        "year_range": year_range,
        "venues": dict(venue_counter.most_common()),
        "themes": dict(theme_counter.most_common()),
    }


def render_narrative_markdown(
    report_title: str,
    profiles: list[PaperProfile],
    arcs: list[StoryArc],
    summary: dict[str, Any],
) -> str:
    lines: list[str] = [f"# {report_title}", ""]
    lines.append("## 总览")
    lines.append("")
    lines.append(f"- 论文总数：{summary['paper_count']}")
    lines.append(f"- 故事线数量：{summary['arc_count']}")
    lines.append(f"- 年份跨度：{summary['year_range']}")
    if summary["venues"]:
        venue_text = "，".join(f"{venue} {count}" for venue, count in summary["venues"].items())
        lines.append(f"- 主要 venue 分布：{venue_text}")
    lines.append("")
    lines.append(
        "这份报告不把论文当作孤立节点来罗列，而是把它们压成几条可读的研究故事线，重点回答“问题怎样被定义、怎样成熟、何时转向，以及今天还卡在哪里”。"
    )
    lines.append("")

    lines.append("## 故事线地图")
    lines.append("")
    for arc in arcs:
        venues_text = " / ".join(arc.venues[:4])
        keyword_text = "、".join(arc.keywords[:5]) if arc.keywords else "暂无稳定关键词"
        lines.append(
            f"- **{arc.title}**：{arc.paper_count} 篇，跨度 {arc.year_range}，主要来自 {venues_text}。关键词：{keyword_text}。"
        )
    lines.append("")

    lines.append("## 推荐阅读方式")
    lines.append("")
    lines.append("1. 先读每条故事线里的第一篇，理解问题最初是如何被表述的。")
    lines.append("2. 再读转折点论文，看研究共同体为什么改变路线。")
    lines.append("3. 最后读近两年的论文，把握今天真正的矛盾和未解问题。")
    lines.append("")

    for arc in arcs:
        lines.append(f"## {arc.title}")
        lines.append("")
        lines.append(arc.synopsis)
        lines.append("")
        lines.append(f"- 覆盖论文：{arc.paper_count}")
        lines.append(f"- 时间跨度：{arc.year_range}")
        lines.append(f"- 主要 venue：{' / '.join(arc.venues[:6])}")
        if arc.keywords:
            lines.append(f"- 关键词：{'、'.join(arc.keywords[:8])}")
        lines.append("")

        lines.append("### 起")
        lines.append("")
        lines.append(arc.setup_text)
        lines.append("")

        lines.append("### 承")
        lines.append("")
        lines.append(arc.build_up_text)
        lines.append("")

        lines.append("### 转")
        lines.append("")
        lines.append(arc.turn_text)
        lines.append("")

        lines.append("### 合")
        lines.append("")
        lines.append(arc.synthesis_text)
        lines.append("")

        if arc.turning_points:
            lines.append("### 代表转折点")
            lines.append("")
            for index, item in enumerate(arc.turning_points, start=1):
                lines.append(f"{index}. {item.paper_label}：{item.reason}")
            lines.append("")

        if arc.reading_path:
            lines.append("### 导师带读路径")
            lines.append("")
            for index, item in enumerate(arc.reading_path, start=1):
                lines.append(f"{index}. {item.paper_label}：{item.reason}")
            lines.append("")

        if arc.year_moments:
            lines.append("### 年度推进")
            lines.append("")
            for moment in arc.year_moments:
                year_label = str(moment.year) if moment.year is not None else "未知年份"
                lines.append(f"- {year_label}：{moment.summary}")
            lines.append("")

        if arc.tensions:
            lines.append("### 仍然悬而未决的问题")
            lines.append("")
            for item in arc.tensions:
                lines.append(f"- {_trim_text(item, 120)}")
            lines.append("")

    if profiles:
        lines.append("## 语料说明")
        lines.append("")
        lines.append(
            "本报告来自已经完成的单篇解析产物聚合，而不是直接重新读取 PDF。它更适合作为领域地图和阅读导航，后续如果需要更强的因果叙事，可以在这层之上再加 LLM 叙事润色阶段。"
        )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _write_profiles_jsonl(output_path: Path, profiles: list[PaperProfile]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(json.dumps(profile.to_dict(), ensure_ascii=False) for profile in profiles)
    write_text(output_path, payload + ("\n" if payload else ""))


def _resolve_output_dir(input_roots: list[Path], explicit_output_dir: str | None) -> Path:
    if explicit_output_dir:
        return Path(explicit_output_dir).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    label = slugify("-".join(Path(root).name for root in input_roots[:2]), fallback="story-arcs")
    return (Path.cwd() / "runs" / f"{timestamp}-story-arcs-{label}").resolve()


def build_narrative_report(
    input_roots: list[Path],
    output_dir: Path,
    report_title: str,
    min_papers_per_arc: int = 6,
    max_arcs: int = 10,
    log_level: str = "INFO",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(level=log_level, run_dir=output_dir)

    append_stage_trace(output_dir, "discover_runs", "started", input_roots=[str(path) for path in input_roots])
    log_event("info", "Narrative report started", output_dir=output_dir, input_roots=[str(path) for path in input_roots])
    run_dir_strings = discover_analysis_run_dirs(input_roots)
    run_dirs = [Path(path) for path in run_dir_strings]
    append_stage_trace(output_dir, "discover_runs", "finished", discovered_runs=len(run_dirs))
    log_event("info", "Narrative run discovery finished", discovered_runs=len(run_dirs))

    append_stage_trace(output_dir, "build_profiles", "started", discovered_runs=len(run_dirs))
    profiles, profile_stats = load_paper_profiles(run_dirs)
    append_stage_trace(output_dir, "build_profiles", "finished", **profile_stats)
    log_event("info", "Narrative paper profiles built", **profile_stats)

    append_stage_trace(
        output_dir,
        "build_arcs",
        "started",
        unique_papers=len(profiles),
        min_papers_per_arc=min_papers_per_arc,
        max_arcs=max_arcs,
    )
    arcs = build_story_arcs(profiles, min_papers_per_arc=min_papers_per_arc, max_arcs=max_arcs)
    summary = _build_global_summary(profiles, arcs)
    append_stage_trace(output_dir, "build_arcs", "finished", arc_count=len(arcs))
    log_event("info", "Narrative story arcs built", arc_count=len(arcs))

    markdown = render_narrative_markdown(report_title, profiles, arcs, summary)
    document = build_report_document(markdown, title=report_title)

    profiles_jsonl_path = output_dir / "paper_profiles.jsonl"
    arcs_json_path = output_dir / "story_arcs.json"
    summary_json_path = output_dir / "narrative_summary.json"
    markdown_path = output_dir / "narrative_report.md"
    html_path = output_dir / "narrative_report.html"
    pdf_path = output_dir / "narrative_report.pdf"

    _write_profiles_jsonl(profiles_jsonl_path, profiles)
    write_json(arcs_json_path, [arc.to_dict() for arc in arcs])
    write_json(
        summary_json_path,
        {
            "report_title": report_title,
            "input_roots": [str(path) for path in input_roots],
            "generated_at": datetime.now().isoformat(),
            "profile_stats": profile_stats,
            "summary": summary,
            "output_files": {
                "paper_profiles_jsonl": str(profiles_jsonl_path),
                "story_arcs_json": str(arcs_json_path),
                "narrative_summary_json": str(summary_json_path),
                "markdown": str(markdown_path),
                "html": str(html_path),
                "pdf": str(pdf_path),
            },
        },
    )
    write_text(markdown_path, markdown)

    export_metadata = {
        "paper_count": len(profiles),
        "arc_count": len(arcs),
        "year_range": summary.get("year_range"),
        "venues": summary.get("venues"),
    }
    html_meta = export_html_report(document, html_path, metadata=export_metadata)
    pdf_meta = export_pdf_report(document, pdf_path, metadata=export_metadata)

    result = {
        "output_dir": str(output_dir),
        "profiles": len(profiles),
        "arcs": len(arcs),
        "paper_profiles_jsonl": str(profiles_jsonl_path),
        "story_arcs_json": str(arcs_json_path),
        "narrative_summary_json": str(summary_json_path),
        "markdown": str(markdown_path),
        "html": html_meta,
        "pdf": pdf_meta,
    }
    write_json(output_dir / "run_summary.json", result)
    log_event("info", "Narrative report finished", output_dir=output_dir, profiles=len(profiles), arcs=len(arcs))
    return result


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    input_roots = [Path(raw_value).expanduser().resolve() for raw_value in args.input_roots]
    output_dir = _resolve_output_dir(input_roots, args.output_dir)
    build_narrative_report(
        input_roots=input_roots,
        output_dir=output_dir,
        report_title=args.report_title,
        min_papers_per_arc=max(2, args.min_papers_per_arc),
        max_arcs=max(1, args.max_arcs),
        log_level=args.log_level,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
