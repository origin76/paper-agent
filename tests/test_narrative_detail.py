from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from paper_agent.narrative_stack import detail as narrative_detail_impl
from paper_agent.narrative import build_story_arcs, load_paper_profiles
from paper_agent.narrative_detail import (
    ArcEvidenceBundle,
    DetailedReadingStep,
    DetailedSection,
    DetailedStoryArc,
    DetailedTension,
    DetailedTurningPoint,
    DetailedYearProgression,
    PaperEvidence,
    build_arc_evidence_bundle,
    load_narrative_inputs,
    _render_single_arc_markdown,
    render_detailed_narrative_markdown,
)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _make_run_dir(
    root: Path,
    *,
    name: str,
    pdf_path: str,
    paper_title: str,
    source_takeaway: str,
    problem_statement: str,
    prior_work_positioning: str,
    core_claims: list[str],
    method_modules: list[str],
    year: int,
    venue: str = "Proceedings of the ACM on Programming Languages (POPL 2024)",
) -> Path:
    run_dir = root / name
    run_dir.mkdir(parents=True, exist_ok=True)

    _write_json(
        run_dir / "run_summary.json",
        {
            "pdf_path": pdf_path,
            "paper_title": paper_title,
        },
    )
    _write_json(
        run_dir / "overview.json",
        {
            "paper_title": paper_title,
            "paper_type": "研究论文",
            "authors": ["Alice Example", "Bob Example"],
            "venue": venue,
            "publication_year": str(year),
            "one_sentence_takeaway": source_takeaway,
            "problem_statement": problem_statement,
            "prior_work_positioning": prior_work_positioning,
            "core_claims": core_claims,
        },
    )
    _write_json(
        run_dir / "structure.json",
        {
            "method_modules": [{"name": value} for value in method_modules],
            "core_pipeline": ["step 1", "step 2", "step 3"],
        },
    )
    _write_json(
        run_dir / "experiment_review.json",
        {
            "evaluation_goal": "Evaluate whether the proposed type-system pipeline scales and remains explainable.",
            "experiments": [{"name": "Main evaluation"}, {"name": "Annotation study"}],
            "missing_ablations": ["Need stronger annotation-effort ablation."],
            "reproducibility_risks": ["Proof scripts are not public."],
        },
    )
    _write_json(
        run_dir / "resource_discovery.json",
        {
            "official_pages": [{"url": "https://example.org/project"}],
            "code_repositories": [{"url": "https://github.com/example/project"}],
        },
    )
    _write_json(
        run_dir / "web_research.json",
        {
            "source_shortlist": [{"url": "https://example.org/paper"}],
        },
    )
    _write_json(
        run_dir / "section_analyses.json",
        [
            {
                "section_title": "1 Introduction",
                "section_role_in_paper": "界定问题、说明旧路线的盲点，并提出新的类型系统接口。",
                "reviewer_view": "问题动机明确，但需要更强的 annotation burden 证据。",
                "engineer_view": "核心实现依赖约束求解器与类型恢复模块的协同。",
                "verification_questions": ["如果没有完整注解，系统能推断到什么程度？"],
            },
            {
                "section_title": "4 Evaluation",
                "section_role_in_paper": "用实验说明新接口是否真正可用。",
                "reviewer_view": "实验口径有说服力，但跨工具基线仍偏少。",
                "engineer_view": "评测需要复现实验脚本和错误报告样例。",
                "verification_questions": ["和旧工具相比，错误解释能力到底提升了多少？"],
            },
        ],
    )
    _write_text(
        run_dir / "critique.md",
        """## 最薄弱环节

1. 缺乏 annotation burden 的定量证据。
2. 与旧工具的跨基线比较还不够强。

## 隐含假设

作者默认用户愿意接受更多类型注解。

## 阻碍接收的关键问题

需要展示更真实的开发体验和失败案例。
""",
    )
    _write_text(
        run_dir / "extensions.md",
        """## 如果我们继续做这条线

应该把重点从表达力转向可用性证据。

## 三个快速跟进实验

1. 做 annotation burden 量化。
2. 做错误报告可调试性实验。

## 仍然开放的问题

如何在不牺牲表达力的前提下降低学习门槛？
""",
    )
    return run_dir


class NarrativeDetailTests(unittest.TestCase):
    def test_reset_detail_output_dir_removes_stale_generated_artifacts(self) -> None:
        with TemporaryDirectory() as tmp:
            output_dir = Path(tmp)
            stale_files = [
                "evidence_bundles.json",
                "detailed_story_arcs.json",
                "detailed_narrative_report.md",
                "detailed_narrative_report.html",
                "detailed_narrative_report.pdf",
                "run_summary.json",
                "run.log",
                "stage_trace.jsonl",
            ]
            stale_dirs = ["arc_reports", "section_details", "debug"]

            for name in stale_files:
                _write_text(output_dir / name, "stale")
            for name in stale_dirs:
                _write_text(output_dir / name / "old.txt", "stale")
            _write_text(output_dir / "keep-me.txt", "keep")

            summary = narrative_detail_impl._reset_detail_output_dir(output_dir)

            self.assertEqual(sorted(summary["removed_files"]), sorted(stale_files))
            self.assertEqual(sorted(summary["removed_dirs"]), sorted(stale_dirs))
            for name in stale_files:
                self.assertFalse((output_dir / name).exists())
            for name in stale_dirs:
                self.assertFalse((output_dir / name).exists())
            self.assertTrue((output_dir / "keep-me.txt").exists())

    def test_load_narrative_inputs_and_build_arc_evidence_bundle(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dirs = [
                _make_run_dir(
                    root,
                    name=f"run-{year}",
                    pdf_path=f"/tmp/papers/{year}/Type Shift {year} [POPL {year}].pdf",
                    paper_title=f"类型路线 {year}",
                    source_takeaway=takeaway,
                    problem_statement=problem,
                    prior_work_positioning=prior,
                    core_claims=[f"Claim {year}", f"Second claim {year}"],
                    method_modules=modules,
                    year=year,
                )
                for year, takeaway, problem, prior, modules in [
                    (2022, "A practical type recovery pipeline for dependent interfaces.", "How can type recovery remain precise?", "Early exploration.", ["Type recovery", "Constraint solver"]),
                    (2023, "This paper reframes type recovery as an annotation-economics problem.", "How can type systems reduce annotation burden?", "Instead of pure expressiveness, focus on annotation cost.", ["Annotation metrics", "Type recovery"]),
                    (2024, "A reviewer-friendly type pipeline with better error messages.", "How can types stay explainable?", "Moves from proof elegance to development workflow.", ["Error reporting", "Constraint solver"]),
                    (2025, "A unified interface for local contextual type inference.", "How can types become modular and local?", "This work unifies multiple prior inference routes.", ["Local inference", "Type interface"]),
                ]
            ]

            profiles, _ = load_paper_profiles(run_dirs)
            arcs = build_story_arcs(profiles, min_papers_per_arc=3, max_arcs=5)

            narrative_root = root / "narrative"
            narrative_root.mkdir(parents=True, exist_ok=True)
            (narrative_root / "paper_profiles.jsonl").write_text(
                "\n".join(json.dumps(profile.to_dict(), ensure_ascii=False) for profile in profiles) + "\n",
                encoding="utf-8",
            )
            _write_json(narrative_root / "story_arcs.json", [arc.to_dict() for arc in arcs])
            _write_json(
                narrative_root / "narrative_summary.json",
                {
                    "report_title": "测试 narrative",
                    "summary": {"paper_count": len(profiles), "arc_count": len(arcs), "year_range": "2022-2025"},
                },
            )

            loaded_profiles, loaded_arcs, loaded_summary = load_narrative_inputs(narrative_root)

            self.assertEqual(len(loaded_profiles), 4)
            self.assertEqual(len(loaded_arcs), 1)
            self.assertEqual(loaded_summary["report_title"], "测试 narrative")

            bundle = build_arc_evidence_bundle(loaded_arcs[0], loaded_profiles, max_papers_per_arc=3)
            self.assertLessEqual(len(bundle.selected_papers), 3)
            self.assertTrue(bundle.selected_papers[0].reviewer_highlights)
            self.assertTrue(bundle.selected_papers[0].extension_highlights)
            self.assertTrue(bundle.selected_papers[0].section_highlights)

    def test_render_detailed_markdown_uses_long_form_sections(self) -> None:
        detailed_arc = DetailedStoryArc(
            theme_id="type_systems",
            title="类型系统与程序语义",
            synopsis="从表达力走向可用性与工程落地。",
            paper_count=4,
            year_range="2022-2025",
            keywords=["类型恢复", "局部推断"],
            venues=["POPL"],
            arc_overview="这条线真正的争议，不在于类型系统是否足够强，而在于强表达力怎样不把用户成本一并抬高。",
            setup=DetailedSection(
                section_key="setup",
                section_title="起：问题最初是如何被定义的",
                section_summary="早期论文把重点放在表达力缺口上。",
                paragraphs=[
                    "早期论文首先把问题定义成一个表达力缺口：旧有接口要么过于简单，无法精确描述依赖关系，要么证明负担过重，无法进入正常开发流程。",
                    "因此最初的任务不是优化工程细节，而是先证明这类类型接口在语义上值得被引入，并且能覆盖真实语言设计里最尴尬的边界案例。",
                ],
            ),
            build_up=DetailedSection(
                section_key="build_up",
                section_title="承：主流技术路线如何逐渐成形",
                section_summary="中段工作开始稳定出重复出现的方法模块。",
                paragraphs=[
                    "当问题被接受后，中段工作逐渐稳定出几类重复出现的模块，例如局部类型恢复、约束求解器、错误报告翻译层，以及将核心演算和实现接口分开的分层架构。",
                    "这说明共同体已经不再争论“该不该做”，而是在逼问“怎样才能让这件事被正常使用”。",
                ],
            ),
            turn=DetailedSection(
                section_key="turn",
                section_title="转：研究共同体为什么改变路线",
                section_summary="真正的转向发生在可用性取代纯表达力之后。",
                paragraphs=[
                    "真正的转向出现在研究者开始把注解成本、错误解释能力和局部推断稳定性纳入主问题之后。此时新工作不再满足于展示一个更强的演算，而是要求它能支撑正常的阅读、调试和维护流程。",
                    "也正因为这样，带有统一接口、局部上下文推断和更强错误反馈的论文，开始成为带动后续路线的核心节点。",
                ],
            ),
            synthesis=DetailedSection(
                section_key="synthesis",
                section_title="合：今天的收束、边界与新张力",
                section_summary="最新工作形成了新的共识，但张力仍未消失。",
                paragraphs=[
                    "今天这条线已经形成一个相对清晰的共识：高表达力本身不再足够，真正有价值的是能否把它做成低摩擦的工作流组件。",
                    "但张力并未消失，因为一旦追求更局部的推断、更友好的错误报告和更低的注解成本，就会立刻反过来压缩系统的表达自由度与证明空间。",
                ],
            ),
            turning_points_detailed=[
                DetailedTurningPoint(
                    paper_id="paper-1",
                    paper_label="《类型路线 2025》 (POPL 2025)",
                    year=2025,
                    what_changed="它把问题从表达力竞赛转成了局部推断和接口组织问题。",
                    why_it_mattered="这迫使后续论文开始同时面对理论强度和开发成本。",
                    reading_question="它到底替换了旧路线中的哪个默认前提？",
                )
            ],
            reading_path_detailed=[
                DetailedReadingStep(
                    paper_id="paper-0",
                    paper_label="《类型路线 2022》 (POPL 2022)",
                    year=2022,
                    stage_label="第一站",
                    why_read_now="先看问题最初怎样被表述。",
                    focus_question="作者最担心的表达力缺口到底是什么？",
                    next_connection="然后再去看中段论文怎样把这个问题转成可操作模块。",
                )
            ],
            year_progression_detailed=[
                DetailedYearProgression(
                    year=2024,
                    narrative="这一年开始明确把错误报告和开发体验写进主问题，而不仅是附属工程细节。",
                    representative_papers=["《类型路线 2024》 (POPL 2024)"],
                    shift="评价口径从语义健全性扩展到可调试性。",
                )
            ],
            open_tensions_detailed=[
                DetailedTension(
                    tension="如何在降低注解负担时保持表达力？",
                    why_it_persists="因为局部推断和弱注解天然会压缩可表达的依赖关系。",
                    what_to_watch="后续是否出现更强的局部推断算法或更可读的错误翻译层。",
                )
            ],
            source_paper_ids=["paper-0", "paper-1"],
            selected_paper_ids=["paper-0", "paper-1"],
            generated_at="2026-04-01T00:00:00",
        )

        markdown = render_detailed_narrative_markdown(
            report_title="测试细化报告",
            detailed_arcs=[detailed_arc],
            source_summary={"summary": {"paper_count": 4, "arc_count": 1, "year_range": "2022-2025"}},
        )

        self.assertIn("## 类型系统与程序语义", markdown)
        self.assertIn("### 起：问题最初是如何被定义的", markdown)
        self.assertIn("### 导师带读路径", markdown)
        self.assertIn("#### 2024", markdown)
        self.assertIn("这条线真正的争议", markdown)

    def test_render_single_arc_markdown_builds_standalone_booklet(self) -> None:
        detailed_arc = DetailedStoryArc(
            theme_id="type_systems",
            title="类型系统与程序语义",
            synopsis="从表达力走向可用性与工程落地。",
            paper_count=4,
            year_range="2022-2025",
            keywords=["类型恢复", "局部推断"],
            venues=["POPL"],
            arc_overview="这条线真正的争议，不在于类型系统是否足够强，而在于强表达力怎样不把用户成本一并抬高。",
            setup=DetailedSection(
                section_key="setup",
                section_title="起：问题最初是如何被定义的",
                section_summary="早期论文把重点放在表达力缺口上。",
                paragraphs=["早期论文首先把问题定义成一个表达力缺口。"],
            ),
            build_up=DetailedSection(
                section_key="build_up",
                section_title="承：主流技术路线如何逐渐成形",
                section_summary="中段工作开始稳定出重复出现的方法模块。",
                paragraphs=["中段工作逐渐稳定出几类重复出现的模块。"],
            ),
            turn=DetailedSection(
                section_key="turn",
                section_title="转：研究共同体为什么改变路线",
                section_summary="真正的转向发生在可用性取代纯表达力之后。",
                paragraphs=["研究者开始把注解成本和错误解释能力纳入主问题。"],
            ),
            synthesis=DetailedSection(
                section_key="synthesis",
                section_title="合：今天的收束、边界与新张力",
                section_summary="最新工作形成了新的共识，但张力仍未消失。",
                paragraphs=["今天的共识是高表达力必须和低摩擦工作流一起出现。"],
            ),
            turning_points_detailed=[
                DetailedTurningPoint(
                    paper_id="paper-1",
                    paper_label="《类型路线 2025》 (POPL 2025)",
                    year=2025,
                    what_changed="它把问题从表达力竞赛转成了局部推断和接口组织问题。",
                    why_it_mattered="这迫使后续论文开始同时面对理论强度和开发成本。",
                    reading_question="它到底替换了旧路线中的哪个默认前提？",
                )
            ],
            reading_path_detailed=[
                DetailedReadingStep(
                    paper_id="paper-0",
                    paper_label="《类型路线 2022》 (POPL 2022)",
                    year=2022,
                    stage_label="第一站",
                    why_read_now="先看问题最初怎样被表述。",
                    focus_question="作者最担心的表达力缺口到底是什么？",
                    next_connection="然后再去看中段论文怎样把这个问题转成可操作模块。",
                )
            ],
            year_progression_detailed=[
                DetailedYearProgression(
                    year=2024,
                    narrative="这一年开始明确把错误报告和开发体验写进主问题。",
                    representative_papers=["《类型路线 2024》 (POPL 2024)"],
                    shift="评价口径从语义健全性扩展到可调试性。",
                )
            ],
            open_tensions_detailed=[
                DetailedTension(
                    tension="如何在降低注解负担时保持表达力？",
                    why_it_persists="因为局部推断和弱注解天然会压缩可表达的依赖关系。",
                    what_to_watch="后续是否出现更强的局部推断算法或更可读的错误翻译层。",
                )
            ],
            source_paper_ids=["paper-0", "paper-1"],
            selected_paper_ids=["paper-0", "paper-1"],
            generated_at="2026-04-01T00:00:00",
        )
        bundle = ArcEvidenceBundle(
            theme_id="type_systems",
            title="类型系统与程序语义",
            synopsis="从表达力走向可用性与工程落地。",
            paper_count=4,
            year_range="2022-2025",
            keywords=["类型恢复", "局部推断"],
            venues=["POPL"],
            selected_paper_ids=["paper-0"],
            selected_papers=[
                PaperEvidence(
                    paper_id="paper-0",
                    paper_label="《类型路线 2022》 (POPL 2022)",
                    year=2022,
                    run_dir="/tmp/run-2022",
                    relevance_tags=["opening", "reading_step_1", "high_signal"],
                    takeaway="用更强的接口去补足旧路线的表达力缺口。",
                    problem_statement="类型恢复如何在表达力和用户成本之间取得平衡？",
                    prior_work_positioning="不同于只追求理论优雅的旧路线，这篇论文强调接口落地。",
                    core_claims=["更强的接口可以覆盖真实依赖关系。"],
                    method_modules=["类型恢复", "约束求解"],
                    core_pipeline=["问题定义", "约束生成", "恢复结果解释"],
                    evaluation_goal="验证新接口是否足够可用。",
                    experiment_names=["Main evaluation", "Annotation study"],
                    missing_ablations=["缺少更强的 annotation burden 对照。"],
                    reproducibility_risks=["Proof scripts 尚未公开。"],
                    section_highlights=["1 Introduction：界定问题并说明旧路线的盲点。"],
                    reviewer_highlights=["缺少 annotation burden 的定量证据。"],
                    extension_highlights=["把重点从表达力转向可用性证据。"],
                    verification_questions=["如果没有完整注解，系统能推断到什么程度？"],
                )
            ],
            turning_points=[],
            reading_path=[],
            year_moments=[],
            tensions=["如何在降低注解负担时保持表达力？"],
            year_distribution={"2022": 1},
        )

        markdown = _render_single_arc_markdown(detailed_arc, bundle)

        self.assertIn("## 这本小册子怎么读", markdown)
        self.assertIn("## 代表论文速读卡", markdown)
        self.assertIn("### 《类型路线 2022》 (POPL 2022)", markdown)
        self.assertIn("它在这条线里的位置：开场问题", markdown)
        self.assertIn("## 附：这条线的证据范围", markdown)
        self.assertIn("<!--PAGE_BREAK-->", markdown)


if __name__ == "__main__":
    unittest.main()
