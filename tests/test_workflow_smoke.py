from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from paper_agent.analysis.workflow import run_analysis
from paper_agent.config import RuntimeConfig


def _make_config(output_root: Path) -> RuntimeConfig:
    return RuntimeConfig(
        api_key="test-key",
        base_url="https://example.com",
        document_model="qwen3.5-plus",
        analysis_model="qwen3.5-plus",
        analysis_fallback_model="qwen3.5-plus",
        analysis_stream=True,
        analysis_retry_attempts=2,
        analysis_retry_backoff_seconds=2.0,
        log_level="WARNING",
        web_search_enabled=False,
        temperature=0.2,
        max_output_tokens=4096,
        request_timeout_seconds=180,
        max_sections=8,
        section_max_workers=2,
        analysis_enable_thinking=True,
        paper_context_max_chars=180000,
        section_target_chars=24000,
        pdf_extract_timeout_seconds=60,
        url_content_enrichment_enabled=False,
        url_content_enrichment_max_urls=8,
        url_fetch_timeout_seconds=12,
        url_fetch_max_bytes=600000,
        url_fetch_max_text_chars=6000,
        output_root=output_root,
    )


def _sample_sections() -> list[dict[str, object]]:
    return [
        {
            "title": "1 Introduction",
            "content": "The paper studies a memory-efficient inference pipeline.",
            "start_line": 1,
            "end_line": 20,
            "char_count": 120,
            "title_quality": 6,
        },
        {
            "title": "2 Method",
            "content": "The method combines cache reuse with request scheduling.",
            "start_line": 21,
            "end_line": 60,
            "char_count": 140,
            "title_quality": 6,
        },
        {
            "title": "4 Evaluation",
            "content": "The evaluation measures latency, throughput, and memory use.",
            "start_line": 61,
            "end_line": 100,
            "char_count": 150,
            "title_quality": 6,
        },
    ]


class FakeKimiClient:
    def __init__(self, config: RuntimeConfig):
        self.config = config

    @staticmethod
    def is_model_availability_error(error: Exception) -> bool:
        return False

    def chat_json_with_text(
        self,
        text: str,
        prompt: str,
        *,
        model: str | None = None,
        enable_search: bool | None = None,
        stage: str = "chat_json_with_text",
        source_label: str | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        meta = {
            "model": model or self.config.document_model,
            "stage": stage,
            "enable_search": enable_search,
            "source_label": source_label,
        }
        if stage == "global_overview":
            return (
                {
                    "paper_title": "Workflow Smoke Paper",
                    "one_sentence_takeaway": "A smoke-tested workflow for memory-efficient inference analysis.",
                    "authors": ["Alice Example", "Bob Example"],
                    "venue": "OSDI",
                    "publication_year": "2026",
                    "paper_type": "研究论文",
                    "problem_statement": "Large-model inference can become memory-bound too early.",
                    "why_this_problem_matters": "That bottleneck limits practical deployment costs.",
                    "prior_work_positioning": "This work focuses on memory pressure instead of only raw throughput.",
                    "core_claims": [
                        "Cache reuse reduces memory pressure.",
                        "A scheduler keeps latency stable.",
                    ],
                    "read_order": ["Introduction", "Method", "Evaluation"],
                    "must_clarify_questions": ["Which bottleneck is actually removed?"],
                },
                meta,
            )
        if stage == "structure_breakdown":
            return (
                {
                    "problem": "Inference systems hit memory limits before they saturate compute.",
                    "assumptions": ["Requests can share cache state."],
                    "method_modules": [
                        {"name": "Paged cache reuse"},
                        {"name": "Latency-aware scheduler"},
                    ],
                    "core_pipeline": [
                        "Extract relevant request state",
                        "Reuse cache pages safely",
                        "Schedule batches under latency limits",
                    ],
                    "decision_points": [
                        {"decision": "Reuse cache pages", "tradeoff": "Saves memory but adds bookkeeping."},
                    ],
                    "claim_to_evidence_map": [
                        {"claim": "Memory drops", "evidence": "Evaluation compares peak usage."},
                    ],
                    "section_map": [
                        {"section_title": "Introduction", "priority": "high"},
                        {"section_title": "Method", "priority": "high"},
                        {"section_title": "Evaluation", "priority": "medium"},
                    ],
                },
                meta,
            )
        if stage == "experiment_review":
            return (
                {
                    "evaluation_goal": "Show that the system lowers memory without breaking latency.",
                    "overall_support_for_claims": "The evidence is directionally strong for the main claim.",
                    "experiments": [
                        {
                            "name": "Peak memory benchmark",
                            "claim_tested": "Cache reuse lowers memory pressure.",
                            "reviewer_notes": "The benchmark directly targets the main bottleneck.",
                            "evidence_strength": "strong",
                        },
                        {
                            "name": "Tail latency benchmark",
                            "claim_tested": "Scheduling overhead stays acceptable.",
                            "reviewer_notes": "The latency story is promising but narrower than the memory story.",
                            "evidence_strength": "medium",
                            "possible_bias": ["Only one deployment regime is shown."],
                        },
                    ],
                    "missing_ablations": ["Separate the cache benefit from the scheduler benefit."],
                    "reproducibility_risks": ["The production traces are not public."],
                },
                meta,
            )
        if stage.startswith("section_deep_dive."):
            section_title = stage.split(".", 1)[1]
            return (
                {
                    "section_title": section_title,
                    "section_role_in_paper": f"{section_title} defines one essential step in the paper argument.",
                    "author_view": f"{section_title} tries to convince the reader that the design is necessary.",
                    "reviewer_view": f"{section_title} needs careful validation of its claimed benefit.",
                    "engineer_view": f"{section_title} is where an implementation team would focus first.",
                    "verification_questions": [f"What would falsify the claims made in {section_title}?"],
                },
                meta,
            )
        raise AssertionError(f"Unexpected chat_json_with_text stage: {stage}")

    def chat_json(
        self,
        messages: list[dict[str, object]],
        *,
        model: str | None = None,
        enable_thinking: bool | None = None,
        enable_search: bool | None = None,
        stage: str = "chat_json",
    ) -> tuple[dict[str, object], dict[str, object]]:
        raise AssertionError(f"chat_json should not be called in this smoke test: {stage}")

    def chat_text(
        self,
        messages: list[dict[str, object]],
        *,
        model: str | None = None,
        enable_thinking: bool | None = None,
        enable_search: bool | None = None,
        stage: str = "chat_text",
    ) -> tuple[str, dict[str, object]]:
        meta = {
            "model": model or self.config.analysis_model,
            "stage": stage,
            "enable_search": enable_search,
        }
        if stage == "critique":
            return (
                "## 最薄弱环节\n\n证据覆盖面还不够广。\n\n## 隐含假设\n\n作者默认共享缓存不会引入额外复杂性。\n",
                meta,
            )
        if stage == "extensions":
            return (
                "## 如果继续做这条线\n\n下一步应该把重点放到更广的部署条件上。\n\n## 三个快速跟进实验\n\n1. 扩展 workload。\n2. 分离模块贡献。\n",
                meta,
            )
        raise AssertionError(f"Unexpected chat_text stage: {stage}")


class WorkflowSmokeTests(unittest.TestCase):
    def test_run_analysis_smoke_builds_expected_artifacts(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            pdf_path = root / "paper.pdf"
            run_dir = root / "workflow-run"
            config = _make_config(root)
            paper_text = (
                "Introduction\n"
                "This paper studies memory-efficient inference.\n\n"
                "Method\n"
                "We combine cache reuse and scheduling.\n\n"
                "Evaluation\n"
                "We measure latency and memory.\n"
            )
            pdf_path.write_bytes(b"%PDF-1.7\nworkflow smoke\n")

            with (
                patch("paper_agent.analysis.workflow.KimiClient", FakeKimiClient),
                patch(
                    "paper_agent.analysis.workflow.extract_pdf_text",
                    return_value=(
                        paper_text,
                        {"char_count": len(paper_text), "extractor": "stub"},
                    ),
                ),
                patch("paper_agent.analysis.workflow.detect_sections", return_value=_sample_sections()),
            ):
                result = run_analysis(
                    pdf_path=str(pdf_path),
                    output_dir=str(run_dir),
                    config=config,
                )

            self.assertEqual(result["overview"]["paper_title"], "Workflow Smoke Paper")
            self.assertEqual(result["run_dir"], str(run_dir.resolve()))

            expected_files = [
                "paper_text.txt",
                "overview.json",
                "structure.json",
                "section_analyses.json",
                "experiment_review.json",
                "critique.md",
                "extensions.md",
                "final_report.md",
                "final_report.html",
                "final_report.pdf",
                "run_summary.json",
                "report_export_meta.json",
                "cleanup_result.json",
                "stage_trace.jsonl",
            ]
            for name in expected_files:
                self.assertTrue((run_dir / name).exists(), msg=f"missing artifact: {name}")

            summary = json.loads((run_dir / "run_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["paper_title"], "Workflow Smoke Paper")
            self.assertEqual(summary["sections"], 3)
            self.assertFalse(summary["web_search_enabled"])


if __name__ == "__main__":
    unittest.main()
