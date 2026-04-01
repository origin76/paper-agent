from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from paper_agent.analysis.report_stage import build_report_run_summary, run_report_render_stage
from paper_agent.analysis.url_resource_stage import run_url_resource_enrichment_stage
from paper_agent.config import RuntimeConfig


def _make_config(output_root: Path, *, url_content_enrichment_enabled: bool = True) -> RuntimeConfig:
    return RuntimeConfig(
        api_key="test-key",
        base_url="https://example.com",
        document_model="qwen3.5-plus",
        analysis_model="qwen3.5-plus",
        analysis_fallback_model="qwen3.5-plus",
        analysis_stream=True,
        analysis_retry_attempts=2,
        analysis_retry_backoff_seconds=2.0,
        log_level="INFO",
        web_search_enabled=True,
        temperature=0.2,
        max_output_tokens=4096,
        request_timeout_seconds=180,
        max_sections=8,
        section_max_workers=4,
        analysis_enable_thinking=True,
        paper_context_max_chars=180000,
        section_target_chars=24000,
        pdf_extract_timeout_seconds=60,
        url_content_enrichment_enabled=url_content_enrichment_enabled,
        url_content_enrichment_max_urls=8,
        url_fetch_timeout_seconds=12,
        url_fetch_max_bytes=600000,
        url_fetch_max_text_chars=6000,
        output_root=output_root,
    )


class AnalysisStageHelperTests(unittest.TestCase):
    def test_build_report_run_summary_collects_key_metrics(self) -> None:
        with TemporaryDirectory() as tmp:
            config = _make_config(Path(tmp))
            summary = build_report_run_summary(
                {
                    "pdf_path": "/tmp/paper.pdf",
                    "run_dir": "/tmp/run",
                    "overview_meta": {"model": "doc-model"},
                    "critique_meta": {"model": "analysis-model", "requested_model": "analysis-model"},
                    "section_targets": ["Intro", "Method"],
                    "paper_text_meta": {"char_count": 12345},
                    "web_search_enabled": True,
                    "web_research": {"source_shortlist": [{"url": "https://example.com"}]},
                    "resource_discovery": {"code_repositories": [{"url": "https://github.com/example/repo"}]},
                    "url_resource_enrichment_meta": {"candidate_count": 3, "fetched_count": 2, "analyzed_page_count": 1},
                },
                config,
                "Test Paper",
            )

        self.assertEqual(summary["paper_title"], "Test Paper")
        self.assertEqual(summary["sections"], 2)
        self.assertEqual(summary["web_sources"], 1)
        self.assertEqual(summary["resource_repositories"], 1)
        self.assertEqual(summary["url_content_enrichment_candidates"], 3)

    def test_run_report_render_stage_writes_exports_and_summary(self) -> None:
        with TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            config = _make_config(run_dir)
            state = {
                "pdf_path": "/tmp/paper.pdf",
                "run_dir": str(run_dir),
                "source_name": "paper.pdf",
                "overview": {"paper_title": "Test Paper"},
                "overview_meta": {"model": "doc-model"},
                "critique_meta": {"model": "analysis-model", "requested_model": "analysis-model"},
                "section_targets": ["Intro"],
                "paper_text_meta": {"char_count": 42},
                "web_search_enabled": True,
                "web_research": {"source_shortlist": []},
                "resource_discovery": {"code_repositories": []},
                "url_resource_enrichment_meta": {"candidate_count": 0, "fetched_count": 0, "analyzed_page_count": 0},
            }

            with (
                patch("paper_agent.analysis.report_stage.render_report", return_value="# Test Paper\n\nBody\n"),
                patch("paper_agent.analysis.report_stage.build_report_document", return_value={"kind": "doc"}),
                patch(
                    "paper_agent.analysis.report_stage.export_html_report",
                    return_value={"format": "html", "path": str(run_dir / "final_report.html")},
                ),
                patch(
                    "paper_agent.analysis.report_stage.export_pdf_report",
                    return_value={"format": "pdf", "path": str(run_dir / "final_report.pdf")},
                ),
            ):
                state_update, finish_fields = run_report_render_stage(
                    config=config,
                    state=state,
                    run_dir=run_dir,
                    stage="render_report",
                )

            self.assertEqual(state_update["report_exports"]["html"]["path"], str(run_dir / "final_report.html"))
            self.assertEqual(state_update["report_exports"]["pdf"]["path"], str(run_dir / "final_report.pdf"))
            self.assertEqual(finish_fields["report_path"], run_dir / "final_report.md")
            self.assertTrue((run_dir / "final_report.md").exists())

            summary = json.loads((run_dir / "run_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["paper_title"], "Test Paper")
            self.assertEqual(summary["html_report_path"], str(run_dir / "final_report.html"))
            self.assertEqual(summary["pdf_report_path"], str(run_dir / "final_report.pdf"))

    def test_run_url_resource_enrichment_stage_handles_disabled_mode_without_candidates(self) -> None:
        with TemporaryDirectory() as tmp:
            run_dir = Path(tmp)
            config = _make_config(run_dir, url_content_enrichment_enabled=False)
            state_update, finish_fields = run_url_resource_enrichment_stage(
                config=config,
                state={
                    "web_research": {"source_shortlist": []},
                    "resource_discovery": {"code_repositories": []},
                },
                run_dir=run_dir,
                stage="url_resource_enrichment",
                candidates=[],
            )

            self.assertEqual(finish_fields["candidate_count"], 0)
            self.assertEqual(finish_fields["fetched_count"], 0)
            self.assertEqual(state_update["url_resource_enrichment"], {"pages": [], "search_fallback_pages": []})
            self.assertEqual(state_update["url_resource_enrichment_meta"]["analysis_meta"]["reason"], "disabled")
            self.assertTrue((run_dir / "url_resource_enrichment_meta.json").exists())
