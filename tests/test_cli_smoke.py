from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from paper_agent import cli
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


class CLISmokeTests(unittest.TestCase):
    def test_main_runs_single_file_entrypoint_with_stubbed_analysis(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            pdf_path = root / "paper.pdf"
            run_dir = root / "run"
            pdf_path.write_bytes(b"%PDF-1.7\nsmoke\n")
            run_dir.mkdir(parents=True, exist_ok=True)
            config = _make_config(root)
            fake_result = {
                "run_dir": str(run_dir),
                "report_exports": {
                    "html": {"path": str(run_dir / "final_report.html")},
                    "pdf": {"path": str(run_dir / "final_report.pdf")},
                },
                "overview": {"paper_title": "CLI Smoke Paper"},
                "report_markdown": "# CLI Smoke Paper\n\nBody\n",
            }

            with (
                patch("paper_agent.cli.RuntimeConfig.from_env", return_value=config),
                patch("paper_agent.cli.run_analysis", return_value=fake_result) as run_analysis_mock,
                patch.object(
                    sys,
                    "argv",
                    [
                        "paper-agent",
                        str(pdf_path),
                        "--output-dir",
                        str(run_dir),
                    ],
                ),
                redirect_stdout(io.StringIO()) as stdout,
            ):
                exit_code = cli.main()

            self.assertEqual(exit_code, 0)
            run_analysis_mock.assert_called_once_with(
                pdf_path=str(pdf_path),
                output_dir=str(run_dir),
                config=config,
            )
            rendered = stdout.getvalue()
            self.assertIn("Artifacts written to:", rendered)
            self.assertIn("Final report:", rendered)
            self.assertIn("HTML report:", rendered)
            self.assertIn("PDF report:", rendered)


if __name__ == "__main__":
    unittest.main()
