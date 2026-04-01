from __future__ import annotations

import unittest

from paper_agent.reporting.report import render_report


class ReportingReportTests(unittest.TestCase):
    def test_render_report_with_validated_reference_entries(self) -> None:
        state = {
            "overview": {
                "paper_title": "Test Paper",
                "one_sentence_takeaway": "一句话总结",
                "authors": ["Alice Example"],
                "venue": "POPL",
                "publication_year": "2025",
                "paper_type": "研究论文",
                "problem_statement": "解决一个问题",
                "prior_work_positioning": "对比旧方法",
                "core_claims": ["更快"],
            },
            "source_name": "test.pdf",
            "pdf_path": "/tmp/test.pdf",
            "paper_text_meta": {"char_count": 123},
            "overview_meta": {"model": "doc-model"},
            "critique_meta": {"model": "analysis-model"},
            "section_targets": ["Introduction"],
            "web_search_enabled": True,
            "web_research": {
                "source_shortlist": [
                    {
                        "title": "Test Paper Artifact",
                        "url": "https://github.com/example/test-paper",
                        "type": "github_repository",
                        "why_relevant": "源码入口",
                    }
                ]
            },
            "resource_discovery": {
                "code_repositories": [
                    {
                        "title": "Test Paper Artifact",
                        "url": "https://github.com/example/test-paper",
                        "repo_kind": "github_repository",
                        "why_relevant": "实现仓库",
                    }
                ]
            },
            "url_resource_contexts": [
                {
                    "url": "https://github.com/example/test-paper",
                    "final_url": "https://github.com/example/test-paper",
                    "html_title": "Test Paper Repository",
                }
            ],
            "url_resource_enrichment": {},
            "url_resource_enrichment_meta": {},
            "structure": {
                "problem": "这是结构化问题摘要",
                "core_pipeline": ["step1", "step2"],
            },
            "section_analyses": [],
            "experiment_review": {},
            "critique": "## 最薄弱环节\n\n这里有批判内容。",
            "extensions": "## 下一步\n\n这里有延伸内容。",
        }

        report = render_report(state)

        self.assertIn("# Test Paper", report)
        self.assertIn("## 3. 外部视角补充", report)
        self.assertIn("## 8. 参考链接页", report)
        self.assertIn("https://github.com/example/test-paper", report)

