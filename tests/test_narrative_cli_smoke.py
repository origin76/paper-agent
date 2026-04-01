from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from paper_agent.narrative import main as narrative_main


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_run_dir(
    root: Path,
    *,
    name: str,
    pdf_path: str,
    paper_title: str,
    takeaway: str,
    problem_statement: str,
    prior_work_positioning: str,
    method_modules: list[str],
    year: int,
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
            "venue": "Proceedings of the ACM on Programming Languages (POPL 2025)",
            "publication_year": str(year),
            "one_sentence_takeaway": takeaway,
            "problem_statement": problem_statement,
            "prior_work_positioning": prior_work_positioning,
            "core_claims": [f"{paper_title} has a strong central claim."],
        },
    )
    _write_json(
        run_dir / "structure.json",
        {
            "method_modules": [{"name": value} for value in method_modules],
            "core_pipeline": ["step 1", "step 2"],
        },
    )
    _write_json(
        run_dir / "experiment_review.json",
        {
            "evaluation_goal": "Evaluate whether the approach scales in practice.",
            "experiments": [{"name": "Main evaluation"}],
            "missing_ablations": ["Need a broader workload study."],
            "reproducibility_risks": ["The artifact bundle is incomplete."],
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
    return run_dir


class NarrativeCLISmokeTests(unittest.TestCase):
    def test_main_generates_local_narrative_report(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            runs_root = root / "runs"
            output_dir = root / "story-arcs"
            runs_root.mkdir(parents=True, exist_ok=True)

            _make_run_dir(
                runs_root,
                name="run-2023",
                pdf_path="/tmp/papers/2023/Verifier.pdf",
                paper_title="验证路线 2023",
                takeaway="This paper unifies proof-guided refinement with verification.",
                problem_statement="How can verification scale?",
                prior_work_positioning="This work unifies two previously separate verification routes.",
                method_modules=["Proof-guided abstraction refinement", "Verified runtime"],
                year=2023,
            )
            _make_run_dir(
                runs_root,
                name="run-2024",
                pdf_path="/tmp/papers/2024/Verifier.pdf",
                paper_title="验证路线 2024",
                takeaway="A practical verified runtime for production systems.",
                problem_statement="How can verified runtimes become practical?",
                prior_work_positioning="Moves from theorem-heavy development to production constraints.",
                method_modules=["Verified runtime", "Runtime monitoring"],
                year=2024,
            )
            _make_run_dir(
                runs_root,
                name="run-2025",
                pdf_path="/tmp/papers/2025/Verifier.pdf",
                paper_title="验证路线 2025",
                takeaway="Scalable verification for heterogeneous systems.",
                problem_statement="How can verified systems survive heterogeneity?",
                prior_work_positioning="Rather than single-machine assumptions, this paper embraces heterogeneous execution.",
                method_modules=["Heterogeneous verification"],
                year=2025,
            )

            with patch.object(
                sys,
                "argv",
                [
                    "paper-agent-narrative",
                    str(runs_root),
                    "--output-dir",
                    str(output_dir),
                    "--min-papers-per-arc",
                    "2",
                    "--max-arcs",
                    "2",
                    "--log-level",
                    "WARNING",
                ],
            ):
                exit_code = narrative_main()

            self.assertEqual(exit_code, 0)
            self.assertTrue((output_dir / "paper_profiles.jsonl").exists())
            self.assertTrue((output_dir / "story_arcs.json").exists())
            self.assertTrue((output_dir / "narrative_report.md").exists())
            self.assertTrue((output_dir / "narrative_report.html").exists())
            self.assertTrue((output_dir / "narrative_report.pdf").exists())

            summary = json.loads((output_dir / "run_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["profiles"], 3)
            self.assertGreaterEqual(summary["arcs"], 1)


if __name__ == "__main__":
    unittest.main()
