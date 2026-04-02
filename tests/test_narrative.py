from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from paper_agent.narrative import PaperProfile, build_paper_profile, build_story_arcs, load_paper_profiles, render_narrative_markdown


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    missing_ablations: list[str] | None = None,
    reproducibility_risks: list[str] | None = None,
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
            "core_pipeline": ["step 1", "step 2"],
        },
    )
    _write_json(
        run_dir / "experiment_review.json",
        {
            "evaluation_goal": "Evaluate whether the proposed method scales.",
            "experiments": [{"name": "Main evaluation"}],
            "missing_ablations": missing_ablations or [],
            "reproducibility_risks": reproducibility_risks or [],
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


class NarrativeModuleTests(unittest.TestCase):
    def test_build_story_arcs_reassigns_over_broad_themes_for_popl_only_corpus(self) -> None:
        def make_profile(
            idx: int,
            title: str,
            primary_theme: str,
            theme_scores: dict[str, int],
        ) -> PaperProfile:
            return PaperProfile(
                paper_id=f"paper-{idx}",
                identity_key=f"id-{idx}",
                run_dir=f"/tmp/run-{idx}",
                pdf_path=f"/tmp/{idx}.pdf",
                display_title=title,
                source_title=title,
                venue="POPL",
                venue_short="POPL",
                publication_year=2024,
                authors=[],
                paper_type="研究论文",
                takeaway="",
                problem_statement="",
                prior_work_positioning="",
                core_claims=[],
                method_modules=[],
                core_pipeline=[],
                evaluation_goal="",
                experiment_names=[],
                missing_ablations=[],
                reproducibility_risks=[],
                code_resources=[],
                official_resources=[],
                keywords=[],
                theme_scores=theme_scores,
                primary_theme=primary_theme,
                secondary_themes=[],
                turning_markers=[],
                profile_quality=1.0,
            )

        profiles = [
            make_profile(
                1,
                "Quantum Local Reasoning",
                "accelerators_gpu",
                {"accelerators_gpu": 5, "quantum_reversible": 5, "verification_logic": 4},
            ),
            make_profile(
                2,
                "Symbolic Evaluation with Merging",
                "distributed_data",
                {"distributed_data": 6, "program_analysis": 3, "verification_logic": 2},
            ),
            make_profile(
                3,
                "Robot Demonstration Programs",
                "ml_ai_systems",
                {"ml_ai_systems": 5, "compilers_synthesis": 3, "type_systems": 2},
            ),
            make_profile(
                4,
                "Quantum Effects and Types",
                "quantum_reversible",
                {"quantum_reversible": 6, "type_systems": 4},
            ),
            make_profile(
                5,
                "Abstract Interpretation Precision",
                "program_analysis",
                {"program_analysis": 6, "verification_logic": 3},
            ),
            make_profile(
                6,
                "Sketch-Guided Synthesis",
                "compilers_synthesis",
                {"compilers_synthesis": 6, "type_systems": 3},
            ),
        ]

        arcs = build_story_arcs(profiles, min_papers_per_arc=1, max_arcs=10)
        theme_ids = {arc.theme_id for arc in arcs}

        self.assertIn("quantum_reversible", theme_ids)
        self.assertIn("program_analysis", theme_ids)
        self.assertIn("compilers_synthesis", theme_ids)
        self.assertNotIn("accelerators_gpu", theme_ids)
        self.assertNotIn("distributed_data", theme_ids)
        self.assertNotIn("ml_ai_systems", theme_ids)

    def test_build_paper_profile_infers_theme_and_source_title(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dir = _make_run_dir(
                root,
                name="run-a",
                pdf_path="/tmp/papers/2024/Gradual Type Recovery [POPL 2024].pdf",
                paper_title="渐进类型恢复",
                source_takeaway="This paper improves gradual typing with a practical type inference pipeline.",
                problem_statement="Gradual typing remains hard to scale in the presence of dependent types.",
                prior_work_positioning="Instead of a monolithic type checker, this paper separates inference from recovery.",
                core_claims=["The system improves gradual typing ergonomics."],
                method_modules=["Gradual typing pipeline", "Type inference repair"],
                year=2024,
            )

            profile = build_paper_profile(run_dir)

            assert profile is not None
            self.assertEqual(profile.source_title, "Gradual Type Recovery")
            self.assertEqual(profile.primary_theme, "type_systems")
            self.assertIn("Gradual typing pipeline", profile.keywords)

    def test_load_paper_profiles_deduplicates_by_pdf_path(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            poorer = _make_run_dir(
                root,
                name="run-old",
                pdf_path="/tmp/papers/2023/Verifier.pdf",
                paper_title="早期验证器",
                source_takeaway="A verifier.",
                problem_statement="Verification is useful.",
                prior_work_positioning="Builds on prior work.",
                core_claims=["One claim."],
                method_modules=["Verifier"],
                year=2023,
            )
            richer = _make_run_dir(
                root,
                name="run-new",
                pdf_path="/tmp/papers/2023/Verifier.pdf",
                paper_title="更完整的验证器",
                source_takeaway="A verified system with proof-guided abstraction refinement.",
                problem_statement="Formal verification for systems remains expensive and brittle.",
                prior_work_positioning="This work unifies verification with proof-guided abstraction refinement.",
                core_claims=["Claim A", "Claim B", "Claim C"],
                method_modules=["Proof-guided abstraction refinement", "Verified runtime"],
                year=2023,
                missing_ablations=["No ablation on proof search."],
            )

            profiles, stats = load_paper_profiles([poorer, richer])

            self.assertEqual(len(profiles), 1)
            self.assertEqual(stats["duplicate_run_dirs"], 1)
            self.assertEqual(profiles[0].display_title, "更完整的验证器")
            self.assertEqual(profiles[0].primary_theme, "verification_logic")

    def test_build_story_arcs_and_markdown_report(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            run_dirs = [
                _make_run_dir(
                    root,
                    name=f"run-{year}",
                    pdf_path=f"/tmp/papers/{year}/Proof Shift {year} [POPL {year}].pdf",
                    paper_title=f"验证路线 {year}",
                    source_takeaway=takeaway,
                    problem_statement=problem,
                    prior_work_positioning=prior,
                    core_claims=[f"Claim {year}", f"Second claim {year}"],
                    method_modules=modules,
                    year=year,
                    missing_ablations=["Need stronger ablations."],
                    reproducibility_risks=["Proof scripts are not public."],
                )
                for year, takeaway, problem, prior, modules in [
                    (2021, "Verification starts from semantic soundness.", "How can verified runtimes be made sound?", "Early exploration.", ["Semantic proof"]),
                    (2022, "Verification becomes more compositional.", "How can proof obligations be modular?", "Builds compositional proof rules.", ["Compositional proofs"]),
                    (2023, "This paper unifies proof-guided abstraction refinement with verification.", "How can verification scale?", "This work unifies two previous verification routes.", ["Proof-guided abstraction refinement"]),
                    (2024, "A practical verified runtime for production systems.", "How can verified runtimes become practical?", "Moves from theorem-heavy development to production constraints.", ["Verified runtime", "Runtime monitoring"]),
                    (2025, "Scalable verification for heterogeneous systems.", "How can verified systems survive heterogeneity?", "Rather than single-machine assumptions, this paper embraces heterogeneous execution.", ["Heterogeneous verification"]),
                    (2026, "Production verification now focuses on operating cost and deployment friction.", "How can verification fit production rollouts?", "Revisiting verification through deployment economics.", ["Deployment-aware verification"]),
                ]
            ]

            profiles, _ = load_paper_profiles(run_dirs)
            arcs = build_story_arcs(profiles, min_papers_per_arc=3, max_arcs=5)
            summary = {
                "paper_count": len(profiles),
                "arc_count": len(arcs),
                "year_range": "2021-2026",
                "venues": {"POPL": len(profiles)},
            }
            markdown = render_narrative_markdown("测试叙事报告", profiles, arcs, summary)

            self.assertEqual(len(arcs), 1)
            self.assertEqual(arcs[0].title, "程序验证与形式化推理")
            self.assertEqual(arcs[0].year_range, "2021-2026")
            self.assertGreaterEqual(len(arcs[0].turning_points), 1)
            self.assertIn("## 程序验证与形式化推理", markdown)
            self.assertIn("### 转", markdown)
            self.assertIn("### 导师带读路径", markdown)


if __name__ == "__main__":
    unittest.main()
