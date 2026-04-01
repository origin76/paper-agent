from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from paper_agent.batch_support import BatchCollector, build_batch_jobs, partition_batch_jobs


class BatchSupportTests(unittest.TestCase):
    def test_batch_collector_build_job_uses_indexed_path_when_present(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            collect_dir = root / "collect"
            collect_dir.mkdir(parents=True, exist_ok=True)
            indexed_output = collect_dir / "indexed.paper_agent.pdf"
            indexed_output.write_bytes(b"%PDF-1.7\nindexed\n")
            source_pdf = root / "papers" / "alpha.pdf"
            source_pdf.parent.mkdir(parents=True, exist_ok=True)
            source_pdf.write_bytes(b"%PDF-1.7\nsource\n")
            (collect_dir / "collection_index.json").write_text(
                json.dumps(
                    {
                        str(source_pdf.resolve()): {
                            "paper_title": "Alpha",
                            "collected_pdf_path": str(indexed_output),
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            collector = BatchCollector(collect_dir)
            job = collector.build_job(source_pdf, root / "runs" / "alpha")

            self.assertEqual(job.collected_pdf_path, str(indexed_output))
            self.assertTrue(job.legacy_collected_pdf_path.endswith("alpha.paper_agent.pdf"))

    def test_batch_collector_collect_report_replaces_previous_output(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            collect_dir = root / "collect"
            source_pdf = root / "papers" / "alpha.pdf"
            source_pdf.parent.mkdir(parents=True, exist_ok=True)
            source_pdf.write_bytes(b"%PDF-1.7\nsource\n")
            old_output = collect_dir / "Old Title.paper_agent.pdf"
            old_output.parent.mkdir(parents=True, exist_ok=True)
            old_output.write_bytes(b"%PDF-1.7\nold\n")
            report_pdf = root / "report.pdf"
            report_pdf.write_bytes(b"%PDF-1.7\nnew report\n")
            (collect_dir / "collection_index.json").write_text(
                json.dumps(
                    {
                        str(source_pdf.resolve()): {
                            "paper_title": "Old Title",
                            "collected_pdf_path": str(old_output),
                        }
                    },
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

            collector = BatchCollector(collect_dir)
            new_path = collector.collect_report_pdf(str(report_pdf), str(source_pdf), "New Title")

            self.assertIsNotNone(new_path)
            self.assertTrue(Path(new_path).exists())
            self.assertFalse(old_output.exists())
            payload = json.loads((collect_dir / "collection_index.json").read_text(encoding="utf-8"))
            self.assertEqual(payload[str(source_pdf.resolve())]["paper_title"], "New Title")
            self.assertEqual(payload[str(source_pdf.resolve())]["collected_pdf_path"], new_path)

    def test_build_batch_jobs_avoids_ambiguous_legacy_skip_paths(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch_root = root / "runs"
            collect_dir = root / "collect"
            papers_dir = root / "papers"
            papers_dir.mkdir(parents=True, exist_ok=True)

            first_pdf = papers_dir / "same.pdf"
            second_pdf = papers_dir / "nested" / "same.pdf"
            third_pdf = papers_dir / "unique.pdf"
            second_pdf.parent.mkdir(parents=True, exist_ok=True)
            first_pdf.write_bytes(b"%PDF-1.7\nfirst\n")
            second_pdf.write_bytes(b"%PDF-1.7\nsecond\n")
            third_pdf.write_bytes(b"%PDF-1.7\nthird\n")

            legacy_output = collect_dir / "unique.paper_agent.pdf"
            legacy_output.parent.mkdir(parents=True, exist_ok=True)
            legacy_output.write_bytes(b"%PDF-1.7\nlegacy\n")

            collector = BatchCollector(collect_dir)
            jobs = build_batch_jobs(batch_root, [first_pdf, second_pdf, third_pdf], collector)
            completed, pending = partition_batch_jobs(jobs, skip_existing=True)

            self.assertEqual(len(jobs), 3)
            self.assertNotEqual(jobs[0].run_dir, jobs[1].run_dir)
            self.assertEqual(len(completed), 1)
            self.assertEqual(completed[0]["status"], "skipped_existing")
            self.assertEqual(completed[0]["pdf_path"], str(third_pdf.resolve()))
            self.assertEqual(len(pending), 2)
            self.assertFalse(any(job.pdf_path == str(first_pdf.resolve()) and job.legacy_collected_pdf_path for job in pending))
            self.assertFalse(any(job.pdf_path == str(second_pdf.resolve()) and job.legacy_collected_pdf_path for job in pending))


if __name__ == "__main__":
    unittest.main()
