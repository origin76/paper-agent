from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.exporters import build_report_document, export_html_report, export_pdf_report
from paper_agent.runtime import configure_logging, log_event


@dataclass(slots=True)
class ExportRebuildJob:
    markdown_path: Path
    output_dir: Path
    summary_path: Path | None
    metadata: dict[str, Any]
    source_pdf_path: str | None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rebuild historical HTML/PDF exports from final_report.md using the current exporter.",
    )
    parser.add_argument(
        "--runs-root",
        default="runs",
        help="Root directory to scan for final_report.md files.",
    )
    parser.add_argument(
        "--collect-dir",
        help="Optional collected-PDF directory. Existing collected PDFs with matching source stems will be replaced.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Only rebuild the first N report directories after sorting.",
    )
    return parser


def _find_nearest_run_summary(report_dir: Path, runs_root: Path) -> Path | None:
    current = report_dir
    while True:
        candidate = current / "run_summary.json"
        if candidate.exists():
            return candidate
        if current == runs_root or current.parent == current:
            return None
        current = current.parent


def _load_metadata(summary_path: Path | None) -> tuple[dict[str, Any], str | None]:
    if summary_path is None:
        return {}, None

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    metadata = {
        "document_model": payload.get("document_model"),
        "analysis_model": payload.get("analysis_model"),
        "sections": payload.get("sections"),
        "web_search_enabled": payload.get("web_search_enabled"),
        "paper_char_count": payload.get("paper_char_count"),
    }
    return metadata, payload.get("pdf_path")


def _discover_jobs(runs_root: Path, limit: int | None = None) -> list[ExportRebuildJob]:
    markdown_paths = sorted(runs_root.rglob("final_report.md"))
    if limit is not None:
        markdown_paths = markdown_paths[: max(0, limit)]

    jobs: list[ExportRebuildJob] = []
    for markdown_path in markdown_paths:
        output_dir = markdown_path.parent
        summary_path = _find_nearest_run_summary(output_dir, runs_root)
        metadata, source_pdf_path = _load_metadata(summary_path)
        jobs.append(
            ExportRebuildJob(
                markdown_path=markdown_path,
                output_dir=output_dir,
                summary_path=summary_path,
                metadata=metadata,
                source_pdf_path=source_pdf_path,
            )
        )
    return jobs


def _rebuild_single_job(job: ExportRebuildJob, collect_dir: Path | None) -> dict[str, Any]:
    markdown_text = job.markdown_path.read_text(encoding="utf-8")
    document = build_report_document(markdown_text)

    html_result = export_html_report(document, job.output_dir / "final_report.html", metadata=job.metadata)
    pdf_result = export_pdf_report(document, job.output_dir / "final_report.pdf", metadata=job.metadata)

    collected_pdf_path: str | None = None
    if collect_dir is not None and job.source_pdf_path:
        destination = collect_dir / f"{Path(job.source_pdf_path).stem}.paper_agent.pdf"
        if destination.exists():
            shutil.copy2(pdf_result["path"], destination)
            collected_pdf_path = str(destination)

    return {
        "report_dir": str(job.output_dir),
        "markdown_path": str(job.markdown_path),
        "summary_path": str(job.summary_path) if job.summary_path else None,
        "source_pdf_path": job.source_pdf_path,
        "html_path": html_result["path"],
        "pdf_path": pdf_result["path"],
        "collected_pdf_path": collected_pdf_path,
    }


def main() -> int:
    args = build_parser().parse_args()
    runs_root = Path(args.runs_root).expanduser().resolve()
    collect_dir = Path(args.collect_dir).expanduser().resolve() if args.collect_dir else None

    maintenance_dir = runs_root / f"export-rebuild-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    maintenance_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(level="INFO", run_dir=maintenance_dir)

    jobs = _discover_jobs(runs_root, limit=args.limit)
    log_event(
        "info",
        "Historical export rebuild started",
        runs_root=runs_root,
        job_count=len(jobs),
        collect_dir=collect_dir,
        maintenance_dir=maintenance_dir,
    )

    rebuilt: list[dict[str, Any]] = []
    for index, job in enumerate(jobs, start=1):
        log_event(
            "info",
            "Historical export rebuild job started",
            index=index,
            total=len(jobs),
            report_dir=job.output_dir,
            summary_path=job.summary_path,
        )
        result = _rebuild_single_job(job, collect_dir=collect_dir)
        rebuilt.append(result)
        log_event(
            "info",
            "Historical export rebuild job finished",
            index=index,
            total=len(jobs),
            report_dir=job.output_dir,
            pdf_path=result["pdf_path"],
            collected_pdf_path=result["collected_pdf_path"],
        )

    summary_path = maintenance_dir / "rebuild_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "runs_root": str(runs_root),
                "collect_dir": str(collect_dir) if collect_dir else None,
                "job_count": len(rebuilt),
                "rebuilt": rebuilt,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    log_event(
        "info",
        "Historical export rebuild finished",
        summary_path=summary_path,
        job_count=len(rebuilt),
    )
    print(summary_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
