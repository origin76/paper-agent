from __future__ import annotations

import argparse
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.runtime import configure_logging, log_event
from paper_agent.utils import build_collected_pdf_name, extract_markdown_title, write_json
from .exporters import build_report_document, export_html_report, export_pdf_report
from .report import render_report


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


def _unique_output_name(base_name: str, used_names: set[str]) -> str:
    if base_name not in used_names:
        used_names.add(base_name)
        return base_name

    stem = Path(base_name).stem
    suffix = Path(base_name).suffix
    counter = 2
    while True:
        candidate = f"{stem}-{counter}{suffix}"
        if candidate not in used_names:
            used_names.add(candidate)
            return candidate
        counter += 1


def _collection_index_path(collect_dir: Path) -> Path:
    return collect_dir / "collection_index.json"


def _load_collection_index(collect_dir: Path) -> dict[str, dict[str, Any]]:
    index_path = _collection_index_path(collect_dir)
    if not index_path.exists():
        return {}
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_collection_index(collect_dir: Path, collection_index: dict[str, dict[str, Any]]) -> None:
    write_json(_collection_index_path(collect_dir), collection_index)


def _rebuild_single_job(
    job: ExportRebuildJob,
    collect_dir: Path | None,
    used_collect_names: set[str],
    collection_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    markdown_text = _load_or_rerender_markdown(job)
    document = build_report_document(markdown_text)
    report_title = extract_markdown_title(markdown_text) or document.title or (Path(job.source_pdf_path).stem if job.source_pdf_path else "paper")

    html_result = export_html_report(document, job.output_dir / "final_report.html", metadata=job.metadata)
    pdf_result = export_pdf_report(document, job.output_dir / "final_report.pdf", metadata=job.metadata)

    collected_pdf_path: str | None = None
    if collect_dir is not None and job.source_pdf_path:
        source_pdf_key = str(Path(job.source_pdf_path).resolve())
        indexed_payload = collection_index.get(source_pdf_key) or {}
        previous_path_text = str(indexed_payload.get("collected_pdf_path") or "").strip()
        previous_path = Path(previous_path_text).resolve() if previous_path_text else None
        previous_name = previous_path.name if previous_path and previous_path.parent == collect_dir else None
        if previous_name:
            used_collect_names.discard(previous_name)

        output_name = build_collected_pdf_name(report_title, job.source_pdf_path)
        if previous_path and previous_path.parent == collect_dir:
            destination = previous_path
            used_collect_names.add(destination.name)
        else:
            output_name = _unique_output_name(output_name, used_collect_names)
            destination = collect_dir / output_name

        shutil.copy2(pdf_result["path"], destination)
        collected_pdf_path = str(destination)

        if previous_path and previous_path.exists() and previous_path != destination and previous_path.parent == collect_dir:
            previous_path.unlink()

        collection_index[source_pdf_key] = {
            "paper_title": report_title,
            "collected_pdf_path": collected_pdf_path,
        }

    return {
        "report_dir": str(job.output_dir),
        "markdown_path": str(job.markdown_path),
        "summary_path": str(job.summary_path) if job.summary_path else None,
        "source_pdf_path": job.source_pdf_path,
        "paper_title": report_title,
        "html_path": html_result["path"],
        "pdf_path": pdf_result["path"],
        "collected_pdf_path": collected_pdf_path,
    }


def _load_or_rerender_markdown(job: ExportRebuildJob) -> str:
    state = _load_report_state(job.output_dir, job.source_pdf_path)
    if state is None:
        return job.markdown_path.read_text(encoding="utf-8")

    markdown_text = render_report(state)
    job.markdown_path.write_text(markdown_text, encoding="utf-8")
    return markdown_text


def _load_report_state(output_dir: Path, source_pdf_path: str | None) -> dict[str, Any] | None:
    required_json = {
        "paper_text_meta": output_dir / "paper_text_meta.json",
        "overview": output_dir / "overview.json",
        "overview_meta": output_dir / "overview_meta.json",
        "web_research": output_dir / "web_research.json",
        "resource_discovery": output_dir / "resource_discovery.json",
        "structure": output_dir / "structure.json",
        "section_analyses": output_dir / "section_analyses.json",
        "experiment_review": output_dir / "experiment_review.json",
        "critique_meta": output_dir / "critique_meta.json",
        "extensions_meta": output_dir / "extensions_meta.json",
        "section_targets": output_dir / "section_targets.json",
        "url_resource_contexts": output_dir / "url_resource_contexts.json",
        "url_resource_enrichment": output_dir / "url_resource_enrichment.json",
        "url_resource_enrichment_meta": output_dir / "url_resource_enrichment_meta.json",
    }
    required_text = {
        "critique": output_dir / "critique.md",
        "extensions": output_dir / "extensions.md",
    }

    if any(not path.exists() for path in [*required_json.values(), *required_text.values()]):
        return None

    state: dict[str, Any] = {
        key: json.loads(path.read_text(encoding="utf-8"))
        for key, path in required_json.items()
    }
    for key, path in required_text.items():
        state[key] = path.read_text(encoding="utf-8")

    state["source_name"] = output_dir.name
    state["pdf_path"] = source_pdf_path or ""
    state["web_search_enabled"] = True
    return state


def _cleanup_untracked_collect_duplicates(collect_dir: Path, collection_index: dict[str, dict[str, Any]]) -> list[str]:
    tracked_paths = {
        str(Path(payload.get("collected_pdf_path") or "").resolve())
        for payload in collection_index.values()
        if str(payload.get("collected_pdf_path") or "").strip()
    }
    removed_paths: list[str] = []
    for path in collect_dir.glob("*.pdf"):
        if not re.search(r"-\d+\.pdf$", path.name):
            continue
        resolved = str(path.resolve())
        if resolved in tracked_paths:
            continue
        path.unlink(missing_ok=True)
        removed_paths.append(resolved)
    return removed_paths


def main() -> int:
    args = build_parser().parse_args()
    runs_root = Path(args.runs_root).expanduser().resolve()
    collect_dir = Path(args.collect_dir).expanduser().resolve() if args.collect_dir else None

    maintenance_dir = runs_root / f"export-rebuild-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    maintenance_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(level="INFO", run_dir=maintenance_dir)

    jobs = _discover_jobs(runs_root, limit=args.limit)
    used_collect_names = {path.name for path in collect_dir.glob("*.pdf")} if collect_dir else set()
    collection_index = _load_collection_index(collect_dir) if collect_dir else {}
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
        result = _rebuild_single_job(
            job,
            collect_dir=collect_dir,
            used_collect_names=used_collect_names,
            collection_index=collection_index,
        )
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

    removed_duplicates: list[str] = []
    if collect_dir:
        _save_collection_index(collect_dir, collection_index)
        removed_duplicates = _cleanup_untracked_collect_duplicates(collect_dir, collection_index)

    summary_path = maintenance_dir / "rebuild_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "runs_root": str(runs_root),
                "collect_dir": str(collect_dir) if collect_dir else None,
                "job_count": len(rebuilt),
                "removed_duplicate_paths": removed_duplicates,
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
