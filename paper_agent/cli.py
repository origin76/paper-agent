from __future__ import annotations

import argparse
import json
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any

from paper_agent.config import RuntimeConfig
from paper_agent.runtime import configure_logging, log_event
from paper_agent.utils import build_collected_pdf_name, extract_markdown_title, slugify, write_json
from paper_agent.workflow import run_analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze one paper PDF or an entire directory of PDFs with a multi-stage Qwen + LangGraph workflow.",
    )
    parser.add_argument("input_path", help="Path to a PDF file or a directory containing PDFs")
    parser.add_argument("--output-dir", help="Directory where artifacts will be written. In batch mode this becomes the batch root.")
    parser.add_argument("--collect-dir", help="Directory where final PDF reports are copied in batch mode")
    parser.add_argument("--batch-workers", type=int, default=2, help="How many papers to process concurrently in directory mode")
    parser.add_argument("--limit", type=int, help="Only process the first N PDFs in directory mode")
    parser.add_argument("--recursive", action="store_true", help="Recursively scan subdirectories for PDFs in directory mode")
    parser.add_argument("--skip-existing", action="store_true", help="Skip a paper if its collected PDF already exists")
    parser.add_argument("--print-report", action="store_true", help="Print the final markdown report to stdout for single-file mode")
    parser.add_argument("--document-model", help="Override the file-grounded model used for PDF analysis stages")
    parser.add_argument("--analysis-model", help="Override the model used for critique and extension stages")
    parser.add_argument(
        "--disable-web-search",
        action="store_true",
        help="Disable model-side web search even if it is enabled in config",
    )
    return parser


def _iter_pdf_paths(input_dir: Path, recursive: bool) -> list[Path]:
    iterator = input_dir.rglob("*") if recursive else input_dir.iterdir()
    pdfs = [path.resolve() for path in iterator if path.is_file() and path.suffix.lower() == ".pdf"]
    return sorted(pdfs, key=lambda path: path.name.lower())


def _resolve_collect_dir(input_dir: Path, collect_dir: str | None) -> Path:
    if collect_dir:
        return Path(collect_dir).expanduser().resolve()
    return (input_dir / "paper-agent-final-pdfs").resolve()


def _resolve_batch_root(config: RuntimeConfig, input_dir: Path, output_dir: str | None) -> Path:
    if output_dir:
        return Path(output_dir).expanduser().resolve()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return (config.output_root / f"{timestamp}-batch-{slugify(input_dir.name or 'papers')}").resolve()


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
    if not isinstance(payload, dict):
        return {}
    return {
        str(source_pdf_path): value
        for source_pdf_path, value in payload.items()
        if isinstance(value, dict)
    }


def _save_collection_index(collect_dir: Path, collection_index: dict[str, dict[str, Any]]) -> None:
    write_json(_collection_index_path(collect_dir), collection_index)


def _collect_report_pdf(
    report_pdf_path: str | None,
    collect_dir: Path,
    source_pdf_path: str,
    report_title: str | None,
    used_names: set[str],
    collection_index: dict[str, dict[str, Any]],
) -> str | None:
    if not report_pdf_path:
        return None

    source_pdf_key = str(Path(source_pdf_path).resolve())
    previous_entry = collection_index.get(source_pdf_key) or {}
    previous_path_value = previous_entry.get("collected_pdf_path")
    previous_path = Path(previous_path_value).resolve() if previous_path_value else None
    previous_name = previous_path.name if previous_path and previous_path.parent == collect_dir else None

    if previous_name in used_names:
        used_names.remove(previous_name)

    output_name = _unique_output_name(build_collected_pdf_name(report_title, source_pdf_path), used_names)
    destination = collect_dir / output_name
    shutil.copy2(report_pdf_path, destination)

    if previous_path and previous_path.exists() and previous_path != destination and previous_path.parent == collect_dir:
        previous_path.unlink()

    collection_index[source_pdf_key] = {
        "paper_title": report_title or "",
        "collected_pdf_path": str(destination),
    }
    return str(destination)


def _make_job_run_dir(batch_root: Path, pdf_path: Path, used_names: dict[str, int]) -> Path:
    base = slugify(pdf_path.stem, fallback="paper")
    count = used_names.get(base, 0) + 1
    used_names[base] = count
    dir_name = base if count == 1 else f"{base}-{count}"
    return batch_root / dir_name


def _write_batch_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _run_single_analysis_job(pdf_path: str, output_dir: str, config: RuntimeConfig) -> dict[str, Any]:
    result = run_analysis(pdf_path=pdf_path, output_dir=output_dir, config=config)
    pdf_export = ((result.get("report_exports") or {}).get("pdf") or {}).get("path")
    report_markdown = str(result.get("report_markdown") or "")
    report_title = (
        (result.get("overview") or {}).get("paper_title")
        or extract_markdown_title(report_markdown)
        or Path(pdf_path).stem
    )
    return {
        "pdf_path": pdf_path,
        "run_dir": str(result["run_dir"]),
        "report_pdf_path": pdf_export,
        "paper_title": report_title,
        "status": "completed",
    }


def _run_single_file(args: argparse.Namespace, config: RuntimeConfig) -> int:
    result = run_analysis(
        pdf_path=args.input_path,
        output_dir=args.output_dir,
        config=config,
    )

    print(f"Artifacts written to: {result['run_dir']}")
    print(f"Final report: {result['run_dir']}/final_report.md")
    if result.get("report_exports", {}).get("html", {}).get("path"):
        print(f"HTML report: {result['report_exports']['html']['path']}")
    if result.get("report_exports", {}).get("pdf", {}).get("path"):
        print(f"PDF report: {result['report_exports']['pdf']['path']}")

    if args.collect_dir and result.get("report_exports", {}).get("pdf", {}).get("path"):
        collect_dir = Path(args.collect_dir).expanduser().resolve()
        collect_dir.mkdir(parents=True, exist_ok=True)
        used_names = {path.name for path in collect_dir.glob("*.pdf")}
        collection_index = _load_collection_index(collect_dir)
        collected_path = _collect_report_pdf(
            result["report_exports"]["pdf"]["path"],
            collect_dir,
            str(Path(args.input_path).resolve()),
            (result.get("overview") or {}).get("paper_title") or extract_markdown_title(result.get("report_markdown") or ""),
            used_names,
            collection_index,
        )
        _save_collection_index(collect_dir, collection_index)
        print(f"Collected PDF: {collected_path}")

    if args.print_report:
        print("")
        print(result["report_markdown"])

    return 0


def _run_directory_batch(args: argparse.Namespace, config: RuntimeConfig) -> int:
    input_dir = Path(args.input_path).expanduser().resolve()
    pdf_paths = _iter_pdf_paths(input_dir, recursive=args.recursive)
    if args.limit is not None:
        pdf_paths = pdf_paths[: max(0, args.limit)]
    if not pdf_paths:
        raise RuntimeError(f"No PDF files found under {input_dir}")

    batch_root = _resolve_batch_root(config, input_dir, args.output_dir)
    collect_dir = _resolve_collect_dir(input_dir, args.collect_dir)
    batch_root.mkdir(parents=True, exist_ok=True)
    collect_dir.mkdir(parents=True, exist_ok=True)
    configure_logging(level=config.log_level, run_dir=batch_root)

    log_event(
        "info",
        "Batch analysis started",
        input_dir=input_dir,
        pdf_count=len(pdf_paths),
        batch_root=batch_root,
        collect_dir=collect_dir,
        batch_workers=args.batch_workers,
        recursive=args.recursive,
        limit=args.limit,
    )

    used_run_dir_names: dict[str, int] = {}
    used_collect_names = {path.name for path in collect_dir.glob("*.pdf")}
    collection_index = _load_collection_index(collect_dir)
    jobs: list[dict[str, Any]] = []
    for pdf_path in pdf_paths:
        run_dir = _make_job_run_dir(batch_root, pdf_path, used_run_dir_names)
        indexed_collected_path = str((collection_index.get(str(pdf_path.resolve())) or {}).get("collected_pdf_path") or "")
        legacy_collected_path = str((collect_dir / f"{pdf_path.stem}.paper_agent.pdf").resolve())
        jobs.append(
            {
                "pdf_path": str(pdf_path),
                "run_dir": str(run_dir),
                "collected_pdf_path": indexed_collected_path or legacy_collected_path,
                "legacy_collected_pdf_path": legacy_collected_path,
            }
        )

    _write_batch_json(batch_root / "batch_inputs.json", {"pdfs": [job["pdf_path"] for job in jobs]})

    completed: list[dict[str, Any]] = []
    pending_jobs: list[dict[str, Any]] = []
    for job in jobs:
        indexed_path = Path(job["collected_pdf_path"]) if job.get("collected_pdf_path") else None
        legacy_path = Path(job["legacy_collected_pdf_path"]) if job.get("legacy_collected_pdf_path") else None
        if args.skip_existing and (
            (indexed_path is not None and indexed_path.exists()) or (legacy_path is not None and legacy_path.exists())
        ):
            skipped = dict(job)
            skipped["status"] = "skipped_existing"
            skipped["collected_pdf_path"] = str(indexed_path if indexed_path and indexed_path.exists() else legacy_path)
            completed.append(skipped)
            log_event("info", "Batch paper skipped", pdf_path=job["pdf_path"], collected_pdf_path=job["collected_pdf_path"])
            continue
        pending_jobs.append(job)

    worker_count = max(1, min(args.batch_workers, len(pending_jobs))) if pending_jobs else 0
    if worker_count <= 1:
        for job in pending_jobs:
            log_event("info", "Batch paper started", pdf_path=job["pdf_path"], run_dir=job["run_dir"])
            try:
                result = _run_single_analysis_job(job["pdf_path"], job["run_dir"], config)
                result["collected_pdf_path"] = _collect_report_pdf(
                    result.get("report_pdf_path"),
                    collect_dir,
                    job["pdf_path"],
                    result.get("paper_title"),
                    used_collect_names,
                    collection_index,
                )
                _save_collection_index(collect_dir, collection_index)
                completed.append(result)
                log_event(
                    "info",
                    "Batch paper finished",
                    pdf_path=job["pdf_path"],
                    run_dir=result["run_dir"],
                    collected_pdf_path=result.get("collected_pdf_path"),
                )
            except Exception as exc:
                failed = dict(job)
                failed["status"] = "failed"
                failed["error"] = str(exc)
                completed.append(failed)
                log_event("error", "Batch paper failed", pdf_path=job["pdf_path"], error=str(exc))
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run_single_analysis_job, job["pdf_path"], job["run_dir"], config): job
                for job in pending_jobs
            }
            for future in as_completed(future_map):
                job = future_map[future]
                log_event("info", "Batch paper joined", pdf_path=job["pdf_path"], run_dir=job["run_dir"])
                try:
                    result = future.result()
                    result["collected_pdf_path"] = _collect_report_pdf(
                        result.get("report_pdf_path"),
                        collect_dir,
                        job["pdf_path"],
                        result.get("paper_title"),
                        used_collect_names,
                        collection_index,
                    )
                    _save_collection_index(collect_dir, collection_index)
                    completed.append(result)
                    log_event(
                        "info",
                        "Batch paper finished",
                        pdf_path=job["pdf_path"],
                        run_dir=result["run_dir"],
                        collected_pdf_path=result.get("collected_pdf_path"),
                    )
                except Exception as exc:
                    failed = dict(job)
                    failed["status"] = "failed"
                    failed["error"] = str(exc)
                    completed.append(failed)
                    log_event("error", "Batch paper failed", pdf_path=job["pdf_path"], error=str(exc))

    completed.sort(key=lambda item: str(item.get("pdf_path") or ""))
    summary = {
        "input_dir": str(input_dir),
        "batch_root": str(batch_root),
        "collect_dir": str(collect_dir),
        "batch_workers": args.batch_workers,
        "pdf_count": len(pdf_paths),
        "completed_count": sum(1 for item in completed if item.get("status") == "completed"),
        "failed_count": sum(1 for item in completed if item.get("status") == "failed"),
        "skipped_count": sum(1 for item in completed if item.get("status") == "skipped_existing"),
        "items": completed,
    }
    _write_batch_json(batch_root / "batch_summary.json", summary)

    print(f"Batch root: {batch_root}")
    print(f"Collected PDFs: {collect_dir}")
    print(f"Completed: {summary['completed_count']} | Failed: {summary['failed_count']} | Skipped: {summary['skipped_count']}")
    for item in completed:
        status = item.get("status")
        if status == "completed":
            print(f"[ok] {item['pdf_path']} -> {item.get('collected_pdf_path') or item.get('report_pdf_path')}")
        elif status == "skipped_existing":
            print(f"[skip] {item['pdf_path']} -> {item['collected_pdf_path']}")
        else:
            print(f"[failed] {item['pdf_path']} -> {item.get('error')}")

    if summary["failed_count"] > 0:
        return 1
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    configure_logging()
    try:
        config = RuntimeConfig.from_env()
        configure_logging(level=config.log_level)

        if args.document_model:
            config = replace(config, document_model=args.document_model)
        if args.analysis_model:
            config = replace(config, analysis_model=args.analysis_model)
        if args.disable_web_search:
            config = replace(config, web_search_enabled=False)

        input_path = Path(args.input_path).expanduser().resolve()
        log_event(
            "info",
            "CLI arguments parsed",
            input_path=input_path,
            output_dir=args.output_dir,
            collect_dir=args.collect_dir,
            batch_workers=args.batch_workers,
            limit=args.limit,
            recursive=args.recursive,
            document_model=config.document_model,
            analysis_model=config.analysis_model,
            analysis_fallback_model=config.analysis_fallback_model,
            analysis_stream=config.analysis_stream,
            web_search_enabled=config.web_search_enabled,
        )

        if input_path.is_dir():
            return _run_directory_batch(args, config)
        if not input_path.is_file():
            raise RuntimeError(f"Input path does not exist: {input_path}")
        return _run_single_file(args, config)
    except Exception as exc:
        log_event("error", "CLI execution failed", error=str(exc))
        parser.exit(status=1, message=f"paper-agent failed: {exc}\n")


if __name__ == "__main__":
    raise SystemExit(main())
