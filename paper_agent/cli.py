from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import replace
from pathlib import Path

from paper_agent.batch_support import (
    BatchCollector,
    build_batch_jobs,
    iter_pdf_paths as _iter_pdf_paths,
    partition_batch_jobs,
    resolve_batch_root as _resolve_batch_root,
    resolve_collect_dir as _resolve_collect_dir,
    write_batch_json as _write_batch_json,
)
from paper_agent.config import RuntimeConfig
from paper_agent.runtime import configure_logging, log_event
from paper_agent.analysis.workflow import run_analysis
from paper_agent.utils import extract_markdown_title


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
        collector = BatchCollector(Path(args.collect_dir).expanduser().resolve())
        collected_path = collector.collect_report_pdf(
            result["report_exports"]["pdf"]["path"],
            str(Path(args.input_path).resolve()),
            (result.get("overview") or {}).get("paper_title") or extract_markdown_title(result.get("report_markdown") or ""),
        )
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

    collector = BatchCollector(collect_dir)
    jobs = build_batch_jobs(batch_root, pdf_paths, collector)

    _write_batch_json(batch_root / "batch_inputs.json", {"pdfs": [job.pdf_path for job in jobs]})

    completed, pending_jobs = partition_batch_jobs(jobs, skip_existing=bool(args.skip_existing))
    for skipped in completed:
        if skipped.get("status") == "skipped_existing":
            log_event("info", "Batch paper skipped", pdf_path=skipped["pdf_path"], collected_pdf_path=skipped["collected_pdf_path"])

    worker_count = max(1, min(args.batch_workers, len(pending_jobs))) if pending_jobs else 0
    if worker_count <= 1:
        for job in pending_jobs:
            log_event("info", "Batch paper started", pdf_path=job.pdf_path, run_dir=job.run_dir)
            try:
                result = _run_single_analysis_job(job.pdf_path, job.run_dir, config)
                result["collected_pdf_path"] = collector.collect_report_pdf(
                    result.get("report_pdf_path"),
                    job.pdf_path,
                    result.get("paper_title"),
                )
                completed.append(result)
                log_event(
                    "info",
                    "Batch paper finished",
                    pdf_path=job.pdf_path,
                    run_dir=result["run_dir"],
                    collected_pdf_path=result.get("collected_pdf_path"),
                )
            except Exception as exc:
                failed = job.to_dict()
                failed["status"] = "failed"
                failed["error"] = str(exc)
                completed.append(failed)
                log_event("error", "Batch paper failed", pdf_path=job.pdf_path, error=str(exc))
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(_run_single_analysis_job, job.pdf_path, job.run_dir, config): job
                for job in pending_jobs
            }
            for future in as_completed(future_map):
                job = future_map[future]
                log_event("info", "Batch paper joined", pdf_path=job.pdf_path, run_dir=job.run_dir)
                try:
                    result = future.result()
                    result["collected_pdf_path"] = collector.collect_report_pdf(
                        result.get("report_pdf_path"),
                        job.pdf_path,
                        result.get("paper_title"),
                    )
                    completed.append(result)
                    log_event(
                        "info",
                        "Batch paper finished",
                        pdf_path=job.pdf_path,
                        run_dir=result["run_dir"],
                        collected_pdf_path=result.get("collected_pdf_path"),
                    )
                except Exception as exc:
                    failed = job.to_dict()
                    failed["status"] = "failed"
                    failed["error"] = str(exc)
                    completed.append(failed)
                    log_event("error", "Batch paper failed", pdf_path=job.pdf_path, error=str(exc))

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
