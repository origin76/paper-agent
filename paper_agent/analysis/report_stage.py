from __future__ import annotations

from pathlib import Path
from typing import Any

from paper_agent.config import RuntimeConfig
from paper_agent.reporting.exporters import build_report_document, export_html_report, export_pdf_report
from paper_agent.reporting.report import render_report
from paper_agent.runtime import append_stage_trace, log_event
from paper_agent.utils import write_json, write_text


def build_report_run_summary(state: dict[str, Any], config: RuntimeConfig, report_title: str) -> dict[str, Any]:
    return {
        "pdf_path": state["pdf_path"],
        "run_dir": state["run_dir"],
        "paper_title": report_title,
        "document_model": state["overview_meta"].get("model"),
        "analysis_model": state["critique_meta"].get("model"),
        "requested_analysis_model": state["critique_meta"].get("requested_model"),
        "analysis_fallback_used": state["critique_meta"].get("fallback_used", False),
        "sections": len(state.get("section_targets") or []),
        "paper_char_count": state["paper_text_meta"].get("char_count"),
        "web_search_enabled": state.get("web_search_enabled", False),
        "web_sources": len((state.get("web_research") or {}).get("source_shortlist", [])),
        "resource_repositories": len((state.get("resource_discovery") or {}).get("code_repositories", [])),
        "section_max_workers": config.section_max_workers,
        "url_content_enrichment_enabled": config.url_content_enrichment_enabled,
        "url_content_enrichment_candidates": (state.get("url_resource_enrichment_meta") or {}).get("candidate_count"),
        "url_content_fetched_pages": (state.get("url_resource_enrichment_meta") or {}).get("fetched_count"),
        "url_content_analyzed_pages": (state.get("url_resource_enrichment_meta") or {}).get("analyzed_page_count"),
    }


def run_report_render_stage(
    *,
    config: RuntimeConfig,
    state: dict[str, Any],
    run_dir: Path,
    stage: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    report_markdown = render_report(state)
    report_title = state["overview"].get("paper_title") or state["source_name"]
    report_document = build_report_document(
        report_markdown,
        title=report_title,
    )
    write_text(run_dir / "final_report.md", report_markdown)

    summary = build_report_run_summary(state, config, report_title)
    html_export_meta = _export_report_format(
        report_document=report_document,
        run_dir=run_dir,
        stage=stage,
        format_name="html",
        output_path=run_dir / "final_report.html",
        metadata=summary,
    )
    pdf_export_meta = _export_report_format(
        report_document=report_document,
        run_dir=run_dir,
        stage=stage,
        format_name="pdf",
        output_path=run_dir / "final_report.pdf",
        metadata=summary,
    )

    report_exports = {
        "markdown": {
            "format": "markdown",
            "path": str(run_dir / "final_report.md"),
        },
        "html": html_export_meta,
        "pdf": pdf_export_meta,
    }
    write_json(run_dir / "report_export_meta.json", report_exports)
    summary.update(
        {
            "html_report_path": html_export_meta["path"],
            "pdf_report_path": pdf_export_meta["path"],
        }
    )
    write_json(run_dir / "run_summary.json", summary)
    return (
        {"report_markdown": report_markdown, "report_exports": report_exports},
        {
            "report_path": run_dir / "final_report.md",
            "html_report_path": run_dir / "final_report.html",
            "pdf_report_path": run_dir / "final_report.pdf",
        },
    )


def _export_report_format(
    *,
    report_document: Any,
    run_dir: Path,
    stage: str,
    format_name: str,
    output_path: Path,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    export_stage = f"{stage}.{format_name}_export"
    append_stage_trace(run_dir, export_stage, "started", output_path=str(output_path))
    log_event("info", f"Report {format_name.upper()} export started", stage=export_stage, output_path=output_path)

    exporter = export_html_report if format_name == "html" else export_pdf_report
    export_meta = exporter(
        report_document,
        output_path,
        metadata=metadata,
    )

    append_stage_trace(run_dir, export_stage, "finished", **export_meta)
    log_event("info", f"Report {format_name.upper()} export finished", stage=export_stage, output_path=output_path)
    return export_meta
