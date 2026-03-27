from __future__ import annotations

import argparse
from dataclasses import replace

from paper_agent.config import RuntimeConfig
from paper_agent.runtime import configure_logging, log_event
from paper_agent.workflow import run_analysis


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Analyze a paper PDF with a multi-stage Qwen + LangGraph workflow.",
    )
    parser.add_argument("pdf", help="Path to the paper PDF")
    parser.add_argument("--output-dir", help="Directory where artifacts and the final report will be written")
    parser.add_argument("--print-report", action="store_true", help="Print the final markdown report to stdout")
    parser.add_argument("--document-model", help="Override the file-grounded model used for PDF analysis stages")
    parser.add_argument("--analysis-model", help="Override the model used for critique and extension stages")
    parser.add_argument(
        "--disable-web-search",
        action="store_true",
        help="Disable model-side web search even if it is enabled in config",
    )
    return parser


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

        log_event(
            "info",
            "CLI arguments parsed",
            pdf=args.pdf,
            output_dir=args.output_dir,
            document_model=config.document_model,
            analysis_model=config.analysis_model,
            analysis_fallback_model=config.analysis_fallback_model,
            analysis_stream=config.analysis_stream,
            web_search_enabled=config.web_search_enabled,
        )
        result = run_analysis(
            pdf_path=args.pdf,
            output_dir=args.output_dir,
            config=config,
        )
    except Exception as exc:
        log_event("error", "CLI execution failed", error=str(exc))
        parser.exit(status=1, message=f"paper-agent failed: {exc}\n")

    print(f"Artifacts written to: {result['run_dir']}")
    print(f"Final report: {result['run_dir']}/final_report.md")

    if args.print_report:
        print("")
        print(result["report_markdown"])

    return 0
