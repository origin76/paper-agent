from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("paper_agent")
LOGGER.setLevel(logging.DEBUG)
LOGGER.propagate = False


def _format_fields(fields: dict[str, Any]) -> str:
    return " ".join(f"{key}={value}" for key, value in fields.items() if value is not None)


def configure_logging(level: str = "INFO", run_dir: Path | None = None) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    console_handler = None
    for handler in LOGGER.handlers:
        if getattr(handler, "_paper_agent_console", False):
            console_handler = handler
            break

    if console_handler is None:
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler._paper_agent_console = True  # type: ignore[attr-defined]
        console_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        LOGGER.addHandler(console_handler)

    console_handler.setLevel(numeric_level)

    if run_dir is not None:
        for handler in list(LOGGER.handlers):
            if getattr(handler, "_paper_agent_file", False):
                LOGGER.removeHandler(handler)
                handler.close()

        file_handler = logging.FileHandler(run_dir / "run.log", mode="w", encoding="utf-8")
        file_handler._paper_agent_file = True  # type: ignore[attr-defined]
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s | %(levelname)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        LOGGER.addHandler(file_handler)


def log_event(level: str, event: str, **fields: Any) -> None:
    message = event
    formatted_fields = _format_fields(fields)
    if formatted_fields:
        message = f"{event} | {formatted_fields}"

    log_method = getattr(LOGGER, level.lower(), LOGGER.info)
    log_method(message)


def append_stage_trace(run_dir: str | Path, stage: str, status: str, **fields: Any) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "status": status,
        "fields": fields,
    }
    trace_path = Path(run_dir) / "stage_trace.jsonl"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    with trace_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
