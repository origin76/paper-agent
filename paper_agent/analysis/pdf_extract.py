from __future__ import annotations

import subprocess
import time
from pathlib import Path

from paper_agent.runtime import log_event
from paper_agent.utils import estimate_tokens, normalize_text


def extract_pdf_text(pdf_path: str, timeout_seconds: int, stage: str = "ingest_pdf") -> tuple[str, dict[str, int | str]]:
    path = Path(pdf_path).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"PDF not found: {path}")

    command = ["pdftotext", str(path), "-"]
    start_time = time.perf_counter()
    log_event("info", "PDF text extraction started", stage=stage, path=path, command=" ".join(command))

    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except Exception as exc:
        log_event(
            "error",
            "PDF text extraction failed",
            stage=stage,
            path=path,
            duration_seconds=f"{time.perf_counter() - start_time:.2f}",
            error=str(exc),
        )
        raise

    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="ignore").strip()
        log_event(
            "error",
            "PDF text extraction failed",
            stage=stage,
            path=path,
            duration_seconds=f"{time.perf_counter() - start_time:.2f}",
            returncode=result.returncode,
            stderr=stderr,
        )
        raise RuntimeError(f"pdftotext failed with exit code {result.returncode}: {stderr}")

    raw_text = result.stdout.decode("utf-8", errors="ignore").replace("\f", "\n\n")
    text = normalize_text(raw_text)
    metadata = {
        "extractor": "pdftotext",
        "char_count": len(text),
        "line_count": len(text.splitlines()),
        "estimated_tokens": estimate_tokens(text),
        "duration_seconds": round(time.perf_counter() - start_time, 2),
    }
    log_event(
        "info",
        "PDF text extraction finished",
        stage=stage,
        path=path,
        char_count=metadata["char_count"],
        estimated_tokens=metadata["estimated_tokens"],
        duration_seconds=metadata["duration_seconds"],
    )
    return text, metadata
