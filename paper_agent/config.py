from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from paper_agent.utils import slugify


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _parse_int(value: str | None, default: int) -> int:
    if value is None:
        return default
    return int(value.strip())


def _parse_float(value: str | None, default: float) -> float:
    if value is None:
        return default
    return float(value.strip())


def _load_local_env_files() -> None:
    candidates = [
        Path.cwd() / ".env",
        Path.cwd() / ".env.example",
        Path(__file__).resolve().parent.parent / ".env",
        Path(__file__).resolve().parent.parent / ".env.example",
    ]
    loaded: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in loaded or not resolved.exists():
            continue
        load_dotenv(dotenv_path=resolved, override=False)
        loaded.add(resolved)


@dataclass(frozen=True)
class RuntimeConfig:
    api_key: str
    base_url: str
    document_model: str
    analysis_model: str
    analysis_fallback_model: str | None
    analysis_stream: bool
    analysis_retry_attempts: int
    analysis_retry_backoff_seconds: float
    log_level: str
    web_search_enabled: bool
    temperature: float
    max_output_tokens: int
    request_timeout_seconds: int
    max_sections: int
    section_max_workers: int
    analysis_enable_thinking: bool
    paper_context_max_chars: int
    section_target_chars: int
    pdf_extract_timeout_seconds: int
    output_root: Path

    @classmethod
    def from_env(cls) -> "RuntimeConfig":
        _load_local_env_files()

        api_key = (
            os.getenv("DASHSCOPE_API_KEY")
            or os.getenv("PAPER_AGENT_API_KEY")
            or os.getenv("MOONSHOT_API_KEY")
        )
        if not api_key:
            raise RuntimeError(
                "DASHSCOPE_API_KEY is required. This version targets DashScope's OpenAI-compatible endpoint."
            )

        output_root = Path(os.getenv("PAPER_AGENT_OUTPUT_ROOT", "runs")).expanduser()
        web_search_enabled = _parse_bool(os.getenv("PAPER_AGENT_WEB_SEARCH_ENABLED"), True)
        document_model = os.getenv("PAPER_AGENT_DOCUMENT_MODEL", "qwen3.5-plus")
        analysis_model = os.getenv("PAPER_AGENT_ANALYSIS_MODEL", "qwen3.5-plus")
        analysis_fallback_model = os.getenv("PAPER_AGENT_ANALYSIS_FALLBACK_MODEL") or document_model

        return cls(
            api_key=api_key,
            base_url=os.getenv("PAPER_AGENT_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
            document_model=document_model,
            analysis_model=analysis_model,
            analysis_fallback_model=analysis_fallback_model,
            analysis_stream=_parse_bool(os.getenv("PAPER_AGENT_ANALYSIS_STREAM"), True),
            analysis_retry_attempts=_parse_int(os.getenv("PAPER_AGENT_ANALYSIS_RETRY_ATTEMPTS"), 2),
            analysis_retry_backoff_seconds=_parse_float(
                os.getenv("PAPER_AGENT_ANALYSIS_RETRY_BACKOFF_SECONDS"),
                2.0,
            ),
            log_level=os.getenv("PAPER_AGENT_LOG_LEVEL", "INFO"),
            web_search_enabled=web_search_enabled,
            temperature=_parse_float(os.getenv("PAPER_AGENT_TEMPERATURE"), 0.2),
            max_output_tokens=_parse_int(os.getenv("PAPER_AGENT_MAX_OUTPUT_TOKENS"), 4096),
            request_timeout_seconds=_parse_int(os.getenv("PAPER_AGENT_TIMEOUT_SECONDS"), 180),
            max_sections=_parse_int(os.getenv("PAPER_AGENT_MAX_SECTIONS"), 8),
            section_max_workers=_parse_int(os.getenv("PAPER_AGENT_SECTION_MAX_WORKERS"), 1),
            analysis_enable_thinking=_parse_bool(os.getenv("PAPER_AGENT_ANALYSIS_ENABLE_THINKING"), True),
            paper_context_max_chars=_parse_int(os.getenv("PAPER_AGENT_PAPER_CONTEXT_MAX_CHARS"), 180000),
            section_target_chars=_parse_int(os.getenv("PAPER_AGENT_SECTION_TARGET_CHARS"), 24000),
            pdf_extract_timeout_seconds=_parse_int(os.getenv("PAPER_AGENT_PDF_EXTRACT_TIMEOUT_SECONDS"), 60),
            output_root=output_root,
        )

    def create_run_dir(self, pdf_path: str, explicit_output_dir: str | None = None) -> Path:
        if explicit_output_dir:
            run_dir = Path(explicit_output_dir).expanduser().resolve()
        else:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            pdf_name = Path(pdf_path).stem or "paper"
            run_dir = (self.output_root / f"{timestamp}-{slugify(pdf_name)}").resolve()

        run_dir.mkdir(parents=True, exist_ok=True)
        return run_dir
