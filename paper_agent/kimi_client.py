from __future__ import annotations

import time
from typing import Any

from openai import OpenAI

from paper_agent.config import RuntimeConfig
from paper_agent.runtime import log_event
from paper_agent.utils import extract_json_object


def _usage_to_dict(usage: Any) -> dict[str, Any]:
    if usage is None:
        return {}
    return {
        "prompt_tokens": getattr(usage, "prompt_tokens", None),
        "completion_tokens": getattr(usage, "completion_tokens", None),
        "total_tokens": getattr(usage, "total_tokens", None),
    }


class KimiClient:
    MODEL_AVAILABILITY_ERROR_MARKERS = (
        "product is not activated",
        "model not exist",
        "model does not exist",
        "unsupported model",
        "invalid model",
        "no such model",
    )
    RETRYABLE_TEXT_ERROR_MARKERS = (
        "peer closed connection",
        "incomplete chunked read",
        "connection reset",
        "timed out",
        "timeout",
        "temporarily unavailable",
        "remote end closed connection",
    )

    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.client = OpenAI(
            api_key=config.api_key,
            base_url=config.base_url,
            timeout=config.request_timeout_seconds,
        )

    @classmethod
    def is_model_availability_error(cls, error: Exception) -> bool:
        message = str(error).lower()
        return any(marker in message for marker in cls.MODEL_AVAILABILITY_ERROR_MARKERS)

    @classmethod
    def is_retryable_text_error(cls, error: Exception) -> bool:
        message = str(error).lower()
        return any(marker in message for marker in cls.RETRYABLE_TEXT_ERROR_MARKERS)

    def _message_char_count(self, messages: list[dict[str, Any]]) -> int:
        total = 0
        for message in messages:
            content = message.get("content", "")
            if isinstance(content, str):
                total += len(content)
            elif isinstance(content, list):
                for item in content:
                    total += len(str(item))
            else:
                total += len(str(content))
        return total

    def _coerce_text_piece(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            parts: list[str] = []
            for item in value:
                if isinstance(item, dict):
                    parts.append(str(item.get("text", "")))
                else:
                    parts.append(str(getattr(item, "text", item)))
            return "".join(parts)
        return str(value)

    def _build_extra_body(
        self,
        enable_thinking: bool | None,
        enable_search: bool | None,
    ) -> dict[str, Any] | None:
        extra_body: dict[str, Any] = {}
        if enable_thinking is not None:
            extra_body["enable_thinking"] = enable_thinking
        if enable_search is not None:
            extra_body["enable_search"] = enable_search
        return extra_body or None

    def _repair_json_payload(
        self,
        raw_content: str,
        model: str,
        stage: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        log_event("warning", "LLM JSON parse failed, attempting repair", stage=stage, model=model)
        response = self.client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "You repair malformed JSON. Return only one valid JSON object. Keep the original meaning. Use Simplified Chinese string values when possible.",
                },
                {
                    "role": "user",
                    "content": f"请把下面这段可能损坏的 JSON 修复成一个合法 JSON 对象，只输出 JSON 本体：\n\n{raw_content}",
                },
            ],
            temperature=0,
            max_tokens=self.config.max_output_tokens,
            timeout=self.config.request_timeout_seconds,
            response_format={"type": "json_object"},
            extra_body=self._build_extra_body(False, False),
        )
        repaired_content = response.choices[0].message.content or "{}"
        payload = extract_json_object(repaired_content)
        meta = {
            "response_id": getattr(response, "id", None),
            "usage": _usage_to_dict(getattr(response, "usage", None)),
        }
        log_event(
            "info",
            "LLM JSON repair finished",
            stage=stage,
            model=model,
            response_id=meta["response_id"],
            total_tokens=meta["usage"].get("total_tokens"),
        )
        return payload, meta

    def chat_json(
        self,
        messages: list[dict[str, Any]],
        model: str | None = None,
        enable_thinking: bool | None = None,
        enable_search: bool | None = None,
        stage: str = "chat_json",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        selected_model = model or self.config.document_model
        start_time = time.perf_counter()
        log_event(
            "info",
            "LLM JSON request started",
            stage=stage,
            model=selected_model,
            message_count=len(messages),
            message_chars=self._message_char_count(messages),
            enable_thinking=enable_thinking,
            enable_search=enable_search,
        )
        try:
            response = self.client.chat.completions.create(
                model=selected_model,
                messages=messages,
                temperature=self.config.temperature,
                max_tokens=self.config.max_output_tokens,
                timeout=self.config.request_timeout_seconds,
                response_format={"type": "json_object"},
                extra_body=self._build_extra_body(enable_thinking, enable_search),
            )
        except Exception as exc:
            log_event(
                "error",
                "LLM JSON request failed",
                stage=stage,
                model=selected_model,
                duration_seconds=f"{time.perf_counter() - start_time:.2f}",
                error=str(exc),
            )
            raise
        content = response.choices[0].message.content or "{}"
        json_repair_meta: dict[str, Any] | None = None
        try:
            payload = extract_json_object(content)
        except Exception:
            payload, json_repair_meta = self._repair_json_payload(content, selected_model, stage)
        meta = {
            "model": selected_model,
            "response_id": getattr(response, "id", None),
            "usage": _usage_to_dict(getattr(response, "usage", None)),
            "enable_search": enable_search,
        }
        if json_repair_meta is not None:
            meta["json_repair_used"] = True
            meta["json_repair_response_id"] = json_repair_meta.get("response_id")
            meta["json_repair_usage"] = json_repair_meta.get("usage")
        log_event(
            "info",
            "LLM JSON request finished",
            stage=stage,
            model=selected_model,
            duration_seconds=f"{time.perf_counter() - start_time:.2f}",
            response_id=meta["response_id"],
            total_tokens=meta["usage"].get("total_tokens"),
        )
        return payload, meta

    def chat_text(
        self,
        messages: list[dict[str, Any]],
        model: str | None = None,
        enable_thinking: bool | None = None,
        enable_search: bool | None = None,
        stage: str = "chat_text",
    ) -> tuple[str, dict[str, Any]]:
        selected_model = model or self.config.analysis_model
        start_time = time.perf_counter()
        max_attempts = max(1, self.config.analysis_retry_attempts)
        log_event(
            "info",
            "LLM text request started",
            stage=stage,
            model=selected_model,
            message_count=len(messages),
            message_chars=self._message_char_count(messages),
            enable_thinking=enable_thinking,
            enable_search=enable_search,
            stream=self.config.analysis_stream,
            max_attempts=max_attempts,
        )
        for attempt in range(1, max_attempts + 1):
            try:
                if self.config.analysis_stream:
                    stream = self.client.chat.completions.create(
                        model=selected_model,
                        messages=messages,
                        temperature=self.config.temperature,
                        max_tokens=self.config.max_output_tokens,
                        timeout=self.config.request_timeout_seconds,
                        extra_body=self._build_extra_body(enable_thinking, enable_search),
                        stream=True,
                    )
                    content_parts: list[str] = []
                    reasoning_parts: list[str] = []
                    response_id = None
                    usage: dict[str, Any] = {}
                    chunk_count = 0
                    for chunk in stream:
                        chunk_count += 1
                        response_id = getattr(chunk, "id", response_id)
                        chunk_usage = _usage_to_dict(getattr(chunk, "usage", None))
                        if chunk_usage:
                            usage = chunk_usage
                        choices = getattr(chunk, "choices", None) or []
                        if not choices:
                            continue
                        delta = getattr(choices[0], "delta", None)
                        if delta is None:
                            continue
                        content_parts.append(self._coerce_text_piece(getattr(delta, "content", None)))
                        reasoning_parts.append(self._coerce_text_piece(getattr(delta, "reasoning_content", None)))
                    content = "".join(content_parts)
                    meta = {
                        "model": selected_model,
                        "response_id": response_id,
                        "usage": usage,
                        "reasoning_content": "".join(reasoning_parts) or None,
                        "stream": True,
                        "chunk_count": chunk_count,
                        "attempt": attempt,
                        "enable_search": enable_search,
                    }
                else:
                    response = self.client.chat.completions.create(
                        model=selected_model,
                        messages=messages,
                        temperature=self.config.temperature,
                        max_tokens=self.config.max_output_tokens,
                        timeout=self.config.request_timeout_seconds,
                        extra_body=self._build_extra_body(enable_thinking, enable_search),
                    )
                    message = response.choices[0].message
                    content = message.content or ""
                    meta = {
                        "model": selected_model,
                        "response_id": getattr(response, "id", None),
                        "usage": _usage_to_dict(getattr(response, "usage", None)),
                        "reasoning_content": getattr(message, "reasoning_content", None),
                        "stream": False,
                        "attempt": attempt,
                        "enable_search": enable_search,
                    }
                break
            except Exception as exc:
                retryable = self.is_retryable_text_error(exc)
                log_event(
                    "warning" if retryable and attempt < max_attempts else "error",
                    "LLM text request failed",
                    stage=stage,
                    model=selected_model,
                    duration_seconds=f"{time.perf_counter() - start_time:.2f}",
                    attempt=attempt,
                    max_attempts=max_attempts,
                    retryable=retryable,
                    error=str(exc),
                )
                if not retryable or attempt >= max_attempts:
                    raise
                time.sleep(self.config.analysis_retry_backoff_seconds * attempt)
        log_event(
            "info",
            "LLM text request finished",
            stage=stage,
            model=selected_model,
            duration_seconds=f"{time.perf_counter() - start_time:.2f}",
            response_id=meta["response_id"],
            total_tokens=meta["usage"].get("total_tokens"),
            response_chars=len(content),
            stream=meta.get("stream"),
            chunk_count=meta.get("chunk_count"),
            attempt=meta.get("attempt"),
        )
        return content, meta

    def _paper_text_messages(self, paper_text: str, prompt: str, source_label: str) -> list[dict[str, str]]:
        return [
            {"role": "system", "content": "You are a careful research-paper reader."},
            {
                "role": "user",
                "content": f"{prompt}\n\n{source_label}:\n```text\n{paper_text}\n```",
            },
        ]

    def chat_json_with_text(
        self,
        paper_text: str,
        prompt: str,
        model: str | None = None,
        enable_search: bool | None = None,
        stage: str = "chat_json_with_text",
        source_label: str = "Paper text",
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        payload, meta = self.chat_json(
            self._paper_text_messages(paper_text, prompt, source_label),
            model=model,
            enable_thinking=False,
            enable_search=enable_search,
            stage=stage,
        )
        meta["source_chars"] = len(paper_text)
        meta["source_label"] = source_label
        return payload, meta
