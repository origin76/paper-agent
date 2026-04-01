from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from paper_agent.config import RuntimeConfig
from paper_agent.runtime import log_event
from paper_agent.utils import write_json

from .kimi_client import KimiClient
from .prompts import (
    BASE_SYSTEM_PROMPT,
    build_url_resource_enrichment_prompt,
    build_url_resource_search_fallback_prompt,
)
from .url_enrichment import (
    apply_resource_url_enrichment,
    build_analysis_map,
    build_enrichment_contexts_for_prompt,
    build_failed_page_contexts_for_prompt,
    collect_resource_url_candidates,
    fetch_url_context,
)


def collect_url_resource_candidates(state: dict[str, Any], config: RuntimeConfig) -> list[dict[str, Any]]:
    return collect_resource_url_candidates(
        state.get("web_research"),
        state.get("resource_discovery"),
        limit=config.url_content_enrichment_max_urls,
    )


def run_url_resource_enrichment_stage(
    *,
    config: RuntimeConfig,
    state: dict[str, Any],
    run_dir: Path,
    stage: str,
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    fetched_contexts: list[dict[str, Any]] = []
    fetch_failures: list[dict[str, Any]] = []
    analyzed_payload: dict[str, Any] = {"pages": [], "search_fallback_pages": []}
    analyzed_pages: dict[str, dict[str, Any]] = {}
    analysis_meta: dict[str, Any] = {"enabled": False, "reason": "not_started"}
    search_fallback_meta: dict[str, Any] = {"enabled": False, "reason": "not_started"}

    try:
        if config.url_content_enrichment_enabled and candidates:
            _fetch_url_contexts(
                config=config,
                stage=stage,
                candidates=candidates,
                fetched_contexts=fetched_contexts,
                fetch_failures=fetch_failures,
            )

            fetched_contexts.sort(key=lambda item: str(item.get("url") or ""))
            prompt_contexts = build_enrichment_contexts_for_prompt(candidates, fetched_contexts)
            fallback_prompt_contexts = (
                build_failed_page_contexts_for_prompt(candidates, fetch_failures) if fetch_failures else []
            )
            analysis_meta, search_fallback_meta = _run_url_enrichment_llm_jobs(
                config=config,
                state=state,
                stage=stage,
                prompt_contexts=prompt_contexts,
                fallback_prompt_contexts=fallback_prompt_contexts,
                analyzed_payload=analyzed_payload,
            )
            analyzed_pages = build_analysis_map(analyzed_payload)
        else:
            disabled_reason = "disabled" if not config.url_content_enrichment_enabled else "no_candidates"
            analysis_meta = {"enabled": False, "reason": disabled_reason}
            search_fallback_meta = {"enabled": False, "reason": disabled_reason}

        enriched_web_research, enriched_resource_discovery = apply_resource_url_enrichment(
            state.get("web_research"),
            state.get("resource_discovery"),
            fetched_contexts,
            analyzed_pages,
        )

        enrichment_meta = _build_enrichment_meta(
            config=config,
            candidates=candidates,
            fetched_contexts=fetched_contexts,
            fetch_failures=fetch_failures,
            analyzed_pages=analyzed_pages,
            analyzed_payload=analyzed_payload,
            analysis_meta=analysis_meta,
            search_fallback_meta=search_fallback_meta,
        )
        _write_url_enrichment_artifacts(
            run_dir=run_dir,
            candidates=candidates,
            fetched_contexts=fetched_contexts,
            analyzed_payload=analyzed_payload,
            enrichment_meta=enrichment_meta,
            enriched_web_research=enriched_web_research,
            enriched_resource_discovery=enriched_resource_discovery,
        )
        return (
            {
                "web_research": enriched_web_research,
                "resource_discovery": enriched_resource_discovery,
                "url_resource_contexts": fetched_contexts,
                "url_resource_enrichment": analyzed_payload,
                "url_resource_enrichment_meta": enrichment_meta,
            },
            {
                "candidate_count": len(candidates),
                "fetched_count": len(fetched_contexts),
                "analyzed_page_count": len(analyzed_pages),
                "search_fallback_page_count": len(analyzed_payload.get("search_fallback_pages") or []),
            },
        )
    except Exception as exc:
        write_json(run_dir / "url_resource_candidates.json", candidates)
        write_json(run_dir / "url_resource_contexts.json", fetched_contexts)
        write_json(run_dir / "url_resource_enrichment.json", analyzed_payload)
        write_json(
            run_dir / "url_resource_enrichment_meta.json",
            {
                "enabled": config.url_content_enrichment_enabled,
                "candidate_count": len(candidates),
                "fetched_count": len(fetched_contexts),
                "fetch_failures": fetch_failures,
                "analysis_meta": analysis_meta,
                "search_fallback_meta": search_fallback_meta,
                "error": str(exc),
            },
        )
        raise


def _fetch_url_contexts(
    *,
    config: RuntimeConfig,
    stage: str,
    candidates: list[dict[str, Any]],
    fetched_contexts: list[dict[str, Any]],
    fetch_failures: list[dict[str, Any]],
) -> None:
    max_workers = min(4, max(1, len(candidates)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(
                fetch_url_context,
                candidate["url"],
                config.url_fetch_timeout_seconds,
                config.url_fetch_max_bytes,
                config.url_fetch_max_text_chars,
            ): candidate["url"]
            for candidate in candidates
        }
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                context = future.result()
                fetched_contexts.append(context)
                log_event(
                    "info",
                    "URL content fetch finished",
                    stage=stage,
                    url=url,
                    final_url=context.get("final_url"),
                    html_title=context.get("html_title"),
                )
            except Exception as exc:
                fetch_failures.append({"url": url, "error": str(exc)})
                log_event("warning", "URL content fetch failed", stage=stage, url=url, error=str(exc))


def _run_url_enrichment_llm_jobs(
    *,
    config: RuntimeConfig,
    state: dict[str, Any],
    stage: str,
    prompt_contexts: list[dict[str, Any]],
    fallback_prompt_contexts: list[dict[str, Any]],
    analyzed_payload: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    llm_jobs: list[str] = []
    if prompt_contexts:
        llm_jobs.append("analyze")
        analysis_meta: dict[str, Any] = {"enabled": True}
    else:
        analysis_meta = {"enabled": False, "reason": "no_fetch_success"}
    if fallback_prompt_contexts:
        llm_jobs.append("search_fallback")
        search_fallback_meta: dict[str, Any] = {"enabled": True}
    else:
        search_fallback_meta = {
            "enabled": False,
            "reason": "no_fetch_failures" if not fallback_prompt_contexts else "no_failed_candidates",
        }

    if not llm_jobs:
        return analysis_meta, search_fallback_meta

    log_event(
        "info",
        "URL enrichment LLM jobs started",
        stage=stage,
        job_count=len(llm_jobs),
        jobs=llm_jobs,
    )
    if len(llm_jobs) == 2:
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_map = {
                executor.submit(_analyze_url_resource_pages, config, stage, prompt_contexts): "analyze",
                executor.submit(
                    _analyze_url_resource_search_fallback,
                    config,
                    stage,
                    state["overview"],
                    fallback_prompt_contexts,
                ): "search_fallback",
            }
            for future in as_completed(future_map):
                job_name = future_map[future]
                payload, meta = future.result()
                if job_name == "analyze":
                    analyzed_payload["pages"] = payload.get("pages", [])
                    analysis_meta = meta
                else:
                    analyzed_payload["search_fallback_pages"] = payload.get("pages", [])
                    search_fallback_meta = meta
                log_event(
                    "info",
                    "URL enrichment LLM job finished",
                    stage=stage,
                    job=job_name,
                    page_count=len(payload.get("pages") or []),
                )
        return analysis_meta, search_fallback_meta

    if prompt_contexts:
        page_payload, analysis_meta = _analyze_url_resource_pages(config, stage, prompt_contexts)
        analyzed_payload["pages"] = page_payload.get("pages", [])
        log_event(
            "info",
            "URL enrichment LLM job finished",
            stage=stage,
            job="analyze",
            page_count=len(page_payload.get("pages") or []),
        )
        return analysis_meta, search_fallback_meta

    fallback_pages_payload, search_fallback_meta = _analyze_url_resource_search_fallback(
        config,
        stage,
        state["overview"],
        fallback_prompt_contexts,
    )
    analyzed_payload["search_fallback_pages"] = fallback_pages_payload.get("pages", [])
    log_event(
        "info",
        "URL enrichment LLM job finished",
        stage=stage,
        job="search_fallback",
        page_count=len(fallback_pages_payload.get("pages") or []),
    )
    return analysis_meta, search_fallback_meta


def _analyze_url_resource_pages(
    config: RuntimeConfig,
    stage: str,
    prompt_contexts: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    client = KimiClient(config)
    return client.chat_json(
        messages=[
            {"role": "system", "content": BASE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": build_url_resource_enrichment_prompt(prompt_contexts),
            },
        ],
        model=config.document_model,
        enable_thinking=False,
        enable_search=False,
        stage=f"{stage}.analyze",
    )


def _analyze_url_resource_search_fallback(
    config: RuntimeConfig,
    stage: str,
    overview: dict[str, Any],
    fallback_prompt_contexts: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]]:
    client = KimiClient(config)
    return client.chat_json(
        messages=[
            {"role": "system", "content": BASE_SYSTEM_PROMPT},
            {
                "role": "user",
                "content": build_url_resource_search_fallback_prompt(
                    overview,
                    fallback_prompt_contexts,
                ),
            },
        ],
        model=config.document_model,
        enable_thinking=False,
        enable_search=True,
        stage=f"{stage}.search_fallback",
    )


def _build_enrichment_meta(
    *,
    config: RuntimeConfig,
    candidates: list[dict[str, Any]],
    fetched_contexts: list[dict[str, Any]],
    fetch_failures: list[dict[str, Any]],
    analyzed_pages: dict[str, dict[str, Any]],
    analyzed_payload: dict[str, Any],
    analysis_meta: dict[str, Any],
    search_fallback_meta: dict[str, Any],
) -> dict[str, Any]:
    return {
        "enabled": config.url_content_enrichment_enabled,
        "candidate_count": len(candidates),
        "fetched_count": len(fetched_contexts),
        "fetch_failures": fetch_failures,
        "analysis_meta": analysis_meta,
        "search_fallback_meta": search_fallback_meta,
        "analyzed_page_count": len(analyzed_pages),
        "search_fallback_page_count": len(analyzed_payload.get("search_fallback_pages") or []),
    }


def _write_url_enrichment_artifacts(
    *,
    run_dir: Path,
    candidates: list[dict[str, Any]],
    fetched_contexts: list[dict[str, Any]],
    analyzed_payload: dict[str, Any],
    enrichment_meta: dict[str, Any],
    enriched_web_research: dict[str, Any],
    enriched_resource_discovery: dict[str, Any],
) -> None:
    write_json(run_dir / "url_resource_candidates.json", candidates)
    write_json(run_dir / "url_resource_contexts.json", fetched_contexts)
    write_json(run_dir / "url_resource_enrichment.json", analyzed_payload)
    write_json(run_dir / "url_resource_enrichment_meta.json", enrichment_meta)
    write_json(run_dir / "web_research.json", enriched_web_research)
    write_json(run_dir / "resource_discovery.json", enriched_resource_discovery)
