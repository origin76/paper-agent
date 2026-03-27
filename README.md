# LongChain Paper Agent

This repository contains a minimal deep paper-reading agent built for one job:

`PDF -> local text extraction -> qwen3.5-plus -> multi-stage analysis -> Markdown report`

The goal is not a cheap summary. The goal is to spend more tokens on one paper and get closer to an advisor-style guided reading.

## What Changed

This version no longer depends on `fileid://...` or Tavily.

Why:

- DashScope file IDs are officially documented for `Qwen-Long` and `Qwen-Doc-Turbo`.
- You asked to switch all model calls to `qwen3.5-plus`.
- `qwen3.5-plus` supports model-side web search via `enable_search=True`.

So the pipeline now works like this:

1. Extract PDF text locally with `pdftotext`
2. Heuristically split the paper into sections
3. Run all analysis stages with `qwen3.5-plus`
4. Enable model-side web search when `PAPER_AGENT_WEB_SEARCH_ENABLED=true`
5. Write every intermediate artifact and log to a run directory

## Workflow

The workflow lives in [paper_agent/workflow.py](/Users/zerick/code/longchain/paper_agent/workflow.py).

Stages:

1. `ingest_pdf`
2. `global_overview`
3. `web_research`
4. `resource_discovery`
5. `structure_breakdown`
6. `section_deep_dive`
7. `experiment_review`
8. `critique`
9. `extensions`
10. `render_report`

## Requirements

- Python 3.12+
- `pdftotext` available in `PATH`
- `DASHSCOPE_API_KEY`

On macOS, `pdftotext` is usually available via Poppler:

```bash
brew install poppler
```

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Environment Variables

Required:

- `DASHSCOPE_API_KEY`

Optional:

- `PAPER_AGENT_BASE_URL`
  - default: `https://dashscope.aliyuncs.com/compatible-mode/v1`
- `PAPER_AGENT_DOCUMENT_MODEL`
  - default: `qwen3.5-plus`
- `PAPER_AGENT_ANALYSIS_MODEL`
  - default: `qwen3.5-plus`
- `PAPER_AGENT_ANALYSIS_FALLBACK_MODEL`
  - default: same as `PAPER_AGENT_DOCUMENT_MODEL`
- `PAPER_AGENT_ANALYSIS_STREAM`
  - default: `true`
- `PAPER_AGENT_ANALYSIS_RETRY_ATTEMPTS`
  - default: `2`
- `PAPER_AGENT_ANALYSIS_RETRY_BACKOFF_SECONDS`
  - default: `2`
- `PAPER_AGENT_WEB_SEARCH_ENABLED`
  - default: `true`
- `PAPER_AGENT_PAPER_CONTEXT_MAX_CHARS`
  - default: `180000`
- `PAPER_AGENT_SECTION_TARGET_CHARS`
  - default: `24000`
- `PAPER_AGENT_PDF_EXTRACT_TIMEOUT_SECONDS`
  - default: `60`
- `PAPER_AGENT_TEMPERATURE`
  - default: `0.2`
- `PAPER_AGENT_MAX_OUTPUT_TOKENS`
  - default: `4096`
- `PAPER_AGENT_TIMEOUT_SECONDS`
  - default: `180`
- `PAPER_AGENT_MAX_SECTIONS`
  - default: `8`
- `PAPER_AGENT_SECTION_MAX_WORKERS`
  - default: `1`
- `PAPER_AGENT_ANALYSIS_ENABLE_THINKING`
  - default: `true`
- `PAPER_AGENT_LOG_LEVEL`
  - default: `INFO`
- `PAPER_AGENT_OUTPUT_ROOT`
  - default: `runs`

The runtime auto-loads `.env` and `.env.example`.

## Usage

```bash
source .venv/bin/activate
paper-agent /absolute/path/to/paper.pdf --output-dir runs/demo
```

Disable model-side web search:

```bash
paper-agent /absolute/path/to/paper.pdf --disable-web-search
```

## Output Artifacts

Each run writes a directory like:

```text
runs/20260327-120000-some-paper/
├── run.log
├── stage_trace.jsonl
├── paper_text.txt
├── paper_text_meta.json
├── extracted_sections.json
├── overview.json
├── web_search_queries.json
├── web_research.json
├── resource_discovery.json
├── structure.json
├── selected_sections.json
├── section_targets.json
├── section_analyses.json
├── experiment_review.json
├── critique.md
├── extensions.md
├── final_report.md
├── cleanup_result.json
└── run_summary.json
```

## Logging

Every run writes:

- `run.log`: human-readable execution log
- `stage_trace.jsonl`: stage start/finish/error trace

The logs include:

- PDF text extraction start and finish
- section detection outputs
- every LLM request start and finish
- `enable_search` usage
- streamed text chunk counts
- retry attempts for interrupted streams

## Limitations

- PDF extraction quality depends on `pdftotext`
- section splitting is heuristic
- figure and table understanding is not separate yet
- the workflow is batch-only, not interactive
- there is no resume/cache layer yet

## Notes

This implementation keeps the OpenAI-compatible DashScope client, but passes DashScope-specific flags through `extra_body`, including `enable_search`.

Relevant docs:

- DashScope OpenAI-compatible file API: [help.aliyun.com/zh/model-studio/openai-file-interface](https://help.aliyun.com/zh/model-studio/openai-file-interface)
- Qwen-Long file document analysis: [help.aliyun.com/zh/model-studio/long-context-qwen-long](https://help.aliyun.com/zh/model-studio/long-context-qwen-long)
- DashScope compatible mode overview: [help.aliyun.com/zh/model-studio/compatibility-of-openai-with-dashscope](https://help.aliyun.com/zh/model-studio/compatibility-of-openai-with-dashscope)
