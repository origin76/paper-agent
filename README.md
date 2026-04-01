# LongChain Paper Agent

This repository contains a minimal deep paper-reading agent built for one job:

`PDF -> local text extraction -> qwen3.5-plus -> multi-stage analysis -> Markdown / HTML / PDF report`

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
5. Fetch high-value resource URLs, parse HTML titles/snippets, and let the model clean up generic link names
6. Render the final report as Markdown, readable HTML, and printable PDF
7. Write every intermediate artifact and log to a run directory

## Workflow

The workflow lives in [paper_agent/workflow.py](/Users/zerick/code/longchain/paper_agent/workflow.py).

Stages:

1. `ingest_pdf`
2. `global_overview`
3. `web_research`
4. `resource_discovery`
5. `url_resource_enrichment`
6. `structure_breakdown`
7. `section_deep_dive`
8. `experiment_review`
9. `critique`
10. `extensions`
11. `render_report`

Parallelism notes:

- `structure_breakdown` now starts immediately after `global_overview`, in parallel with the web research chain.
- `section_deep_dive` and `experiment_review` now start as soon as both `structure_breakdown` and `url_resource_enrichment` are ready, and they run in parallel.
- Inside `section_deep_dive`, multiple section-level LLM calls can run concurrently via `PAPER_AGENT_SECTION_MAX_WORKERS`.
- Inside `url_resource_enrichment`, fetched-page analysis and search-based weak fallback can run concurrently when both are needed.

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
- `PAPER_AGENT_URL_CONTENT_ENRICHMENT_ENABLED`
  - default: `true`
- `PAPER_AGENT_URL_CONTENT_ENRICHMENT_MAX_URLS`
  - default: `8`
- `PAPER_AGENT_URL_FETCH_TIMEOUT_SECONDS`
  - default: `12`
- `PAPER_AGENT_URL_FETCH_MAX_BYTES`
  - default: `600000`
- `PAPER_AGENT_URL_FETCH_MAX_TEXT_CHARS`
  - default: `6000`
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
  - default: `4`
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

Batch-process an entire Zotero storage tree and collect the final report PDFs into one project-local folder:

```bash
paper-agent ~/zotero/storage \
  --recursive \
  --collect-dir /Users/zerick/code/longchain/zotero-paper-agent-final-pdfs \
  --batch-workers 4 \
  --skip-existing
```

Notes for the Zotero workflow:

- `~/zotero/storage` can be scanned recursively because Zotero stores attachments under hashed subdirectories.
- The project-local collect directory is ignored by Git via `.gitignore`, so generated PDFs stay out of version control.
- `zotero-paper-agent-final-pdfs/collection_index.json` records the mapping from the original Zotero attachment PDF to the generated report PDF.
- The per-batch execution summary still lives under `runs/.../batch_summary.json`.

Fetch recent conference papers into a reusable local workspace before analysis:

```bash
paper-agent-fetch \
  --venues osdi,sosp,pldi,popl \
  --years 2023-2025 \
  --output-root conference-papers \
  --skip-existing
```

If ACM-hosted PDFs are blocked by Cloudflare or campus-network gating, you can run the same fetch with browser-style cookies:

```bash
export PAPER_AGENT_ACM_COOKIE_HEADER='cf_clearance=...; session=...'

paper-agent-fetch \
  --venues pldi,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --skip-existing
```

You can also point the fetcher at a cookie file instead of an env var:

```bash
paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --acm-cookie-file ~/Downloads/acm-cookies.txt \
  --skip-existing
```

The ACM cookie file can be either:

- a raw `Cookie:` header line
- a raw cookie string such as `cf_clearance=...; session=...`
- a Netscape-format cookie jar exported from a browser extension

If cookies are still not enough because ACM / Cloudflare only trusts a real browser session, the fetcher can fall back to Playwright for ACM PDF URLs. The most reliable mode is to attach to your already-authenticated Chrome via CDP:

```bash
eval "$(paper-agent-chrome-cdp prepare --profile-directory Default --output shell)"
```

If your environment has not been reinstalled since the new script entry was added, use `python -m paper_agent.chrome_cdp ...` instead of `paper-agent-chrome-cdp ...`.

Then verify the browser session and run the fetcher against that CDP endpoint:

```bash
paper-agent-chrome-cdp status \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"

paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --playwright-cdp-url "$PAPER_AGENT_PLAYWRIGHT_CDP_URL" \
  --skip-existing
```

When the batch is done, stop the cloned-profile browser session:

```bash
paper-agent-chrome-cdp stop \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"
```

If you also want to remove the temporary cloned Chrome profile plus the generated `session.json` and `chrome.log`:

```bash
paper-agent-chrome-cdp stop \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE" \
  --cleanup-artifacts
```

The `paper-agent-chrome-cdp` helper does four things for you:

- clones the selected Chrome profile into a temporary directory so the live profile lock is never touched
- launches a real Chrome process with `--remote-debugging-port`
- waits until `http://127.0.0.1:<port>/json/version` is actually ready
- prints reusable shell exports such as `PAPER_AGENT_PLAYWRIGHT_CDP_URL` and `PAPER_AGENT_CHROME_CDP_SESSION_FILE`

If you still want the lower-level manual modes, both are supported:

```bash
paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --playwright-cdp-url http://127.0.0.1:9222 \
  --skip-existing
```

```bash
paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --playwright-user-data-dir "$HOME/Library/Application Support/Google/Chrome" \
  --playwright-profile-directory Default \
  --skip-existing
```

Notes for the reusable Playwright ACM workflow:

- The recommended path is `paper-agent-chrome-cdp prepare --output shell` plus `paper-agent-fetch --playwright-cdp-url ...`.
- ACM PDF URLs now prefer Playwright as the primary transport when a browser session is configured, instead of waiting for HTTP 403 / challenge failures first.
- `run.log` will show `Conference PDF using Playwright as primary transport` and `Playwright PDF download ...` events for browser-driven ACM downloads.
- The manifest metadata records `download_transport=http` or `download_transport=playwright:...`.
- Reusing a cloned-profile CDP Chrome session is usually more reliable than pointing Playwright directly at the live profile.
- If your normal Chrome is already running, `--playwright-user-data-dir ~/Library/Application Support/Google/Chrome` can fail with a profile lock. The cloned-profile CDP helper avoids that.
- The helper is especially useful when ACM shows Cloudflare `请稍候...` or similar interstitials that only clear in a real user Chrome session.
- `paper-agent-chrome-cdp status` now returns structured JSON on both success and failure, which makes it easier to script health checks.
- A fuller operational guide, including Chrome profile prep and shell profile snippets, lives in [docs/playwright_acm_workflow.md](docs/playwright_acm_workflow.md).

The fetch workspace is organized like this:

```text
conference-papers/
├── downloads/
│   ├── osdi/2024/*.pdf
│   ├── pldi/2024/*.pdf
│   └── sosp/2024/*.pdf
├── manifests/
├── unresolved/
├── indexes/
└── logs/
```

Then feed the downloaded PDFs back into the existing batch analyzer:

```bash
paper-agent conference-papers/downloads \
  --recursive \
  --collect-dir /Users/zerick/code/longchain/conference-paper-agent-final-pdfs
```

Notes for the conference fetch layer:

- `manifests/*.json` records every discovered paper plus the resolved metadata / download path.
- `unresolved/*.json` collects papers whose PDF could not be resolved or downloaded.
- `indexes/download_index.json` provides one flat list of all downloaded PDFs.
- OSDI, PLDI, and POPL rely primarily on official conference pages.
- SOSP starts from the official accepted-paper page and then uses DBLP / arXiv as weak supplemental resolution when no direct PDF is available.
- the fetcher now sends browser-style HTTP headers by default and can attach ACM-specific cookies via `PAPER_AGENT_ACM_COOKIE_HEADER`, `--acm-cookie-file`, or the generic HTTP cookie options
- ACM downloads can escalate to a real Chrome session via Playwright using either `--playwright-cdp-url` or `--playwright-user-data-dir`
- If `--playwright-cdp-url` or `--playwright-user-data-dir` is configured, ACM Playwright handling auto-enables unless you explicitly pass `--no-acm-browser-fallback`
- The recommended reusable setup is `paper-agent-chrome-cdp prepare --output shell`, which launches a cloned-profile Chrome CDP session without touching the live profile lock
- Every HTTP request and venue-year phase is logged under `conference-papers/logs/.../run.log` and `stage_trace.jsonl`.

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
├── url_resource_candidates.json
├── url_resource_contexts.json
├── url_resource_enrichment.json
├── url_resource_enrichment_meta.json
├── structure.json
├── selected_sections.json
├── section_targets.json
├── section_analyses.json
├── experiment_review.json
├── critique.md
├── extensions.md
├── final_report.md
├── final_report.html
├── final_report.pdf
├── report_export_meta.json
├── cleanup_result.json
└── run_summary.json
```

The HTML output is styled for comfortable on-screen reading, while the PDF is generated locally with `reportlab` so it does not depend on browser print pipelines or system-specific converters.

The URL enrichment artifacts are especially useful when a report contains vague resource names. They show:

- which URLs were selected for HTML fetching
- which fetches failed because of 403/404 or other network restrictions
- which cleaned titles and one-line summaries were produced from fetched page content

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
