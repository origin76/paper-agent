# LongChain Paper Agent 工作流手册

这份手册面向“以后继续稳定使用”这个目标来写，不假设你还记得之前的上下文。你可以把它当成抓取论文、批量解析、生成故事线讲义的统一操作说明。

## 1. 项目现在有哪三条链路

### A. 抓论文

入口：

- `paper-agent-fetch`
- 辅助工具：`paper-agent-chrome-cdp`

适用场景：

- 批量抓取 OSDI / SOSP / PLDI / POPL 等会议论文
- ACM 直链会 `403`，必须借助浏览器态和 Playwright

最终产物：

- `downloads/<venue>/<year>/*.pdf`
- `manifests/*.json`
- `unresolved/*.json`
- `indexes/download_index.json`
- `logs/.../run.log`

### B. 单篇 / 批量解析

入口：

- `paper-agent`

适用场景：

- 解析单篇 PDF
- 递归解析整个目录，例如 `~/paper` 或 Zotero storage
- 把最终 PDF 报告统一收集到单独目录

最终产物：

- 每篇论文一个 run 目录，里面有中间 JSON / Markdown / HTML / PDF
- 一个收集目录，里面只放最终成品 PDF

### C. 故事线聚合与深化

入口：

- `paper-agent-narrative`
- `paper-agent-narrative-detail`

适用场景：

- 已经有一批单篇 run，希望看领域发展脉络
- 把数百篇论文压成 5 到 10 条“研究故事线”
- 为每条故事线单独导出可读性更强的 PDF 讲义

最终产物：

- `story_arcs.json`
- `narrative_report.{md,html,pdf}`
- `detailed/detailed_story_arcs.json`
- `detailed/arc_reports/*.pdf`

## 2. 环境准备

需要的基础依赖：

- Python 3.12+
- `pdftotext`
- DashScope API Key
- 如要抓 ACM，建议本机安装 Google Chrome

macOS 安装 `pdftotext`：

```bash
brew install poppler
```

安装项目：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

环境变量建议直接从 [.env.example](/Users/zerick/code/longchain/.env.example) 复制一份：

```bash
cp .env.example .env
```

最少要填：

- `DASHSCOPE_API_KEY`

常用默认值已经在 `.env.example` 里整理好了，包括：

- `qwen3.5-plus`
- `enable_thinking`
- 搜索开关
- Playwright / ACM 相关配置

## 3. 最常用的四种跑法

### 跑法 1：解析一篇本地 PDF

```bash
paper-agent /absolute/path/to/paper.pdf --output-dir runs/demo-paper
```

常用补充参数：

- `--disable-web-search`
- `--print-report`

### 跑法 2：解析一个本地论文目录

```bash
paper-agent ~/paper \
  --recursive \
  --batch-workers 8 \
  --collect-dir /Users/zerick/code/longchain/final-pdfs \
  --skip-existing
```

这条命令会做两件事：

1. 在 `runs/...` 下保留每篇论文的完整中间产物
2. 在 `final-pdfs/` 下收集最终导出的 PDF 成品

适合：

- `~/paper`
- `~/zotero/storage`
- `conference-papers/downloads`

### 跑法 3：先抓会议论文，再接批量解析

先抓论文：

```bash
paper-agent-fetch \
  --venues osdi,sosp,pldi,popl \
  --years 2023-2025 \
  --output-root conference-papers \
  --skip-existing
```

再接批量解析：

```bash
paper-agent conference-papers/downloads \
  --recursive \
  --batch-workers 8 \
  --collect-dir /Users/zerick/code/longchain/conference-final-pdfs \
  --skip-existing
```

### 跑法 4：从已有 run 构建故事线，再导出单条讲义

先聚合：

```bash
paper-agent-narrative \
  runs \
  popl-analysis-runs \
  --output-dir story-arcs
```

再深化：

```bash
paper-agent-narrative-detail \
  story-arcs \
  --output-dir story-arcs/detailed \
  --skip-existing
```

如果想让 story-arc 细化阶段也允许联网搜索：

```bash
paper-agent-narrative-detail \
  story-arcs \
  --output-dir story-arcs/detailed \
  --enable-search \
  --skip-existing
```

## 4. ACM / Playwright 推荐姿势

如果 ACM 直链基本必定 `403`，推荐不要硬撞 HTTP，而是直接走可复用的 Chrome CDP 方案。

准备浏览器态：

```bash
eval "$(paper-agent-chrome-cdp prepare --profile-directory Default --output shell)"
```

检查是否可用：

```bash
paper-agent-chrome-cdp status \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"
```

然后抓 ACM：

```bash
paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --playwright-cdp-url "$PAPER_AGENT_PLAYWRIGHT_CDP_URL" \
  --skip-existing
```

收尾：

```bash
paper-agent-chrome-cdp stop \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE" \
  --cleanup-artifacts
```

更详细的浏览器准备、Cookiebot、Cloudflare 处理方式见：

- [playwright_acm_workflow.md](/Users/zerick/code/longchain/docs/playwright_acm_workflow.md)

## 5. 三条链路各自的输入输出约定

### 5.1 抓取层输出结构

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

最常回看的文件：

- `indexes/download_index.json`
- `manifests/*.json`
- `unresolved/*.json`

### 5.2 单篇分析输出结构

```text
runs/<run-name>/
├── paper_text.txt
├── overview.json
├── web_research.json
├── resource_discovery.json
├── structure.json
├── section_analyses.json
├── experiment_review.json
├── critique.md
├── extensions.md
├── final_report.md
├── final_report.html
├── final_report.pdf
├── run.log
├── stage_trace.jsonl
└── run_summary.json
```

最常回看的文件：

- `overview.json`
- `structure.json`
- `section_analyses.json`
- `final_report.pdf`
- `run_summary.json`

### 5.3 故事线输出结构

```text
story-arcs/
├── paper_profiles.jsonl
├── story_arcs.json
├── narrative_report.md
├── narrative_report.html
├── narrative_report.pdf
├── narrative_summary.json
└── detailed/
    ├── detailed_story_arcs.json
    ├── detailed_narrative_report.pdf
    └── arc_reports/
        ├── index.json
        └── *.pdf
```

最常回看的文件：

- `story_arcs.json`
- `detailed/detailed_story_arcs.json`
- `detailed/arc_reports/index.json`
- `detailed/arc_reports/*.pdf`

## 6. 如何判断现在该跑哪条链路

如果你手头是论文 PDF：

- 直接跑 `paper-agent`

如果你手头是会议名和年份：

- 先跑 `paper-agent-fetch`
- 再把 `downloads/` 喂给 `paper-agent`

如果你手头已经有很多 run 目录：

- 直接跑 `paper-agent-narrative`
- 然后再跑 `paper-agent-narrative-detail`

如果你已经有故事线，但只是想重导 PDF：

- 直接跑 `paper-agent-narrative-detail ... --skip-existing`

## 7. 日志与排障

三条链路都统一会写：

- `run.log`
- `stage_trace.jsonl`

推荐排障顺序：

1. 看 `run.log`
2. 再看 `stage_trace.jsonl`
3. 如果是抓 ACM，再看 Playwright / CDP 临时目录里的 `chrome.log`

最常见的问题：

- `403 Forbidden`
  通常是 ACM 直链，需要 CDP + Playwright。
- `No PDF files found`
  通常是忘了加 `--recursive`。
- `skip-existing` 没生效
  通常是 `collect-dir` 改了，导致索引路径不一致。
- 细化故事线时仍发起很多 LLM 请求
  说明 `--skip-existing` 没有指向已有的 detail 输出目录。

## 8. 以后继续扩展时的建议

最稳的开发顺序是：

1. 先改命令和输出契约
2. 再改中间 JSON
3. 最后再改最终 HTML / PDF 展示层

具体改哪块代码，请直接看：

- [codebase_map_zh.md](/Users/zerick/code/longchain/docs/codebase_map_zh.md)
