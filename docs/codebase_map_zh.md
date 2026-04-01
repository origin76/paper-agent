# LongChain Paper Agent 代码地图

这份文档不是给“第一次运行”的，而是给以后继续维护、扩 venue、改提示词、改叙事链路时用的。

可以把仓库理解成三层：

1. 抓取层
2. 单篇分析层
3. 故事线聚合层

底下再共用一批运行时、导出器、提示词和工具模块。

## 1. 顶层入口

CLI 入口都定义在 [pyproject.toml](/Users/zerick/code/longchain/pyproject.toml)：

- `paper-agent` -> [cli.py](/Users/zerick/code/longchain/paper_agent/cli.py)
- `paper-agent-fetch` -> [conference/fetch.py](/Users/zerick/code/longchain/paper_agent/conference/fetch.py)
- `paper-agent-chrome-cdp` -> [chrome_cdp.py](/Users/zerick/code/longchain/paper_agent/chrome_cdp.py)
- `paper-agent-narrative` -> [narrative.py](/Users/zerick/code/longchain/paper_agent/narrative.py)
- `paper-agent-narrative-detail` -> [narrative_detail.py](/Users/zerick/code/longchain/paper_agent/narrative_detail.py)

如果以后要增加新的高层工作流，优先沿用这种“一条链路一个脚本入口”的组织方式。

补充说明：

- 根目录下的 [chrome_cdp.py](/Users/zerick/code/longchain/paper_agent/chrome_cdp.py)、[playwright_download.py](/Users/zerick/code/longchain/paper_agent/playwright_download.py)、[narrative.py](/Users/zerick/code/longchain/paper_agent/narrative.py)、[narrative_detail.py](/Users/zerick/code/longchain/paper_agent/narrative_detail.py) 现在主要是兼容 shim。
- 真正实现已经收进 [browser/](/Users/zerick/code/longchain/paper_agent/browser) 和 [narrative_stack/](/Users/zerick/code/longchain/paper_agent/narrative_stack)。

## 2. 抓取层

### 2.1 总入口

[conference/fetch.py](/Users/zerick/code/longchain/paper_agent/conference/fetch.py)

职责：

- 作为抓取总调度器
- 调度 venue 发现
- 串接元数据补全与下载流水线
- 写 `downloads / manifests / unresolved / indexes / logs`

你以后如果要：

- 新增 venue
- 改下载并发
- 改 fallback 策略
- 改 manifest 结构

优先从这里开始。

### 2.2 Venue 发现器

- [conference/venues/osdi.py](/Users/zerick/code/longchain/paper_agent/conference/venues/osdi.py)
- [conference/venues/sosp.py](/Users/zerick/code/longchain/paper_agent/conference/venues/sosp.py)
- [conference/venues/pldi.py](/Users/zerick/code/longchain/paper_agent/conference/venues/pldi.py)
- [conference/venues/popl.py](/Users/zerick/code/longchain/paper_agent/conference/venues/popl.py)

职责：

- 从会议主页、接收列表或轨道页发现论文条目
- 归一化标题、年份、详情页、PDF 候选链接
- 做 venue-specific 过滤

新增 venue 的推荐路径：

1. 复制一个最接近的 `conference/venues/*.py`
2. 输出统一的 paper entry 结构
3. 在 [conference/fetch.py](/Users/zerick/code/longchain/paper_agent/conference/fetch.py) 注册

### 2.3 浏览器下载链路

- [browser/playwright_download.py](/Users/zerick/code/longchain/paper_agent/browser/playwright_download.py)
- [browser/chrome_cdp.py](/Users/zerick/code/longchain/paper_agent/browser/chrome_cdp.py)

职责：

- 处理 ACM / Cloudflare / Cookiebot
- 复用现成 Chrome profile 或 CDP 会话
- 对下载过程做分阶段日志和超时控制

如果以后 ACM 逻辑要继续增强，通常只需要改这两个实现文件，不要把浏览器细节再散回 venue 模块。

### 2.4 抓取层数据契约

抓取层最重要的契约不是 HTML 结构，而是最终输出目录：

- `downloads/<venue>/<year>/*.pdf`
- `manifests/*.json`
- `unresolved/*.json`
- `indexes/download_index.json`

后续单篇分析层只依赖 `downloads/` 里的 PDF，所以抓取层可以独立演化，只要这层契约不乱。

## 3. 单篇分析层

### 3.1 总入口

[cli.py](/Users/zerick/code/longchain/paper_agent/cli.py)

职责：

- 单篇 / 批量模式切换
- 目录扫描
- 并发调度
- 最终 PDF 收集到 `collect-dir`
- 写 `collection_index.json`

如果你以后要：

- 调整 batch 运行方式
- 改 `skip-existing` 逻辑
- 改收集目录命名

优先从这里改。

### 3.2 工作流编排

[analysis/workflow.py](/Users/zerick/code/longchain/paper_agent/analysis/workflow.py)

这是单篇分析的核心。

当前主要阶段：

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

如果你以后要增强“单篇分析深度”，最常改的是这里。

### 3.3 单篇分析层的支撑模块

- [analysis/pdf_extract.py](/Users/zerick/code/longchain/paper_agent/analysis/pdf_extract.py)
  负责 `pdftotext` 提取和原始文本元数据。
- [reporting/sections.py](/Users/zerick/code/longchain/paper_agent/reporting/sections.py)
  负责章节切分和标题清洗。
- [analysis/prompts.py](/Users/zerick/code/longchain/paper_agent/analysis/prompts.py)
  负责所有 prompt 模板。
- [analysis/kimi_client.py](/Users/zerick/code/longchain/paper_agent/analysis/kimi_client.py)
  负责 DashScope OpenAI-compatible 调用、stream、thinking、search 参数。
- [analysis/web_search.py](/Users/zerick/code/longchain/paper_agent/analysis/web_search.py)
  负责检索 query 构造。
- [analysis/url_enrichment.py](/Users/zerick/code/longchain/paper_agent/analysis/url_enrichment.py)
  负责抓取外链 HTML，再让模型做标题和摘要净化。
- [analysis/workflow_support.py](/Users/zerick/code/longchain/paper_agent/analysis/workflow_support.py)
  负责 workflow 里可抽离的通用逻辑，例如 section 选择、paper signal 合并、critique/extensions 的 fallback 调用。
- [analysis/url_resource_stage.py](/Users/zerick/code/longchain/paper_agent/analysis/url_resource_stage.py)
  负责 URL 资源富化阶段的候选收集、抓取、LLM 弱补全与产物落盘。
- [analysis/report_stage.py](/Users/zerick/code/longchain/paper_agent/analysis/report_stage.py)
  负责最终报告渲染、HTML/PDF 导出与 `run_summary.json` 生成。
- [reporting/report.py](/Users/zerick/code/longchain/paper_agent/reporting/report.py)
  现在主要负责把各块渲染器装配成最终 Markdown。
- [reporting/text_utils.py](/Users/zerick/code/longchain/paper_agent/reporting/text_utils.py)
  负责中文叙述压缩、句子拼接、章节标题净化等文本细活。
- [reporting/narrative_markdown.py](/Users/zerick/code/longchain/paper_agent/reporting/narrative_markdown.py)
  负责把 critique / extension 这类 markdown 草稿改写成更顺的叙述块。
- [reporting/section_renderers.py](/Users/zerick/code/longchain/paper_agent/reporting/section_renderers.py)
  负责正文各章节的自然语言渲染。
- [reporting/reference_appendix.py](/Users/zerick/code/longchain/paper_agent/reporting/reference_appendix.py)
  负责参考页过滤、验证状态折叠、分桶和附录渲染。
- [reporting/exporters.py](/Users/zerick/code/longchain/paper_agent/reporting/exporters.py)
  负责 HTML / PDF 导出、中文字体、简单公式渲染。

### 3.4 单篇分析层最重要的输入输出契约

输入：

- 一篇 PDF

输出：

- `overview.json`
- `structure.json`
- `section_analyses.json`
- `experiment_review.json`
- `critique.md`
- `extensions.md`
- `final_report.{md,html,pdf}`
- `run_summary.json`

如果以后要做新的聚合层，优先复用这些标准化中间件，不要重新从 PDF 再跑一遍。

## 4. 故事线聚合层

### 4.1 第一层聚合

[narrative_stack/narrative.py](/Users/zerick/code/longchain/paper_agent/narrative_stack/narrative.py)

职责：

- 读取多个 run 目录
- 抽取 `PaperProfile`
- 自动聚类成 `StoryArc`
- 输出首版 narrative 报告

适合改的内容：

- 主题分类逻辑
- turning point 打分
- reading path 选择
- 年度推进摘要

### 4.2 第二层深化

[narrative_stack/detail.py](/Users/zerick/code/longchain/paper_agent/narrative_stack/detail.py)

职责：

- 复用第一层 story arc
- 为每条 arc 再做更深的 LLM 细化
- 输出 combined report
- 输出每条 arc 的独立 Markdown / HTML / PDF 讲义

单条故事线小册子的导出逻辑则在 [narrative_stack/detail_export.py](/Users/zerick/code/longchain/paper_agent/narrative_stack/detail_export.py)。

如果以后要继续深化内容，最优先改这里：

- 单条故事线正文结构
- 代表论文卡片
- 导师带读路径
- 单独 arc PDF 的版式

## 5. 共用基础设施

- [config.py](/Users/zerick/code/longchain/paper_agent/config.py)
  统一读取 `.env` 并生成运行配置；`.env.example` 仅作为模板，不会自动生效。
- [runtime.py](/Users/zerick/code/longchain/paper_agent/runtime.py)
  统一日志和 `stage_trace.jsonl`。
- [utils.py](/Users/zerick/code/longchain/paper_agent/utils.py)
  文件名清洗、JSON 写入、URL 提取等通用工具。
- [conference/types.py](/Users/zerick/code/longchain/paper_agent/conference/types.py)
  抓取层共用的数据结构。
- [conference/parsing.py](/Users/zerick/code/longchain/paper_agent/conference/parsing.py)
  会议页面解析通用逻辑。
- [reporting/rebuild_exports.py](/Users/zerick/code/longchain/paper_agent/reporting/rebuild_exports.py)
  对已有 Markdown / 元数据重建 HTML / PDF 导出。

## 6. 如果你想改某个需求，应该从哪里下手

### 想新增一个 venue

从：

- `conference/venues/xxx.py`
- `conference/fetch.py`

开始。

### 想让单篇分析更深

从：

- `workflow.py`
- `prompts.py`
- `report.py`

开始。

### 想让最终 PDF 更好看

从：

- `report.py`
- `exporters.py`
- `narrative_stack/detail.py`

开始。

### 想让故事线更像“导师带读”

从：

- `narrative_stack/narrative.py`
- `narrative_stack/detail.py`

开始。

### 想让 ACM 下载更稳

从：

- `browser/playwright_download.py`
- `browser/chrome_cdp.py`
- `conference/fetch.py`

开始。

## 7. 推荐的后续扩展顺序

如果以后继续迭代，建议按这个顺序来：

1. 先稳数据契约
2. 再稳日志和超时
3. 再加模型深度
4. 最后再打磨展示层

原因很简单：

- 契约不稳，后面 narrative 层一定会痛苦
- 日志不稳，批量跑几百篇时很难排障
- 展示层可以晚一点，因为前两层稳定后重导出很便宜

## 8. 当前仓库里最值得长期保留的几个设计决策

- 抓取、解析、故事线聚合分成独立 CLI，而不是一个大命令硬塞全部逻辑
- narrative 层复用 run 目录中间产物，而不是重新解析 PDF
- ACM 浏览器态通过 Chrome CDP 克隆 profile 复用，而不是碰 live profile lock
- 最终 PDF 用本地导出器生成，而不是依赖浏览器打印

这些约束让项目虽然功能越来越多，但仍然保持了比较清晰的边界。
