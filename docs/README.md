# 文档导航

这套项目现在可以分成三条连续可复用的链路：

1. 抓论文：`paper-agent-fetch`
2. 单篇 / 批量解析：`paper-agent`
3. 故事线聚合与深化：`paper-agent-narrative` + `paper-agent-narrative-detail`

推荐按下面顺序阅读文档：

- [pipeline_guide_zh.md](./pipeline_guide_zh.md)
  面向日常使用的中文工作流手册，覆盖环境准备、常用命令、目录结构和端到端流程。
- [codebase_map_zh.md](./codebase_map_zh.md)
  面向后续维护和扩展的代码地图，说明每个模块负责什么、要改哪里。
- [playwright_acm_workflow.md](./playwright_acm_workflow.md)
  面向 ACM / Cloudflare / Cookiebot 场景的 Playwright + Chrome CDP 复用手册。

## 最常用命令

抓会议论文：

```bash
paper-agent-fetch \
  --venues osdi,sosp,pldi,popl \
  --years 2023-2025 \
  --output-root conference-papers \
  --skip-existing
```

批量解析本地 PDF：

```bash
paper-agent ~/paper \
  --recursive \
  --batch-workers 8 \
  --collect-dir /absolute/path/to/final-pdfs \
  --skip-existing
```

从已有 run 目录构建故事线：

```bash
paper-agent-narrative runs popl-analysis-runs --output-dir story-arcs
```

把故事线继续深化成“单独讲义”：

```bash
paper-agent-narrative-detail \
  story-arcs \
  --output-dir story-arcs/detailed \
  --skip-existing
```

## 你以后最常回看的几个位置

- 根目录 [README.md](/Users/zerick/code/longchain/README.md)
  项目总入口和快速链接。
- [pipeline_guide_zh.md](/Users/zerick/code/longchain/docs/pipeline_guide_zh.md)
  真正用于“今天我要跑什么命令”的文档。
- [codebase_map_zh.md](/Users/zerick/code/longchain/docs/codebase_map_zh.md)
  真正用于“我要改哪块代码”的文档。
- [playwright_acm_workflow.md](/Users/zerick/code/longchain/docs/playwright_acm_workflow.md)
  真正用于“ACM 又 403 / Cookiebot 又卡住了”的文档。
