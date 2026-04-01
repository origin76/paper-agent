# Playwright ACM 复用手册

这份文档把项目里和 ACM 论文下载相关的 Playwright 链路完整收拢成一套可复用流程，目标是以后换 venue、换批次、换机器时，都能直接照着恢复。

适用场景：

- ACM PDF 直链返回 `403 Forbidden`
- 校园网可访问 ACM，但只有真实浏览器态能通过 Cloudflare / Cookiebot
- 想把已经在 Chrome 里验证通过的登录态、cookie、站点信任状态复用到批量下载

不推荐的旧路径：

- 直接让 Playwright 指向正在使用的 Chrome profile
- 在 shell 里长期写死 `PAPER_AGENT_PLAYWRIGHT_CDP_URL=http://127.0.0.1:9222`

推荐路径：

1. 复制一个可用的 Chrome profile 到临时目录
2. 用这个克隆 profile 启动一个带 `--remote-debugging-port` 的专用 Chrome
3. 让 Playwright 通过 CDP 连接到这个 Chrome
4. 让 `paper-agent-fetch` 在 ACM URL 上优先走 Playwright

## 一、链路结构

代码入口分成三层：

- [paper_agent/chrome_cdp.py](/Users/zerick/code/longchain/paper_agent/chrome_cdp.py)
  负责克隆 Chrome profile、启动 CDP、检查状态、停止和清理会话
- [paper_agent/playwright_download.py](/Users/zerick/code/longchain/paper_agent/playwright_download.py)
  负责 Playwright 下载 PDF、处理 Cookiebot/Cloudflare、记录浏览器链路日志
- [paper_agent/conference_fetch.py](/Users/zerick/code/longchain/paper_agent/conference_fetch.py)
  负责 venue 发现、HTTP 下载、ACM browser-first 策略、失败回退和最终落盘

现在的 ACM 策略是：

- 如果配置了 `--playwright-cdp-url` 或可推导出的 Playwright 浏览器配置，ACM PDF 会优先走 Playwright
- Playwright 失败后会自动回退到 HTTP
- 如果先走 HTTP，遇到 `401/403/429`、DNS 问题、Cloudflare 样式失败，也会切到 Playwright fallback

## 二、前置条件

需要满足下面四件事：

- 本机装有 Google Chrome
- 当前 Python 环境已安装 `playwright`
- 你选中的 Chrome profile 在手工浏览时已经能正常打开 ACM 论文页
- 运行批量下载时，本机网络环境与手工验证时一致，尤其是校园网 / VPN / 代理状态

安装依赖后，如果还没装 Playwright Python 包：

```bash
python3 -m pip install playwright
```

对于当前推荐的 CDP 方案，不强依赖 Playwright 自带 Chromium，因为我们复用的是本机 Chrome。

## 三、用户侧 Chrome Profile 准备

这是最关键的一步。后面代码再正确，如果 profile 侧状态不干净，也会卡在 Cookiebot 或 Cloudflare。

推荐做法：

1. 在日常 Chrome 中新建一个专用 profile，例如 `PaperAgent`
2. 用这个 profile 手工打开一篇 ACM 论文详情页，例如 `https://dl.acm.org/doi/10.1145/...`
3. 手工完成：
   - 校园网认证
   - Cloudflare 校验
   - Cookiebot 的 `Accept all` / `Allow all`
   - ACM 页面首次访问后的站点授权
4. 再手工点一次 PDF，确认浏览器态真的能下载

确认 profile 名称的方法：

- 打开 Chrome，访问 `chrome://version`
- 查看 `Profile Path`
- 最后一级目录通常就是要传给 `--profile-directory` 的值，例如 `Default`、`Profile 1`、`Profile 2`

如果你准备长期跑批量任务，推荐把这个 profile 只用于论文抓取，避免日常 browsing 把 cookie 和状态不断搅乱。

## 四、推荐命令流

### 1. 准备 CDP 会话

优先用脚本入口：

```bash
eval "$(paper-agent-chrome-cdp prepare --profile-directory Default --output shell)"
```

如果当前环境还没重新安装到带新 script entry 的版本，直接用模块入口：

```bash
eval "$(python -m paper_agent.chrome_cdp prepare --profile-directory Default --output shell)"
```

这一步会做五件事：

- 从本机 Chrome 用户目录复制 `Local State`
- 复制目标 profile 目录
- 跳过 cache、singleton lock、service worker cache 等高噪声目录
- 启动一个临时 Chrome，并打开本地 CDP 端口
- 导出两个后续命令直接可用的环境变量：
  - `PAPER_AGENT_PLAYWRIGHT_CDP_URL`
  - `PAPER_AGENT_CHROME_CDP_SESSION_FILE`

### 2. 检查会话是否可用

```bash
paper-agent-chrome-cdp status \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"
```

模块入口等价写法：

```bash
python -m paper_agent.chrome_cdp status \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"
```

`status` 现在会输出结构化 JSON：

- `reachable=true` 说明本地 CDP 已经 ready
- `reachable=false` 时也会返回 JSON 和错误信息，便于脚本判断，而不是直接抛回一串异常

### 3. 执行 ACM 抓取

```bash
paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2023-2025 \
  --output-root conference-papers \
  --playwright-cdp-url "$PAPER_AGENT_PLAYWRIGHT_CDP_URL" \
  --skip-existing
```

现在在 ACM 链路上，配置了 `--playwright-cdp-url` 后会优先走浏览器传输，不再先硬撞一次 HTTP 403。

### 4. 结束并清理

如果只是停浏览器，不删调试痕迹：

```bash
paper-agent-chrome-cdp stop \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"
```

如果这轮任务已经跑完，想连临时 profile、session.json、chrome.log 一起清掉：

```bash
paper-agent-chrome-cdp stop \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE" \
  --cleanup-artifacts
```

## 五、环境变量约定

这套链路相关的环境变量如下。

| 变量名 | 用途 | 常见设置方式 |
| --- | --- | --- |
| `PAPER_AGENT_PLAYWRIGHT_CDP_URL` | Playwright 连接的 CDP 地址 | 由 `prepare --output shell` 自动导出 |
| `PAPER_AGENT_CHROME_CDP_SESSION_FILE` | 当前会话元数据文件 | 由 `prepare --output shell` 自动导出 |
| `PAPER_AGENT_CHROME_SOURCE_USER_DATA_DIR` | Chrome 用户目录根路径 | 只有自动探测失败时再手动设 |
| `PAPER_AGENT_PLAYWRIGHT_PROFILE_DIRECTORY` | 默认 profile 名称 | 想固定 profile 时可写到 shell profile |
| `PAPER_AGENT_CHROME_CDP_PORT` | CDP 端口 | 默认 `9222`，并发多会话时可改 |
| `PAPER_AGENT_ACM_BROWSER_FALLBACK` | 是否启用 Playwright browser fallback | 一般不需要手工设，传入 CDP URL 后会自动启用 |

推荐的 shell profile 片段：

```bash
export PAPER_AGENT_PLAYWRIGHT_PROFILE_DIRECTORY="Default"

paper-agent-acm-up() {
  eval "$(python -m paper_agent.chrome_cdp prepare --output shell)"
}

paper-agent-acm-down() {
  python -m paper_agent.chrome_cdp stop \
    --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE" \
    --cleanup-artifacts
}
```

如果你已经重新安装过带 script entry 的版本，也可以把上面两条函数里的 `python -m paper_agent.chrome_cdp` 改成 `paper-agent-chrome-cdp`。

## 六、日志与调试文件

这条链路的调试信息分成两层。

### 1. CDP 会话层

`prepare` 会在临时 session 根目录生成：

- `chrome.log`
- `session.json`
- `chrome-user-data/`

默认路径形态大致像：

```text
/tmp/paper-agent-chrome-cdp-xxxxxx/
```

### 2. 抓取任务层

`paper-agent-fetch` 会在输出目录下生成：

- `logs/.../run.log`
- `logs/.../stage_trace.jsonl`
- `manifests/*.json`
- `unresolved/*.json`

遇到 ACM 问题时，先看这些关键词：

- `Conference PDF using Playwright as primary transport`
- `Conference PDF Playwright primary transport failed, falling back to HTTP`
- `Conference PDF switching to Playwright fallback`
- `Playwright cookie banner accepted`
- `Playwright waiting for browser challenge`
- `Playwright bootstrap page ready`

## 七、典型问题与处理方式

### 1. `HTTP Error 403: Forbidden`

含义：

- HTTP 直链被 ACM / Cloudflare 挡住了

处理：

- 确认本轮抓取是否真的传入了 `--playwright-cdp-url`
- 确认 `status` 返回 `reachable=true`
- 确认手工浏览器里同一个 profile 确实能打开该 ACM 论文 PDF

### 2. `ECONNREFUSED 127.0.0.1:9222`

含义：

- shell 里有 CDP URL，但并没有真实 Chrome 在监听

处理：

- 重新执行 `prepare --output shell`
- 不要在长期 shell 中写死旧的 `PAPER_AGENT_PLAYWRIGHT_CDP_URL`

### 3. `ProcessSingleton` / `SingletonLock`

含义：

- Playwright 碰到了正在使用的 live Chrome profile

处理：

- 不要把 `--playwright-user-data-dir` 直接指向当前正在使用的 Chrome 用户目录
- 用 `paper-agent-chrome-cdp prepare` 的 cloned-profile 方案

### 4. 一直卡在 Cookiebot 的 `Accept all`

含义：

- 页面还没真正获得所需 cookie

处理：

- 先手工用同一个 profile 点一遍
- 保持这个 profile 的站点状态稳定
- 复用 cloned-profile CDP，而不是每次 fresh profile

### 5. `reachable=false`

含义：

- CDP 会话未启动完成、已退出，或端口被占用

处理：

- 看 `chrome.log`
- 如需换端口，准备时传 `--remote-debugging-port`
- 旧端口被占用时，先停旧 session 再重开

## 八、推荐的批量抓取模式

对于 ACM 站点，推荐固定成下面这套日常流程：

```bash
eval "$(python -m paper_agent.chrome_cdp prepare --profile-directory Default --output shell)"

python -m paper_agent.chrome_cdp status \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE"

paper-agent-fetch \
  --venues pldi,popl,sosp \
  --years 2022-2026 \
  --output-root /private/tmp/acm-cdp-batch \
  --playwright-cdp-url "$PAPER_AGENT_PLAYWRIGHT_CDP_URL" \
  --skip-existing

python -m paper_agent.chrome_cdp stop \
  --session-file "$PAPER_AGENT_CHROME_CDP_SESSION_FILE" \
  --cleanup-artifacts
```

如果想把准备和清理完全标准化，最省心的做法是：

- shell profile 中固定 `PAPER_AGENT_PLAYWRIGHT_PROFILE_DIRECTORY`
- 每次任务前只跑 `paper-agent-acm-up`
- 每次任务后只跑 `paper-agent-acm-down`

## 九、代码层面的复用结论

目前这套实现的关键复用点是：

- ACM 下载已经变成 browser-first，不再依赖先失败一次 HTTP
- Playwright 侧已经补了 Cookiebot / Cloudflare 等待和调试日志
- Chrome profile 不再直连 live profile，而是统一走 cloned-profile CDP
- `status`/`stop` 已经可以直接脚本化
- `stop --cleanup-artifacts` 可以把一轮会话的临时痕迹完整收掉

如果以后要扩到更多被浏览器态保护的站点，这条链路可以直接复用，只需要在 `conference_fetch.py` 里扩展“哪些 host 走 browser-first”即可。
