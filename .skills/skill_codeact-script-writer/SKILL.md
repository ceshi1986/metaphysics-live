---
name: codeact-script-writer
description: 提供CodeAct脚本编写、调试、优化和审查的渐进式规则路由，按任务类型加载核心、联网、并发、报告、状态和Skill/CLI规范，保证脚本可运行、可复用、可追踪；当用户需要生成脚本、修复脚本、优化脚本或评估脚本合规性时使用
---

# CodeAct 脚本编写器

使用本 Skill 编写、调试、优化或审查 CodeAct Python 脚本。

核心原则是渐进式加载规则：不要一开始读取全部规则文件。先判断用户需求类型，再只读取当前任务需要的 reference 分片；如果任务范围扩大，再追加读取对应分片。

## 必须先做

1. 将用户请求分类为一个或多个任务类型。
2. 所有任务都先读取 `references/core.md`。
3. 只读取下表中与任务类型匹配的 reference 文件。
4. 如果任务功能接近 `assets/script/` 目录内的参考脚本，读取对应脚本作为实现参考。
5. 如果是编写或修改脚本，除非用户明确只要分析，否则需要实现并运行验证后再交付。

## 任务类型路由

| 用户意图 | 需要加载的规则 |
|---|---|
| 通用 CodeAct 脚本生成、调试、合规检查 | `references/core.md` |
| 联网搜索、网页抓取、API 数据、新闻、政策、行情、榜单 | `references/web-data.md` |
| 批量抓取、多次 LLM 调用、长文本分块、多文件/多 item 处理 | `references/concurrency.md` |
| 日报、周报、监控简报、调研报告、分析报告 | `references/reporting.md` |
| 定时/重复执行、去重、增量运行、历史基线库 | `references/state.md` |
| 跨来源报告、增量报告、带状态库的 agentic reporting | `references/agentic-report-blocks.md` |
| 任务需要使用其他 Skill、Skill CLI、本地脚本、飞书 lark-cli | `references/skill-cli.md` |

## 参考脚本索引

`assets/script/` 目录保存可复用的 CodeAct 脚本参考实现。写相近功能时先读规则，再按需打开对应脚本：

| 功能类型 | 参考脚本 | 重点学习点 |
|---|---|---|
| 天气查询、确定 URL 抓取、搜索兜底 | `assets/script/weather_query.py` | `codeact_fetch_web` 主源、`codeact_search_web` 兜底、ResponseFormat 抽取、用户简报 |
| 行情/价格监控、阈值告警 | `assets/script/gold_price_monitor.py` | schema version、主源 + 搜索兜底、数值解析、`auto` 分流和 @主人 |
| 长文翻译/改写/多块 LLM | `assets/script/long_text_translate.py` | Markdown 分块、术语表、上下文窗口、LLM 并发、失败块回填 |
| 飞书文档监控、lark-cli、增量 diff | `assets/script/feishu_doc_watch.py` | CLI 边界、授权检查、SQLite 状态、冷启动基线、内容快照、diff 分块总结 |
| 跨来源报告、增量报告、agentic reporting | `assets/script/moore_threads_daily_briefing.py` + `references/agentic-report-blocks.md` | 动态检索、候选筛选、内容获取、事实抽取、信息单元聚合、SQLite 状态、冷启动基线、短 message + 长报告 |
| 数据+新闻融合日报、多章节长报告、带图表 | `assets/script/daily_stock_summary.py` | DomainConfig 领域配置集中管理、CLI 行情 + 分章节搜索、分块串行章节化写作（滚动上下文）、matplotlib 生成图表→上传→Markdown 引用、版本化缓存、baseline/增量双检索窗口、正文直喂+prompt 质量约束的简化取舍 |
| 定时提醒、轻量 scaffold | `assets/script/reminder.py` | 最小脚本结构、`display_only` 提醒、@主人、安装时自定义文案 |

## 分类启发式

- 出现“最新、今日、近期、政策、新闻、榜单、价格、行情、网页、搜索、抓取、API”等意图时，加载 `web-data.md`。
- 脚本会处理多个 URL、query、文件、item、chunk 或多次 LLM 抽取时，加载 `concurrency.md`。
- 用户期待可读的日报、周报、摘要、调研结论或分析报告时，加载 `reporting.md`。
- 用户要做跨来源信息聚合、增量报告、带状态库的日报/周报/监控简报时，追加加载 `agentic-report-blocks.md`。
- 脚本会定时运行、重复运行、监控、增量更新，或需要避免重复通知时，加载 `state.md`。
- 输出较长、有表格、有附件、有报告文件或需要下载链接时，使用 `core.md` 中的输出与提交规则；如属于报告类，再加载 `reporting.md`。
- 用户指定某个 Skill，或任务需要使用 Skill 提供的 CLI、本地脚本、飞书 lark-cli 等能力时，加载 `skill-cli.md`。

## 执行风格

- 优先遵循当前仓库和工作区已有约定。
- 可变业务值需要抽象为命令行参数，并提供安全默认值。
- 脚本内只能使用 CodeAct SDK 侧工具；不得在脚本里调用 Agent 侧工具。
- 工具 schema version 必须使用实际版本常量，并按工具分别引用。
- 批量独立 SDK 调用默认使用 `asyncio.gather` + `asyncio.Semaphore` 并发执行。
- 所有代码路径都必须 `submit_result`；错误提交必须使用 `result_mode="notify"`。

## 交付格式

完成后说明：

- 脚本路径或修改文件
- 已实现或已审查的内容
- 验证结果
- 如相关，说明 `result_mode` 行为

## 资源索引

- 参考:见 [references/core.md](references/core.md)(何时读取:所有 CodeAct 脚本任务先读取)
- 参考:见 [references/web-data.md](references/web-data.md)(何时读取:联网搜索、网页抓取、API 数据、新闻、政策、行情、榜单)
- 参考:见 [references/concurrency.md](references/concurrency.md)(何时读取:批量抓取、多次 LLM、长文本分块、多文件或多 item 处理)
- 参考:见 [references/reporting.md](references/reporting.md)(何时读取:日报、周报、监控简报、调研报告、分析报告)
- 参考:见 [references/state.md](references/state.md)(何时读取:定时/重复执行、去重、增量运行、历史基线库)
- 参考:见 [references/agentic-report-blocks.md](references/agentic-report-blocks.md)(何时读取:跨来源报告、增量报告、带状态库的 agentic reporting)
- 参考:见 [references/skill-cli.md](references/skill-cli.md)(何时读取:任务需要使用其他 Skill、Skill CLI、本地脚本、飞书 lark-cli)
- 资产:见 [assets/script/](assets/script/)(用途:CodeAct 脚本参考实现，按“参考脚本索引”选择读取，不直接作为 Skill 工具执行)
