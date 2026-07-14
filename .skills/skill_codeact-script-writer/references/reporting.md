# 报告生成规则

当用户需要日报、周报、监控简报、调研摘要、分析报告等面向阅读的结果时加载本文件。

## 基本方法

- 跨来源报告、增量报告、带状态库的 agentic reporting 可参考 `assets/script/moore_threads_daily_briefing.py`。
  - 重点参考：短 `submit_result.message` + 长 Markdown 报告、结构化事实、信息单元聚合、冷启动基线、来源与证据追溯。
- 数据 + 新闻融合的多章节长报告、需要配图时参考 `assets/script/daily_stock_summary.py`。
  - 重点参考：DomainConfig 集中管理章节/实体/图表配置、分块串行按章节写作（滚动上下文）、
    matplotlib 生成图表→`file_to_url` 上传→作为 Markdown 素材交给 LLM 引用、`_QUALITY_RULES` 强制内联来源与 `[INFO_GAP]`。
  - 取舍提醒：该模板为控本把 fetch 正文分块后直接喂给报告 LLM，省略了结构化事实抽取；
    这偏离下文“先做结构化事实再总结”的默认路径，属**有意简化**，靠“只喂全文 + 分块防截断 + prompt 质量约束”兜底。
    对事实精度要求高时，仍应采用 `moore_threads_daily_briefing.py` 的先抽取事实再总结方案。
- 若需要按模块复用该类脚本，先读 `references/agentic-report-blocks.md`，再按需打开完整脚本。
- 报告生成应作为独立环节，不要把抓取结果简单拼列表。
- 先做结构化事实：数据抓取、去重、时效筛选、字段提取、排序。
- 再让 LLM 基于已核验的结构化数据生成总结，不直接读取原始 HTML、完整 JSON 或搜索摘要。
- 报告结构建议：标题/日期 → 一句话结论 → 检索概况/核心指标 → 重点变化/风险/机会 → 明细 → 口径说明。

## 用户摘要

- `submit_result.message` 只放用户需要马上看到的摘要。
- 摘要应包含：一句话结论、2-4 条重点变化或风险、样本/候选/成功数量、报告文件位置（如有）。
- 不要在 message 中塞入整篇长文、原始数据或错误栈。

## 长文报告

- 如果报告包含大量明细、证据、表格或长分析，应写入 `./codeact/output/`。
- 文件名简短，无空格。
- 需要下载链接时调用 `file_to_url`。
- 在 `submit_result.data` 中返回 `report_path` 或 `report_url`。

报告文件 + 用户摘要模板：

```python
import os
from pydantic import BaseModel

class ReportSummary(BaseModel):
    overview: str
    key_changes: list[str]
    watch_points: list[str]

report_path = "./codeact/output/policy_report.md"
os.makedirs("./codeact/output", exist_ok=True)
with open(report_path, "w", encoding="utf-8") as f:
    f.write(long_report_markdown)

summary = await sdk.call_llm(
    messages=[{"role": "user", "content": f"请基于以下已核验事实生成简短摘要，不得编造输入外事实：\n{facts}"}],
    response_format=ReportSummary,
)

message = "\n".join([
    f"报告标题 | {today}",
    "",
    summary.overview,
    "",
    "重点变化",
    *[f"- {x}" for x in summary.key_changes],
    "",
    f"完整报告：{report_path}",
])
await sdk.submit_result(
    result_mode=result_mode,
    status="success",
    message=message,
    data={"report_path": report_path},
)
```

## LLM 总结

推荐使用结构化输出固定报告骨架：

```python
class ReportSummary(BaseModel):
    conclusion: str
    key_points: list[str]
    risks: list[str] = []
    next_steps: list[str] = []
```

prompt 必须要求：

- 只能基于输入事实生成总结。
- 不得编造输入外的机构、数字、政策、事件或结论。
- 重要判断应能追溯到来源 URL、发布日期、字段或证据摘要。

## 无结果报告

- 无结果也要说明结论和口径。
- 示例：“未确认到新政策；本次检索 N 条，进入正文核验 M 条，已排除新闻报道和招投标内容。”
