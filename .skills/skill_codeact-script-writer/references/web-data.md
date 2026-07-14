# 联网与数据源规则

当任务涉及搜索、网页抓取、API、新闻、政策、行情、榜单等动态数据时加载本文件。

## 参考脚本

- 天气/固定网页 + 搜索兜底：参考 `assets/script/weather_query.py`。
  - 适用于“查天气、查某城市预报、固定数据页优先、失败后搜索兜底”。
  - 重点参考：确定 URL 主源、`codeact_fetch_web`、搜索候选 URL 筛选、ResponseFormat 抽取、用户摘要。
- 行情/价格/阈值告警：参考 `assets/script/gold_price_monitor.py`。
  - 适用于“金价、股价、汇率、商品价格、超过/低于阈值告警”。
  - 重点参考：主数据源 + 搜索兜底、时效筛选、价格数值解析、`auto` 分流到 `display_only/no_reply`。
- 跨来源动态报告：参考 `assets/script/moore_threads_daily_briefing.py`。
  - 适用于“目标实体/主题的每日动态、政策跟踪、产品监控、竞品分析、技术巡检、安全简报、文档变更总结”。
  - 重点参考：基础线索 + LLM 动态扩展、候选筛选、内容获取后事实抽取、同信息单元多来源聚合。
  - 模块化最佳实践见 `references/agentic-report-blocks.md`。
- 数据 + 新闻融合日报：参考 `assets/script/daily_stock_summary.py`。
  - 适用于“行情/指标日报、把结构化数值数据和新闻正文融合成多章节长报告并配图”。
  - 重点参考：CLI 取结构化行情 + 按报告章节分组搜索、`_build_publish_time_window` 时效过滤、
    LLM 候选筛选（`source_tier` 来源分级）、多章节 URL 归属合并、正文分块直喂报告 LLM 的取舍。

参考脚本中的 `TOOL_SCHEMA_VERSIONS` 可能是模板占位值；编写实际脚本时必须先获取当前 CodeAct 工具真实 schema version 后替换。

## 数据源选择

- 已知 JSON API：优先用 `requests.get/post` 直接请求。
- 已知静态网页：使用 `codeact_fetch_web` 获取正文。
- 未知来源或需要最新信息：使用 `codeact_search_web` 搜索，再对候选 URL 调用 `codeact_fetch_web`。
- 官方源优先；没有官方源时选择知名第三方或权威媒体/机构。

HTTP API 请求模板：

```python
import requests

resp = requests.get(
    "https://api.example.com/data",
    params={"key": "value"},
    timeout=10,
)
resp.raise_for_status()
data = resp.json()
```

固定网页 fetch 模板：

```python
page = await sdk.call_tool(
    "codeact_fetch_web",
    {"url": target_url},
    schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
)
```

## 搜索结果处理

- 搜索结果的 `title`、`snippet`、`publish_time` 只能作为候选线索。
- 不得把 `snippet` 拼起来直接让 LLM 生成最终事实、政策结论、价格、榜单或阶段判断。
- 最终事实必须来自 fetch 后的页面正文、API 返回 JSON 或用户明确提供的数据。

## 时效性

- “最新、今日、本周、近期”类任务必须做时间筛选。
- 优先在 `codeact_search_web` 请求端就传 `publish_time` 时间窗口过滤，从源头只召回窗口内结果；
  再在结果端用 `publish_time` 字段二次兜底。只做结果端过滤会浪费搜索配额、且近期结果可能被挤出首页而漏采。
- 不得只在 query 中硬编码年份/月日来假装完成时效筛选。
- `publish_time` 解析应兼容 ISO 带时区格式，例如 `2026-06-15T18:03:57+08:00`、`Z`，并提供中文日期、斜杠、点号等兜底格式。
- fetch 后还要从页面标题、正文日期或 LLM 判断确认是否符合时效要求。

search 请求带时间窗口模板：

```python
from datetime import datetime, timedelta, timezone

def build_publish_time_window(lookback_days: int, tz: timezone = timezone(timedelta(hours=8))) -> dict[str, str]:
    """构造 codeact_search_web 的发布时间过滤窗口（RFC3339 带时区）。"""
    end = datetime.now(tz)
    start = end - timedelta(days=lookback_days)
    return {
        "start": start.isoformat(timespec="seconds"),
        "end": end.isoformat(timespec="seconds"),
    }

search = await sdk.call_tool(
    "codeact_search_web",
    {
        "query": query,
        # 请求端时效过滤：只召回近 lookback_days 天内发布的结果
        "publish_time": build_publish_time_window(lookback_days=7),
    },
    schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
)
```

时间解析模板：

```python
from datetime import datetime
from typing import Optional
import re

def parse_date_str(date_str: str) -> Optional[datetime]:
    raw = (date_str or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    normalized = re.sub(r"([+-]\d{2}:?\d{2})$", "", raw.replace("T", " ")).strip()
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y年%m月%d日", "%Y/%m/%d", "%Y.%m.%d"]:
        try:
            return datetime.strptime(normalized[:len(datetime.now().strftime(fmt))], fmt)
        except Exception:
            continue
    return None
```

## 候选筛选

- 搜索结果较多时，先按 URL、标题、发布时间、来源质量去重排序。
- 每批约 10 条候选，把 `title`、完整 `url`、`publish_time`、适度截断的 `snippet` 交给 LLM 判断是否值得 fetch。
- LLM 候选筛选应返回 `selected_urls: list[str]`，不要返回 index/序号。
- 返回 URL 后在当前 batch 内做规范化 URL 回查，避免模型输出错位。

LLM 候选筛选模板：

```python
import asyncio
import re
from pydantic import BaseModel

class FetchCandidateSelection(BaseModel):
    selected_urls: list[str]
    reason: str = ""

def truncate_text(text: str, limit: int = 240) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= limit else text[:limit] + "..."

def batches(items: list[dict], size: int = 10) -> list[list[dict]]:
    return [items[i:i + size] for i in range(0, len(items), size)]

def norm_url(url: str) -> str:
    return (url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/")

async def select_batch(batch: list[dict], sem: asyncio.Semaphore) -> list[dict]:
    prompt_lines = []
    for item in batch:
        prompt_lines.append(
            f"标题：{item.get('title', '')}\n"
            f"URL：{item.get('url', '')}\n"
            f"发布时间：{item.get('publish_time', '') or '未提供'}\n"
            f"摘要：{truncate_text(item.get('snippet', ''))}"
        )
    async with sem:
        selection = await sdk.call_llm(
            messages=[{"role": "user", "content": "从以下搜索结果中选择需要 fetch 正文核验的完整 URL；只返回本批 URL，不要返回序号。\n\n" + "\n\n".join(prompt_lines)}],
            response_format=FetchCandidateSelection,
        )
    selected_urls = {norm_url(url) for url in selection.selected_urls}
    return [item for item in batch if norm_url(item.get("url", "")) in selected_urls]

sem = asyncio.Semaphore(3)
batch_results = await asyncio.gather(
    *(select_batch(batch, sem) for batch in batches(candidates[:30], 10)),
    return_exceptions=True,
)
fetch_candidates = []
for result in batch_results:
    if not isinstance(result, Exception):
        fetch_candidates.extend(result)
```

## 正文抽取

- 对 HTML/自由文本使用 LLM + Pydantic BaseModel 做结构化抽取。
- 不要靠关键词抽段落作为最终事实判断。
- 长正文不要直接 `content[:N]` 后做关键决策；应分块、分页、换源或让 LLM 逐块抽取候选后汇总。
- 如果已观察目标网页结构并确认目标数据稳定出现在正文前段，可以像 `assets/script/weather_query.py`、`assets/script/gold_price_monitor.py` 那样做“输入预算控制”，但注释必须说明依据和不会截断有效信息的原因。
