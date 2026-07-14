# 并发规则

当脚本会处理多个 URL、query、文件、item、chunk 或多次 LLM 调用时加载本文件。

## 参考脚本

- 长文翻译/改写：参考 `assets/script/long_text_translate.py`。
  - 重点参考：Markdown 结构感知分块、代码块跳过、相邻小块合并、前后文窗口、术语表按块数启用、LLM 并发和失败块回填。
- 文档 diff/概览总结：参考 `assets/script/feishu_doc_watch.py`。
  - 重点参考：先计算真实 diff，再按 hunk/行分块；首次长文概览用重叠分块；分块总结并发执行，最后合并摘要。
- 天气/金价搜索兜底：参考 `assets/script/weather_query.py`、`assets/script/gold_price_monitor.py`。
  - 重点参考：搜索候选筛选和多候选 fetch 的顺序/并发取舍；强依赖“第一个有效结果”的场景可以串行推进。
- 多章节长报告的并发检索 + 串行写作：参考 `assets/script/daily_stock_summary.py`。
  - 重点参考：各章节 query 用 `asyncio.gather` + `Semaphore` 并发搜索、URL 去重后统一并发 fetch；
    报告写作反而串行推进，靠“最近章节全文 + 更早章节压缩摘要”滚动控制 prompt 体积（`_truncate_ctx`）、
    单章节失败用占位文本兜底不中断整篇。

## 默认原则

- 批量独立的 SDK 工具调用和 LLM 调用默认并发执行。
- 不要写 `for item in items: await ...`，除非后一轮确实依赖前一轮结果。
- 工具调用并发建议 `asyncio.Semaphore(5)`。
- LLM 调用并发建议 `asyncio.Semaphore(3)`。
- 批量结果必须按原始顺序合并。
- 部分失败可容忍时使用 `asyncio.gather(..., return_exceptions=True)`。

## 工具并发模板

```python
async def fetch_one(item: dict, sem: asyncio.Semaphore) -> dict:
    async with sem:
        page = await sdk.call_tool(
            "codeact_fetch_web",
            {"url": item["url"]},
            schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
        )
        return {"item": item, "page": page}

sem = asyncio.Semaphore(5)
tasks = [fetch_one(item, sem) for item in candidates if item.get("url")]
results = await asyncio.gather(*tasks, return_exceptions=True)
valid = [r for r in results if not isinstance(r, Exception)]
```

## LLM 并发模板

```python
async def extract_one(chunk: str, sem: asyncio.Semaphore) -> ExtractResult:
    async with sem:
        return await sdk.call_llm(
            messages=[{"role": "user", "content": chunk}],
            response_format=ExtractResult,
        )

sem = asyncio.Semaphore(3)
results = await asyncio.gather(
    *(extract_one(chunk, sem) for chunk in chunks),
    return_exceptions=True,
)
results = [r for r in results if not isinstance(r, Exception)]
```

## 长正文分块抽取模板

```python
import asyncio

def chunk_text(text: str, chunk_size: int = 6000, overlap: int = 300) -> list[str]:
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks

async def extract_chunk(chunk: str, sem: asyncio.Semaphore) -> ExtractResult:
    async with sem:
        return await sdk.call_llm(
            messages=[{"role": "user", "content": f"只从本块内容提取候选信息；没有则 extracted=false：\n{chunk}"}],
            response_format=ExtractResult,
        )

chunks = chunk_text(page.get("content") or "")
sem = asyncio.Semaphore(3)
partial_infos = await asyncio.gather(
    *(extract_chunk(chunk, sem) for chunk in chunks),
    return_exceptions=True,
)
partial_infos = [info for info in partial_infos if not isinstance(info, Exception)]
```

## 长文本翻译/改写/摘要模板

处理长文本时，先按任务性质决定分块大小：

- 总结/摘要类：输出远小于输入，单块可相对较大，如 10000-20000 字符。
- 翻译/改写类：输出与输入接近，单块应适当缩小，如 4000-6000 字符。
- 不要过度碎片化；相邻小块应合并，块数过多时优先调大 `chunk_size`。

翻译、改写等需要连续性的任务，应给每个块注入前后文窗口，并用分隔标记限定当前块：

```python
def build_chunk_prompt(prev_tail: str, current: str, next_head: str) -> str:
    return (
        "请只处理 ===CURRENT_START=== 与 ===CURRENT_END=== 之间的正文。"
        "前后文仅用于理解衔接，不要重复输出前后文。\n\n"
        f"前文参考：\n{prev_tail}\n\n"
        f"===CURRENT_START===\n{current}\n===CURRENT_END===\n\n"
        f"后文参考：\n{next_head}"
    )
```

多块处理仍然并发执行，最终必须按原始块顺序合并写入输出文件。对代码块、表格块或需要原样保留的片段，先用代码分离并跳过 LLM 改写。

## 允许串行的情况

- 下一轮 query 依赖上一轮 LLM 判断。
- API 链后一步依赖前一步返回值。
- 用户要求严格顺序执行。
- 服务端限流严重，需要主动降并发或串行。
