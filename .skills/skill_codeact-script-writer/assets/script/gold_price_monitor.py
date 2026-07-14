#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""国际金价阈值监控 CodeAct 脚本（展示网页抓取、结构化抽取、阈值告警）。

这个示例用于凸显 codeact-script-writer 中「联网数据 + 结构化抽取 + 状态分流」的实现：
- 工具调用必须使用 CodeAct SDK 侧工具，并显式传入实际 schema_version；
- 主数据源先用 codeact_fetch_web 抓取权威页面，再用 LLM ResponseFormat 抽取字段；
- 主源失败后进入搜索兜底：search → 候选筛选 → fetch → 结构化抽取；
- 抽取后只做数值解析，不设置固定价格区间，避免极端行情被误判为无效；
- result_mode=auto 时由阈值触发状态决定 display_only / no_reply，触发告警时 @主人；
- 所有成功、失败路径都通过 submit_result 返回。

主数据源：同花顺期货通贵金属现货页面
  https://fupage.10jqka.com.cn/futures-frontend-kamis-renderer/index.0.3.6.html?token=KC9MTg2MDQB5
兜底数据源：联网搜索 → fetch 正文 → LLM 结构化抽取

参数（codeact_args）：result_mode, threshold, direction
- result_mode: auto / display_only / notify / no_reply
  auto 时由脚本按阈值判断 display_only / no_reply
- threshold: 触发阈值，美元/盎司，默认 5000
- direction: above / below，默认 above
"""

import asyncio
import json
import math
import re
import statistics
import sys
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from codeact_sdk import CodeActSDK

# ===== SDK 工具版本占位符（skill 模板版）=====
# 安装时由 CodeAct agent 用 get_codeact_tool_schemas 取真实版本号替换下列占位值。
TOOL_SCHEMA_VERSIONS = {
    "codeact_fetch_web": "__FILL_FETCH_WEB_VERSION__",
    "codeact_search_web": "__FILL_SEARCH_WEB_VERSION__",
}

# ===== 常量 =====
FRESH_WINDOW_DAYS = 3
MAIN_SOURCE_URL = "https://fupage.10jqka.com.cn/futures-frontend-kamis-renderer/index.0.3.6.html?token=KC9MTg2MDQB5"
MAIN_SOURCE_NAME = "同花顺期货通"

SEARCH_QUERIES = [
    "伦敦金现 国际现货黄金价格 USD 盎司 实时",
    "live gold spot price USD per ounce XAU",
    "国际金价 现货 实时行情 美元/盎司",
]

PREFERRED_SEARCH_DOMAINS = (
    "10jqka.com.cn",
    "kitco.com",
    "investing.com",
    "tradingview.com",
    "sina.com.cn",
    "eastmoney.com",
)


class GoldPriceError(Exception):
    pass


# ===== ResponseFormat 结构化模型：让 LLM 直接返回可校验的业务字段 =====

class MetalPrice(BaseModel):
    """单个贵金属品种行情"""
    name: str = Field(description="品种名称，如'伦敦金现'、'黄金T+D'")
    price: Optional[float] = Field(default=None, description="最新价（数值）")
    change: Optional[float] = Field(default=None, description="涨跌额")
    change_pct: Optional[str] = Field(default=None, description="涨跌幅，如'-0.50%'")


class GoldPriceExtract(BaseModel):
    """从同花顺期货通页面抽取的结构化贵金属行情数据"""
    ok: bool = Field(default=False, description="是否成功抽取到有效行情数据")
    london_gold_price: Optional[float] = Field(default=None, description="伦敦金现价格（USD/盎司），仅填数值")
    london_silver_price: Optional[float] = Field(default=None, description="伦敦银现价格")
    gold_td_price: Optional[float] = Field(default=None, description="黄金T+D价格")
    silver_td_price: Optional[float] = Field(default=None, description="白银T+D价格")
    metals: List[MetalPrice] = Field(default_factory=list, description="所有品种行情列表")
    update_time: Optional[str] = Field(default=None, description="数据更新时间")
    evidence: str = Field(default="", description="价格提取依据原文")


class _CandidateSelection(BaseModel):
    """搜索候选筛选"""
    selected_urls: List[str] = Field(default_factory=list)
    reason: str = ""


# ===== 工具函数 =====

def _coerce_model(v: Any, model_cls: type) -> Any:
    """安全提取 LLM 结构化输出，兼容 SDK 返回模型、dict 或 JSON 字符串。"""
    if isinstance(v, model_cls):
        return v
    if isinstance(v, str):
        return model_cls(**json.loads(v))
    if isinstance(v, dict):
        return model_cls(**v)
    raise GoldPriceError(f"LLM 返回格式不可用，期望 {model_cls.__name__}")


def parse_gold_price(value: Any) -> Optional[float]:
    """将抽取出的金价转为有限数值；不设置固定价格区间，避免极端行情 bad case。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        val = float(value)
        return val if math.isfinite(val) else None
    try:
        val = float(str(value).replace(",", "").strip())
        return val if math.isfinite(val) else None
    except (ValueError, TypeError):
        return None


def fmt_money(value: float) -> str:
    return f"${value:,.2f}"


def normalize_direction(raw: str) -> str:
    direction = (raw or "above").strip().lower()
    if direction not in {"above", "below"}:
        raise ValueError("direction 只能是 above 或 below")
    return direction


def parse_threshold(raw: str) -> float:
    try:
        value = float(str(raw).replace(",", "").strip())
    except (ValueError, TypeError):
        raise ValueError("threshold 必须是正数（美元/盎司）")
    if value <= 0:
        raise ValueError("threshold 必须是正数（美元/盎司）")
    return value


def is_triggered(price: float, threshold: float, direction: str) -> bool:
    return price > threshold if direction == "above" else price < threshold


def trigger_phrase(threshold: float, direction: str) -> str:
    if direction == "above":
        return f"已高于阈值 {fmt_money(threshold)}/盎司，触发 above 告警"
    return f"已低于阈值 {fmt_money(threshold)}/盎司，触发 below 告警"


def _norm_url(url: str) -> str:
    return (url or "").split("#", 1)[0].rstrip("/")


def collapse_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or " ")
    text = text.replace("\\$", "$").replace("\\n", "\n").replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def split_extract_chunks(content: str, limit: int) -> List[str]:
    """压缩正文并分块覆盖全文，避免基于前段截断内容做最终价格判断。"""
    text = collapse_text(content)
    if len(text) <= limit:
        return [text] if text else []
    chunks: List[str] = []
    start = 0
    overlap = min(500, limit // 10)
    while start < len(text):
        end = min(len(text), start + limit)
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return chunks


# ===== 主数据源：同花顺期货通 fetch + LLM 结构化抽取 =====

async def fetch_main_source(sdk: CodeActSDK) -> Dict[str, Any]:
    """主数据源：fetch 权威行情页，再用 ResponseFormat 结构化抽取。"""
    url = MAIN_SOURCE_URL
    print(f"[主数据源] 正在获取 {MAIN_SOURCE_NAME}: {url}")

    page = await sdk.call_tool(
        "codeact_fetch_web",
        {"url": url},
        schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
    )

    if page.get("is_success") is False:
        raise GoldPriceError(f"{MAIN_SOURCE_NAME}页面获取失败: {page.get('error', '未知错误')}")

    content = str(page.get("content") or "").strip()
    title = str(page.get("title") or "")

    if not content:
        raise GoldPriceError(f"{MAIN_SOURCE_NAME}页面正文为空")

    print(f"[主数据源] 页面获取成功，正文长度={len(content)}")

    # 使用统一页面抽取管线：短页单次抽取，长页分块覆盖全文后择优。
    extract = await extract_gold_from_page(sdk, title, content, MAIN_SOURCE_NAME)

    if not extract.ok or extract.london_gold_price is None:
        raise GoldPriceError(f"{MAIN_SOURCE_NAME}结构化抽取失败，未获取到有效伦敦金现价格")

    price = parse_gold_price(extract.london_gold_price)
    if price is None:
        raise GoldPriceError(f"{MAIN_SOURCE_NAME}抽取的伦敦金现价格 {extract.london_gold_price} 不是有效数值")

    print(f"[主数据源] 结构化抽取成功，伦敦金现={price:.2f}")
    return {
        "source": MAIN_SOURCE_NAME,
        "price": price,
        "extract": extract,
        "url": url,
    }


async def extract_gold_from_page(
    sdk: CodeActSDK, title: str, content: str, source_hint: str
) -> GoldPriceExtract:
    """统一页面抽取管线：主源和搜索兜底都走这里，避免两套近似抽取逻辑。"""
    chunks = split_extract_chunks(content, 15000)
    if not chunks:
        return GoldPriceExtract(ok=False)

    sem = asyncio.Semaphore(3)

    async def extract_chunk(idx: int, text: str) -> GoldPriceExtract:
        async with sem:
            prompt = (
                f"从以下网页正文分块提取贵金属现货行情数据。\n\n"
                f"来源类型：{source_hint}\n"
                f"页面标题：{title}\n"
                f"分块：{idx + 1}/{len(chunks)}\n\n"
                f"页面正文分块：\n{text}\n\n"
                "提取要求：\n"
                "1. 只基于当前分块中明确出现的信息抽取，不要根据标题、搜索摘要或常识补全\n"
                "2. 伦敦金现价格（london_gold_price）：这是国际现货黄金 USD/盎司价格，仅填数值\n"
                "3. 伦敦银现价格（london_silver_price）：国际现货白银价格\n"
                "4. 黄金T+D价格（gold_td_price）：上海黄金交易所黄金T+D价格\n"
                "5. 白银T+D价格（silver_td_price）：上海黄金交易所白银T+D价格\n"
                "6. metals 列表：逐行提取页面上所有品种的名称、最新价、涨跌、涨跌幅\n"
                "7. 如果当前分块无法提取有效价格数据，设 ok=false\n"
                "8. evidence 填写价格对应的原文片段，用于核验\n"
            )
            result = await sdk.call_llm(
                messages=[{"role": "user", "content": prompt}],
                response_format=GoldPriceExtract,
            )
            return _coerce_model(result, GoldPriceExtract)

    parts = await asyncio.gather(*(extract_chunk(i, chunk) for i, chunk in enumerate(chunks)))
    for part in parts:
        if part.ok and parse_gold_price(part.london_gold_price) is not None:
            return part
    return GoldPriceExtract(ok=False)


# ===== 兜底数据源：搜索 + fetch + LLM 抽取 =====

async def fetch_search_fallback(sdk: CodeActSDK) -> Dict[str, Any]:
    """搜索兜底：search 扩展候选 → LLM 选 URL → fetch 正文 → LLM 抽取。"""
    print("[搜索兜底] 正在搜索国际金价...")

    candidates: List[Dict[str, Any]] = []
    seen_urls: set = set()
    now = datetime.utcnow()

    for query in SEARCH_QUERIES:
        try:
            search = await sdk.call_tool(
                "codeact_search_web",
                {"query": query, "response_length": "long"},
                schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
            )
            if not search or search.get("is_success") is False:
                continue

            for item in search.get("results") or []:
                url = _norm_url(str(item.get("url") or ""))
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)

                # 时效性筛选：行情类任务优先近期页面，降低历史文章误抽取概率。
                dt_str = item.get("publish_time") or ""
                if dt_str:
                    try:
                        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)
                        if now - timedelta(days=FRESH_WINDOW_DAYS) > dt:
                            continue
                    except Exception:
                        pass

                snippet = collapse_text(str(item.get("snippet") or ""))[:240]
                candidates.append({
                    "title": item.get("title") or "",
                    "url": url,
                    "publish_time": dt_str,
                    "snippet": snippet,
                })
        except Exception as e:
            print(f"[搜索兜底] 搜索失败[{query}]: {e}")
            continue

    if not candidates:
        raise GoldPriceError("搜索结果为空")

    print(f"[搜索兜底] 搜索返回 {len(candidates)} 条候选")

    # LLM 筛选候选 URL：把“哪个页面值得 fetch”的判断交给结构化模型输出。
    selected = await _select_search_candidates(sdk, candidates)

    # 对筛选后的候选逐一 fetch 并抽取；第一个通过价格校验的页面即作为兜底结果。
    for item in selected[:5]:
        url = item.get("url", "")
        if not url:
            continue
        try:
            page = await sdk.call_tool(
                "codeact_fetch_web",
                {"url": url},
                schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
            )
            if page.get("is_success") is False or not str(page.get("content") or "").strip():
                continue

            extract = await extract_gold_from_page(
                sdk,
                str(page.get("title") or item.get("title") or ""),
                str(page.get("content") or ""),
                "搜索兜底抓页",
            )
            if extract.ok and extract.london_gold_price is not None:
                price = parse_gold_price(extract.london_gold_price)
                if price is not None:
                    print(f"[搜索兜底] 从 {url} 抽取成功，伦敦金现={price:.2f}")
                    return {
                        "source": f"搜索抓页: {item.get('title', '')}",
                        "price": price,
                        "extract": extract,
                        "url": url,
                    }
        except Exception as e:
            print(f"[搜索兜底] 跳过 {url}: {e}")
            continue

    raise GoldPriceError("搜索兜底未获得有效金价数据")


async def _select_search_candidates(
    sdk: CodeActSDK, candidates: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """LLM 筛选搜索候选 URL；失败时退回规则排序，保证兜底链路继续推进。"""
    if not candidates:
        return []

    lines = []
    for item in candidates[:15]:
        lines.append(
            f"标题：{item.get('title')}\n"
            f"URL：{item.get('url')}\n"
            f"发布时间：{item.get('publish_time') or '未提供'}\n"
            f"摘要：{item.get('snippet')}"
        )

    prompt = (
        "从以下搜索结果中选择需要 fetch 正文核验「当前国际现货黄金 USD/盎司价格」的完整 URL。"
        "只返回 URL，不返回序号；"
        "优先实时行情页（live/spot/XAU/USD），排除历史文章和预测文章。\n\n"
        + "\n\n".join(lines)
    )

    try:
        result = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=_CandidateSelection,
        )
        selection = _coerce_model(result, _CandidateSelection)
        selected_urls = {_norm_url(url) for url in selection.selected_urls}
        picked = [item for item in candidates if _norm_url(str(item.get("url") or "")) in selected_urls]
        return picked[:6] if picked else candidates[:4]
    except Exception as e:
        print(f"[搜索候选筛选] LLM 失败，使用规则排序: {e}")
        return candidates[:4]


# ===== 综合获取逻辑 =====

async def get_gold_price(sdk: CodeActSDK) -> Dict[str, Any]:
    """两层降级：主源失败不直接报错，而是继续走搜索兜底。"""
    # 第一层：同花顺期货通主数据源
    try:
        data = await fetch_main_source(sdk)
        print(f"[数据源] {MAIN_SOURCE_NAME} 成功，伦敦金现={data['price']:.2f}")
        return data
    except Exception as e:
        print(f"[数据源] {MAIN_SOURCE_NAME} 失败: {e}")

    # 第二层：搜索兜底
    try:
        data = await fetch_search_fallback(sdk)
        print(f"[数据源] 搜索兜底 成功，伦敦金现={data['price']:.2f}")
        return data
    except Exception as e:
        print(f"[数据源] 搜索兜底 失败: {e}")

    raise GoldPriceError("所有数据源均失败，无法获取金价")


# ===== 消息构建 =====

def build_summary(data: Dict[str, Any], threshold: float, direction: str) -> str:
    """构建面向用户的简报：先结论后细节，再展示阈值监控状态。"""
    price = data["price"]
    source = data.get("source", "未知")
    url = data.get("url", "")
    extract = data.get("extract")
    triggered = is_triggered(price, threshold, direction)

    lines: List[str] = []

    # 标题行
    lines.append("💰 国际金价行情简报")
    lines.append(f"数据源：{source}")

    # 结论行（先结论后细节）
    if triggered:
        lines.extend(["", f"👉 国际现货黄金当前 {fmt_money(price)}/盎司，{trigger_phrase(threshold, direction)}。"])
    else:
        lines.extend(["", f"👉 国际现货黄金当前 {fmt_money(price)}/盎司，未达到 {direction} {fmt_money(threshold)}/盎司的提醒条件。"])

    # 如果主数据源成功，展示详细行情表
    if isinstance(extract, GoldPriceExtract) and extract.metals:
        lines.extend(["", "【贵金属行情】"])
        lines.append(f"  {'品种':<12}{'最新价':>12}{'涨跌':>12}{'涨跌幅':>10}")
        lines.append(f"  {'─'*12}{'─'*12}{'─'*12}{'─'*10}")
        for m in extract.metals:
            name = m.name or ""
            price_str = f"{m.price:.2f}" if m.price is not None else "-"
            change_str = f"{m.change:+.2f}" if m.change is not None else "-"
            pct_str = m.change_pct or "-"
            lines.append(f"  {name:<12}{price_str:>12}{change_str:>12}{pct_str:>10}")
    elif isinstance(extract, GoldPriceExtract):
        # 没有 metals 列表但有单个价格
        items = []
        if extract.london_gold_price is not None:
            items.append(f"  伦敦金现：{fmt_money(extract.london_gold_price)}/盎司")
        if extract.london_silver_price is not None:
            items.append(f"  伦敦银现：{extract.london_silver_price}")
        if extract.gold_td_price is not None:
            items.append(f"  黄金T+D：{extract.gold_td_price}")
        if extract.silver_td_price is not None:
            items.append(f"  白银T+D：{extract.silver_td_price}")
        if items:
            lines.extend(["", "【贵金属行情】"])
            lines.extend(items)

    # 阈值监控信息
    lines.extend(["", "【阈值监控】"])
    lines.append(f"  当前价格：{fmt_money(price)}/盎司")
    lines.append(f"  监控阈值：{fmt_money(threshold)}/盎司（{direction}）")
    status_text = "⚠️ 已触发" if triggered else "✅ 未触发"
    lines.append(f"  触发状态：{status_text}")

    return "\n".join(lines)


# ===== 主入口 =====

async def main() -> None:
    result_mode_raw = sys.argv[1] if len(sys.argv) > 1 else "auto"
    threshold_raw = sys.argv[2] if len(sys.argv) > 2 else "5000"
    direction_raw = sys.argv[3] if len(sys.argv) > 3 else "above"

    sdk = CodeActSDK()
    try:
        # 参数解析
        mode = (result_mode_raw or "auto").strip().lower()
        if mode not in {"auto", "display_only", "notify", "no_reply"}:
            raise ValueError("result_mode 只能是 auto / display_only / notify / no_reply")
        threshold = parse_threshold(threshold_raw)
        direction = normalize_direction(direction_raw)

        print(f"[参数] result_mode={mode}, threshold={threshold}, direction={direction}")

        # 获取金价
        data = await get_gold_price(sdk)
        price = data["price"]

        # 判断阈值
        triggered = is_triggered(price, threshold, direction)
        print(
            f"[判断] price={price:.4f}, threshold={threshold:.4f}, "
            f"direction={direction}, triggered={triggered}, source={data.get('source')}"
        )

        # auto 由脚本按阈值分流；显式 mode 直接沿用
        if mode == "auto":
            actual_mode = "display_only" if triggered else "no_reply"
        else:
            actual_mode = mode

        # 构建消息
        message = build_summary(data, threshold, direction)

        # 触发时在消息前加 @主人
        if triggered:
            message = f"[主人](at://owner) " + message
        elif actual_mode == "no_reply":
            message = "NO_REPLY"

        await sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message=message,
            data={
                "price_usd_per_oz": round(price, 4),
                "threshold": threshold,
                "direction": direction,
                "triggered": triggered,
                "source": data.get("source"),
                "source_url": data.get("url", ""),
            },
        )
    except Exception as e:
        print(f"[错误] {e}\n{traceback.format_exc()}")
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"国际金价阈值监控取价失败：{e}",
        )


if __name__ == "__main__":
    asyncio.run(main())
