#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""摩尔线程每日动态分析简报 CodeAct 脚本（专用版）。

这个脚本只服务一个任务：每天收盘后生成摩尔线程（688795.SH）动态简报。

设计重点：
- 无 CLI 入参：固定股票、固定输出目录、固定 result_mode/display_only，减少模板噪声。
- 状态库：SQLite 记录历史 URL、事件指纹和每日快照，支持跨天去重和“新增/延续”判断。
- 动态搜索：基础 query + 历史事件 + LLM 扩展 query，避免长期只搜固定关键词。
- 事件聚合：先抽取事实，再让 LLM 合并同事件多来源报道，替代 URL/标题前缀粗去重。
- Agentic workflow：搜索规划、候选筛选、正文抓取、事实抽取、事件聚合、报告生成、状态写入逐步推进。
- 质量门控：冷启动只建基线；来源等级和强 claim 由 LLM 基于搜索结果/正文动态判断。
- 分析报告：输出一屏摘要、核心主线、事件解读、风险与后续关注，而不是新闻列表。

通用 Building Blocks 实例：
- 领域配置：用 STOCK_NAME/STOCK_CODE/BASE_SEARCH_QUERIES 固定目标实体与关注范围。
- 结构化模型：SearchQueryPlan、ExtractedFacts、AggregatedEvents、BriefingTakeaways 分别承接不同 LLM 阶段。
- 状态库：seen_urls/events/daily_events/runs 记录来源、信息单元、每日快照和运行记录。
- 权威锚点：先取新浪/东方财富行情，作为后续新闻口径检查和报告表格的基准。
- 信息发现：基础 query + 历史事件 + LLM 动态扩展，避免长期只搜固定关键词。
- 候选筛选：搜索结果只作为线索，由 LLM 选择值得 fetch 的 URL 并动态判断来源质量。
- 内容获取：只抓取筛选后的正文，并把来源判断传入事实抽取。
- 原子事实抽取：每页抽成可核验事实，不把搜索摘要或原始 HTML 直接交给报告生成。
- 信息单元聚合：把同一事件的多来源报道合并，输出为什么重要、影响、证据和来源。
- 状态增强：冷启动标 baseline，后续运行区分 new/continuing。
- 事实边界检查：用确定行情数据发现新闻口径冲突，并把提示写入报告。
- 一屏摘要：短结论只基于已聚合事件生成，避免过强投研判断。
- 长报告：完整 Markdown 写入 ./codeact/output/，保留明细表和来源列表。
- 短消息：submit_result.message 只放摘要和 computer:// 报告入口。
- 成功后写状态：报告生成成功后才写 SQLite，失败不污染状态库。
"""

import asyncio
import hashlib
import json
import os
import re
import sqlite3
import traceback
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Optional

import requests
from codeact_sdk import CodeActSDK
from pydantic import BaseModel, Field


# ===== SDK 工具版本占位符（skill 模板版）=====
# 安装时由 CodeAct agent 用 get_codeact_tool_schemas 取真实版本号替换下列占位值。
TOOL_SCHEMA_VERSIONS = {
    "codeact_search_web": "__FILL_SEARCH_WEB_VERSION__",
    "codeact_fetch_web": "__FILL_FETCH_WEB_VERSION__",
}

STOCK_NAME = "摩尔线程"
STOCK_CODE = "688795.SH"
RESULT_MODE = "display_only"
OUTPUT_DIR = "./codeact/output"
STATE_DIR = "./codeact/state"
STATE_DB = os.path.join(STATE_DIR, "moore_threads_daily_briefing.sqlite")
SEARCH_DAYS = 3
MAX_SEARCH_RESULTS = 40
MAX_FETCH_PAGES = 18
MAX_FACTS_FOR_CLUSTER = 45
FETCH_CONTENT_CHARS = 12000
TZ = timezone(timedelta(hours=8))

BASE_SEARCH_QUERIES = [
    "摩尔线程 688795 最新 动态 公告",
    "摩尔线程 国产GPU AI算力 产品 合作",
    "摩尔线程 MTT GPU 生态 适配 最新",
    "摩尔线程 投资者关系 机构调研 最新",
    "国产GPU 算力芯片 政策 竞品 壁仞 燧原 沐曦",
]


# ===== ResponseFormat 模型：每个 LLM 阶段只返回结构化字段，避免自由文本难以接入后续流程 =====

class SearchQueryPlan(BaseModel):
    """动态搜索规划：让 LLM 基于历史事件补充 query，而不是长期只跑固定搜索词。"""
    queries: list[str] = Field(default_factory=list)
    reason: str = ""


class CandidateSourceAssessment(BaseModel):
    """候选来源质量判断：由 LLM 按搜索结果语义动态判断，不在代码中写死域名表。"""
    url: str = ""
    source_tier: str = "unverified"
    reason: str = ""


class FetchCandidateSelection(BaseModel):
    """搜索候选筛选：既选出值得 fetch 的 URL，也把来源判断传给后续抽取阶段。"""
    selected_urls: list[str] = Field(default_factory=list)
    source_assessments: list[CandidateSourceAssessment] = Field(default_factory=list)
    reason: str = ""


class ExtractedFact(BaseModel):
    """网页正文中的原子事实。后续事件聚合只基于这些事实，不直接读搜索摘要。"""
    category: str = ""
    title: str = ""
    content: str = ""
    source_url: str = ""
    publish_date: str = ""
    source_tier: str = "unverified"
    source_assessment: str = ""
    claim_strength: str = "normal"
    fact_boundary: str = ""


class ExtractedFacts(BaseModel):
    """单页抽取结果。has_new_info=false 时整页不参与聚合。"""
    facts: list[ExtractedFact] = Field(default_factory=list)
    has_new_info: bool = False


class AggregatedEvent(BaseModel):
    """聚合后的事件。一个事件可合并多个来源，避免同一新闻被重复展示。"""
    event_title: str = ""
    category: str = "company"
    event_summary: str = ""
    signal_type: str = "其他"
    importance: str = "medium"
    confidence: str = "medium"
    why_it_matters: str = ""
    impact: str = ""
    evidence: str = ""
    publish_date: str = ""
    source_urls: list[str] = Field(default_factory=list)
    source_titles: list[str] = Field(default_factory=list)
    source_tier: str = "unverified"
    claim_strength: str = "normal"
    lifecycle: str = "new"
    fact_boundary: str = ""
    conflict_note: str = ""
    event_key: str = ""
    is_new: bool = True


class AggregatedEvents(BaseModel):
    events: list[AggregatedEvent] = Field(default_factory=list)


class BriefingTakeaways(BaseModel):
    """一屏摘要：面向用户的短结论，与长报告正文分离。"""
    headline: str = ""
    overall_summary: str = ""
    key_changes: list[str] = Field(default_factory=list)
    impact_assessment: str = ""
    risk_flags: list[str] = Field(default_factory=list)
    watch_points: list[str] = Field(default_factory=list)


def now_cn() -> datetime:
    return datetime.now(TZ)


def norm_url(url: str) -> str:
    return (url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/")


def normalize_text(text: str) -> str:
    """生成事件匹配用的弱归一化文本。

    模板说明：这里不做业务词硬替换，只去 URL、符号和大小写差异；
    更复杂的“是否同一事件”判断交给 LLM 聚合，代码只做状态库兜底匹配。
    """
    text = re.sub(r"https?://\S+", "", text or "")
    return re.sub(r"[^\w\u4e00-\u9fa5]+", "", text.lower())[:120]


def event_fingerprint(title: str, summary: str = "") -> str:
    """为事件生成稳定指纹，用于跨天状态库去重。"""
    base = normalize_text(title) or normalize_text(summary)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def parse_date(date_str: str) -> Optional[datetime]:
    if not date_str:
        return None
    raw = date_str.strip()
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%Y年%m月%d日",
        "%Y/%m/%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt if dt.tzinfo else dt.replace(tzinfo=TZ)
        except ValueError:
            pass
    match = re.search(r"\d{4}-\d{2}-\d{2}", raw)
    if match:
        return datetime.strptime(match.group(0), "%Y-%m-%d").replace(tzinfo=TZ)
    return None


def truncate_text(text: str, limit: int = 240) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= limit else text[:limit] + "..."


def collapse_text(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text or "")
    text = text.replace("\\n", "\n").replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def split_page_chunks(content: str, limit: int = FETCH_CONTENT_CHARS) -> list[str]:
    """将网页正文分块覆盖全文，避免只基于前段截断内容抽取事实。"""
    text = collapse_text(content)
    if len(text) <= limit:
        return [text] if text else []
    chunks: list[str] = []
    start = 0
    overlap = min(600, limit // 10)
    while start < len(text):
        end = min(len(text), start + limit)
        chunks.append(text[start:end])
        if end >= len(text):
            break
        start = max(end - overlap, start + 1)
    return chunks


def parse_model_safe(model_cls, value):
    """兼容 SDK 返回 Pydantic 对象、dict、JSON 字符串或夹杂解释文本的兜底解析。"""
    if isinstance(value, model_cls):
        return value
    if isinstance(value, dict):
        try:
            return model_cls(**value)
        except Exception:
            return model_cls()
    if isinstance(value, str):
        try:
            return model_cls(**json.loads(value))
        except Exception:
            match = re.search(r"\{.*\}", value, re.DOTALL)
            if match:
                try:
                    return model_cls(**json.loads(match.group()))
                except Exception:
                    pass
    return model_cls()


def init_state_db() -> sqlite3.Connection:
    """初始化增量状态库。

    模板说明：
    - seen_urls：URL 级去重，避免重复 fetch/展示同一来源。
    - events：事件级去重，解决“同一事件多来源/换标题”的跨天重复问题。
    - daily_events：每日快照，用于报告“新增/延续”和历史对比。
    - runs：运行记录，用于识别冷启动基线。
    """
    os.makedirs(STATE_DIR, exist_ok=True)
    conn = sqlite3.connect(STATE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_urls (
            url TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            title TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            event_key TEXT PRIMARY KEY,
            canonical_title TEXT NOT NULL,
            normalized_title TEXT NOT NULL,
            category TEXT,
            summary TEXT,
            first_seen TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            source_count INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_events (
            run_date TEXT NOT NULL,
            event_key TEXT NOT NULL,
            is_new INTEGER NOT NULL,
            title TEXT NOT NULL,
            category TEXT,
            importance TEXT,
            PRIMARY KEY (run_date, event_key)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_date TEXT PRIMARY KEY,
            generated_at TEXT NOT NULL,
            report_path TEXT,
            total_events INTEGER,
            new_events INTEGER
        )
    """)
    conn.commit()
    return conn


def load_recent_event_titles(conn: sqlite3.Connection, limit: int = 20) -> list[str]:
    rows = conn.execute(
        "SELECT canonical_title FROM events ORDER BY last_seen DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [r[0] for r in rows]


def load_previous_daily_titles(conn: sqlite3.Connection, run_date: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT title FROM daily_events
        WHERE run_date < ?
        ORDER BY run_date DESC
        LIMIT 20
        """,
        (run_date,),
    ).fetchall()
    return [r[0] for r in rows]


def has_prior_runs(conn: sqlite3.Connection, run_date: str) -> bool:
    row = conn.execute("SELECT COUNT(*) FROM runs WHERE run_date < ?", (run_date,)).fetchone()
    return bool(row and row[0] > 0)


def find_existing_event_key(conn: sqlite3.Connection, title: str) -> Optional[str]:
    norm = normalize_text(title)
    if not norm:
        return None
    rows = conn.execute(
        "SELECT event_key, normalized_title FROM events ORDER BY last_seen DESC LIMIT 200"
    ).fetchall()
    for key, old_norm in rows:
        if old_norm == norm:
            return key
        if old_norm and SequenceMatcher(None, norm, old_norm).ratio() >= 0.72:
            return key
    return None


def is_url_seen(conn: sqlite3.Connection, url: str) -> bool:
    if not url:
        return False
    row = conn.execute("SELECT 1 FROM seen_urls WHERE url=?", (norm_url(url),)).fetchone()
    return row is not None


def save_state(
    conn: sqlite3.Connection,
    run_date: str,
    report_path: str,
    events: list[AggregatedEvent],
) -> None:
    """只在成功生成报告后写状态，避免失败运行污染增量基线。"""
    ts = now_cn().isoformat()
    for event in events:
        norm_title = normalize_text(event.event_title)
        conn.execute(
            """
            INSERT INTO events(event_key, canonical_title, normalized_title, category, summary,
                               first_seen, last_seen, source_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_key) DO UPDATE SET
                canonical_title=excluded.canonical_title,
                normalized_title=excluded.normalized_title,
                category=excluded.category,
                summary=excluded.summary,
                last_seen=excluded.last_seen,
                source_count=excluded.source_count
            """,
            (
                event.event_key,
                event.event_title,
                norm_title,
                event.category,
                event.event_summary,
                ts,
                ts,
                len(event.source_urls),
            ),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO daily_events(run_date, event_key, is_new, title, category, importance)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_date,
                event.event_key,
                1 if event.is_new else 0,
                event.event_title,
                event.category,
                event.importance,
            ),
        )
        for url, title in zip(event.source_urls, event.source_titles or []):
            clean_url = norm_url(url)
            if not clean_url:
                continue
            conn.execute(
                """
                INSERT INTO seen_urls(url, first_seen, last_seen, title)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(url) DO UPDATE SET last_seen=excluded.last_seen, title=excluded.title
                """,
                (clean_url, ts, ts, title or event.event_title),
            )
    conn.execute(
        """
        INSERT OR REPLACE INTO runs(run_date, generated_at, report_path, total_events, new_events)
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_date, ts, report_path, len(events), sum(1 for e in events if e.is_new)),
    )
    conn.commit()


def fetch_stock_data_sina() -> Optional[dict]:
    """行情主源示例：优先使用确定 API，失败时返回 None 进入兜底源。"""
    try:
        symbol = "sh688795"
        resp = requests.get(
            f"https://hq.sinajs.cn/list={symbol}",
            headers={
                "Referer": "https://finance.sina.com.cn",
                "User-Agent": "Mozilla/5.0",
            },
            timeout=10,
        )
        resp.encoding = "gbk"
        match = re.search(r'var hq_str_[^=]+="(.*)"', resp.text)
        if not match:
            return None
        fields = match.group(1).split(",")
        if len(fields) < 10 or not fields[0]:
            return None
        open_price = float(fields[1] or 0)
        prev_close = float(fields[2] or 0)
        current_price = float(fields[3] or 0)
        high_price = float(fields[4] or 0)
        low_price = float(fields[5] or 0)
        volume = int(float(fields[8] or 0)) // 100
        turnover = float(fields[9] or 0) / 10000
        if current_price == 0 and open_price == 0:
            return None
        return {
            "name": fields[0],
            "code": STOCK_CODE,
            "open_price": open_price,
            "prev_close": prev_close,
            "current_price": current_price,
            "high_price": high_price,
            "low_price": low_price,
            "volume": volume,
            "turnover": round(turnover, 2),
            "change_amount": round(current_price - prev_close, 2),
            "change_pct": round((current_price - prev_close) / prev_close * 100, 2) if prev_close else 0.0,
            "is_trading_day": True,
            "data_source": "新浪财经",
            "error": "",
        }
    except Exception as e:
        print(f"[行情] 新浪财经获取失败: {e}")
        return None


def fetch_stock_data_eastmoney() -> Optional[dict]:
    """行情兜底源示例：与主源保持相同字段结构，方便报告层无感使用。"""
    try:
        resp = requests.get(
            "https://push2.eastmoney.com/api/qt/stock/get",
            params={
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "fltt": "2",
                "invt": "2",
                "fields": "f43,f44,f45,f46,f47,f48,f58,f60,f170",
                "secid": "1.688795",
            },
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"},
            timeout=10,
        )
        d = (resp.json() or {}).get("data") or {}
        if not d.get("f58"):
            return None
        current_price = float(d.get("f43", 0) or 0)
        open_price = float(d.get("f46", 0) or 0)
        prev_close = float(d.get("f60", 0) or 0)
        if current_price == 0 and open_price == 0:
            return None
        return {
            "name": d.get("f58", STOCK_NAME),
            "code": STOCK_CODE,
            "open_price": open_price,
            "prev_close": prev_close,
            "current_price": current_price,
            "high_price": float(d.get("f44", 0) or 0),
            "low_price": float(d.get("f45", 0) or 0),
            "volume": int(float(d.get("f47", 0) or 0)),
            "turnover": round(float(d.get("f48", 0) or 0) / 10000, 2),
            "change_amount": round(current_price - prev_close, 2),
            "change_pct": float(d.get("f170", 0) or 0),
            "is_trading_day": True,
            "data_source": "东方财富",
            "error": "",
        }
    except Exception as e:
        print(f"[行情] 东方财富获取失败: {e}")
        return None


def fetch_stock_data() -> dict:
    """统一行情入口：主源 → 兜底源 → 非交易/失败占位。"""
    fallback = {
        "name": STOCK_NAME,
        "code": STOCK_CODE,
        "open_price": 0.0,
        "prev_close": 0.0,
        "current_price": 0.0,
        "high_price": 0.0,
        "low_price": 0.0,
        "volume": 0,
        "turnover": 0.0,
        "change_amount": 0.0,
        "change_pct": 0.0,
        "is_trading_day": False,
        "data_source": "",
        "error": "行情数据获取失败或今日非交易日",
    }
    if now_cn().weekday() >= 5:
        fallback["error"] = "今日非交易日（周末）"
        return fallback
    return fetch_stock_data_sina() or fetch_stock_data_eastmoney() or fallback


def format_stock_table(stock: dict) -> str:
    if not stock["is_trading_day"]:
        return f"⚠️ {stock['error']}"

    def fmt_price(v: float) -> str:
        return f"{v:.2f}" if v else "-"

    sign = "+" if stock["change_amount"] > 0 else ""
    volume = f"{stock['volume'] / 10000:.2f}万手" if stock["volume"] >= 10000 else f"{stock['volume']:,}手"
    turnover = f"{stock['turnover'] / 10000:.2f}亿元" if stock["turnover"] >= 10000 else f"{stock['turnover']:,.2f}万元"
    return "\n".join([
        "| 指标 | 数值 |",
        "|---|---:|",
        f"| 开盘价 | {fmt_price(stock['open_price'])} |",
        f"| 昨收价 | {fmt_price(stock['prev_close'])} |",
        f"| 收盘价 | {fmt_price(stock['current_price'])} |",
        f"| 最高价 | {fmt_price(stock['high_price'])} |",
        f"| 最低价 | {fmt_price(stock['low_price'])} |",
        f"| 涨跌额/幅 | {sign}{stock['change_amount']:.2f} ({sign}{stock['change_pct']:.2f}%) |",
        f"| 成交量 | {volume} |",
        f"| 成交额 | {turnover} |",
        f"| 数据来源 | {stock['data_source']} |",
    ])


def format_stock_for_prompt(stock: dict) -> str:
    if not stock["is_trading_day"]:
        return f"行情数据：{stock['error']}。"
    sign = "+" if stock["change_amount"] > 0 else ""
    return (
        f"{stock['name']}({stock['code']}) 收盘价{stock['current_price']:.2f}，"
        f"涨跌幅{sign}{stock['change_pct']:.2f}%，成交额{stock['turnover']:,.2f}万元，"
        f"最高{stock['high_price']:.2f}，最低{stock['low_price']:.2f}，"
        f"数据来源：{stock['data_source']}。"
    )


async def build_search_queries(sdk: CodeActSDK, recent_titles: list[str]) -> list[str]:
    """搜索规划阶段。

    模板说明：基础 query 保证覆盖核心主题；历史事件交给 LLM 扩展 query，
    让脚本能随近期热点调整检索方向，而不是永远跑同一批固定关键词。
    """
    prompt = (
        "为摩尔线程（688795.SH）每日动态简报生成搜索词。"
        "要求覆盖公司公告、产品技术、生态合作、机构调研、国产GPU政策、竞品动态。"
        "结合近期已跟踪事件扩展搜索，但不要生成太泛的词。最多返回8个 query。\n\n"
        f"基础 query：{BASE_SEARCH_QUERIES}\n"
        f"近期事件：{recent_titles[:12]}"
    )
    try:
        result = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=SearchQueryPlan,
        )
        plan = parse_model_safe(SearchQueryPlan, result)
        dynamic = [q.strip() for q in plan.queries if q.strip()]
    except Exception as e:
        print(f"[搜索词] LLM 扩展失败，使用基础 query: {e}")
        dynamic = []
    queries = []
    for q in BASE_SEARCH_QUERIES + dynamic:
        if q and q not in queries:
            queries.append(q)
    return queries[:10]


async def search_all(sdk: CodeActSDK, queries: list[str], start: str, end: str) -> list[dict]:
    """并发搜索阶段：多个 query 独立执行，用 Semaphore 控制工具调用并发。"""
    sem = asyncio.Semaphore(3)

    async def search_one(query: str) -> list[dict]:
        async with sem:
            try:
                result = await sdk.call_tool(
                    "codeact_search_web",
                    {
                        "query": query,
                        "publish_time": {"start": start, "end": end},
                    },
                    schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
                )
                if result.get("is_success"):
                    return result.get("results", [])
            except Exception as e:
                print(f"[搜索] {query} 失败: {e}")
            return []

    result_groups = await asyncio.gather(*(search_one(q) for q in queries))
    candidates, seen = [], set()
    for group in result_groups:
        for item in group:
            url = norm_url(item.get("url", ""))
            if url and url not in seen:
                seen.add(url)
                candidates.append(item)
    return candidates[:MAX_SEARCH_RESULTS]


async def select_candidates(sdk: CodeActSDK, candidates: list[dict]) -> list[dict]:
    """候选筛选阶段。

    模板说明：搜索结果只作为线索，不直接进报告；先让 LLM 选择值得 fetch 的 URL，
    同时动态判断来源等级，并把判断结果随 URL 传递给正文抽取阶段。
    """
    if not candidates:
        return []
    lines = []
    for idx, item in enumerate(candidates[:MAX_SEARCH_RESULTS], 1):
        lines.append(
            f"[{idx}] 标题：{item.get('title', '')}\n"
            f"URL：{item.get('url', '')}\n"
            f"发布时间：{item.get('publish_time') or '未知'}\n"
            f"摘要：{truncate_text(item.get('snippet', ''))}"
        )
    prompt = (
        "从以下搜索结果中选择值得 fetch 正文核验的 URL。"
        "优先：摩尔线程公司动态、公告、投资者关系、产品技术、客户/生态合作、国产GPU政策、关键竞品动态。"
        "排除：广告、纯股吧情绪、重复转载、无明确来源的内容。最多选择18个。"
        "同时为候选来源动态判断 source_tier，只能使用 official / finance_media / registry / aggregator / unverified，"
        "并在 reason 中说明判断依据，不要依赖固定域名表。\n\n"
        + "\n\n".join(lines)
    )
    try:
        result = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=FetchCandidateSelection,
        )
        selection = parse_model_safe(FetchCandidateSelection, result)
        selected = {norm_url(u) for u in selection.selected_urls}
        picked = [item for item in candidates if norm_url(item.get("url", "")) in selected]
        assessment_by_url = {norm_url(a.url): a for a in selection.source_assessments}
        for item in picked:
            assessment = assessment_by_url.get(norm_url(item.get("url", "")))
            if assessment:
                item["source_tier"] = assessment.source_tier or "unverified"
                item["source_assessment"] = assessment.reason
        return picked[:MAX_FETCH_PAGES] if picked else candidates[:10]
    except Exception as e:
        print(f"[候选筛选] LLM 失败，使用前10条: {e}")
        return candidates[:10]


async def fetch_pages(sdk: CodeActSDK, candidates: list[dict]) -> list[dict]:
    """正文抓取阶段：只对筛选后的候选 fetch，减少无效网页消耗。"""
    sem = asyncio.Semaphore(5)

    async def fetch_one(item: dict) -> Optional[dict]:
        async with sem:
            url = item.get("url", "")
            try:
                page = await sdk.call_tool(
                    "codeact_fetch_web",
                    {"url": url},
                    schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
                )
                content_chunks = split_page_chunks(page.get("content", ""))
                if page.get("is_success") and content_chunks:
                    # 长网页保留为多个正文块，事实抽取阶段逐块覆盖，避免前段截断导致漏事实。
                    return {
                        "title": page.get("title") or item.get("title", ""),
                        "url": url,
                        "publish_time": item.get("publish_time", ""),
                        "source_tier": item.get("source_tier", "unverified"),
                        "source_assessment": item.get("source_assessment", ""),
                        "content_chunks": content_chunks,
                    }
            except Exception as e:
                print(f"[fetch] {url} 失败: {e}")
            return None

    results = await asyncio.gather(*(fetch_one(c) for c in candidates[:MAX_FETCH_PAGES]))
    return [r for r in results if r]


async def extract_facts(sdk: CodeActSDK, pages: list[dict]) -> list[ExtractedFact]:
    """事实抽取阶段。

    模板说明：每个网页先抽成可核验的原子事实，后续报告只基于事实和事件，
    不把搜索摘要或原始 HTML 直接交给最终总结。
    """
    sem = asyncio.Semaphore(3)

    async def extract_one(page: dict, chunk_idx: int, chunk_text: str, total_chunks: int) -> ExtractedFacts:
        async with sem:
            prompt = (
                "从网页正文分块中抽取与摩尔线程、国产GPU/算力芯片行业相关的可核验事实。"
                "分类只能使用 market/company/industry/competitor/risk。"
                "如果没有新信息，has_new_info=false。"
                "请基于搜索候选判断和网页正文动态给出 source_tier、source_assessment、claim_strength、fact_boundary：\n"
                "- source_tier 只能使用 official / finance_media / registry / aggregator / unverified；\n"
                "- claim_strength 只能使用 normal / strong；强 claim 指需要更高证据门槛的表述，由你基于正文语义判断；\n"
                "- fact_boundary 说明该事实的证据边界，若证据充分可留空；\n"
                "- 只基于当前分块中明确出现的信息抽取，不要根据标题、搜索摘要或其他分块补全。\n\n"
                f"标题：{page['title']}\nURL：{page['url']}\n发布时间：{page['publish_time']}\n"
                f"候选阶段来源判断：{page.get('source_tier', 'unverified')}，{page.get('source_assessment', '')}\n"
                f"分块：{chunk_idx + 1}/{total_chunks}\n"
                f"正文分块：\n{chunk_text}"
            )
            try:
                result = await sdk.call_llm(
                    messages=[{"role": "user", "content": prompt}],
                    response_format=ExtractedFacts,
                )
                parsed = parse_model_safe(ExtractedFacts, result)
                for fact in parsed.facts:
                    if not fact.source_url:
                        fact.source_url = page["url"]
                    if not fact.publish_date:
                        fact.publish_date = page.get("publish_time", "")
                    if not fact.source_tier or fact.source_tier == "unverified":
                        fact.source_tier = page.get("source_tier", "unverified")
                    if not fact.source_assessment:
                        fact.source_assessment = page.get("source_assessment", "")
                return parsed
            except Exception as e:
                print(f"[事实抽取] {page.get('title', '')} 失败: {e}")
                return ExtractedFacts()

    tasks = []
    for page in pages:
        chunks = page.get("content_chunks") or []
        for idx, chunk in enumerate(chunks):
            tasks.append(extract_one(page, idx, chunk, len(chunks)))
    groups = await asyncio.gather(*tasks) if tasks else []
    facts = []
    for group in groups:
        if group.has_new_info:
            facts.extend(group.facts)
    return facts


def filter_recent_facts(facts: list[ExtractedFact], run_date: str, days: int = 7) -> list[ExtractedFact]:
    """时效过滤：日期不明的事实保留给聚合阶段判断，明确过期的事实不上屏。"""
    ref = parse_date(run_date) or now_cn()
    cutoff = ref - timedelta(days=days)
    kept = []
    for fact in facts:
        dt = parse_date(fact.publish_date)
        if dt is None or dt >= cutoff:
            kept.append(fact)
    return kept


async def aggregate_events(
    sdk: CodeActSDK,
    facts: list[ExtractedFact],
    stock_text: str,
    previous_titles: list[str],
) -> list[AggregatedEvent]:
    """事件聚合阶段。

    模板说明：这是日报质量的关键步骤。把多页、多来源的原子事实合并为“事件”，
    解决同事件重复报道、不同标题、不同来源口径不一致的问题。
    """
    if not facts:
        return []

    payload = []
    for idx, fact in enumerate(facts[:MAX_FACTS_FOR_CLUSTER], 1):
        payload.append({
            "index": idx,
            "category": fact.category,
            "title": fact.title,
            "content": truncate_text(fact.content, 700),
            "publish_date": fact.publish_date,
            "source_url": fact.source_url,
            "source_tier": fact.source_tier,
            "source_assessment": fact.source_assessment,
            "claim_strength": fact.claim_strength,
            "fact_boundary": fact.fact_boundary,
        })

    prompt = (
        "你是一名财经科技编辑。请把以下事实合并为“事件”，同一事件的多来源报道必须合并，"
        "不要按 URL 或标题逐条罗列。每个事件要给出判断字段。\n\n"
        f"今日实测行情：{stock_text}\n"
        f"历史已跟踪事件：{previous_titles[:20]}\n\n"
        f"事实列表 JSON：\n{json.dumps(payload, ensure_ascii=False)}\n\n"
        "输出要求：\n"
        "- events 中每个元素是一件独立事件，不是单篇新闻。\n"
        "- category 使用 market/company/industry/competitor/risk。\n"
        "- importance/confidence 使用 high/medium/low。\n"
        "- publish_date 填该事件最核心事实的日期；无法确认时填来源发布时间中最可信的一项。\n"
        "- source_tier 和 claim_strength 必须继承并综合事实列表中的动态判断，不要按域名或关键词重新硬编码判断。\n"
        "- why_it_matters 回答这件事说明什么。\n"
        "- impact 写对公司、股价叙事、行业竞争或后续观察的影响。\n"
        "- evidence 写可追溯的证据短句。\n"
        "- fact_boundary 说明证据边界；对于 strong claim 且证据不足的事件，confidence 应降为 low/medium。\n"
        "- source_urls/source_titles 合并所有相关来源。\n"
        "- 如果行情新闻与实测行情口径冲突，在 conflict_note 中说明。"
    )
    try:
        result = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=AggregatedEvents,
        )
        return parse_model_safe(AggregatedEvents, result).events
    except Exception as e:
        print(f"[事件聚合] LLM 失败，降级为逐事实事件: {e}")
        return [
            AggregatedEvent(
                event_title=f.title,
                category=f.category or "company",
                event_summary=f.content,
                why_it_matters="该事实来自搜索抓取结果，尚未完成多来源聚合。",
                impact="需结合后续信息判断影响。",
                evidence=truncate_text(f.content, 120),
                publish_date=f.publish_date,
                source_tier=f.source_tier,
                claim_strength=f.claim_strength,
                fact_boundary=f.fact_boundary,
                source_urls=[f.source_url] if f.source_url else [],
                source_titles=[f.title] if f.title else [],
            )
            for f in facts[:20]
        ]


def quality_gate_event(event: AggregatedEvent) -> AggregatedEvent:
    """Verifier：只做结构兜底，来源等级和 claim 强度由 LLM 在抽取/聚合阶段判断。"""
    event.source_tier = event.source_tier or "unverified"
    event.claim_strength = event.claim_strength or "normal"
    event.confidence = event.confidence or "medium"
    return event


def enrich_event_state(
    conn: sqlite3.Connection,
    events: list[AggregatedEvent],
    baseline_mode: bool,
) -> list[AggregatedEvent]:
    """状态增强阶段：把聚合事件与历史库匹配，标记 baseline/new/continuing。"""
    enriched = []
    for event in events:
        title = event.event_title or event.event_summary[:40] or "未命名事件"
        existing = find_existing_event_key(conn, title)
        key = existing or event_fingerprint(title, event.event_summary)
        urls = [norm_url(u) for u in event.source_urls if norm_url(u)]
        url_seen = bool(urls) and all(is_url_seen(conn, u) for u in urls)
        event.event_title = title
        event.event_key = key
        event.is_new = False if baseline_mode else (existing is None and not url_seen)
        event.lifecycle = "baseline" if baseline_mode else ("new" if event.is_new else "continuing")
        event.source_urls = urls
        if not event.source_titles:
            event.source_titles = [title for _ in urls]
        enriched.append(quality_gate_event(event))
    enriched.sort(key=lambda e: (0 if e.is_new else 1, {"high": 0, "medium": 1, "low": 2}.get(e.importance, 1)))
    return enriched


def detect_market_conflicts(stock: dict, events: list[AggregatedEvent]) -> list[str]:
    """口径检查示例：用确定行情数据约束新闻中的涨跌描述。"""
    if not stock.get("is_trading_day"):
        return []
    change_pct = float(stock.get("change_pct") or 0)
    notes = []
    for event in events:
        if event.category != "market":
            continue
        text = f"{event.event_title} {event.event_summary} {event.evidence}"
        if change_pct >= 0 and re.search(r"跌|下挫|回调|走低", text):
            notes.append(f"实测行情为 {change_pct:+.2f}%，但「{event.event_title}」含下跌口径，需核验统计时点。")
        elif change_pct < 0 and re.search(r"涨|上涨|拉升|走高", text):
            notes.append(f"实测行情为 {change_pct:+.2f}%，但「{event.event_title}」含上涨口径，需核验统计时点。")
        if event.conflict_note:
            notes.append(event.conflict_note)
    return notes[:4]


async def generate_takeaways(
    sdk: CodeActSDK,
    stock_text: str,
    events: list[AggregatedEvent],
    previous_titles: list[str],
    conflict_notes: list[str],
    baseline_mode: bool,
) -> BriefingTakeaways:
    """报告分析阶段：基于已聚合事件生成面向用户的一屏摘要。"""
    event_lines = []
    for event in events[:18]:
        status = {"baseline": "基线", "new": "新增", "continuing": "延续"}.get(event.lifecycle, "新增" if event.is_new else "延续")
        event_lines.append(
            f"- [{status}/{event.importance}/{event.confidence}/{event.source_tier}/{event.category}] {event.event_title}: "
            f"{event.why_it_matters or event.event_summary} 影响：{event.impact}"
        )
    prompt = (
        "请基于已核验行情和聚合事件生成摩尔线程每日简报的一屏摘要。\n"
        f"状态库模式：{'冷启动基线，不得把存量事件表述为新增催化' if baseline_mode else '增量跟踪，可区分新增和延续'}\n"
        f"行情：{stock_text}\n"
        f"昨日/历史事件参考：{previous_titles[:10]}\n"
        f"口径提示：{conflict_notes}\n"
        f"今日事件：\n{chr(10).join(event_lines)}\n\n"
        "字段要求：headline 不超过35字；overall_summary 120-220字；"
        "key_changes 写3-5条“发生了什么+为什么重要”；"
        "impact_assessment 只能写“对市场叙事偏正面/偏中性/偏风险/信息不足”，不要写“支撑股价维持强势”等投研式强判断；"
        "risk_flags 和 watch_points 必须具体可跟踪。不得编造输入外事实。"
    )
    try:
        result = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=BriefingTakeaways,
        )
        return parse_model_safe(BriefingTakeaways, result)
    except Exception as e:
        print(f"[摘要] LLM 失败: {e}")
        return BriefingTakeaways(
            headline="摩尔线程新增动态待复核",
            overall_summary="今日简报已聚合行情与资讯事件，建议重点查看事件解读与来源。",
            key_changes=[e.event_title for e in events[:3]],
            impact_assessment="信息不足，暂不形成明确影响判断。",
            risk_flags=conflict_notes or ["摘要生成失败，需人工复核。"],
            watch_points=["关注公司公告、投资者关系记录和后续行情表现。"],
        )


def importance_label(value: str) -> str:
    return {"high": "高", "medium": "中", "low": "低"}.get((value or "").lower(), value or "中")


def format_bullets(items: list[str], fallback: str) -> list[str]:
    clean = [str(x).strip() for x in items if str(x).strip()]
    return [f"- {x}" for x in clean] if clean else [f"- {fallback}"]


def group_events(events: list[AggregatedEvent]) -> dict[str, list[AggregatedEvent]]:
    """报告编排辅助：按业务语义分组，而不是按抓取来源分组。"""
    names = {
        "market": "行情与资金",
        "company": "公司动态",
        "industry": "行业与政策",
        "competitor": "竞品动态",
        "risk": "风险信号",
    }
    grouped = {}
    for event in events:
        title = names.get(event.category, "其他动态")
        grouped.setdefault(title, []).append(event)
    return grouped


def report_path_for(run_date: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    return os.path.join(OUTPUT_DIR, f"摩尔线程_briefing_{run_date}.md")


def generate_report(
    run_date: str,
    report_path: str,
    stock: dict,
    takeaways: BriefingTakeaways,
    events: list[AggregatedEvent],
    conflict_notes: list[str],
    previous_titles: list[str],
    baseline_mode: bool,
) -> str:
    """长报告生成阶段。

    模板说明：
    - 长正文写入 ./codeact/output/，message 只放摘要和文件入口。
    - 事件表保留日期、来源等级、置信度、证据和来源链接，方便用户追溯判断依据。
    - 冷启动时报告写“基线”，不把历史存量误报为新增。
    """
    grouped = group_events(events)
    new_count = sum(1 for e in events if e.is_new)
    baseline_note = "冷启动基线，本次仅建立历史参照，不判定新增催化。" if baseline_mode else f"本次识别 **{new_count}** 个状态库新增事件。"
    lines = [
        f"# 摩尔线程({STOCK_CODE})每日动态简报",
        "",
        f"**报告日期**: {run_date}",
        f"**生成时间**: {now_cn().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "---",
        "",
        "## 一屏摘要",
        "",
        f"**今日结论：{takeaways.headline or '暂无明确结论'}**",
        "",
        takeaways.overall_summary or "暂无摘要。",
        "",
        "**关键变化**",
        *format_bullets(takeaways.key_changes, "暂无关键变化。"),
        "",
        "**影响判断**",
        takeaways.impact_assessment or "信息不足，暂不形成明确影响判断。",
        "",
        f"> 本次聚合 **{len(events)}** 个事件；{baseline_note} 历史参考 {len(previous_titles)} 条。",
        "",
        "---",
        "",
        "## 行情数据",
        "",
        format_stock_table(stock),
        "",
    ]

    if conflict_notes:
        lines.extend(["### 口径提示", "", *format_bullets(conflict_notes, "暂无口径冲突。"), ""])

    lines.extend([
        "---",
        "",
        "## 风险与后续关注",
        "",
        "**风险与不确定性**",
        *format_bullets(takeaways.risk_flags + conflict_notes, "暂无明确风险提示。"),
        "",
        "**后续关注**",
        *format_bullets(takeaways.watch_points, "关注后续公告、行情表现和权威媒体更新。"),
        "",
        "---",
        "",
        "## 事件解读",
        "",
    ])

    for section, section_events in grouped.items():
        lines.extend([f"### {section}", ""])
        lines.append("| 状态 | 日期 | 事件 | 重要性 | 来源等级 | 置信度 | 信号 | 判断 | 证据 | 来源 |")
        lines.append("|---|---|---|---:|---|---|---|---|---|---|")
        for event in section_events:
            source_links = "<br>".join(f"[{i+1}]({url})" for i, url in enumerate(event.source_urls[:4])) or "-"
            judgement = event.why_it_matters or event.event_summary
            if event.impact:
                judgement += f"<br><br>影响：{event.impact}"
            if event.conflict_note:
                judgement += f"<br><br>口径提示：{event.conflict_note}"
            lines.append(
                "| "
                + " | ".join([
                    {"baseline": "基线", "new": "新增", "continuing": "延续"}.get(event.lifecycle, "新增" if event.is_new else "延续"),
                    event.publish_date or "-",
                    event.event_title.replace("|", "｜"),
                    importance_label(event.importance),
                    event.source_tier,
                    event.confidence,
                    (event.signal_type or "-").replace("|", "｜"),
                    judgement.replace("\n", " ").replace("|", "｜"),
                    (event.evidence or event.event_summary).replace("\n", " ").replace("|", "｜"),
                    source_links,
                ])
                + " |"
            )
        lines.append("")
        low_conf = [e for e in section_events if e.fact_boundary]
        if low_conf:
            lines.append("**事实边界**")
            lines.extend(format_bullets([f"{e.event_title}：{e.fact_boundary}" for e in low_conf], "暂无额外事实边界。"))
            lines.append("")

    lines.extend([
        "---",
        "",
        "## 来源列表",
        "",
    ])
    seen = set()
    idx = 1
    for event in events:
        for url, title in zip(event.source_urls, event.source_titles or []):
            if not url or url in seen:
                continue
            seen.add(url)
            lines.append(f"[{idx}] {title or event.event_title}")
            lines.append(f"    链接: {url}")
            lines.append("")
            idx += 1
    if stock.get("data_source"):
        lines.append(f"行情数据来源: {stock['data_source']}")
        lines.append("")
    lines.extend([
        "---",
        "",
        "*本简报由 AI 自动生成，仅供参考，不构成投资建议。*",
        f"*报告文件: {os.path.basename(report_path)}*",
    ])
    return "\n".join(lines)


def build_message(
    run_date: str,
    report_path: str,
    stock: dict,
    takeaways: BriefingTakeaways,
    events: list[AggregatedEvent],
    conflict_notes: list[str],
    baseline_mode: bool,
) -> str:
    """submit_result.message 生成阶段。

    模板说明：message 保持短摘要，并用 computer:// 绝对路径链接交付本地报告文件；
    不把完整 Markdown 报告塞进聊天消息。
    """
    sign = "+" if stock.get("change_amount", 0) > 0 else ""
    if stock.get("is_trading_day"):
        market = f"收盘价 {stock['current_price']:.2f}，涨跌幅 {sign}{stock['change_pct']:.2f}%，成交额 {stock['turnover']:,.2f}万元"
    else:
        market = stock.get("error", "暂无行情")
    new_count = sum(1 for e in events if e.is_new)
    count_line = "本次为冷启动基线，不判定新增催化。" if baseline_mode else f"共聚合 **{len(events)}** 个事件，其中 **{new_count}** 个新增。"
    abs_path = os.path.abspath(report_path)
    parts = [
        f"📊 **摩尔线程每日动态 | {run_date}**",
        "",
        f"**市场表现**: {market}",
        "",
        f"**今日结论**: {takeaways.headline or '暂无明确结论'}",
        "",
        "**关键变化**：",
        *format_bullets(takeaways.key_changes[:3], "暂无关键变化。"),
        "",
        f"**影响判断**: {takeaways.impact_assessment or '信息不足，暂不形成明确影响判断。'}",
        "",
        count_line,
    ]
    if conflict_notes:
        parts.extend(["", "**口径提示**：", *format_bullets(conflict_notes[:2], "暂无口径冲突。")])
    parts.append(f"\n📎 [完整报告 - {os.path.basename(report_path)}](computer://{abs_path})")
    return "\n".join(parts)


async def main() -> None:
    """主流程：规划 → 搜索 → 抓取 → 抽取 → 聚合 → 报告 → 写状态 → submit_result。"""
    sdk = CodeActSDK()
    conn = init_state_db()
    run_date = now_cn().strftime("%Y-%m-%d")
    report_path = report_path_for(run_date)

    try:
        print("[Planner] 判断状态库模式并获取行情")
        baseline_mode = not has_prior_runs(conn, run_date)
        stock = fetch_stock_data()
        stock_text = format_stock_for_prompt(stock)
        print(f"[Planner] baseline={baseline_mode}, stock_source={stock.get('data_source') or 'none'}")

        print("[Planner] 生成动态搜索词")
        recent_titles = load_recent_event_titles(conn)
        queries = await build_search_queries(sdk, recent_titles)
        start = (now_cn() - timedelta(days=SEARCH_DAYS)).strftime("%Y-%m-%dT00:00:00+08:00")
        end = now_cn().strftime("%Y-%m-%dT23:59:59+08:00")
        print(f"[Planner] queries={len(queries)}, recent_titles={len(recent_titles)}")

        print(f"[Collector] 搜索 {len(queries)} 个 query")
        candidates = await search_all(sdk, queries, start, end)
        print(f"[Collector] candidates={len(candidates)}")

        print("[Curator] 筛选并 fetch 正文")
        selected = await select_candidates(sdk, candidates)
        pages = await fetch_pages(sdk, selected)
        print(f"[Curator] selected={len(selected)}, pages={len(pages)}")

        print("[Extractor] 抽取事实")
        facts = filter_recent_facts(await extract_facts(sdk, pages), run_date)
        print(f"[Extractor] facts={len(facts)}")

        print("[Aggregator] 聚合同事件多来源")
        previous_titles = load_previous_daily_titles(conn, run_date)
        events = await aggregate_events(sdk, facts, stock_text, previous_titles)
        events = enrich_event_state(conn, events, baseline_mode)
        print(f"[Aggregator] events={len(events)}, new_events={sum(1 for e in events if e.is_new)}")

        print("[Verifier] 口径冲突与事实边界检查")
        conflict_notes = detect_market_conflicts(stock, events)
        low_conf = sum(1 for e in events if e.confidence == "low" or e.fact_boundary)
        print(f"[Verifier] low_confidence={low_conf}, conflicts={len(conflict_notes)}")

        print("[Analyst] 生成摘要和报告")
        takeaways = await generate_takeaways(sdk, stock_text, events, previous_titles, conflict_notes, baseline_mode)
        print(f"[Analyst] takeaways={len(takeaways.key_changes)}")
        report_md = generate_report(run_date, report_path, stock, takeaways, events, conflict_notes, previous_titles, baseline_mode)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report_md)

        print("[Memory] 写入状态库并提交")
        save_state(conn, run_date, report_path, events)
        await sdk.submit_result(
            result_mode=RESULT_MODE,
            status="success",
            message=build_message(run_date, report_path, stock, takeaways, events, conflict_notes, baseline_mode),
            data={
                "report_path": report_path,
                "state_db": STATE_DB,
                "total_events": len(events),
                "new_events": sum(1 for e in events if e.is_new),
                "baseline_mode": baseline_mode,
                "queries": queries,
            },
        )
    except Exception as e:
        print(traceback.format_exc())
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"摩尔线程每日简报执行失败：{e}",
            data={"error_type": type(e).__name__, "state_db": STATE_DB},
        )
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
