#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每日股市总结与操作建议脚本。

自动获取养猪板块 + 恒生科技核心公司行情数据（westockdata CLI），
搜索行业新闻与催化剂，搜索生猪均价/猪粮比/饲料价格并用 LLM 结构化提取，
经 LLM 综合分析后生成结构化 Markdown 报告 + 数据图表。

整体流程：获取行情 → 分章节搜索 → LLM 候选筛选 → fetch 正文 →
按章节分组整理材料 → 分块串行报告生成 → 图表生成并嵌入 → 写状态并提交。

关键设计集中说明（细节见对应函数注释）：
- 报告链路把 fetch 到的正文分块后直接交给报告 LLM，不额外做结构化事实抽取，
  以更短链路换更低开销；失真风险由“只喂全文/分块防截断/prompt 质量约束”兜底，
  详见 llm_analysis_block_wise 与 _QUALITY_RULES。
- 检索、抓取、候选筛选、报告生成的结果都进 SQLite 缓存（DailyCache），可重复运行。
- 状态库区分 baseline 冷启动与增量运行，靠 seen_sources 去重避免重复翻炒旧闻，
  详见 collect_signals 与 write_state_after_success。
"""

import asyncio
from collections import deque
from dataclasses import dataclass
import hashlib
from itertools import islice
import json
import os
import re
import shlex
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Iterable, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

from codeact_sdk import CodeActSDK
from pydantic import BaseModel, Field


@dataclass(frozen=True)
class EntityConfig:
    """领域实体配置：代码、展示颜色、搜索章节和别名只在这里维护。"""

    name: str
    code: str
    color: str = "#333333"
    section_id: str = ""
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class NewsSectionConfig:
    """信息发现章节配置。workflow 只消费这里生成的 section plan。"""

    section_id: str
    label: str
    max_fetch: int
    templates: tuple[str, ...]
    semantic_group: str
    entity_name: str = ""


@dataclass(frozen=True)
class ReportSectionBlock:
    """报告生成 block 配置，承载标题、证据范围和图表槽位。"""

    section_id: str
    title: str
    evidence_scope: tuple[str, ...] = ()
    chart_slots: tuple[int, ...] = ()


@dataclass(frozen=True)
class DomainConfig:
    """专用日报的领域配置；通用 workflow 不直接依赖具体公司关键词。"""

    report_name: str
    pig_entities: tuple[EntityConfig, ...]
    hk_entities: tuple[EntityConfig, ...]
    index_entities: tuple[EntityConfig, ...]
    news_sections: tuple[NewsSectionConfig, ...]
    report_sections: tuple[ReportSectionBlock, ...]
    chart_defs: dict[int, str]

    @property
    def colors(self) -> dict[str, str]:
        return {
            entity.name: entity.color
            for entity in (*self.pig_entities, *self.hk_entities, *self.index_entities)
            if entity.color
        }

    @property
    def pig_codes(self) -> dict[str, str]:
        return {entity.name: entity.code for entity in self.pig_entities}

    @property
    def hk_codes(self) -> dict[str, str]:
        return {entity.name: entity.code for entity in self.hk_entities}

    @property
    def index_codes(self) -> dict[str, str]:
        return {entity.name: entity.code for entity in self.index_entities}

    @property
    def section_plan(self) -> dict[str, dict]:
        return {
            section.section_id: {
                "label": section.label,
                "max_fetch": section.max_fetch,
                "templates": list(section.templates),
                "semantic_group": section.semantic_group,
                "entity_name": section.entity_name,
            }
            for section in self.news_sections
        }

    @property
    def section_labels(self) -> dict[str, str]:
        return {section.section_id: section.label for section in self.news_sections}

    @property
    def hk_company_section_to_name(self) -> dict[str, str]:
        return {
            entity.section_id: entity.name
            for entity in self.hk_entities
            if entity.name != "恒生科技" and entity.section_id
        }

    def company_aliases(self, company: str) -> list[str]:
        for entity in (*self.pig_entities, *self.hk_entities, *self.index_entities):
            if entity.name == company:
                return [entity.name, *entity.aliases]
        return [company]


# ===== SDK 工具版本占位符（skill 模板版）=====
# 安装时由 CodeAct agent 用 get_codeact_tool_schemas 取真实版本号替换下列占位值。
# 每个工具单独锁定 schema version：SDK 侧工具契约可能各自演进，混用会导致参数校验失败，
# 因此严禁跨脚本照抄真实版本号，必须逐个按当前环境实探替换。
TOOL_SCHEMA_VERSIONS = {
    "codeact_search_web": "__FILL_SEARCH_WEB_VERSION__",
    "codeact_fetch_web": "__FILL_FETCH_WEB_VERSION__",
    "file_to_url": "__FILL_FILE_TO_URL_VERSION__",
}

# 这几个版本串会拼进缓存 key（见 _versioned_cache_key）：脚本逻辑或 prompt 改动时
# 改动对应版本串即可让旧缓存自然失效，无需手动清库。
SCRIPT_VERSION = "daily-stock-summary"
CACHE_SCHEMA_VERSION = "cache-v2"
CANDIDATE_SELECTION_PROMPT_VERSION = "candidate-selection"
REPORT_PROMPT_VERSION = "report-blocks"
CACHE_TTL_DAYS = {
    "stock_code": 3650,
    "kline": 3,
    "search_results": 3,
    "fetched_page": 7,
    "candidate_selection": 3,
    "pig_price_data": 3,
    "analysis_md": 7,
}

TZ = timezone(timedelta(hours=8))
WESTOCK_CMD = ["npx", "-y", "westock-data-clawhub@1.0.4"]
STATE_DB_NAME = "daily_stock_summary_state.db"
SEARCH_LOOKBACK_DAYS = 45
PIG_PRICE_LOOKBACK_DAYS = 45
DAILY_SEARCH_LOOKBACK_DAYS = 7
DAILY_PIG_PRICE_LOOKBACK_DAYS = 14
WESTOCK_CLI_CONCURRENCY = 4

STANDARD_COMPANY_SEARCH_TEMPLATES = (
    "{company} {year}年{month}月 最新公告 最新动向",
    "{company} {year}年{month}月 业绩 财报 回购",
    "{company} {year}年{month}月 核心业务 新产品 战略合作 最新进展",
)

DOMAIN = DomainConfig(
    report_name="每日股市总结",
    pig_entities=(
        EntityConfig("牧原股份", "sz002714", "#1F4E79", aliases=("牧原",)),
        EntityConfig("温氏股份", "sz300498", "#5B8DB8", aliases=("温氏",)),
        EntityConfig("新希望", "sz000876", "#D9822B", aliases=("新希望六和",)),
    ),
    hk_entities=(
        EntityConfig("恒生科技", "hkHSTECH", "#2A9D8F", aliases=("恒生科技指数", "HSTECH")),
        EntityConfig("腾讯控股", "hk00700", "#0072B2", "hk_tencent", ("腾讯", "Tencent", "00700")),
        EntityConfig("阿里巴巴", "hk09988", "#D55E00", "hk_alibaba", ("阿里", "Alibaba", "BABA", "09988", "9988")),
        EntityConfig("美团", "hk03690", "#009E73", "hk_meituan", ("Meituan", "03690", "3690")),
        EntityConfig("小米集团", "hk01810", "#CC79A7", "hk_xiaomi", ("小米", "Xiaomi", "小米汽车", "智能汽车", "01810", "1810")),
    ),
    index_entities=(
        EntityConfig("上证指数", "sh000001"),
        EntityConfig("沪深300", "sh000300"),
        EntityConfig("创业板指", "sz399006"),
        EntityConfig("深证成指", "sz399001"),
    ),
    news_sections=(
        NewsSectionConfig(
            "market",
            "大盘/A股",
            3,
            ("{year}年{month}月{day}日 A股收盘 上证指数 涨跌幅",),
            "market",
        ),
        NewsSectionConfig(
            "pig",
            "养猪行业",
            6,
            (
                "{year}年{month}月{day}日生猪价格 全国均价",
                "{month}月 豆粕价格 玉米价格",
                "{year}年{month}月{day}日 牧原股份 温氏股份 新希望 股价",
                "猪周期 {year} 行业分析 产能去化",
                "生猪均价 猪粮比 {year}年{month}月",
                "玉米价格 豆粕价格 {year}年{month}月 饲料成本",
            ),
            "pig",
        ),
        NewsSectionConfig(
            "hk",
            "恒生科技/港股板块",
            3,
            (
                "恒生科技指数 {year}年{month}月{day}日 收盘 涨跌幅",
                "恒生科技指数 {year}年{month}月 最新动向 成分股",
                "港股科技股 {year}年{month}月 最新动向 恒生科技",
            ),
            "hk",
        ),
        *(
            NewsSectionConfig(entity.section_id, f"{entity.name}最新动向", 3, STANDARD_COMPANY_SEARCH_TEMPLATES, "hk", entity.name)
            for entity in (
                EntityConfig("腾讯控股", "hk00700", section_id="hk_tencent"),
                EntityConfig("阿里巴巴", "hk09988", section_id="hk_alibaba"),
                EntityConfig("美团", "hk03690", section_id="hk_meituan"),
                EntityConfig("小米集团", "hk01810", section_id="hk_xiaomi"),
            )
        ),
    ),
    report_sections=(
        ReportSectionBlock("core", "核心摘要与数据来源", ("all",), (5,)),
        ReportSectionBlock("market", "一、大盘综述", ("market",), (5,)),
        ReportSectionBlock("pig", "二、养猪行业行情", ("pig",), (1, 3, 6, 7)),
        ReportSectionBlock("hk", "三、恒生科技指数及核心公司", ("hk",), (2, 4)),
        ReportSectionBlock("advice", "四、综合投资建议", ("all",), ()),
    ),
    chart_defs={
        1: "图1：养猪股近30日股价走势（指数化）",
        2: "图2：恒生科技核心公司近30日股价走势（指数化）",
        3: "图3：养猪股今日涨跌幅对比",
        4: "图4：恒生科技今日涨跌幅对比",
        5: "图5：A股主要指数走势",
        6: "图6：养猪股累计涨跌幅与成交量",
        7: "图7：猪价与猪粮比",
    },
)

COLORS = DOMAIN.colors
DEFAULT_PIG_CODES = DOMAIN.pig_codes
DEFAULT_HK_CODES = DOMAIN.hk_codes
DEFAULT_INDEX_CODES = DOMAIN.index_codes
HK_COMPANY_SECTION_TO_NAME = DOMAIN.hk_company_section_to_name
SECTION_NEWS_PLAN = DOMAIN.section_plan
SECTION_LABELS = DOMAIN.section_labels
CHART_DEFS = DOMAIN.chart_defs

# ============================================================
# 工具函数
# ============================================================

def _run_cli(cmd: list[str], timeout: int = 60) -> str:
    """执行 CLI 命令，返回 stdout 文本。"""
    print(f"[CLI] {shlex.join(cmd)}")
    try:
        r = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout
        )
        if r.returncode != 0:
            err = (r.stderr or "").strip()
            print(f"[CLI] 返回码 {r.returncode}: {err}")
        return r.stdout.strip()
    except subprocess.TimeoutExpired:
        print(f"[CLI] 超时: {shlex.join(cmd)}")
        return ""


async def _run_cli_async(cmd: list[str], timeout: int = 60) -> str:
    """在线程中执行阻塞 CLI，避免卡住 CodeAct SDK 的异步工具调用。"""
    return await asyncio.to_thread(_run_cli, cmd, timeout)


def parse_markdown_table(text: str) -> list[dict]:
    """将 westockdata 返回的 Markdown 表格解析为字典列表。"""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if len(lines) < 2:
        return []
    header_idx = None
    for i, line in enumerate(lines):
        if line.startswith("|"):
            header_idx = i
            break
    if header_idx is None:
        return []
    headers = [h.strip() for h in lines[header_idx].split("|") if h.strip()]
    rows = []
    for line in lines[header_idx + 1:]:
        if line.startswith("|") and not re.match(r"^\|[\s\-:|]+\|$", line):
            raw = [c.strip() for c in line.strip("|").split("|")]
            row = {}
            for j, h in enumerate(headers):
                if j < len(raw):
                    row[h] = raw[j].strip()
                else:
                    row[h] = ""
            rows.append(row)
    return rows


def _safe_float(val, default=0.0):
    """安全转换浮点数。"""
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return default


def _take(items: Iterable, limit: int) -> list:
    """返回前 limit 个元素，避免在业务文本上使用静默切片。"""
    return list(islice(items, max(0, limit)))


def _tail(items: Iterable, limit: int) -> list:
    """返回最后 limit 个元素。"""
    if limit <= 0:
        return []
    return list(deque(items, maxlen=limit))


def _chunk_text(text: str, chunk_size: int = 1800) -> list[str]:
    """按字符预算分块；所有长文本进入 LLM 前都显式分块，避免静默截断。"""
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return []
    return [cleaned[start:start + chunk_size] for start in range(0, len(cleaned), chunk_size)]


def _format_chunked_excerpt(text: str, chunk_size: int, max_chunks: int, label: str) -> str:
    """格式化可追溯的分块摘录，并明确说明是否还有未纳入的块。"""
    chunks = _chunk_text(text, chunk_size=chunk_size)
    if not chunks:
        return ""
    selected_chunks = _take(chunks, max_chunks)
    parts = [f"[{label} 分块摘录：纳入 {len(selected_chunks)}/{len(chunks)} 块]"]
    for idx, chunk in enumerate(selected_chunks, 1):
        parts.append(f"[{label} chunk {idx}/{len(chunks)}]\n{chunk}")
    if len(selected_chunks) < len(chunks):
        parts.append(f"[{label} 仍有 {len(chunks) - len(selected_chunks)} 块未纳入本次提示词，请勿基于未纳入内容生成事实。]")
    return "\n".join(parts)


def _md_block(title: str, content: str, lang: str = "markdown") -> str:
    """把 prompt 中的动态材料包进 Markdown 代码块，降低材料与指令混淆。"""
    safe_content = (content or "无").replace("````", "```` ")
    return f"### {title}\n\n````{lang}\n{safe_content}\n````"


def _parse_date_str(date_str: str) -> Optional[datetime]:
    """兼容搜索结果常见发布时间格式。"""
    raw = (date_str or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            return dt.astimezone(TZ).replace(tzinfo=None)
        return dt
    except Exception:
        pass
    normalized = re.sub(r"([+-]\d{2}:?\d{2})$", "", raw.replace("T", " ")).strip()
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y年%m月%d日", "%Y/%m/%d", "%Y.%m.%d"]:
        try:
            return datetime.strptime(normalized, fmt)
        except Exception:
            continue
    return None


def _norm_url(url: str) -> str:
    """规范化 URL，用于搜索候选去重和 LLM 选中 URL 回查。"""
    return (url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/")


def _build_publish_time_window(now: datetime, lookback_days: int) -> dict[str, str]:
    """构造 codeact_search_web 支持的 RFC3339 发布时间过滤范围。

    时效性仅在检索阶段用该窗口过滤；fetch 正文后未做二次日期复核，极端情况下
    仍可能纳入过期正文。如需强时效，可在正文抽取处再加一道发布日期校验。
    """
    end = now.astimezone(TZ)
    start = end - timedelta(days=lookback_days)
    return {
        "start": start.isoformat(timespec="seconds"),
        "end": end.isoformat(timespec="seconds"),
    }


def _sort_recent_candidates(candidates: list[dict], now: datetime, recent_days: int = 45) -> list[dict]:
    """优先保留近期候选；无发布时间的候选降权但不直接丢弃。"""
    cutoff = now.replace(tzinfo=None) - timedelta(days=recent_days)
    dated_recent = []
    undated = []
    for item in candidates:
        parsed = _parse_date_str(item.get("publish_time", ""))
        item["_parsed_time"] = parsed
        if parsed is None:
            undated.append(item)
        elif parsed >= cutoff:
            dated_recent.append(item)
    dated_recent.sort(key=lambda x: x.get("_parsed_time") or datetime.min, reverse=True)
    return dated_recent + undated


def _normalize_result_mode(result_mode: str) -> str:
    """CodeAct submit_result 只接受 display_only/notify/no_reply；auto 在主流程中按结果分流。"""
    allowed = {"display_only", "notify", "no_reply", "auto"}
    mode = (result_mode or "display_only").strip()
    if mode not in allowed:
        print(f"[参数] result_mode={mode} 非法，回退到 display_only")
        return "display_only"
    return mode


def _stable_key(*parts: str) -> str:
    """对多段文本生成稳定 sha256 key，用于来源去重和运行记录主键。"""
    raw = "\n".join((p or "").strip().lower() for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _versioned_cache_key(namespace: str, *parts: str) -> str:
    """缓存 key 显式绑定脚本、工具 schema 和相关 prompt 版本。"""
    namespace_versions = {
        "candidate_selection": CANDIDATE_SELECTION_PROMPT_VERSION,
        "analysis_md": REPORT_PROMPT_VERSION,
    }
    version_payload = json.dumps(
        {
            "cache_schema": CACHE_SCHEMA_VERSION,
            "script": SCRIPT_VERSION,
            "tools": TOOL_SCHEMA_VERSIONS,
            "namespace_prompt": namespace_versions.get(namespace, ""),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return _stable_key(namespace, version_payload, *parts)


def _now_iso_for_cache(now_iso: str = "") -> str:
    parsed = _parse_date_str(now_iso) if now_iso else None
    if parsed is None:
        parsed = datetime.now(TZ).replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds")


def _cache_expires_at(namespace: str, now_iso: str = "") -> str:
    ttl_days = CACHE_TTL_DAYS.get(namespace, 14)
    now = _parse_date_str(_now_iso_for_cache(now_iso)) or datetime.now(TZ).replace(tzinfo=None)
    return (now + timedelta(days=ttl_days)).isoformat(timespec="seconds")


def build_section_queries(year: str, month: str, day: str) -> dict[str, dict]:
    """按报告章节生成搜索计划：section -> {label, max_fetch, queries}。"""
    base_values = {"year": year, "month": month, "day": day}
    plan = {}
    for section, cfg in SECTION_NEWS_PLAN.items():
        values = {**base_values, "company": cfg.get("entity_name", "")}
        plan[section] = {
            "label": cfg["label"],
            "max_fetch": cfg["max_fetch"],
            "queries": [t.format(**values) for t in cfg["templates"]],
            "semantic_group": cfg.get("semantic_group", section),
            "entity_name": cfg.get("entity_name", ""),
        }
    return plan


def _ensure_table_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    # 旧版本状态库可能缺少后加的列；用 PRAGMA 探测后补列，实现无损向后兼容升级。
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def init_state_db(output_dir: str) -> sqlite3.Connection:
    """初始化日报状态库。只在成功路径写状态，失败不污染历史。"""
    db_path = os.path.join(output_dir, STATE_DB_NAME)
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS seen_sources (
            url TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            publish_time TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            run_date TEXT NOT NULL,
            report_path TEXT,
            source_count INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_entries (
            namespace TEXT NOT NULL,
            cache_key TEXT NOT NULL,
            cache_date TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT,
            hit_count INTEGER NOT NULL DEFAULT 0,
            last_hit_at TEXT,
            PRIMARY KEY (namespace, cache_key)
        )
    """)
    _ensure_table_column(conn, "cache_entries", "expires_at", "TEXT")
    _ensure_table_column(conn, "cache_entries", "hit_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_table_column(conn, "cache_entries", "last_hit_at", "TEXT")
    conn.commit()
    return conn


def _get_meta(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


class DailyCache:
    """SQLite-backed cache for repeat runs; cached data is always recomputable."""

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def get(self, namespace: str, cache_key: str, cache_date: str) -> Optional[Any]:
        row = self.conn.execute(
            "SELECT payload_json, cache_date, expires_at FROM cache_entries "
            "WHERE namespace = ? AND cache_key = ?",
            (namespace, cache_key),
        ).fetchone()
        if not row:
            return None
        payload_json, stored_date, expires_at = row
        if stored_date != cache_date:
            return None
        parsed_expiry = _parse_date_str(expires_at or "")
        if parsed_expiry and parsed_expiry < datetime.now(TZ).replace(tzinfo=None):
            return None
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError:
            return None
        self.conn.execute(
            "UPDATE cache_entries SET hit_count = hit_count + 1, last_hit_at = ? "
            "WHERE namespace = ? AND cache_key = ?",
            (_now_iso_for_cache(), namespace, cache_key),
        )
        self.conn.commit()
        return payload

    def put(self, namespace: str, cache_key: str, cache_date: str, payload: Any, now_iso: str) -> None:
        expires_at = _cache_expires_at(namespace, now_iso)
        self.conn.execute(
            "INSERT INTO cache_entries (namespace, cache_key, cache_date, payload_json, updated_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(namespace, cache_key) DO UPDATE SET "
            "cache_date = excluded.cache_date, payload_json = excluded.payload_json, "
            "updated_at = excluded.updated_at, expires_at = excluded.expires_at",
            (namespace, cache_key, cache_date, json.dumps(payload, ensure_ascii=False), now_iso, expires_at),
        )
        self.conn.commit()

    def prune(self, now_iso: str = "") -> None:
        now = _now_iso_for_cache(now_iso)
        self.conn.execute(
            "DELETE FROM cache_entries WHERE expires_at IS NOT NULL AND expires_at < ?",
            (now,),
        )
        self.conn.commit()


@dataclass
class StockSummaryConfig:
    result_mode: str
    pig_stock_names: list[str]
    hk_stock_names: list[str]
    output_dir: str
    kline_days: int

    @classmethod
    def from_argv(cls, argv: list[str]) -> "StockSummaryConfig":
        result_mode = _normalize_result_mode(argv[1] if len(argv) > 1 else "display_only")
        pig_stocks_str = argv[2] if len(argv) > 2 else "牧原股份,温氏股份,新希望"
        hk_stocks_str = argv[3] if len(argv) > 3 else "腾讯控股,阿里巴巴,美团,小米集团"
        output_dir = argv[4] if len(argv) > 4 else "./codeact/output"
        kline_days_str = argv[5] if len(argv) > 5 else "30"

        try:
            kline_days = int(kline_days_str)
        except ValueError:
            print(f"[参数] kline_days={kline_days_str} 非法，回退到 30")
            kline_days = 30
        kline_days = min(max(kline_days, 5), 120)

        return cls(
            result_mode=result_mode,
            pig_stock_names=[s.strip() for s in pig_stocks_str.split(",") if s.strip()],
            hk_stock_names=[s.strip() for s in hk_stocks_str.split(",") if s.strip()],
            output_dir=output_dir,
            kline_days=kline_days,
        )


@dataclass
class WorkflowContext:
    config: StockSummaryConfig
    sdk: CodeActSDK
    state_conn: sqlite3.Connection
    cache: DailyCache
    now: datetime
    date_str: str
    date_short: str
    year: str
    month: str
    day: str
    now_iso: str

    @classmethod
    def create(cls, config: StockSummaryConfig) -> "WorkflowContext":
        os.makedirs(config.output_dir, exist_ok=True)
        now = datetime.now(TZ)
        state_conn = init_state_db(config.output_dir)
        cache = DailyCache(state_conn)
        cache.prune(now.isoformat(timespec="seconds"))
        return cls(
            config=config,
            sdk=CodeActSDK(),
            state_conn=state_conn,
            cache=cache,
            now=now,
            date_str=now.strftime("%Y年%m月%d日"),
            date_short=now.strftime("%Y%m%d"),
            year=now.strftime("%Y"),
            month=now.strftime("%m"),
            day=now.strftime("%d"),
            now_iso=now.isoformat(timespec="seconds"),
        )


@dataclass
class MarketSnapshot:
    stock_codes: dict[str, str]
    pig_kline: dict[str, list[dict]]
    hk_kline: dict[str, list[dict]]
    index_kline: dict[str, list[dict]]

    @property
    def all_kline(self) -> dict[str, list[dict]]:
        return {**self.pig_kline, **self.hk_kline, **self.index_kline}


@dataclass
class SignalBundle:
    market: MarketSnapshot
    news_data: list[dict]
    pig_price_data: list["PigPriceDataPoint"]


@dataclass
class ReportArtifacts:
    evidence_path: str
    report_path: str
    chart_paths: list[str]
    chart_urls: list[str]
    uploaded_chart_count: int


@dataclass
class ChartReference:
    """已生成并上传的图表引用素材，直接交给 LLM 以 Markdown 方式引用。"""

    chart_id: int
    title: str
    path: str
    url: str

    @property
    def markdown(self) -> str:
        return f"![{self.title}]({self.url})" if self.url else ""


@dataclass
class WorkflowBlock:
    name: str
    action: Callable[[], Awaitable[Any]]


def _is_baseline_run(conn: sqlite3.Connection) -> bool:
    """未写过 baseline_completed 标记即为冷启动首跑，据此放宽检索窗口。"""
    return _get_meta(conn, "baseline_completed", "0") != "1"


def write_state_after_success(
    conn: sqlite3.Connection,
    run_date: str,
    report_path: str,
    news_data: list[dict],
    now_iso: str,
) -> None:
    """报告成功产出后一次性落状态：来源去重表 + 运行记录 + baseline 标记。

    严格遵循“成功后写、失败不写”：只在 submit_result 前的成功路径调用，
    避免某次失败把未真正展示的来源标成已见、造成后续增量漏采。
    """
    for item in news_data:
        url = item.get("url", "")
        if not url:
            continue
        conn.execute(
            "INSERT INTO seen_sources (url, title, publish_time, first_seen_at, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(url) DO UPDATE SET "
            "title = excluded.title, publish_time = excluded.publish_time, last_seen_at = excluded.last_seen_at",
            (url, item.get("title", ""), item.get("publish_time", ""), now_iso, now_iso),
        )
    conn.execute(
        "INSERT INTO runs (run_id, run_date, report_path, source_count, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (_stable_key(run_date, now_iso), run_date, report_path, len(news_data), now_iso),
    )
    # 首次成功即视为 baseline 完成，之后切到增量窗口（见 collect_signals）。
    _set_meta(conn, "baseline_completed", "1")
    _set_meta(conn, "baseline_completed_at", now_iso)
    conn.commit()


def kline_has_recent_data(kline_data: dict[str, list[dict]], now: datetime, max_age_days: int = 5) -> bool:
    """判断行情数据是否足够新，避免非交易/接口异常时继续烧 LLM。"""
    latest = None
    for rows in kline_data.values():
        for row in rows:
            parsed = _parse_date_str(row.get("date", ""))
            if parsed and (latest is None or parsed > latest):
                latest = parsed
    if latest is None:
        return False
    return (now.replace(tzinfo=None) - latest).days <= max_age_days


def _ensure_cjk_font():
    """配置 matplotlib 中文字体。"""
    font_name = None
    for name in ["WenQuanYi Micro Hei", "Noto Sans CJK SC", "Noto Sans CJK JP", "Droid Sans Fallback"]:
        if any(f.name == name for f in fm.fontManager.ttflist):
            font_name = name
            break
    if font_name:
        plt.rcParams["font.sans-serif"] = [font_name]
    plt.rcParams["axes.unicode_minus"] = False
    print(f"[字体] 使用: {font_name or '默认'}")


def _build_kline_series(rows: list[dict], field: str = "last") -> tuple[list[str], list[float]]:
    """从 K 线行中提取日期和价格序列（按日期升序）。"""
    sorted_rows = sorted(rows, key=lambda r: r.get("date", ""))
    dates = [r.get("date", "") for r in sorted_rows]
    values = [_safe_float(r.get(field, 0)) for r in sorted_rows]
    return dates, values


# ============================================================
# 绘图辅助函数（公共抽取，消除重复）
# ============================================================

def _format_date_axis_indexed(ax, kline_data_dict: dict) -> None:
    """为指数化走势图设置日期刻度（图1/2/6复用）。"""
    all_dates_set = set()
    for rows in kline_data_dict.values():
        for r in rows:
            all_dates_set.add(r.get("date", ""))
    all_dates = sorted(all_dates_set)
    step = max(1, len(all_dates) // 6)
    tick_positions = list(range(0, len(all_dates), step))
    tick_labels = [all_dates[i] if i < len(all_dates) else "" for i in tick_positions]
    ax.set_xticks(tick_positions)
    ax.set_xticklabels(tick_labels, fontsize=9, rotation=30)


def _save_chart(fig, filepath: str) -> Optional[str]:
    """保存图表到文件并关闭figure，返回文件路径或None。"""
    try:
        fig.savefig(filepath, dpi=150)
        plt.close(fig)
        print(f"[图表] 已保存: {filepath}")
        return filepath
    except Exception as e:
        print(f"[图表] 保存失败: {e}")
        plt.close(fig)
        return None


# ============================================================
# Step 1: 获取股票代码
# ============================================================

async def search_stock_codes(
    stock_names: list[str],
    cache: Optional[DailyCache] = None,
    now_iso: str = "",
) -> dict[str, str]:
    """使用 westockdata CLI 搜索股票代码，返回 {名称: 代码}。"""
    sem = asyncio.Semaphore(WESTOCK_CLI_CONCURRENCY)

    async def _search_one_name(name: str) -> tuple[str, str]:
        if cache:
            cached_code = cache.get("stock_code", name, "persistent")
            if isinstance(cached_code, str) and cached_code:
                print(f"[缓存] 股票代码命中: {name} -> {cached_code}")
                return name, cached_code
        async with sem:
            out = await _run_cli_async([*WESTOCK_CMD, "search", name], timeout=30)
        rows = parse_markdown_table(out)
        a_code = None
        hk_code = None
        for row in rows:
            code = row.get("code", "")
            stype = row.get("type", "")
            if "GP-A" in stype and not a_code:
                a_code = code
            elif "GP" in stype and code.startswith("hk") and not hk_code:
                hk_code = code
            elif "ZS" in stype and not hk_code:
                hk_code = code
        if name in DEFAULT_PIG_CODES or "股份" in name or "希望" in name:
            code = a_code or hk_code or ""
        else:
            code = hk_code or a_code or ""
        if code and cache:
            cache.put("stock_code", name, "persistent", code, now_iso)
        return name, code

    resolved = await asyncio.gather(
        *(_search_one_name(name) for name in stock_names),
        return_exceptions=True,
    )
    result = {}
    for item in resolved:
        if isinstance(item, Exception):
            print(f"[代码搜索异常] {item}")
            continue
        name, code = item
        if code:
            result[name] = code
    return result


# ============================================================
# Step 2: 获取 K 线数据
# ============================================================

async def get_kline_data(
    codes: dict[str, str],
    period: str = "day",
    limit: int = 30,
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
) -> dict[str, list[dict]]:
    """获取 K 线数据，支持批量降级。"""
    cache_key = _versioned_cache_key(
        "kline",
        json.dumps(codes, sort_keys=True, ensure_ascii=False),
        period,
        str(limit),
    )
    if cache and cache_date:
        cached = cache.get("kline", cache_key, cache_date)
        if isinstance(cached, dict):
            print(f"[缓存] K线命中: {len(codes)} 个标的, period={period}, limit={limit}")
            return cached

    all_data: dict[str, list[dict]] = {}
    a_codes = {n: c for n, c in codes.items() if c.startswith("sz") or c.startswith("sh")}
    hk_codes = {n: c for n, c in codes.items() if c.startswith("hk")}

    if hk_codes:
        code_list = ",".join(hk_codes.values())
        out = await _run_cli_async([*WESTOCK_CMD, "kline", code_list, "--period", period, "--limit", str(limit)], timeout=60)
        rows = parse_markdown_table(out)
        for name, code in hk_codes.items():
            kline_rows = [r for r in rows if r.get("symbol", "") == code]
            if kline_rows:
                all_data[name] = kline_rows
            else:
                single_out = await _run_cli_async([*WESTOCK_CMD, "kline", code, "--period", period, "--limit", str(limit)], timeout=30)
                single_rows = parse_markdown_table(single_out)
                if single_rows:
                    all_data[name] = single_rows

    if a_codes:
        code_list = ",".join(a_codes.values())
        out = await _run_cli_async([*WESTOCK_CMD, "kline", code_list, "--period", period, "--limit", str(limit)], timeout=60)
        rows = parse_markdown_table(out)
        if rows:
            for name, code in a_codes.items():
                kline_rows = [r for r in rows if r.get("symbol", "") == code]
                if kline_rows:
                    all_data[name] = kline_rows
                else:
                    all_data[name] = rows
                    break
        for name, code in a_codes.items():
            if name not in all_data or not all_data[name]:
                single_out = await _run_cli_async([*WESTOCK_CMD, "kline", code, "--period", period, "--limit", str(limit)], timeout=30)
                single_rows = parse_markdown_table(single_out)
                if single_rows:
                    all_data[name] = single_rows

    if cache and cache_date and all_data:
        cache.put("kline", cache_key, cache_date, all_data, now_iso)
    return all_data


# ============================================================
# Step 3: 搜索新闻与行业数据
# ============================================================

class CandidateSourceAssessment(BaseModel):
    """候选筛选阶段对来源质量的判断。"""

    url: str = ""
    source_tier: str = Field(default="unverified", description="official/trusted_third_party/community/unverified")
    source_assessment: str = Field(default="", description="来源可靠性和入选原因")


class FetchSelection(BaseModel):
    """搜索候选筛选结果。"""

    selected_urls: list[str] = Field(default_factory=list)
    source_assessments: list[CandidateSourceAssessment] = Field(default_factory=list)
    reason: str = ""


def _candidate_sections(item: dict) -> list[str]:
    sections = item.get("_sections")
    if isinstance(sections, list):
        values = [str(s) for s in sections if s]
    else:
        values = [item.get("_section", "")]
    return list(dict.fromkeys([s for s in values if s]))


def _candidate_companies(item: dict) -> list[str]:
    companies = item.get("_companies")
    if isinstance(companies, list):
        values = [str(c) for c in companies if c]
    else:
        values = [item.get("_company", "")]
    return list(dict.fromkeys([c for c in values if c]))


def _candidate_queries(item: dict) -> list[str]:
    queries = item.get("_queries")
    if isinstance(queries, list):
        values = [str(q) for q in queries if q]
    else:
        values = [item.get("_query", "")]
    return list(dict.fromkeys([q for q in values if q]))


def _ensure_candidate_source_fields(item: dict, assessment: str = "") -> dict:
    enriched = dict(item)
    enriched.setdefault("source_tier", "unverified")
    enriched.setdefault("source_assessment", assessment or "未经过LLM来源评估，按确定性配额入选。")
    return enriched


def _apply_source_assessments(
    candidates: list[dict],
    assessments: list[CandidateSourceAssessment],
) -> list[dict]:
    by_url = {_norm_url(a.url): a for a in assessments if a.url}
    enriched = []
    for item in candidates:
        copy_item = dict(item)
        assessment = by_url.get(_norm_url(copy_item.get("url", "")))
        if assessment:
            copy_item["source_tier"] = assessment.source_tier or "unverified"
            copy_item["source_assessment"] = assessment.source_assessment or ""
        else:
            copy_item = _ensure_candidate_source_fields(copy_item)
        enriched.append(copy_item)
    return enriched


def _merge_candidate_attributions(candidates: list[dict]) -> list[dict]:
    """按 URL 合并 fetch 候选，但保留多章节、多公司归属。"""
    merged: dict[str, dict] = {}
    for item in candidates:
        url_key = _norm_url(item.get("url", ""))
        if not url_key:
            continue
        if url_key not in merged:
            base = dict(item)
            base["_sections"] = _candidate_sections(item)
            base["_companies"] = _candidate_companies(item)
            base["_queries"] = _candidate_queries(item)
            merged[url_key] = base
            continue
        existing = merged[url_key]
        existing["_sections"] = list(dict.fromkeys(existing.get("_sections", []) + _candidate_sections(item)))
        existing["_companies"] = list(dict.fromkeys(existing.get("_companies", []) + _candidate_companies(item)))
        existing["_queries"] = list(dict.fromkeys(existing.get("_queries", []) + _candidate_queries(item)))
        if existing.get("source_tier", "unverified") == "unverified" and item.get("source_tier"):
            existing["source_tier"] = item.get("source_tier")
        if not existing.get("source_assessment") and item.get("source_assessment"):
            existing["source_assessment"] = item.get("source_assessment")
    return list(merged.values())


def _apply_fetch_attribution(page: dict, item: dict) -> dict:
    """把候选归属和来源判断贴回 fetch 正文，不把这些归属写入正文缓存。"""
    result = dict(page)
    sections = _candidate_sections(item)
    companies = _candidate_companies(item)
    queries = _candidate_queries(item)
    result["_sections"] = sections
    result["_section"] = sections[0] if len(sections) == 1 else ""
    result["_companies"] = companies
    result["_company"] = companies[0] if len(companies) == 1 else ""
    result["_queries"] = queries
    result["_query"] = queries[0] if len(queries) == 1 else ""
    result["source_tier"] = item.get("source_tier", result.get("source_tier", "unverified"))
    result["source_assessment"] = item.get("source_assessment", result.get("source_assessment", ""))
    return result


async def _search_queries(
    sdk: CodeActSDK,
    queries: list[str],
    sem: asyncio.Semaphore,
    publish_time: Optional[dict[str, str]] = None,
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
    cache_scope: str = "",
) -> list[dict]:
    """并发搜索多个 query，返回去重后的候选（保留 _query 标签）。"""

    async def _search_one(query: str) -> list[dict]:
        cache_key = _versioned_cache_key(
            "search_results",
            query,
            cache_scope or json.dumps(publish_time or {}, sort_keys=True, ensure_ascii=False),
        )
        if cache and cache_date:
            cached = cache.get("search_results", cache_key, cache_date)
            if isinstance(cached, list):
                print(f"[缓存] 搜索命中: {query}")
                return cached
        async with sem:
            try:
                tool_args = {"query": query, "response_length": "medium"}
                if publish_time:
                    tool_args["publish_time"] = publish_time
                resp = await sdk.call_tool(
                    "codeact_search_web",
                    tool_args,
                    schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
                )
                results = resp.get("results", []) if isinstance(resp, dict) else []
                for item in results:
                    if isinstance(item, dict):
                        item["_query"] = query
                results = _take(results, 5)
                if cache and cache_date:
                    cache.put("search_results", cache_key, cache_date, results, now_iso)
                return results
            except Exception as e:
                print(f"[搜索异常] {query}: {e}")
                return []

    search_results = await asyncio.gather(*[_search_one(q) for q in queries], return_exceptions=True)
    candidates, seen = [], set()
    for result in search_results:
        if isinstance(result, Exception):
            continue
        for item in result:
            url = item.get("url", "")
            url_key = _norm_url(url)
            if url_key and url_key not in seen:
                seen.add(url_key)
                candidates.append(item)
    return candidates


async def _fetch_one_candidate(
    sdk: CodeActSDK,
    item: dict,
    sem: asyncio.Semaphore,
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
) -> dict:
    """抓取单条候选正文，异常时返回空正文占位；正文缓存与语义归属分离。"""
    normalized_url = _norm_url(item.get("url", ""))
    cache_key = _versioned_cache_key("fetched_page", normalized_url) if normalized_url else ""
    if cache and cache_date and cache_key:
        cached = cache.get("fetched_page", cache_key, cache_date)
        if isinstance(cached, dict):
            print(f"[缓存] Fetch命中: {item.get('url', '')}")
            return _apply_fetch_attribution(cached, item)
    async with sem:
        try:
            resp = await sdk.call_tool(
                "codeact_fetch_web",
                {"url": item["url"]},
                schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
            )
            content = resp.get("content", "") if isinstance(resp, dict) else ""
            title = resp.get("title", item.get("title", "")) if isinstance(resp, dict) else item.get("title", "")
            publish_time = resp.get("publish_time", "") if isinstance(resp, dict) else ""
            fetched = {
                "title": title,
                "url": item["url"],
                "content": content,
                "snippet": item.get("snippet", ""),
                "publish_time": publish_time or item.get("publish_time", ""),
            }
            if cache and cache_date and content and cache_key:
                cache.put("fetched_page", cache_key, cache_date, fetched, now_iso)
            return _apply_fetch_attribution(fetched, item)
        except Exception as e:
            print(f"[Fetch 异常] {item.get('url', '')}: {e}")
            return _apply_fetch_attribution({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "content": "",
                "snippet": item.get("snippet", ""),
                "publish_time": item.get("publish_time", ""),
            }, item)


async def _select_for_section(
    sdk: CodeActSDK,
    candidates: list[dict],
    queries: list[str],
    max_fetch: int,
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
) -> list[dict]:
    """在单个章节候选内选取待抓取 URL：先按 query 确定性覆盖，不足再用 LLM 补足。"""
    selected, selected_urls = [], set()

    def _try_add(item: dict) -> bool:
        url = item.get("url", "")
        url_key = _norm_url(url)
        if not url_key or url_key in selected_urls:
            return False
        selected.append(_ensure_candidate_source_fields(item))
        selected_urls.add(url_key)
        return True

    # 每个 query 至少覆盖一条，避免单公司查询被章节内其它 query 挤掉。
    for query in queries:
        for item in candidates:
            if item.get("_query") == query and _try_add(item):
                break

    if len(selected) < max_fetch:
        remaining = [c for c in candidates if _norm_url(c.get("url", "")) not in selected_urls]
        llm_selected = await _select_candidates(
            sdk,
            remaining,
            max_fetch - len(selected),
            cache,
            cache_date,
            now_iso,
        )
        for item in llm_selected:
            if len(selected) >= max_fetch:
                break
            _try_add(item)

    return _take(selected, max_fetch)


async def search_and_fetch(
    sdk: CodeActSDK,
    section_plan: dict[str, dict],
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
    lookback_days: int = SEARCH_LOOKBACK_DAYS,
) -> list[dict]:
    """按报告章节分组收集新闻：每章节独立搜索、独立配额选取，抓取正文并打 _section 标签。

    section_plan: section -> {label, max_fetch, queries}
    返回扁平新闻列表，每条含 _section，供写作层按章节依次取用。
    """
    sem_search = asyncio.Semaphore(3)
    sem_fetch = asyncio.Semaphore(3)
    now = datetime.now(TZ)
    publish_time = _build_publish_time_window(now, lookback_days)

    # 各章节并发搜索候选。
    sections = list(section_plan.keys())
    section_candidates = await asyncio.gather(
        *[
            _search_queries(
                sdk,
                section_plan[s]["queries"],
                sem_search,
                publish_time,
                cache,
                cache_date,
                now_iso,
                f"{cache_date}:{lookback_days}",
            )
            for s in sections
        ]
    )

    # 逐章节独立选取：URL 可以归属多个章节；fetch 前再按 URL 合并。
    selected_all = []
    for section, candidates in zip(sections, section_candidates):
        candidates = _sort_recent_candidates(candidates, now)
        company = HK_COMPANY_SECTION_TO_NAME.get(section, "")
        for c in candidates:
            c["_section"] = section
            if company:
                c["_company"] = company
        cfg = section_plan[section]
        picked = await _select_for_section(
            sdk, candidates, cfg["queries"], cfg["max_fetch"], cache, cache_date, now_iso
        )
        print(f"[新闻] 章节 {cfg['label']}: 候选 {len(candidates)} 条，选取 {len(picked)} 条")
        selected_all.extend(picked)

    # 统一抓取正文：同一 URL 只 fetch 一次，但保留多章节/公司归属。
    fetch_candidates = _merge_candidate_attributions(selected_all)
    fetched = await asyncio.gather(
        *[_fetch_one_candidate(sdk, item, sem_fetch, cache, cache_date, now_iso) for item in fetch_candidates],
        return_exceptions=True,
    )
    valid = [f for f in fetched if isinstance(f, dict) and f.get("content")]
    print(
        f"[新闻] 分章节收集完成，章节引用 {len(selected_all)} 条，"
        f"唯一URL {len(fetch_candidates)} 条，fetch 成功 {len(valid)} 条"
    )
    return valid


async def _select_candidates(
    sdk: CodeActSDK,
    candidates: list[dict],
    max_fetch: int,
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
) -> list[dict]:
    """用 LLM 从搜索候选中筛选值得 fetch 的 URL。"""

    if len(candidates) <= max_fetch:
        return _take([_ensure_candidate_source_fields(item) for item in candidates], max_fetch)

    candidate_fingerprint = json.dumps(
        [
            {
                "title": c.get("title", ""),
                "url": _norm_url(c.get("url", "")),
                "publish_time": c.get("publish_time", ""),
            }
            for c in _take(candidates, 20)
        ],
        ensure_ascii=False,
        sort_keys=True,
    )
    cache_key = _versioned_cache_key("candidate_selection", str(max_fetch), candidate_fingerprint)
    if cache and cache_date:
        cached_selection = cache.get("candidate_selection", cache_key, cache_date)
        if isinstance(cached_selection, list):
            if all(isinstance(x, str) for x in cached_selection):
                selected_urls = {_norm_url(url) for url in cached_selection}
                assessments: list[CandidateSourceAssessment] = []
            else:
                assessments = [
                    CandidateSourceAssessment(**x)
                    for x in cached_selection
                    if isinstance(x, dict)
                ]
                selected_urls = {_norm_url(a.url) for a in assessments}
            picked = [c for c in candidates if _norm_url(c.get("url", "")) in selected_urls]
            if picked:
                print(f"[缓存] 候选筛选命中: {len(picked)} 条")
                return _take(_apply_source_assessments(picked, assessments), max_fetch)

    prompt_lines = []
    for i, item in enumerate(_take(candidates, 20)):
        prompt_lines.append(
            f"{i+1}. 标题：{item.get('title', '')}\n"
            f"   URL：{item.get('url', '')}\n"
            f"   发布时间：{item.get('publish_time', '') or '未提供'}\n"
            f"   摘要：{_format_chunked_excerpt(item.get('snippet', ''), 240, 1, '搜索摘要')}"
        )
    candidates_block = _md_block("搜索候选材料", "\n\n".join(prompt_lines))
    prompt = (
        f"从以下搜索结果中选出最值得获取正文的完整 URL（最多{max_fetch}条），"
        "selected_urls 只能包含本批候选中出现过的 URL，不要返回序号。"
        "同时为入选 URL 填写 source_assessments，source_tier 从 "
        "official/trusted_third_party/community/unverified 中选择，并说明来源质量和入选原因。"
        "优先选择：官方公告/交易所/公司投资者关系、权威财经媒体、"
        "含具体数据/价格/涨跌幅且近期发布的文章。注意：摘要只能作为选择线索，最终事实必须来自后续正文 fetch。\n\n"
        + candidates_block
    )
    try:
        selection = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=FetchSelection,
        )
        selected_urls = {_norm_url(url) for url in selection.selected_urls}
        picked = [c for c in candidates if _norm_url(c.get("url", "")) in selected_urls]
        picked = _apply_source_assessments(picked, selection.source_assessments)
        if cache and cache_date and picked:
            cache.put(
                "candidate_selection",
                cache_key,
                cache_date,
                [
                    {
                        "url": item.get("url", ""),
                        "source_tier": item.get("source_tier", "unverified"),
                        "source_assessment": item.get("source_assessment", ""),
                    }
                    for item in picked
                ],
                now_iso,
            )
        return _take(picked if picked else [_ensure_candidate_source_fields(c) for c in candidates], max_fetch)
    except Exception as e:
        print(f"[LLM筛选异常] {e}，取前 {max_fetch} 条")
        return _take([_ensure_candidate_source_fields(c, "LLM筛选失败，按排序兜底入选。") for c in candidates], max_fetch)


# ============================================================
# Step 3.5: 搜索并提取猪价与猪粮比数据
# ============================================================

class PigPriceDataPoint(BaseModel):
    """单日猪价数据点。"""
    date: str = Field(description="日期，格式 YYYY-MM-DD")
    pig_price: Optional[float] = Field(default=None, description="生猪均价（元/公斤）")
    pig_grain_ratio: Optional[float] = Field(default=None, description="猪粮比")
    corn_price: Optional[float] = Field(default=None, description="玉米价格（元/公斤）")
    soybean_meal_price: Optional[float] = Field(default=None, description="豆粕价格（元/公斤）")


class PigPriceExtraction(BaseModel):
    """LLM 提取的猪价数据集合。"""
    data_points: list[PigPriceDataPoint] = Field(
        default_factory=list,
        description="最近7-14天的生猪均价、猪粮比、玉米价、豆粕价数据点列表"
    )
    source_summary: str = Field(default="", description="数据来源说明")


async def search_pig_price_data(
    sdk: CodeActSDK,
    year: str,
    month: str,
    day: str,
    cache: Optional[DailyCache] = None,
    cache_date: str = "",
    now_iso: str = "",
    lookback_days: int = PIG_PRICE_LOOKBACK_DAYS,
) -> list[PigPriceDataPoint]:
    """搜索生猪均价、猪粮比、饲料价格数据，用LLM结构化提取。"""
    cache_key = _versioned_cache_key("pig_price_data", year, month, day, str(lookback_days))
    if cache and cache_date:
        cached = cache.get("pig_price_data", cache_key, cache_date)
        if isinstance(cached, list):
            print("[缓存] 猪价数据命中")
            return [PigPriceDataPoint(**item) for item in cached if isinstance(item, dict)]

    pig_price_queries = [
        f"{year}年{month}月 生猪均价 全国均价 元/公斤",
        f"{year}年{month}月 猪粮比 最新数据",
        f"{year}年{month}月 玉米价格 豆粕价格 饲料",
    ]

    sem = asyncio.Semaphore(3)
    publish_time = _build_publish_time_window(datetime.now(TZ), lookback_days)

    async def _search_one(query: str) -> list[dict]:
        async with sem:
            try:
                tool_args = {
                    "query": query,
                    "response_length": "long",
                    "publish_time": publish_time,
                }
                resp = await sdk.call_tool(
                    "codeact_search_web",
                    tool_args,
                    schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
                )
                return resp.get("results", []) if isinstance(resp, dict) else []
            except Exception as e:
                print(f"[猪价搜索异常] {query}: {e}")
                return []

    search_tasks = [_search_one(q) for q in pig_price_queries]
    search_results = await asyncio.gather(*search_tasks, return_exceptions=True)

    all_candidates = []
    seen_urls = set()
    for result in search_results:
        if isinstance(result, Exception):
            continue
        for item in result:
            url = item.get("url", "")
            url_key = _norm_url(url)
            if url_key and url_key not in seen_urls:
                seen_urls.add(url_key)
                all_candidates.append(item)

    all_candidates = _sort_recent_candidates(all_candidates, datetime.now(TZ))
    print(f"[猪价] 搜索到 {len(all_candidates)} 条候选")

    fetch_limit = 6

    async def _fetch_one(item: dict) -> dict:
        async with sem:
            try:
                resp = await sdk.call_tool(
                    "codeact_fetch_web",
                    {"url": item["url"]},
                    schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
                )
                content = resp.get("content", "") if isinstance(resp, dict) else ""
                return {
                    "title": item.get("title", ""),
                    "url": item["url"],
                    "snippet": item.get("snippet", ""),
                    "content": content,
                    "publish_time": item.get("publish_time", ""),
                }
            except Exception as e:
                print(f"[猪价Fetch异常] {item.get('url', '')}: {e}")
                return {"title": item.get("title", ""), "url": item.get("url", ""),
                        "snippet": item.get("snippet", ""), "content": ""}

    fetch_tasks = [_fetch_one(item) for item in _take(all_candidates, fetch_limit)]
    fetched = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    valid = [f for f in fetched if isinstance(f, dict) and f.get("content")]
    print(f"[猪价] Fetch 成功 {len(valid)} 条")

    if not valid:
        return []

    context_parts = []
    for item in valid:
        context_parts.append(
            f"来源：{item.get('title', '')}\n"
            f"URL：{item.get('url', '')}\n"
            f"发布时间：{item.get('publish_time', '') or '未提供'}\n"
            f"正文分块摘录：\n{_format_chunked_excerpt(item.get('content', ''), 1800, 2, '猪价正文')}\n"
        )
    pig_context_block = _md_block("搜索结果正文材料", "".join(context_parts))

    extraction_prompt = f"""从以下搜索结果中提取最近7-14天的生猪价格、猪粮比、玉米价格和豆粕价格数据。

要求：
1. 每个数据点必须包含日期（尽量精确到日，至少精确到月）
2. 如果某项数据某天缺失，对应字段留空
3. 只提取有明确数值的数据，不要推测或编造
4. 优先提取最近的数据
5. 日期格式统一为 YYYY-MM-DD

当前日期：{year}-{month}-{day}

{pig_context_block}
"""

    try:
        extraction = await sdk.call_llm(
            messages=[{"role": "user", "content": extraction_prompt}],
            response_format=PigPriceExtraction,
        )
        if isinstance(extraction, str):
            print(f"[猪价] LLM返回非结构化文本，跳过猪价数据提取")
            return []
        if hasattr(extraction, "data_points"):
            points = extraction.data_points
            print(f"[猪价] 提取到 {len(points)} 个数据点")
            if cache and cache_date:
                cache.put("pig_price_data", cache_key, cache_date, [p.dict() for p in points], now_iso)
            return points
        return []
    except Exception as e:
        print(f"[猪价提取异常] {e}")
        return []


# ============================================================
# Step 4: LLM 综合分析（分块串行生成报告）
# ============================================================

class BlockContent(BaseModel):
    """单个block的LLM输出。"""
    content: str = Field(description="本block的Markdown报告内容")


# ----- 数据格式化辅助 -----

def _fmt_kline_latest(name: str, rows: list[dict]) -> str:
    """格式化单只股票/指数的最新K线摘要。"""
    if not rows:
        return f"- {name}: 无数据"
    sorted_rows = sorted(rows, key=lambda r: r.get("date", ""), reverse=True)
    latest = sorted_rows[0]
    prev = sorted_rows[1] if len(sorted_rows) > 1 else None
    chg_str = ""
    if prev:
        cur = _safe_float(latest.get("last", 0))
        p = _safe_float(prev.get("last", 0))
        if p > 0:
            chg_str = f"，涨跌幅 {(cur - p) / p * 100:+.2f}%"
    return (
        f"- {name}: 收盘 {latest.get('last', 'N/A')}，"
        f"开盘 {latest.get('open', 'N/A')}，"
        f"最高 {latest.get('high', 'N/A')}，"
        f"最低 {latest.get('low', 'N/A')}"
        f"{chg_str}"
    )


def _fmt_kline_table(name: str, rows: list[dict], max_rows: int = 20) -> str:
    """格式化完整K线数据表格（近max_rows日）。"""
    if not rows:
        return f"**{name}**: 无数据"
    sorted_rows = sorted(rows, key=lambda r: r.get("date", ""))
    recent = _tail(sorted_rows, max_rows)
    lines = [f"**{name}**（近{len(recent)}个交易日）:"]
    lines.append("| 日期 | 开盘 | 最高 | 最低 | 收盘 | 换手率% |")
    lines.append("|------|------|------|------|------|---------|")
    for r in recent:
        lines.append(
            f"| {r.get('date','')} | {r.get('open','')} | {r.get('high','')} | "
            f"{r.get('low','')} | {r.get('last','')} | {r.get('exchange','')} |"
        )
    return "\n".join(lines)


def _fmt_pig_price_summary(pig_price_data: list) -> str:
    """格式化猪价数据摘要。"""
    if not pig_price_data:
        return "未获取到猪价与猪粮比数据"
    parts = []
    for dp in sorted(pig_price_data, key=lambda x: x.date):
        line = f"- 日期：{dp.date}"
        if dp.pig_price is not None:
            line += f" | 生猪均价：{dp.pig_price}元/公斤"
        if dp.pig_grain_ratio is not None:
            line += f" | 猪粮比：{dp.pig_grain_ratio}"
        if dp.corn_price is not None:
            line += f" | 玉米：{dp.corn_price}元/公斤"
        if dp.soybean_meal_price is not None:
            line += f" | 豆粕：{dp.soybean_meal_price}元/公斤"
        parts.append(line)
    return "\n".join(parts)


def _fmt_news_titles(news_list: list[dict]) -> str:
    """格式化新闻标题列表（仅标题+URL+日期）。"""
    if not news_list:
        return "无相关新闻"
    parts = []
    for item in news_list:
        parts.append(
            f"- {item.get('title', '无标题')} "
            f"[{item.get('publish_time', '')}]({item.get('url', '')})"
        )
    return "\n".join(parts)


def _fmt_news_full(news_list: list[dict], max_items: int = 5, max_content_len: int = 2000) -> str:
    """格式化新闻完整内容（标题+URL+日期+正文分块摘录）。"""
    if not news_list:
        return "无相关新闻"
    parts = []
    chunk_size = max(600, max_content_len)
    for item in _take(news_list, max_items):
        parts.append(
            f"### {item.get('title', '无标题')}\n"
            f"URL：{item.get('url', '')}\n"
            f"日期：{item.get('publish_time', '')}\n"
            f"来源等级：{item.get('source_tier', 'unverified')}\n"
            f"来源评估：{item.get('source_assessment', '') or '未提供'}\n"
            f"归属章节：{', '.join(SECTION_LABELS.get(s, s) for s in _candidate_sections(item)) or '未标注'}\n"
            f"归属公司：{', '.join(_candidate_companies(item)) or '未标注'}\n"
            f"正文：\n{_format_chunked_excerpt(item.get('content', ''), chunk_size, 1, '新闻正文')}"
        )
    return "\n".join(parts)


def _company_aliases(company: str) -> list[str]:
    """公司名到新闻匹配关键词。只用于把材料分桶，不用于生成事实。"""
    return DOMAIN.company_aliases(company)


def _split_company_news(news_data: list[dict], company_names: list[str]) -> dict[str, list[dict]]:
    """将新闻按公司拆桶，避免把港股/指数/其他公司材料喂给错误的小节。"""
    buckets = {name: [] for name in company_names}
    seen_by_company = {name: set() for name in company_names}
    for item in news_data:
        tagged_companies = [c for c in _candidate_companies(item) if c in buckets]
        if tagged_companies:
            for tagged_company in tagged_companies:
                url = _norm_url(item.get("url", ""))
                if url not in seen_by_company[tagged_company]:
                    buckets[tagged_company].append(item)
                    seen_by_company[tagged_company].add(url)
            continue

        text = " ".join([
            item.get("title", ""),
            item.get("snippet", ""),
            _format_chunked_excerpt(item.get("content", ""), 1200, 1, "公司新闻匹配"),
            item.get("url", ""),
        ])
        for company in company_names:
            if any(alias in text for alias in _company_aliases(company)):
                url = _norm_url(item.get("url", ""))
                if url not in seen_by_company[company]:
                    buckets[company].append(item)
                    seen_by_company[company].add(url)
    return buckets


def _fmt_company_evidence(company_kline: dict[str, list[dict]], company_news: dict[str, list[dict]]) -> str:
    """生成公司级证据包；Block 4 只能使用这里列出的公司对应材料。"""
    parts = []
    for company, rows in company_kline.items():
        parts.append(f"## {company} 证据包")
        parts.append("### 行情证据（westockdata kline）")
        parts.append(_fmt_kline_latest(company, rows))
        parts.append(_fmt_kline_table(company, rows, max_rows=10))

        news_items = company_news.get(company, [])
        parts.append("### 公司专属新闻/公告证据")
        if news_items:
            parts.append(_fmt_news_full(news_items, max_items=4, max_content_len=1600))
        else:
            parts.append(
                "[INFO_GAP] 未检索到该公司的专属新闻/公告正文。"
                "本公司小节只能基于行情数据讨论价格表现；不得补充业务、业绩、回购、AI、云业务等外部常识论据。"
            )
        parts.append("")
    return "\n".join(parts)


def _categorize_news(news_data: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """将新闻分为大盘、养猪、港股三类。

    优先使用收集阶段打的 _section 标签（收集意图即章节归属）；
    仅对无标签的历史/兜底数据回退到关键词分类。
    """
    pig_keywords = ["猪", "生猪", "猪价", "猪周期", "猪粮", "饲料", "玉米", "豆粕",
                    "养殖", "牧原", "温氏", "新希望", "产能去化"]
    hk_keywords = ["港股", "恒生", "腾讯", "阿里", "美团", "小米", "科网", "香港",
                   "科网股", "小米汽车", "智能汽车", "回购"]
    market_keywords = ["A股", "上证", "沪深300", "创业板", "深证", "收盘", "大盘",
                       "指数", "行情", "两市"]
    section_map = {"market": "market", "pig": "pig", "hk": "hk"}

    market_news, pig_news, hk_news = [], [], []
    seen = {"market": set(), "pig": set(), "hk": set()}

    def _append(category: str, item: dict) -> None:
        url_key = _norm_url(item.get("url", "")) or _stable_key(item.get("title", ""), category)
        if url_key in seen[category]:
            return
        seen[category].add(url_key)
        if category == "market":
            market_news.append(item)
        elif category == "pig":
            pig_news.append(item)
        elif category == "hk":
            hk_news.append(item)

    for item in news_data:
        matched_by_section = False
        for raw_section in _candidate_sections(item):
            section = "hk" if raw_section in HK_COMPANY_SECTION_TO_NAME else section_map.get(raw_section, "")
            if section:
                _append(section, item)
                matched_by_section = True
        if matched_by_section:
            continue

        # 无 _section 标签：关键词兜底（允许跨类）。
        text = item.get("title", "") + " " + item.get("snippet", "")
        is_pig = any(k in text for k in pig_keywords)
        is_hk = any(k in text for k in hk_keywords)
        is_market = any(k in text for k in market_keywords)
        if is_pig:
            _append("pig", item)
        if is_hk:
            _append("hk", item)
        if is_market or (not is_pig and not is_hk):
            _append("market", item)
    return market_news, pig_news, hk_news


def _truncate_ctx(text: str, max_chars: int = 1500) -> str:
    """按分块摘录压缩上下文，并显式标记未纳入内容。"""
    if len(text) <= max_chars:
        return text
    return _format_chunked_excerpt(text, max_chars, 1, "前文上下文")


def _extract_block_content(raw: str) -> str:
    """从可能的JSON/markdown包装中提取纯文本内容。

    LLM有时不遵循response_format，返回 ```json {"content":"..."} ``` 格式。
    本函数剥离这些包装，只保留实际Markdown内容。
    """
    import json as _json
    stripped = raw.strip()

    # Case 1: markdown code block wrapping JSON (```json ... ``` or ``` ... ```)
    code_block_match = re.match(r'^```(?:json)?\s*\n(.+?)\n```\s*$', stripped, re.DOTALL)
    if code_block_match:
        inner = code_block_match.group(1).strip()
        try:
            data = _json.loads(inner)
            if isinstance(data, dict) and "content" in data:
                return data["content"]
        except (_json.JSONDecodeError, TypeError):
            pass

    # Case 2: raw JSON object {"content": "..."}
    if stripped.startswith("{") and stripped.endswith("}"):
        try:
            data = _json.loads(stripped)
            if isinstance(data, dict) and "content" in data:
                return data["content"]
        except (_json.JSONDecodeError, TypeError):
            pass

    # Case 3: plain text (the desired case) - return as-is
    return raw


async def _call_llm_block(sdk: CodeActSDK, prompt: str, block_name: str) -> str:
    """调用LLM生成单个block内容，失败时返回占位文本。"""
    print(f"[Block] 开始生成: {block_name}")
    try:
        result = await sdk.call_llm(
            messages=[{"role": "user", "content": prompt}],
            response_format=BlockContent,
        )
        if isinstance(result, str):
            content = _extract_block_content(result)
        elif hasattr(result, "content"):
            content = result.content
        else:
            content = _extract_block_content(str(result))
        print(f"[Block] {block_name} 完成，{len(content)} 字符")
        return content
    except Exception as e:
        print(f"[Block] {block_name} 失败: {e}")
        return f"\n\n<!-- {block_name} 生成失败: {e} -->\n\n（本部分因LLM调用异常而缺失，请人工补充。）\n"


# ----- 共享质量要求文本 -----
_QUALITY_RULES = """**📌 质量要求（适用于所有部分）：**
1. **标题风格——主题+核心判断**：章节标题必须是"主题+核心判断"格式。示例：`## 一、大盘综述：风格再平衡加速，科技回调防御走强`
2. **内联来源引用**：每个关键论点、数据、事件后必须附引用，格式严格为 `[(来源媒体 · 简要描述, 日期)](url)`。注意 `·` 两侧各有一个空格。每条引用必须包含完整 URL。只能引用本次提供的新闻/公告/K线材料；严禁编造URL、媒体名或日期。
   - ✅ 正确：`[(36氪 · 上证指数跌逾1%, 2026-07-07)](https://36kr.com/newsflashes/3884969620877319)`
   - ✅ 正确：`[(凤凰网 · 港股科技表现活跃, 2026-07-07)](https://news.ifeng.com/c/8uYvi8ryGRb)`
   - ❌ 错误（缺空格）：`[(36氪·上证指数跌逾1%, 2026-07-07)](url)`
   - ❌ 错误（缺URL）：`[(36氪 · 上证指数跌逾1%, 2026-07-07)]`
   - 行情数据等无URL的引用，使用来源说明文字而非引用格式，如"（数据来源：westockdata 实时行情）"
3. **数据准确**：所有数据基于提供的信息，涨跌幅需根据K线数据自行计算，不得编造价格或涨跌幅。
4. **不重复前文**：不要重复已完成部分已说过的数据和结论，而是引用和深化。
5. **证据不足处理**：如果某个结论在材料中找不到来源，必须写 `[INFO_GAP]`，不得用行业常识、历史记忆或推测补齐。
6. **图表引用**：只能引用“本部分可用图表素材”中给出的 Markdown 图片；不得编造图片URL，不得使用占位符。
7. **分析深度与篇幅**：每个部分必须有充分的分析深度，不得简单罗列数据。要求：
   - 核心摘要（Block1）：至少1500字符，核心逻辑后必须展开3段以上关键数据支撑
   - 大盘综述（Block2）：至少2500字符，必须包含指数表格、量能分析、风格判断、后市研判4个维度
   - 养猪行业（Block3）：至少4000字符，必须覆盖板块表现、猪价/猪周期、饲料成本、后续判断4个子节
   - 恒生科技（Block4）：至少5000字符，指数分析+每个公司必须有投资亮点和风险提示各2-3条
   - 投资建议（Block5）：至少4000字符，短线/中线/长线各有具体价位区间和操作方向，风险提示至少5条"""


def _report_block_spec(section_id: str) -> ReportSectionBlock:
    for block in DOMAIN.report_sections:
        if block.section_id == section_id:
            return block
    return ReportSectionBlock(section_id, section_id)


def _chart_id_from_path(path: str) -> Optional[int]:
    match = re.search(r"chart(\d+)_", os.path.basename(path or ""))
    return int(match.group(1)) if match else None


def build_chart_references(chart_paths: list[str], chart_urls: list[str]) -> list[ChartReference]:
    """把上传后的图表转成可直接给 LLM 使用的 Markdown 引用素材。"""
    refs: list[ChartReference] = []
    for path, url in zip(chart_paths, chart_urls):
        chart_id = _chart_id_from_path(path)
        if not chart_id or chart_id not in CHART_DEFS:
            continue
        refs.append(
                ChartReference(
                    chart_id=chart_id,
                    title=CHART_DEFS[chart_id],
                    path=path,
                    url=url,
                )
        )
    return sorted(refs, key=lambda ref: ref.chart_id)


def _chart_refs_for_section(chart_refs: list[ChartReference], spec: ReportSectionBlock) -> list[ChartReference]:
    wanted = set(spec.chart_slots)
    return [ref for ref in chart_refs if ref.chart_id in wanted]


def _fmt_chart_refs(chart_refs: list[ChartReference]) -> str:
    if not chart_refs:
        return "本部分无可用图表素材。"
    lines = []
    for ref in chart_refs:
        if ref.url:
            lines.append(
                f"- 图{ref.chart_id}：{ref.title}\n"
                f"  Markdown引用：{ref.markdown}\n"
                f"  使用方式：放在分析该图对应数据的段落之后，并用正文解释图表传递的信息。"
            )
        else:
            lines.append(
                f"- 图{ref.chart_id}：{ref.title}\n"
                "  状态：生成了本地图表但上传失败，本次报告不要嵌入该图。"
            )
    return "\n".join(lines)


async def llm_analysis_block_wise(
    sdk: CodeActSDK,
    pig_kline: dict[str, list[dict]],
    hk_kline: dict[str, list[dict]],
    index_kline: dict[str, list[dict]],
    news_data: list[dict],
    chart_refs: list[ChartReference],
    pig_price_data: list,
    date_str: str,
) -> str:
    """分块串行编写报告：按章节依次调用 LLM，每次带前文上下文、本章专属材料和图表素材。

    设计取舍：这里把 fetch 到的正文（而非搜索摘要）分块后直接作为写作材料，
    不先抽取结构化事实。为控制“正文直喂”的失真，靠三重约束兜底：
    - 只用 fetch 全文，绝不把搜索 snippet 当事实来源；
    - 长材料一律经 _chunk_text/_format_chunked_excerpt 分块，禁止静默截断丢内容；
    - 每个 prompt 都拼入 _QUALITY_RULES，强制内联标注来源、材料不足处写 [INFO_GAP]，
      把事实核验责任显式压给 LLM 并可在产物中审计。
    上下文按“最近章节全文 + 更早章节压缩摘要”滚动推进，避免 prompt 随章节累积膨胀。

    章节顺序：核心摘要+数据来源 → 大盘综述 → 养猪行业行情 →
    恒生科技指数及核心公司 → 综合投资建议。
    """

    # ----- 数据准备 -----
    core_spec = _report_block_spec("core")
    market_spec = _report_block_spec("market")
    pig_spec = _report_block_spec("pig")
    hk_spec = _report_block_spec("hk")
    advice_spec = _report_block_spec("advice")
    core_charts_block = _md_block("本部分可用图表素材", _fmt_chart_refs(_chart_refs_for_section(chart_refs, core_spec)))
    market_charts_block = _md_block("本部分可用图表素材", _fmt_chart_refs(_chart_refs_for_section(chart_refs, market_spec)))
    pig_charts_block = _md_block("本部分可用图表素材", _fmt_chart_refs(_chart_refs_for_section(chart_refs, pig_spec)))
    hk_charts_block = _md_block("本部分可用图表素材", _fmt_chart_refs(_chart_refs_for_section(chart_refs, hk_spec)))
    all_kline = {**pig_kline, **hk_kline, **index_kline}
    market_news, pig_news, hk_news = _categorize_news(news_data)
    print(f"[Block] 新闻分类: 大盘{len(market_news)}条, 养猪{len(pig_news)}条, 港股{len(hk_news)}条")

    # 各股最新摘要
    all_latest = "\n".join(_fmt_kline_latest(n, r) for n, r in all_kline.items() if r)
    pig_price_summary = _fmt_pig_price_summary(pig_price_data)
    all_news_titles = _fmt_news_titles(news_data)
    all_latest_block = _md_block("各股/指数最新行情摘要", all_latest)
    pig_price_summary_block = _md_block("猪价与猪粮比数据", pig_price_summary)
    all_news_titles_block = _md_block("新闻标题列表（仅标题，供引用参考）", all_news_titles)

    # A股指数完整K线
    index_kline_tables = "\n\n".join(_fmt_kline_table(n, r) for n, r in index_kline.items() if r)
    market_news_full = _fmt_news_full(market_news)
    index_kline_tables_block = _md_block("A股指数完整K线数据（近30日）", index_kline_tables)
    market_news_full_block = _md_block("大盘相关新闻（含正文）", market_news_full)

    # 养猪股完整K线
    pig_kline_tables = "\n\n".join(_fmt_kline_table(n, r) for n, r in pig_kline.items() if r)
    pig_news_full = _fmt_news_full(pig_news)
    pig_kline_tables_block = _md_block("养猪股完整K线数据（近30日）", pig_kline_tables)
    pig_news_full_block = _md_block("养猪相关新闻（含正文）", pig_news_full)

    # 港股完整K线
    hk_kline_tables = "\n\n".join(_fmt_kline_table(n, r) for n, r in hk_kline.items() if r)
    hk_index_news = [item for item in hk_news if not _candidate_companies(item)]
    hk_news_full = _fmt_news_full(hk_index_news)
    hk_company_kline = {n: r for n, r in hk_kline.items() if n != "恒生科技" and r}
    hk_company_names = list(hk_company_kline.keys())
    hk_company_label = "、".join(hk_company_names) or "核心公司"
    hk_company_news = _split_company_news(hk_news, list(hk_company_kline.keys()))
    hk_company_evidence = _fmt_company_evidence(hk_company_kline, hk_company_news)
    hk_kline_tables_block = _md_block(f"港股完整K线数据（近30日，含恒生科技指数+{hk_company_label}）", hk_kline_tables)
    hk_news_full_block = _md_block("恒生科技/港股板块新闻（仅用于3.1指数与板块分析，不用于单公司小节）", hk_news_full)
    hk_company_evidence_block = _md_block(f"公司级证据包（{hk_company_label} 小节只能使用各自证据包）", hk_company_evidence)
    hk_company_subsections = "\n\n".join(
        f"### 3.{idx} {company}\n（含 **投资亮点**（2-3条）和 **风险提示**（1-2条），只能结合该公司的行情与专属新闻/公告证据）"
        for idx, company in enumerate(hk_company_names, 2)
    )
    hk_company_subsections_prompt = hk_company_subsections or (
        "### 3.2 核心公司\n"
        "（未获取到核心公司行情数据，本节只说明信息缺口 [INFO_GAP]）"
    )

    blocks = []

    # ===== Block 1: 核心摘要 + 数据来源 =====
    block1_prompt = f"""你是一位资深股市分析师，正在撰写 **{date_str}** 的每日股市总结报告。
这是报告的第 **1** 部分（共5部分），你负责撰写"{core_spec.title}"。

## 本部分专属数据材料

{all_latest_block}

{pig_price_summary_block}

{all_news_titles_block}

{core_charts_block}

---

{_QUALITY_RULES}

请撰写以下内容（只输出本部分，不要输出后续章节）：
本部分篇幅不少于1500字符，分析必须有深度，不得简单罗列数据。

**⚠️ 编号例外说明：** 本部分（第1部分）不使用序号前缀（一、二、三等），以下标题直接以 `##` 开头，不加"一、""二、"等序号。正文章节编号从第2部分（大盘综述）开始。

## 核心摘要
（1句话核心驱动逻辑 + 2-3段关键数据支撑，每个关键论点后附内联引用）

## 数据来源
（Markdown表格列出各数据维度和来源：行情数据(westockdata)、猪价数据(网络搜索)、新闻来源(各财经媒体URL)）

**特别注意：**
- 核心摘要必须提炼本次已核验数据共同指向的核心驱动逻辑，证据不足时写 `[INFO_GAP]`
- 先给出一句话核心逻辑，再展开2-3段关键数据支撑
- 如果本部分可用图表素材中有图表，请在数据来源表格之后直接插入对应 Markdown 图片引用；没有可用 URL 时不要嵌图
- 不要单独创建"图表素材"章节标题，图表直接嵌入在数据来源表格之后"""

    block1 = await _call_llm_block(sdk, block1_prompt, "Block1-核心摘要")
    blocks.append(block1)

    # ===== Block 2: 一、大盘综述 =====
    block2_prompt = f"""你是一位资深股市分析师，正在撰写 **{date_str}** 的每日股市总结报告。
这是报告的第 **2** 部分（共5部分），你负责撰写"{market_spec.title}"章节。

## 已完成的前文（核心摘要+数据来源）
{_md_block("已完成前文（核心摘要+数据来源）", block1)}

## 本部分专属数据材料

{index_kline_tables_block}

{market_news_full_block}

{market_charts_block}

---

{_QUALITY_RULES}

请撰写以下内容（只输出本部分章节，不要输出核心摘要或其他章节）：
本部分篇幅不少于2500字符，分析必须有深度，不得简单罗列数据。

## {market_spec.title}：[你的核心判断]
（包含：A股整体表现分析、指数数据表格、风格判断（大盘vs小盘/价值vs成长）、关键论点附内联引用。
要求分析深度，不要简单罗列数据，要解读数据背后的市场逻辑。）

**图表引用：**
如果前文核心摘要已嵌入同一张图，本章无需重复嵌入；否则请在指数数据分析之后直接插入本部分可用图表素材里的 Markdown 图片引用。"""

    block2 = await _call_llm_block(sdk, block2_prompt, "Block2-大盘综述")
    blocks.append(block2)

    # ===== Block 3: 二、养猪行业行情 =====
    block3_prompt = f"""你是一位资深股市分析师，正在撰写 **{date_str}** 的每日股市总结报告。
这是报告的第 **3** 部分（共5部分），你负责撰写"{pig_spec.title}"章节。

## 已完成的前文
{_md_block("已完成前文：核心摘要+数据来源", block1)}

{_md_block("已完成前文：一、大盘综述", block2)}

## 本部分专属数据材料

{pig_kline_tables_block}

{pig_price_summary_block}

{pig_news_full_block}

{pig_charts_block}

---

{_QUALITY_RULES}

请撰写以下内容（只输出本部分章节）：
本部分篇幅不少于4000字符，分析必须有深度，不得简单罗列数据。

## {pig_spec.title}：[你的核心判断]

### 2.1 板块表现
（养猪股整体表现分析、个股数据表格、板块涨跌幅对比。分析完对应数据后，直接插入本部分可用图表素材中的养猪股走势和涨跌幅图。）

### 2.2 生猪价格与猪周期
（深度使用猪价/猪粮比数据，结合猪周期逻辑分析价格走势。分析完对应数据后，直接插入本部分可用图表素材中的累计涨跌幅/成交量图。）

### 2.3 饲料成本与猪粮比
（分析玉米、豆粕等饲料成本对养殖利润的影响。若猪价与猪粮比图有可用 URL，直接插入该 Markdown 图片引用。）

### 2.4 后续判断
（基于以上分析给出养猪板块后续走势判断）

**特别注意：**
- 必须深度使用猪价与猪粮比数据，结合猪周期逻辑
- 个股分析要有数据支撑，不要泛泛而谈
- 不要重复前文已出现的大盘数据"""

    block3 = await _call_llm_block(sdk, block3_prompt, "Block3-养猪行业")
    blocks.append(block3)

    # ===== Block 4: 三、恒生科技指数及核心公司 =====
    # 传 Block3 全文（衔接最紧）+ Block1&2 压缩摘要。
    ctx_b1_b2 = _truncate_ctx(block1 + "\n\n" + block2, max_chars=2000)

    block4_prompt = f"""你是一位资深股市分析师，正在撰写 **{date_str}** 的每日股市总结报告。
这是报告的第 **4** 部分（共5部分），你负责撰写"{hk_spec.title}"章节。

## 前文摘要（核心摘要+大盘综述，已截断）
{_md_block("前文摘要（核心摘要+大盘综述，已压缩）", ctx_b1_b2)}

## 最近完成的部分（养猪行业行情，完整）
{_md_block("最近完成的部分：养猪行业行情", block3)}

## 本部分专属数据材料

{hk_kline_tables_block}

{hk_news_full_block}

{hk_company_evidence_block}

{hk_charts_block}

---

{_QUALITY_RULES}

请撰写以下内容（只输出本部分章节）：
本部分篇幅不少于5000字符，分析必须有深度，不得简单罗列数据。

## {hk_spec.title}：[你的核心判断]

### 3.1 恒生科技指数表现
（指数表现分析、核心公司涨跌幅表格。分析完对应数据后，直接插入本部分可用图表素材中的港股走势和涨跌幅图。）

{hk_company_subsections_prompt}

**特别注意：**
- 每个个股分析必须包含结构化的"投资亮点"和"风险提示"子段，但只能使用该公司自己的证据包。
- 如果某公司证据包只有K线、没有专属新闻/公告正文，则该公司的投资亮点/风险提示只能围绕价格趋势和技术表现；业务、业绩、回购、云业务、AI、电商、汽车等论据必须标 `[INFO_GAP]`，不得补常识。
- 公司小节不得引用其他公司或恒生科技指数材料作为该公司自身论据。
- 不要重复前文已出现的数据，而是引用和深化
- 结合K线数据趋势和新闻催化剂进行分析；无新闻催化剂时明确写“未检索到专属催化剂证据 [INFO_GAP]”"""

    block4 = await _call_llm_block(sdk, block4_prompt, "Block4-恒生科技")
    blocks.append(block4)

    # ===== Block 5: 四、综合投资建议 =====
    # 投资建议主要依赖养猪/恒生科技两块结论，故传 Block3/4 全文，Block1 只留摘要、
    # Block2 大盘不再带入。
    ctx_b1 = _truncate_ctx(block1, max_chars=1200)

    block5_prompt = f"""你是一位资深股市分析师，正在撰写 **{date_str}** 的每日股市总结报告。
这是报告的第 **5** 部分（共5部分，最后一部分），你负责撰写"{advice_spec.title}"章节。

## 前文摘要（核心摘要，已截断）
{_md_block("前文摘要（核心摘要，已压缩）", ctx_b1)}

## 最近完成的部分

{_md_block("二、养猪行业行情（完整）", block3)}

{_md_block("三、恒生科技指数及核心公司（完整）", block4)}

---

{_QUALITY_RULES}

请撰写以下内容（只输出本部分章节，无需额外材料，基于前文分析综合）：
本部分篇幅不少于4000字符，分析必须有深度，不得简单罗列数据。

## {advice_spec.title}：[你的核心判断]

### 4.1 养猪板块操作建议
- **短线**（1-5个交易日）：具体价位区间和操作方向
- **中线**（1-3个月）：核心逻辑和仓位建议
- **长线**（3个月以上）：基本面判断和配置建议

### 4.2 恒生科技操作建议
- **短线**（1-5个交易日）：具体价位区间和操作方向
- **中线**（1-3个月）：核心逻辑和仓位建议
- **长线**（3个月以上）：基本面判断和配置建议

### 4.3 风险提示
（具体风险，不泛泛而谈，结合前文分析指出关键风险点）

**特别注意：**
- 操作建议必须区分短线/中线/长线三个维度
- 短线建议需有具体价位区间
- 风险提示要具体，不泛泛而谈
- 不要重复前文数据，而是基于前文结论给出操作建议
- 本部分无需图表。"""

    block5 = await _call_llm_block(sdk, block5_prompt, "Block5-投资建议")
    blocks.append(block5)

    # ===== 合并所有block =====
    full_report = "\n\n---\n\n".join(blocks)
    print(f"[Block] 5个block全部完成，总字符数: {len(full_report)}")
    return full_report


# ============================================================
# Step 5: 生成图表（拆分为7个独立函数 + 调度器）
# ============================================================

def chart1_pig_trend(pig_kline: dict, assets_dir: str) -> Optional[str]:
    """图1：养猪股近30日股价走势对比（指数化，起点=100）。"""
    fig, ax = plt.subplots(figsize=(12, 6))
    for name, rows in pig_kline.items():
        if not rows:
            continue
        dates, values = _build_kline_series(rows)
        if not values or values[0] == 0:
            continue
        indexed = [v / values[0] * 100 for v in values]
        color = COLORS.get(name, "#333333")
        ax.plot(range(len(dates)), indexed, label=name, color=color, linewidth=2)
    ax.set_title("养猪股近30日股价走势（指数化，起点=100）", fontsize=14)
    ax.set_xlabel("交易日", fontsize=11)
    ax.set_ylabel("指数", fontsize=11)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    _format_date_axis_indexed(ax, pig_kline)
    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart1_pig_trend.png"))


def chart2_hk_tech_trend(hk_kline: dict, assets_dir: str) -> Optional[str]:
    """图2：恒生科技核心公司近30日股价走势（指数化）。"""
    fig, ax = plt.subplots(figsize=(12, 6))
    hk_stocks_only = {n: r for n, r in hk_kline.items() if n != "恒生科技"}
    for name, rows in hk_stocks_only.items():
        if not rows:
            continue
        dates, values = _build_kline_series(rows)
        if not values or values[0] == 0:
            continue
        indexed = [v / values[0] * 100 for v in values]
        color = COLORS.get(name, "#333333")
        ax.plot(range(len(dates)), indexed, label=name, color=color, linewidth=2)
    if "恒生科技" in hk_kline and hk_kline["恒生科技"]:
        dates, values = _build_kline_series(hk_kline["恒生科技"])
        if values and values[0] != 0:
            indexed = [v / values[0] * 100 for v in values]
            ax.plot(range(len(dates)), indexed, label="恒生科技指数", color=COLORS["恒生科技"],
                    linewidth=2, linestyle="--")
    ax.set_title("恒生科技核心公司近30日股价走势（指数化，起点=100）", fontsize=14)
    ax.set_xlabel("交易日", fontsize=11)
    ax.set_ylabel("指数", fontsize=11)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    _format_date_axis_indexed(ax, hk_kline)
    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart2_hk_tech_trend.png"))


def chart3_pig_change(pig_kline: dict, assets_dir: str) -> Optional[str]:
    """图3：养猪股今日涨幅对比（水平柱状图）。"""
    fig, ax = plt.subplots(figsize=(10, 5))
    names, changes = [], []
    for name, rows in pig_kline.items():
        sorted_rows = sorted(rows, key=lambda r: r.get("date", ""), reverse=True)
        if len(sorted_rows) >= 2:
            cur = _safe_float(sorted_rows[0].get("last", 0))
            prev = _safe_float(sorted_rows[1].get("last", 0))
            if prev > 0:
                chg = (cur - prev) / prev * 100
                names.append(name)
                changes.append(chg)
    if not names:
        plt.close(fig)
        return None
    colors = [COLORS.get(n, "#333333") for n in names]
    bars = ax.barh(names, changes, color=colors, height=0.5)
    ax.set_title("养猪股今日涨跌幅对比", fontsize=14)
    ax.set_xlabel("涨跌幅 (%)", fontsize=11)
    ax.axvline(x=0, color="gray", linewidth=0.8)
    for bar, chg in zip(bars, changes):
        ax.text(bar.get_width() + 0.1 if chg >= 0 else bar.get_width() - 0.1,
                bar.get_y() + bar.get_height() / 2,
                f"{chg:+.2f}%", va="center",
                ha="left" if chg >= 0 else "right", fontsize=10)
    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart3_pig_change.png"))


def chart4_hk_change(hk_kline: dict, assets_dir: str) -> Optional[str]:
    """图4：恒生科技今日涨幅对比（水平柱状图）。"""
    fig, ax = plt.subplots(figsize=(10, 5))
    names, changes = [], []
    for name, rows in hk_kline.items():
        sorted_rows = sorted(rows, key=lambda r: r.get("date", ""), reverse=True)
        if len(sorted_rows) >= 2:
            cur = _safe_float(sorted_rows[0].get("last", 0))
            prev = _safe_float(sorted_rows[1].get("last", 0))
            if prev > 0:
                chg = (cur - prev) / prev * 100
                names.append(name)
                changes.append(chg)
    if not names:
        plt.close(fig)
        return None
    colors = [COLORS.get(n, "#333333") for n in names]
    bars = ax.barh(names, changes, color=colors, height=0.5)
    ax.set_title("恒生科技今日涨跌幅对比", fontsize=14)
    ax.set_xlabel("涨跌幅 (%)", fontsize=11)
    ax.axvline(x=0, color="gray", linewidth=0.8)
    for bar, chg in zip(bars, changes):
        ax.text(bar.get_width() + 0.1 if chg >= 0 else bar.get_width() - 0.1,
                bar.get_y() + bar.get_height() / 2,
                f"{chg:+.2f}%", va="center",
                ha="left" if chg >= 0 else "right", fontsize=10)
    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart4_hk_change.png"))


def chart5_index_trend(index_kline: dict, assets_dir: str) -> Optional[str]:
    """图5：A股主要指数近 N 日走势。"""
    fig, ax = plt.subplots(figsize=(12, 6))
    for name, rows in index_kline.items():
        if not rows:
            continue
        dates, values = _build_kline_series(rows)
        color = COLORS.get(name, "#333333")
        ax.plot(dates, values, label=name, color=color, linewidth=2, marker="o", markersize=4)
    ax.set_title("A股主要指数走势", fontsize=14)
    ax.set_xlabel("日期", fontsize=11)
    ax.set_ylabel("点位", fontsize=11)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis="x", rotation=30, labelsize=9)
    fig.autofmt_xdate()
    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart5_index_trend.png"))


def chart6_pig_volume(pig_kline: dict, assets_dir: str) -> Optional[str]:
    """图6：养猪股累计涨跌幅与成交量。"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    for name, rows in pig_kline.items():
        if len(rows) < 2:
            continue
        dates, values = _build_kline_series(rows)
        if not values or values[0] == 0:
            continue
        chg_series = []
        for i in range(len(values)):
            if i == 0:
                chg_series.append(0)
            else:
                chg_series.append((values[i] - values[0]) / values[0] * 100)
        ax1.plot(range(len(dates)), chg_series, label=name, color=COLORS.get(name, "#333"), linewidth=2)
    ax1.set_title("养猪股近30日累计涨跌幅", fontsize=13)
    ax1.set_xlabel("交易日", fontsize=10)
    ax1.set_ylabel("累计涨跌幅 (%)", fontsize=10)
    ax1.legend(fontsize=9)
    ax1.grid(True, alpha=0.3)
    ax1.axhline(y=0, color="gray", linewidth=0.8)

    for name, rows in pig_kline.items():
        if not rows:
            continue
        dates, volumes = _build_kline_series(rows, "volume")
        color = COLORS.get(name, "#333333")
        ax2.plot(range(len(dates)), [v / 1e6 for v in volumes], label=name, color=color, linewidth=1.5, alpha=0.7)
    ax2.set_title("养猪股近30日成交量（百万股）", fontsize=13)
    ax2.set_xlabel("交易日", fontsize=10)
    ax2.set_ylabel("成交量（百万股）", fontsize=10)
    ax2.legend(fontsize=9)
    ax2.grid(True, alpha=0.3)

    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart6_pig_volume.png"))


def chart7_pig_price_ratio(pig_price_data: list, assets_dir: str) -> Optional[str]:
    """图7：猪价与猪粮比（双Y轴折线图）。"""
    if not pig_price_data:
        print("[图表] 图7跳过：未获取到猪价数据")
        return None

    sorted_data = sorted(pig_price_data, key=lambda dp: dp.date)
    dates = [dp.date for dp in sorted_data]
    pig_prices = [dp.pig_price for dp in sorted_data]
    pig_grain_ratios = [dp.pig_grain_ratio for dp in sorted_data]

    has_pig_price = any(p is not None for p in pig_prices)
    has_ratio = any(r is not None for r in pig_grain_ratios)

    if not has_pig_price and not has_ratio:
        print("[图表] 图7跳过：猪价数据点无有效数值")
        return None

    fig, ax1 = plt.subplots(figsize=(12, 6))

    if has_pig_price:
        valid_dates_p = [d for d, p in zip(dates, pig_prices) if p is not None]
        valid_prices = [p for p in pig_prices if p is not None]
        ax1.plot(valid_dates_p, valid_prices, "o-", color="#D62728", linewidth=2.5,
                 markersize=6, label="生猪均价（元/公斤）", zorder=3)
    ax1.set_xlabel("日期", fontsize=11)
    ax1.set_ylabel("生猪均价（元/公斤）", fontsize=11, color="#D62728")
    ax1.tick_params(axis="y", labelcolor="#D62728")
    ax1.grid(True, alpha=0.3)

    if has_ratio:
        ax2 = ax1.twinx()
        valid_dates_r = [d for d, r in zip(dates, pig_grain_ratios) if r is not None]
        valid_ratios = [r for r in pig_grain_ratios if r is not None]
        ax2.plot(valid_dates_r, valid_ratios, "s--", color="#1F77B4", linewidth=2,
                 markersize=6, label="猪粮比", zorder=2)
        ax2.set_ylabel("猪粮比", fontsize=11, color="#1F77B4")
        ax2.tick_params(axis="y", labelcolor="#1F77B4")
        ax2.axhline(y=6.0, color="#1F77B4", linewidth=1, linestyle=":", alpha=0.5, label="盈亏平衡线(6:1)")

    lines1, labels1 = ax1.get_legend_handles_labels()
    if has_ratio:
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", fontsize=9)
    else:
        ax1.legend(loc="upper left", fontsize=9)

    ax1.set_title("猪价与猪粮比", fontsize=14)
    fig.autofmt_xdate()
    fig.tight_layout()
    return _save_chart(fig, os.path.join(assets_dir, "chart7_pig_price_ratio.png"))


def generate_charts(
    pig_kline: dict[str, list[dict]],
    hk_kline: dict[str, list[dict]],
    index_kline: dict[str, list[dict]],
    pig_price_data: list[PigPriceDataPoint],
    output_dir: str,
    date_short: str,
) -> list[str]:
    """生成 7 张图表的调度器，返回文件路径列表。"""
    _ensure_cjk_font()
    assets_dir = os.path.join(output_dir, f"股市总结{date_short}_assets")
    os.makedirs(assets_dir, exist_ok=True)

    chart_funcs = [
        lambda: chart1_pig_trend(pig_kline, assets_dir),
        lambda: chart2_hk_tech_trend(hk_kline, assets_dir),
        lambda: chart3_pig_change(pig_kline, assets_dir),
        lambda: chart4_hk_change(hk_kline, assets_dir),
        lambda: chart5_index_trend(index_kline, assets_dir),
        lambda: chart6_pig_volume(pig_kline, assets_dir),
        lambda: chart7_pig_price_ratio(pig_price_data, assets_dir),
    ]

    chart_paths = []
    for i, func in enumerate(chart_funcs, 1):
        try:
            path = func()
            if path:
                chart_paths.append(path)
        except Exception as e:
            print(f"[图表] 图{i}生成失败: {e}")

    return chart_paths


# ============================================================
# Step 6: 上传图表
# ============================================================

async def upload_charts(sdk: CodeActSDK, chart_paths: list[str]) -> list[str]:
    """上传图表，返回与 chart_paths 等长的公网 URL 列表；失败位置保留空串。"""
    sem = asyncio.Semaphore(3)

    async def _upload_one(path: str) -> str:
        async with sem:
            try:
                resp = await sdk.call_tool(
                    "file_to_url",
                    {"file_path": path},
                    schema_version=TOOL_SCHEMA_VERSIONS["file_to_url"],
                )
                url = resp.get("url", "") if isinstance(resp, dict) else ""
                if url:
                    print(f"[上传] {path} -> {url}")
                return url
            except Exception as e:
                print(f"[上传异常] {path}: {e}")
                return ""

    tasks = [_upload_one(p) for p in chart_paths]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return [r if isinstance(r, str) else "" for r in results]


# ============================================================
# Step 7: 证据文件独立生成
# ============================================================

def save_evidence_file(
    date_str: str,
    kline_data: dict,
    news_data: list[dict],
    output_dir: str,
    date_short: str,
) -> str:
    """独立生成证据文件，返回文件路径。"""
    evidence_parts = [f"# 证据文件 - {date_str}\n"]
    evidence_parts.append("## 行情原始数据\n")
    for name, rows in kline_data.items():
        evidence_parts.append(f"### {name}\n")
        for r in _take(rows, 5):
            evidence_parts.append(
                f"- {r.get('date', '')} | O:{r.get('open', '')} H:{r.get('high', '')} "
                f"L:{r.get('low', '')} C:{r.get('last', '')}"
            )
    evidence_parts.append("\n## 新闻来源（按章节）\n")
    market_news, pig_news, hk_news = _categorize_news(news_data)
    for label, items in (
        (SECTION_LABELS.get("market", "大盘"), market_news),
        (SECTION_LABELS.get("pig", "养猪"), pig_news),
        (SECTION_LABELS.get("hk", "港股"), hk_news),
    ):
        evidence_parts.append(f"### {label}（{len(items)} 条）")
        if items:
            for item in items:
                evidence_parts.append(f"- [{item.get('title', '无标题')}]({item.get('url', '')})")
        else:
            evidence_parts.append("- 未收集到该章节新闻")
    evidence_parts.append("\n## 港股公司新闻来源（独立搜索分桶）\n")
    company_names = [name for name in DEFAULT_HK_CODES if name != "恒生科技"]
    company_news = _split_company_news(hk_news, company_names)
    for company in company_names:
        items = company_news.get(company, [])
        evidence_parts.append(f"### {company}（{len(items)} 条）")
        if items:
            for item in items:
                evidence_parts.append(f"- [{item.get('title', '无标题')}]({item.get('url', '')})")
        else:
            evidence_parts.append("- 未收集到该公司专属新闻")
    evidence_path = os.path.join(output_dir, f"股市总结{date_short}_evidence.md")
    with open(evidence_path, "w", encoding="utf-8") as f:
        f.write("\n".join(evidence_parts))
    print(f"[证据] 已保存: {evidence_path}")
    return evidence_path


# ============================================================
# Step 8: 组装最终报告
# ============================================================

def build_final_report(
    date_str: str,
    analysis_md: str,
    output_dir: str,
    date_short: str,
) -> str:
    """组装最终 Markdown 报告并保存；图表 Markdown 已由 LLM 直接写入正文。"""
    report = f"""# 股市总结{date_str}

**完成日期：** {date_str}
**报告类型：** 每日市场总结与操作建议

---

{analysis_md}

---

*本报告由自动化脚本生成，数据来源于 westockdata 行情接口和网络公开信息，仅供参考，不构成投资建议。*
"""
    report_path = os.path.join(output_dir, f"股市总结{date_short}_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"[报告] 已保存: {report_path}")

    return report_path


class DailyStockSummaryWorkflow:
    """Composable workflow blocks for the daily stock summary report."""

    def __init__(self, ctx: WorkflowContext):
        self.ctx = ctx

    async def _run_block(self, block: WorkflowBlock) -> Any:
        print(f"\n===== {block.name} =====")
        return await block.action()

    async def run(self) -> None:
        """串行编排四个工作流阶段，每阶段产物喂给下一阶段：
        行情快照 → 信息发现 → 报告产物 → 写状态并提交。
        行情是整篇报告的锚点，拿不到有效行情就早停走无数据分流，
        不再空跑后续搜索与 LLM，避免烧 token 产出空报告。
        """
        market = await self._run_block(WorkflowBlock("Block 1: 市场锚点与行情快照", self.collect_market_snapshot))
        if not market.all_kline or not kline_has_recent_data(market.all_kline, self.ctx.now):
            await self._run_block(
                WorkflowBlock("Block 1b: 无有效行情数据分流", lambda: self.submit_no_recent_market_data(market))
            )
            return

        signals = await self._run_block(
            WorkflowBlock("Block 2: 信息发现与内容获取", lambda: self.collect_signals(market))
        )
        artifacts = await self._run_block(
            WorkflowBlock("Block 3: 报告生成与产物渲染", lambda: self.build_report_artifacts(signals))
        )
        await self._run_block(
            WorkflowBlock("Block 4: 成功后写状态并提交结果", lambda: self.commit_and_submit(signals, artifacts))
        )

    async def collect_market_snapshot(self) -> MarketSnapshot:
        config = self.ctx.config
        all_stock_names = config.pig_stock_names + config.hk_stock_names
        stock_codes = await search_stock_codes(
            all_stock_names,
            cache=self.ctx.cache,
            now_iso=self.ctx.now_iso,
        )
        for name in config.pig_stock_names:
            if name not in stock_codes and name in DEFAULT_PIG_CODES:
                stock_codes[name] = DEFAULT_PIG_CODES[name]
        for name in config.hk_stock_names:
            if name not in stock_codes and name in DEFAULT_HK_CODES:
                stock_codes[name] = DEFAULT_HK_CODES[name]
        stock_codes["恒生科技"] = DEFAULT_HK_CODES["恒生科技"]
        print(f"[代码] {stock_codes}")

        pig_codes = {n: c for n, c in stock_codes.items() if n in config.pig_stock_names}
        hk_codes = {n: c for n, c in stock_codes.items() if n in config.hk_stock_names or n == "恒生科技"}
        index_codes = dict(DEFAULT_INDEX_CODES)

        pig_kline_task = get_kline_data(
            pig_codes,
            period="day",
            limit=config.kline_days,
            cache=self.ctx.cache,
            cache_date=self.ctx.date_short,
            now_iso=self.ctx.now_iso,
        )
        hk_kline_task = get_kline_data(
            hk_codes,
            period="day",
            limit=config.kline_days,
            cache=self.ctx.cache,
            cache_date=self.ctx.date_short,
            now_iso=self.ctx.now_iso,
        )
        index_kline_task = get_kline_data(
            index_codes,
            period="day",
            limit=config.kline_days,
            cache=self.ctx.cache,
            cache_date=self.ctx.date_short,
            now_iso=self.ctx.now_iso,
        )
        pig_kline, hk_kline, index_kline = await asyncio.gather(
            pig_kline_task, hk_kline_task, index_kline_task
        )
        print(f"[K线] 养猪股 {len(pig_kline)} 只，恒生科技 {len(hk_kline)} 只，指数 {len(index_kline)} 只")
        return MarketSnapshot(
            stock_codes=stock_codes,
            pig_kline=pig_kline,
            hk_kline=hk_kline,
            index_kline=index_kline,
        )

    async def collect_signals(self, market: MarketSnapshot) -> SignalBundle:
        section_plan = build_section_queries(self.ctx.year, self.ctx.month, self.ctx.day)
        # baseline 首跑用长回溯窗口补齐历史底料；之后切到短窗口只捞增量，
        # 既降低重复抓取，又靠 seen_sources 去重避免每天翻炒旧闻。
        baseline_mode = _is_baseline_run(self.ctx.state_conn)
        news_lookback_days = SEARCH_LOOKBACK_DAYS if baseline_mode else DAILY_SEARCH_LOOKBACK_DAYS
        pig_lookback_days = PIG_PRICE_LOOKBACK_DAYS if baseline_mode else DAILY_PIG_PRICE_LOOKBACK_DAYS
        print(
            f"[运行策略] baseline={baseline_mode}, "
            f"news_lookback_days={news_lookback_days}, pig_lookback_days={pig_lookback_days}"
        )
        news_task = search_and_fetch(
            self.ctx.sdk,
            section_plan,
            cache=self.ctx.cache,
            cache_date=self.ctx.date_short,
            now_iso=self.ctx.now_iso,
            lookback_days=news_lookback_days,
        )
        pig_price_task = search_pig_price_data(
            self.ctx.sdk,
            self.ctx.year,
            self.ctx.month,
            self.ctx.day,
            cache=self.ctx.cache,
            cache_date=self.ctx.date_short,
            now_iso=self.ctx.now_iso,
            lookback_days=pig_lookback_days,
        )
        news_result, pig_price_result = await asyncio.gather(
            news_task,
            pig_price_task,
            return_exceptions=True,
        )
        news_data = [] if isinstance(news_result, Exception) else news_result
        pig_price_data = [] if isinstance(pig_price_result, Exception) else pig_price_result
        print(f"[新闻] 共获取 {len(news_data)} 条新闻正文")
        print(f"[猪价] 提取到 {len(pig_price_data)} 个数据点")
        return SignalBundle(market=market, news_data=news_data, pig_price_data=pig_price_data)

    def _analysis_cache_key(
        self,
        signals: SignalBundle,
        chart_refs: list[ChartReference],
    ) -> str:
        payload = {
            "versions": {
                "script": SCRIPT_VERSION,
                "cache_schema": CACHE_SCHEMA_VERSION,
                "tools": TOOL_SCHEMA_VERSIONS,
                "candidate_prompt": CANDIDATE_SELECTION_PROMPT_VERSION,
                "report_prompt": REPORT_PROMPT_VERSION,
            },
            "date": self.ctx.date_short,
            "pig_kline": signals.market.pig_kline,
            "hk_kline": signals.market.hk_kline,
            "index_kline": signals.market.index_kline,
            "news_sources": [
                {
                    "url": item.get("url", ""),
                    "sections": _candidate_sections(item),
                    "companies": _candidate_companies(item),
                    "source_tier": item.get("source_tier", "unverified"),
                    "source_assessment": item.get("source_assessment", ""),
                }
                for item in signals.news_data
            ],
            "pig_price_data": [p.dict() for p in signals.pig_price_data],
            "charts": [
                {
                    "chart_id": ref.chart_id,
                    "title": ref.title,
                    "url": ref.url,
                }
                for ref in chart_refs
            ],
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        return _versioned_cache_key("analysis_md", raw)

    async def build_report_artifacts(self, signals: SignalBundle) -> ReportArtifacts:
        evidence_path = save_evidence_file(
            self.ctx.date_str,
            signals.market.all_kline,
            signals.news_data,
            self.ctx.config.output_dir,
            self.ctx.date_short,
        )

        chart_paths = generate_charts(
            signals.market.pig_kline,
            signals.market.hk_kline,
            signals.market.index_kline,
            signals.pig_price_data,
            self.ctx.config.output_dir,
            self.ctx.date_short,
        )
        print(f"[图表] 生成 {len(chart_paths)} 张图表")
        chart_urls = await upload_charts(self.ctx.sdk, chart_paths)
        uploaded_chart_count = sum(1 for url in chart_urls if url)
        chart_refs = build_chart_references(chart_paths, chart_urls)

        analysis_cache_key = self._analysis_cache_key(signals, chart_refs)
        cached_analysis = self.ctx.cache.get("analysis_md", analysis_cache_key, self.ctx.date_short)
        if isinstance(cached_analysis, str) and cached_analysis:
            print("[缓存] LLM综合分析命中")
            analysis_md = cached_analysis
        else:
            analysis_md = await llm_analysis_block_wise(
                self.ctx.sdk,
                signals.market.pig_kline,
                signals.market.hk_kline,
                signals.market.index_kline,
                signals.news_data,
                chart_refs,
                signals.pig_price_data,
                self.ctx.date_str,
            )
            self.ctx.cache.put(
                "analysis_md",
                analysis_cache_key,
                self.ctx.date_short,
                analysis_md,
                self.ctx.now_iso,
            )
        print(f"[分析] 生成报告文本 {len(analysis_md)} 字符")

        report_path = build_final_report(
            self.ctx.date_str,
            analysis_md,
            self.ctx.config.output_dir,
            self.ctx.date_short,
        )
        return ReportArtifacts(
            evidence_path=evidence_path,
            report_path=report_path,
            chart_paths=chart_paths,
            chart_urls=chart_urls,
            uploaded_chart_count=uploaded_chart_count,
        )

    async def commit_and_submit(
        self,
        signals: SignalBundle,
        artifacts: ReportArtifacts,
    ) -> None:
        write_state_after_success(
            self.ctx.state_conn,
            self.ctx.date_str,
            artifacts.report_path,
            signals.news_data,
            self.ctx.now_iso,
        )

        abs_report_path = os.path.abspath(artifacts.report_path)
        abs_evidence_path = os.path.abspath(artifacts.evidence_path)
        summary_message = (
            f"股市总结{self.ctx.date_str}已生成\n\n"
            f"完整报告：[股市总结{self.ctx.date_short}_report.md](computer://{abs_report_path})\n\n"
            f"证据文件：[股市总结{self.ctx.date_short}_evidence.md](computer://{abs_evidence_path})\n\n"
            f"数据覆盖：养猪板块({len(signals.market.pig_kline)}只)、"
            f"恒生科技({len(signals.market.hk_kline)}只)、A股指数({len(signals.market.index_kline)}只)\n"
            f"图表：生成{len(artifacts.chart_paths)}张，上传成功{artifacts.uploaded_chart_count}张 | "
            f"新闻源：{len(signals.news_data)}条 | "
            f"猪价数据点：{len(signals.pig_price_data)}个"
        )

        # auto 归一化：成功产出报告时视为可直接展示（display_only），不打扰主人；
        # 真正需要 @主人 的错误路径走 notify，见 main() 的异常兜底。
        actual_mode = "display_only" if self.ctx.config.result_mode == "auto" else self.ctx.config.result_mode
        await self.ctx.sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message=summary_message,
            data={
                "report_path": artifacts.report_path,
                "evidence_path": artifacts.evidence_path,
                "chart_count": len(artifacts.chart_paths),
                "uploaded_chart_count": artifacts.uploaded_chart_count,
                "news_count": len(signals.news_data),
                "kline_stocks": len(signals.market.all_kline),
                "pig_price_data_points": len(signals.pig_price_data),
                "state_db_path": os.path.join(self.ctx.config.output_dir, STATE_DB_NAME),
            },
        )

    async def submit_no_recent_market_data(self, market: MarketSnapshot) -> None:
        # 非交易日/接口异常等无有效行情场景：这属于“正常无内容”，不是错误。
        # auto 下归一到 no_reply（静默不打扰），仍以 status=success 提交满足契约。
        no_data_message = (
            f"股市总结{self.ctx.date_str}未生成：未获取到足够新的行情数据。"
            f"已跳过新闻抽取、LLM报告和图表生成。"
        )
        actual_mode = "no_reply" if self.ctx.config.result_mode == "auto" else self.ctx.config.result_mode
        await self.ctx.sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message="NO_REPLY" if actual_mode == "no_reply" else no_data_message,
            data={"reason": "no_recent_kline", "kline_stocks": len(market.all_kline)},
        )


# ============================================================
# 主函数
# ============================================================

async def main():
    config = StockSummaryConfig.from_argv(sys.argv)
    print(
        f"[参数] result_mode={config.result_mode}, "
        f"pig_stocks={config.pig_stock_names}, hk_stocks={config.hk_stock_names}, "
        f"kline_days={config.kline_days}"
    )
    ctx = WorkflowContext.create(config)
    try:
        await DailyStockSummaryWorkflow(ctx).run()
    except Exception as e:
        # CodeAct 契约：任何未捕获异常也必须落一次 submit_result，且错误走 notify
        # 提醒主人，绝不能静默退出让调度端拿不到结果。
        print(f"[错误] {type(e).__name__}: {e}")
        await ctx.sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"每日股市总结脚本执行失败: {e}",
            data={"error_type": type(e).__name__},
        )
    finally:
        # 状态库连接无论成功失败都要关闭，避免 SQLite 文件锁残留影响下次运行。
        ctx.state_conn.close()


if __name__ == "__main__":
    asyncio.run(main())
