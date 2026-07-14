#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""通用天气查询 CodeAct 脚本（展示网页抓取、结构化抽取、兜底链路）。

这个示例用于凸显 codeact-script-writer 中「联网数据查询」的推荐实现：
- 工具调用只使用 CodeAct SDK 侧工具，并显式传入实际 schema_version；
- 主数据源优先走确定 URL：codeact_fetch_web 获取 weather.com.cn 页面；
- 页面正文交给 LLM + ResponseFormat 结构化抽取，避免脆弱的 HTML 选择器解析；
- 主源失败后进入搜索兜底：search → LLM 筛选候选 URL → fetch → 结构化抽取；
- 查询参数从 codeact_args 读取，auto 归一化为 display_only；
- 所有成功、失败和提前返回路径都通过 submit_result 交付。

参数（codeact_args）：result_mode, city, days, city_code
- result_mode: display_only / notify / no_reply / auto（auto 按 display_only 处理）
- city: 城市名，支持中文，默认「北京」
- days: 预报天数 0-7（0=仅今日），默认 0
- city_code: 中国天气网城市代码，默认 101010100（北京）
"""

import asyncio
import json
import re
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from codeact_sdk import CodeActSDK

# ===== SDK 工具版本占位符（skill 模板版）=====
# 安装时由 CodeAct agent 用 get_codeact_tool_schemas 取真实版本号替换下列占位值。
TOOL_SCHEMA_VERSIONS = {
    "codeact_fetch_web": "__FILL_FETCH_WEB_VERSION__",
    "codeact_search_web": "__FILL_SEARCH_WEB_VERSION__",
}

# ===== 中国天气网主要城市代码映射 =====
CITY_CODE_MAP: Dict[str, str] = {
    "北京": "101010100", "beijing": "101010100",
    "上海": "101020100", "shanghai": "101020100",
    "广州": "101280101", "guangzhou": "101280101",
    "深圳": "101280601", "shenzhen": "101280601",
    "天津": "101030100", "tianjin": "101030100",
    "重庆": "101040100", "chongqing": "101040100",
    "成都": "101270101", "chengdu": "101270101",
    "杭州": "101210101", "hangzhou": "101210101",
    "武汉": "101200101", "wuhan": "101200101",
    "南京": "101190101", "nanjing": "101190101",
    "西安": "101110101", "xian": "101110101",
    "长沙": "101250101", "changsha": "101250101",
    "沈阳": "101070101", "shenyang": "101070101",
    "哈尔滨": "101050101", "haerbin": "101050101",
    "大连": "101070201", "dalian": "101070201",
    "青岛": "101120201", "qingdao": "101120201",
    "郑州": "101180101", "zhengzhou": "101180101",
    "济南": "101120101", "jinan": "101120101",
    "福州": "101230101", "fuzhou": "101230101",
    "厦门": "101230201", "xiamen": "101230201",
    "昆明": "101290101", "kunming": "101290101",
    "贵阳": "101260101", "guiyang": "101260101",
    "南宁": "101300101", "nanning": "101300101",
    "海口": "101310101", "haikou": "101310101",
    "三亚": "101310201", "sanya": "101310201",
    "合肥": "101220101", "hefei": "101220101",
    "南昌": "101240101", "nanchang": "101240101",
    "太原": "101100101", "taiyuan": "101100101",
    "石家庄": "101090101", "shijiazhuang": "101090101",
    "兰州": "101160101", "lanzhou": "101160101",
    "西宁": "101150101", "xining": "101150101",
    "银川": "101170101", "yinchuan": "101170101",
    "呼和浩特": "101080101", "huhehaote": "101080101",
    "乌鲁木齐": "101130101", "wulumuqi": "101130101",
    "拉萨": "101140101", "lasa": "101140101",
    "苏州": "101190401", "suzhou": "101190401",
    "无锡": "101190201", "wuxi": "101190201",
    "宁波": "101210401", "ningbo": "101210401",
    "东莞": "101281601", "dongguan": "101281601",
    "佛山": "101280800", "foshan": "101280800",
    "珠海": "101280701", "zhuhai": "101280701",
    "烟台": "101120501", "yantai": "101120501",
    "温州": "101210701", "wenzhou": "101210701",
    "常州": "101191101", "changzhou": "101191101",
    "徐州": "101190301", "xuzhou": "101190301",
    "扬州": "101190701", "yangzhou": "101190701",
    "中山": "101281701", "zhongshan": "101281701",
    "惠州": "101280301", "huizhou": "101280301",
    "泉州": "101230501", "quanzhou": "101230501",
}

# ===== WMO 天气代码 → 中文（仅搜索兜底可能用到）=====
WMO = {
    0: "晴", 1: "大部晴", 2: "局部多云", 3: "阴", 45: "雾", 48: "冻雾",
    51: "小毛毛雨", 53: "毛毛雨", 55: "较强毛毛雨", 56: "冻毛毛雨", 57: "强冻毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨", 66: "冻雨", 67: "强冻雨",
    71: "小雪", 73: "中雪", 75: "大雪", 77: "雪粒",
    80: "小阵雨", 81: "阵雨", 82: "强阵雨", 85: "小阵雪", 86: "强阵雪",
    95: "雷暴", 96: "雷暴伴冰雹", 99: "强雷暴伴冰雹",
}


class WeatherError(Exception):
    pass


# ===== ResponseFormat 结构化模型：让 LLM 输出可直接校验和渲染的天气字段 =====

class DayForecast(BaseModel):
    """单日天气预报"""
    date: str = Field(description="日期，如'24日（今天）'")
    condition: str = Field(description="天气状况，如'多云转雷阵雨'")
    high_temp: Optional[str] = Field(default=None, description="最高温度，如'29'")
    low_temp: Optional[str] = Field(default=None, description="最低温度，如'21'")
    wind: Optional[str] = Field(default=None, description="风力风向，如'<3级'或'南风 3级'")


class LifeIndex(BaseModel):
    """生活指数"""
    name: str = Field(description="指数名称，如'穿衣指数'")
    level: str = Field(description="指数等级，如'热'")
    advice: str = Field(default="", description="建议，如'适合穿T恤、短薄外套等夏季服装'")


class HourlyDetail(BaseModel):
    """分时段预报"""
    time: str = Field(description="时间，如'11时'")
    temp: Optional[str] = Field(default=None, description="温度")
    wind: Optional[str] = Field(default=None, description="风向")
    wind_level: Optional[str] = Field(default=None, description="风力等级")


class WeatherExtract(BaseModel):
    """从中国天气网页面抽取的结构化天气数据"""
    ok: bool = Field(default=False, description="是否成功抽取到有效天气数据")
    location: Optional[str] = Field(default=None, description="地点，如'北京 > 城区'")
    update_time: Optional[str] = Field(default=None, description="更新时间，如'11:30更新'")
    data_source: Optional[str] = Field(default=None, description="数据来源，如'中央气象台'")
    days: List[DayForecast] = Field(default_factory=list, description="逐日天气预报列表")
    life_indices: List[LifeIndex] = Field(default_factory=list, description="今日生活指数列表")
    hourly: List[HourlyDetail] = Field(default_factory=list, description="今日分时段预报")
    advice: List[str] = Field(default_factory=list, description="出行建议（基于天气自动生成）")


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
    raise WeatherError(f"LLM 返回格式不可用，期望 {model_cls.__name__}")


def parse_days(raw: str) -> int:
    m = re.search(r"-?\d+", "" if raw is None else str(raw))
    return max(0, min(7, int(m.group(0)))) if m else 0


def norm_mode(raw: str) -> str:
    mode = (raw or "display_only").strip().lower()
    if mode == "auto":
        return "display_only"
    return mode if mode in {"display_only", "notify", "no_reply"} else "display_only"


def resolve_city_code(city: str, explicit_code: str) -> str:
    """根据城市名解析中国天气网城市代码"""
    if explicit_code and explicit_code.strip():
        return explicit_code.strip()
    key = city.strip().lower()
    return CITY_CODE_MAP.get(key, CITY_CODE_MAP.get(city.strip(), ""))


def _norm_url(url: str) -> str:
    return (url or "").split("#", 1)[0].split("?", 1)[0].rstrip("/")


def _strip_unit(v: Optional[str]) -> Optional[str]:
    """去除温度值中可能已包含的℃单位，避免重复拼接"""
    if v is None:
        return None
    return re.sub(r"[℃°C c]+$", "", str(v).strip()) or None

def split_extract_chunks(content: str, limit: int) -> List[str]:
    """压缩正文并分块覆盖全文，避免基于前段截断内容做最终判断。"""
    text = re.sub(r"\s+", " ", content or "").strip()
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


def merge_weather_extracts(parts: List[WeatherExtract]) -> WeatherExtract:
    """合并多块抽取结果；短文只有一个结果时等价于直接返回。"""
    merged = WeatherExtract(ok=False)
    seen_days, seen_indices, seen_hourly, seen_advice = set(), set(), set(), set()
    for part in parts:
        if not part.ok:
            continue
        merged.ok = True
        merged.location = merged.location or part.location
        merged.update_time = merged.update_time or part.update_time
        merged.data_source = merged.data_source or part.data_source
        for day in part.days:
            key = day.date
            if key and key not in seen_days:
                seen_days.add(key)
                merged.days.append(day)
        for idx in part.life_indices:
            key = idx.name
            if key and key not in seen_indices:
                seen_indices.add(key)
                merged.life_indices.append(idx)
        for item in part.hourly:
            key = item.time
            if key and key not in seen_hourly:
                seen_hourly.add(key)
                merged.hourly.append(item)
        for advice in part.advice:
            if advice and advice not in seen_advice:
                seen_advice.add(advice)
                merged.advice.append(advice)
    merged.ok = bool(merged.days)
    return merged




async def fetch_weather_com_cn(sdk: CodeActSDK, city: str, city_code: str, days: int) -> Dict[str, Any]:
    """主数据源：fetch 中国天气网确定 URL，再用 ResponseFormat 抽取天气字段。"""
    url = f"https://www.weather.com.cn/weather/{city_code}.shtml"
    print(f"[主数据源] 正在获取中国天气网: {url}")

    page = await sdk.call_tool(
        "codeact_fetch_web",
        {"url": url},
        schema_version=TOOL_SCHEMA_VERSIONS["codeact_fetch_web"],
    )

    if page.get("is_success") is False:
        raise WeatherError(f"中国天气网页面获取失败: {page.get('error', '未知错误')}")

    content = str(page.get("content") or "").strip()
    title = str(page.get("title") or "")

    if not content:
        raise WeatherError("中国天气网页面正文为空")

    print(f"[主数据源] 页面获取成功，正文长度={len(content)}")

    # 使用统一页面抽取管线：短页单次抽取，长页分块覆盖全文后合并。
    extract = await extract_weather_from_page(sdk, city, days, title, content, "中国天气网确定 URL")

    if not extract.ok or not extract.days:
        raise WeatherError("中国天气网页面结构化抽取失败，未获取到有效天气数据")

    print(f"[主数据源] 结构化抽取成功，共 {len(extract.days)} 天数据")
    return {
        "source": "中国天气网",
        "extract": extract,
    }


async def extract_weather_from_page(
    sdk: CodeActSDK, city: str, days: int, title: str, content: str, source_hint: str
) -> WeatherExtract:
    """统一页面抽取管线：主源和搜索兜底都走这里，避免两套近似抽取逻辑。"""
    chunks = split_extract_chunks(content, 15000)
    if not chunks:
        return WeatherExtract(ok=False)

    sem = asyncio.Semaphore(3)

    async def extract_chunk(idx: int, text: str) -> WeatherExtract:
        async with sem:
            prompt = (
                f"从以下网页正文分块中提取「{city}」的天气信息。"
                f"需要提取今日及未来{days}天的逐日天气预报（最多7天），以及今日的生活指数和分时段预报。\n\n"
                f"来源类型：{source_hint}\n"
                f"页面标题：{title}\n"
                f"分块：{idx + 1}/{len(chunks)}\n\n"
                f"页面正文分块：\n{text}\n\n"
                "提取要求：\n"
                "1. 只基于当前分块中明确出现的信息抽取，不要根据标题、搜索摘要或常识补全\n"
                "2. date 字段保留原文格式，如'24日（今天）'、'25日（明天）'\n"
                "3. condition 天气状况保留原文，如'多云转雷阵雨'\n"
                "4. high_temp / low_temp 只填数字，如'29'、'21'\n"
                "5. wind 保留原文，如'<3级'或'南风 <3级'\n"
                "6. 如果当前分块无法提取有效天气数据，设 ok=false\n"
            )
            result = await sdk.call_llm(
                messages=[{"role": "user", "content": prompt}],
                response_format=WeatherExtract,
            )
            return _coerce_model(result, WeatherExtract)

    parts = await asyncio.gather(*(extract_chunk(i, chunk) for i, chunk in enumerate(chunks)))
    return merge_weather_extracts(parts)


# ===== 兜底数据源：搜索 + fetch + LLM 抽取 =====

async def fetch_search_fallback(sdk: CodeActSDK, city: str, days: int) -> Dict[str, Any]:
    """搜索兜底：search 扩展候选 → LLM 选 URL → fetch 正文 → LLM 抽取。"""
    print(f"[搜索兜底] 正在搜索 {city} 天气...")

    search = await sdk.call_tool(
        "codeact_search_web",
        {"query": f"{city} 天气预报 今天 未来{days}天 温度 风力", "response_length": "long"},
        schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
    )

    if not search or search.get("is_success") is False:
        raise WeatherError("联网搜索失败")

    # 收集候选：先做 URL 归一和去重，避免重复 fetch 同一页面。
    candidates = []
    seen_urls = set()
    for item in search.get("results") or []:
        url = _norm_url(str(item.get("url") or ""))
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        candidates.append(dict(item, url=url))

    if not candidates:
        raise WeatherError("搜索结果为空")

    print(f"[搜索兜底] 搜索返回 {len(candidates)} 条候选")

    # LLM 筛选候选 URL：把“哪个页面最适合抓取”的判断结构化。
    selected = await _select_search_candidates(sdk, candidates, city, days)

    # 对筛选后的候选逐一 fetch 并抽取；第一个成功抽取的页面即作为兜底结果。
    sem = asyncio.Semaphore(2)
    for item in selected[:5]:
        url = item.get("url")
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

            extract = await extract_weather_from_page(
                sdk, city, days,
                str(page.get("title") or item.get("title") or ""),
                str(page.get("content") or ""),
                "搜索兜底抓页",
            )
            if extract.ok and extract.days:
                print(f"[搜索兜底] 从 {url} 抽取成功")
                return {"source": "搜索抓页", "extract": extract}
        except Exception as e:
            print(f"[搜索兜底] 跳过 {url}: {e}")
            continue

    raise WeatherError("搜索兜底未获得有效天气数据")


async def _select_search_candidates(
    sdk: CodeActSDK, candidates: List[Dict[str, Any]], city: str, days: int
) -> List[Dict[str, Any]]:
    """LLM 筛选搜索候选 URL；失败时退回规则排序，保证兜底链路继续推进。"""
    if not candidates:
        return []

    lines = []
    for item in candidates[:15]:
        snippet = re.sub(r"\s+", " ", str(item.get("snippet") or "")).strip()[:220]
        lines.append(
            f"标题：{item.get('title') or ''}\n"
            f"URL：{item.get('url') or ''}\n"
            f"摘要：{snippet}"
        )

    prompt = (
        f"从以下搜索结果中选择需要 fetch 正文核验的完整 URL，用于提取「{city}」今天"
        f"及未来 {days} 天天气。只返回 URL，不返回序号；"
        "优先官方/权威/实时天气页，排除旧新闻和无关页面。\n\n"
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

async def get_weather(sdk: CodeActSDK, city: str, days: int, city_code: str) -> Dict[str, Any]:
    """两层降级：中国天气网确定 URL 失败后，继续搜索兜底。"""
    # 第一层：中国天气网 fetch
    if city_code:
        try:
            data = await fetch_weather_com_cn(sdk, city, city_code, days)
            print("[数据源] 中国天气网 成功")
            return data
        except Exception as e:
            print(f"[数据源] 中国天气网 失败: {e}")

    # 第二层：搜索兜底
    try:
        data = await fetch_search_fallback(sdk, city, days)
        print("[数据源] 搜索兜底 成功")
        return data
    except Exception as e:
        print(f"[数据源] 搜索兜底 失败: {e}")

    raise WeatherError("所有数据源均失败，无法获取天气数据")


# ===== 出行建议生成 =====

def build_advice_from_extract(extract: WeatherExtract) -> List[str]:
    """基于结构化抽取字段生成出行建议，避免再次调用 LLM。"""
    if extract.advice:
        return extract.advice[:3]

    today = extract.days[0] if extract.days else None
    if not today:
        return ["请关注实时天气变化，合理安排出行。"]

    cond = today.condition or ""
    advice: List[str] = []

    if any(word in cond for word in ["雨", "雷", "阵雨"]):
        advice.append("带好雨具，雷雨时注意安全。")
    if any(word in cond for word in ["雪", "冰"]):
        advice.append("注意保暖防滑，道路可能结冰。")
    if today.high_temp and int(re.sub(r"[^\d]", "", today.high_temp) or "0") >= 35:
        advice.append("气温较高，注意防暑补水。")
    if today.low_temp and int(re.sub(r"[^\d]", "", today.low_temp) or "99") <= 0:
        advice.append("气温偏低，注意保暖。")

    # 检查生活指数
    for idx in extract.life_indices:
        if idx.name == "紫外线指数" and any(w in idx.level for w in ["很强", "强"]):
            advice.append("紫外线较强，注意防晒。")
        if idx.name == "洗车指数" and "不宜" in idx.level:
            advice.append("不宜洗车，有雨或路况较差。")

    return advice[:3] or ["天气总体平稳，按日常安排出行。"]


# ===== 消息构建 =====

def build_summary(data: Dict[str, Any], city: str, days: int) -> str:
    """构建面向用户的天气简报：先结论后细节，再补充预报、指数和建议。"""
    return _build_weather_com_cn_summary(data, city, days)


def _format_day_line(d: DayForecast) -> str:
    """格式化单日预报为一行"""
    parts = [d.date]
    if d.condition:
        parts.append(d.condition)
    dh = _strip_unit(d.high_temp)
    dl = _strip_unit(d.low_temp)
    if dh and dl:
        parts.append(f"{dl}~{dh}℃")
    elif dh:
        parts.append(f"最高{dh}℃")
    if d.wind:
        parts.append(d.wind)
    return "，".join(parts)


def _build_weather_com_cn_summary(data: Dict[str, Any], city: str, days: int) -> str:
    """构建中国天气网来源的简报"""
    ext: WeatherExtract = data["extract"]
    location = ext.location or city
    source = data.get("source", "中国天气网")

    # 标题行
    lines: List[str] = []
    date_str = ext.days[0].date if ext.days else ""
    lines.append(f"📍 {location}天气简报" + (f"（{date_str}）" if date_str else ""))
    lines.append(f"数据源：{source}" + (f"（{ext.data_source}）" if ext.data_source else ""))
    if ext.update_time:
        lines.append(f"更新时间：{ext.update_time}")

    # 结论行（先结论后细节）
    today = ext.days[0] if ext.days else None
    if today:
        cond = today.condition or "天气不明"
        high = _strip_unit(today.high_temp)
        low = _strip_unit(today.low_temp)
        temp_range = ""
        if high and low:
            temp_range = f"，{low}~{high}℃"
        elif high:
            temp_range = f"，最高{high}℃"
        advice = build_advice_from_extract(ext)[0].rstrip("。")
        lines.extend(["", f"👉 {location}今日{cond}{temp_range}，{advice}。"])
    else:
        lines.extend(["", f"👉 {location}天气信息请参考下方详情。"])

    # 逐日预报
    if ext.days:
        day_count = min(len(ext.days), days + 1) if days > 0 else 1
        label = "今日天气" if days == 0 else f"今日及未来{days}天天气"
        lines.extend(["", f"【{label}】"])
        for d in ext.days[:day_count]:
            lines.append(f"  - {_format_day_line(d)}")

    # 分时段预报（仅今日）
    if ext.hourly and days == 0:
        lines.extend(["", "【分时段预报】"])
        for h in ext.hourly[:8]:
            ht = _strip_unit(h.temp)
            parts = [h.time]
            if ht:
                parts.append(f"{ht}℃")
            wind_parts = []
            if h.wind:
                wind_parts.append(h.wind)
            if h.wind_level:
                wind_parts.append(h.wind_level)
            if wind_parts:
                parts.append("".join(wind_parts))
            lines.append(f"  - {','.join(parts)}")

    # 生活指数（仅今日关键指数）
    if ext.life_indices:
        # 去重并只取关键指数
        seen_names = set()
        key_indices = []
        priority = {"穿衣指数", "紫外线指数", "洗车指数", "运动指数", "感冒指数"}
        for idx in ext.life_indices:
            if idx.name not in seen_names and idx.name in priority:
                seen_names.add(idx.name)
                key_indices.append(idx)
            if len(key_indices) >= 4:
                break
        if key_indices:
            lines.extend(["", "【生活指数】"])
            for idx in key_indices:
                advice_text = f"：{idx.advice}" if idx.advice else ""
                lines.append(f"  - {idx.name} {idx.level}{advice_text}")

    # 出行建议
    advice = build_advice_from_extract(ext)
    lines.extend(["", "【出行建议】"])
    for a in advice:
        text = a.rstrip("。！;；")
        lines.append(f"  - {text}")

    return "\n".join(lines)


# ===== 主入口 =====

async def main() -> None:
    result_mode = norm_mode(sys.argv[1] if len(sys.argv) > 1 else "display_only")
    city = (sys.argv[2].strip() if len(sys.argv) > 2 and sys.argv[2].strip() else "北京")
    days = parse_days(sys.argv[3] if len(sys.argv) > 3 else "0")
    city_code = (sys.argv[4].strip() if len(sys.argv) > 4 and sys.argv[4].strip() else "")

    # 自动解析城市代码
    if not city_code:
        city_code = resolve_city_code(city, "")
        if city_code:
            print(f"[参数] 自动匹配城市代码: {city} → {city_code}")
        else:
            print(f"[参数] 未找到 {city} 的城市代码，将跳过中国天气网直接搜索")

    print(f"[参数] result_mode={result_mode}, city={city}, days={days}, city_code={city_code or '无'}")

    sdk = CodeActSDK()
    try:
        data = await get_weather(sdk, city, days, city_code)
        message = build_summary(data, city, days)
        await sdk.submit_result(
            result_mode=result_mode,
            status="success",
            message=message,
            data={"city": city, "days": days, "source": data.get("source"), "city_code": city_code},
        )
    except Exception as e:
        print(f"[错误] {e}\n{traceback.format_exc()}")
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"天气查询失败：暂时无法获取{city}天气，请稍后再试。",
        )


if __name__ == "__main__":
    asyncio.run(main())
