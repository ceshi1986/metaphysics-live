#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每日自动交易脚本 - 投资人完整策略版 v4.0 - 优化版

策略六层体系：
Layer 1 - 大盘状态（仅显示，不限制仓位）：
  仓位完全由择时信号决定，最大仓位100%

Layer 2 - 择时买入信号：
  ① 跳涨>0.3%：加仓信号
  ② 跳跌>0.5% + 距20日高>6% + 近5日≥3天跌：低位止跌买入
  ③ 跳空低开>1% + 放量>1.2倍5日均量：恐慌洗盘买入

Layer 3 - 择时卖出信号：
  ① 高位缩量新高(创20日高但量<0.7倍均量)：减仓30%
  ② 单日暴跌>3%：止损减仓20%
  ③ 跌破20日均线且20日均线向下：减半仓(卖出50%)
  ④ 跌破60日均线且60日均线向下：清仓（终极止损）
  ※ 持仓标的仍在60日线上方且近30日涨幅>10%时，信号①②③不触发（强持规则）

Layer 4 - 候选池（赛道龙头更新）：
  人工智能/半导体/新能源车/PCB覆铜板/光纤光缆/氟化工/有色金属

Layer 5 - 资金轮动：
  减仓/清仓释放资金 → 寻找最强标的(近20日涨幅最大+量能放大)
  资金不闲置：有可用现金且有强势标的 → 必须买入

Layer 6 - 趋势股判断：
  趋势股(未来趋势行业+龙头) → 止损时保留30%轻仓
  非趋势股 → 止损时全清

数据源：腾讯行情API(实时) + 腾讯K线API(历史K线)
"""

import asyncio
import base64
import json
import math
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from codeact_sdk import CodeActSDK

# ===== 代理绕过（沙箱代理有时不可用）=====
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'


def _make_session():
    """创建绕过代理的 requests session"""
    s = requests.Session()
    s.trust_env = False
    return s


HTTP = _make_session()

# ===== SDK 工具版本 =====
TOOL_SCHEMA_VERSIONS = {
    "codeact_fetch_web": "v1_2c8d0580b3f93a58",
    "codeact_search_web": "v1_5ac1b0eba8c26f2a",
}

# ===== 常量 =====
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={codes}"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={code},day,{start_date},{end_date},{count},{adjust}"
GITHUB_API_BASE = "https://api.github.com/repos/{repo}/contents/portfolio.json"
LOT_SIZE = 100
CST = timezone(timedelta(hours=8))

# 大盘指数代码
MARKET_INDEX_CODE = "sh000001"

# ===== Layer 4: 候选池（赛道龙头）=====
CANDIDATE_POOL = [
    # 人工智能/半导体（行业前三）
    {"code": "sh688981", "name": "中芯国际", "sector": "半导体"},
    {"code": "sh688256", "name": "寒武纪", "sector": "人工智能"},
    {"code": "sz002230", "name": "科大讯飞", "sector": "人工智能"},
    {"code": "sh688008", "name": "澜起科技", "sector": "半导体"},
    {"code": "sz002371", "name": "北方华创", "sector": "半导体"},
    # 新能源车/储能（行业前三）
    {"code": "sz002594", "name": "比亚迪", "sector": "新能源车"},
    {"code": "sz300750", "name": "宁德时代", "sector": "储能"},
    # 机器人/工业自动化（未来趋势）
    {"code": "sz300124", "name": "汇川技术", "sector": "机器人"},
    # 光通信/算力基建（行业前三）
    {"code": "sz300308", "name": "中际旭创", "sector": "光通信"},
    # 有色金属/新能源材料
    {"code": "sh601899", "name": "紫金矿业", "sector": "有色金属"},
    {"code": "sz002460", "name": "赣锋锂业", "sector": "有色金属"},
    # 白酒/消费（行业龙头）
    {"code": "sh600519", "name": "贵州茅台", "sector": "白酒"},
    {"code": "sz000858", "name": "五粮液", "sector": "白酒"},
    {"code": "sz000568", "name": "泸州老窖", "sector": "白酒"},
]

# ===== Layer 6: 未来趋势行业 =====
FUTURE_TREND_SECTORS = {
    "人工智能", "半导体", "新能源车", "芯片", "AI",
    "光伏", "储能", "光通信", "机器人",
    "生物医药", "创新药", "军工", "航空航天",
    "数字经济", "云计算", "5G", "物联网",
    "有色金属", "新能源材料",
    "白酒", "消费",
}

# ===== 交易参数 =====
# Layer 1 大盘状态（仅显示，不限制仓位）
# 仓位完全由择时信号决定，最大仓位100%
POSITION_MAX = 1.00  # 最大仓位上限

# Layer 2 买入信号阈值
GAP_UP_THRESHOLD = 0.003          # 跳涨>0.3%
OVERSOLD_GAP_DOWN = 0.005         # 跳跌>0.5%
OVERSOLD_FROM_HIGH = 0.06         # 距20日高>6%
OVERSOLD_LOOKBACK = 5             # 近5日
OVERSOLD_DROP_DAYS = 3            # ≥3天跌
PANIC_GAP_DOWN = 0.01             # 跳空低开>1%
PANIC_VOL_RATIO = 1.2             # 放量>1.2倍5日均量

# Layer 3 卖出信号阈值
HIGH_VOL_SHRINK_RATIO = 0.7       # 缩量<0.7倍均量
SINGLE_DAY_CRASH = 0.03           # 单日暴跌>3%

# 止损 + Layer 6 趋势股保留
STOP_LOSS_PCT = -0.10             # 止损线-10%
TREND_STOCK_KEEP_RATIO = 0.30     # 趋势股止损保留30%

# 买入资金分配
BUY_CASH_RATIO = 0.30             # 每次买入用30%可用资金


# ============================================================
# 腾讯行情 API（实时）
# ============================================================

def fetch_quotes(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量获取腾讯实时行情数据

    返回 {code: {price, name, open, yesterday_close, change_pct, high, low,
                amplitude, turnover_yi, volume, ...}}
    """
    if not codes:
        return {}

    codes_str = ",".join(codes)
    url = TENCENT_QUOTE_URL.format(codes=codes_str)

    try:
        resp = HTTP.get(url, timeout=15)
        resp.encoding = "gbk"
        text = resp.text.strip()
    except Exception as e:
        print(f"[行情] 请求失败: {e}")
        return {}

    result = {}
    for line in text.split(";"):
        line = line.strip()
        if not line or "~" not in line:
            continue
        parts = line.split("~")
        if len(parts) < 50:
            continue

        try:
            full_code = line.split("=")[0].replace("v_", "").strip()
            if not full_code:
                full_code = parts[2]

            name = parts[1].replace(" ", "")
            current_price = float(parts[3]) if parts[3] else 0
            yesterday_close = float(parts[4]) if parts[4] else current_price
            open_price = float(parts[5]) if len(parts) > 5 and parts[5] else current_price
            volume = float(parts[6]) if len(parts) > 6 and parts[6] else 0  # 成交量（手）
            change_pct = float(parts[32]) if parts[32] else 0.0
            high = float(parts[33]) if parts[33] else current_price
            low = float(parts[34]) if parts[34] else current_price
            amplitude = float(parts[43]) if len(parts) > 43 and parts[43] else 0.0
            turnover_wan = float(parts[57]) if len(parts) > 57 and parts[57] else 0.0
            turnover_yi = turnover_wan / 10000.0

            result[full_code] = {
                "code": full_code,
                "name": name,
                "price": current_price,
                "yesterday_close": yesterday_close,
                "open": open_price,
                "change_pct": change_pct,
                "high": high,
                "low": low,
                "amplitude": amplitude,
                "turnover_yi": turnover_yi,
                "volume": volume,  # 手
                "raw_code": parts[2],
            }
        except (ValueError, IndexError) as e:
            print(f"[行情] 解析行失败: {e}")
            continue

    return result


# ============================================================
# 腾讯K线 API（历史数据）
# ============================================================

def fetch_kline(code: str, count: int = 80, start_date: str = "") -> List[Dict[str, Any]]:
    """获取日K线数据

    API: https://web.ifzq.gtimg.cn/appstock/app/fqkline/get
    返回格式: data.{code}.qfqday 或 data.{code}.day
    每条: [date, open, close, high, low, volume]
    """
    adjust = "qfq"
    url = TENCENT_KLINE_URL.format(
        code=code, start_date=start_date,
        end_date="", count=count, adjust=adjust,
    )

    try:
        resp = HTTP.get(url, timeout=15)
        data = resp.json()

        if data.get("code") != 0:
            print(f"[K线] API返回错误: {data.get('msg', 'unknown')}")
            return []

        # 提取K线数据：优先 qfqday，回退 day
        code_data = data.get("data", {}).get(code, {})
        kline_raw = code_data.get("qfqday", []) or code_data.get("day", [])

        result = []
        for item in kline_raw:
            if len(item) >= 6:
                result.append({
                    "date": item[0],
                    "open": float(item[1]),
                    "close": float(item[2]),
                    "high": float(item[3]),
                    "low": float(item[4]),
                    "volume": float(item[5]),
                })
        return result
    except Exception as e:
        print(f"[K线] 获取 {code} 失败: {e}")
        return []


# ============================================================
# 技术分析函数
# ============================================================

def calc_ma(kline: List[Dict], period: int) -> List[float]:
    """计算移动平均线，返回与 kline 对齐的 MA 列表（前 period-1 个为 NaN 用 None 占位）"""
    if len(kline) < period:
        return []
    closes = [d["close"] for d in kline]
    ma_list = []
    for i in range(len(closes)):
        if i < period - 1:
            ma_list.append(None)
        else:
            ma_list.append(sum(closes[i - period + 1:i + 1]) / period)
    return ma_list


def calc_avg_volume(kline: List[Dict], period: int = 5) -> float:
    """计算最近N日平均成交量"""
    if len(kline) < period:
        return 0
    volumes = [d["volume"] for d in kline[-period:]]
    return sum(volumes) / len(volumes)


def count_down_days(kline: List[Dict], lookback: int = 5) -> int:
    """统计最近N天中收盘下跌天数"""
    if len(kline) < lookback + 1:
        return 0
    count = 0
    for i in range(-lookback, 0):
        if kline[i]["close"] < kline[i - 1]["close"]:
            count += 1
    return count


def get_period_high_close(kline: List[Dict], period: int = 20) -> float:
    """获取最近N日最高收盘价"""
    if not kline:
        return 0
    recent = kline[-period:] if len(kline) >= period else kline
    return max(d["close"] for d in recent)


# ============================================================
# Layer 1: 大盘状态判断 → 仓位管理
# ============================================================

def determine_market_status(index_kline: List[Dict]) -> str:
    """判断大盘状态（仅用于显示，不限制仓位）

    Returns: status: "up" / "shake" / "down"
    """
    if len(index_kline) < 62:
        print(f"[大盘] 数据不足({len(index_kline)}日)，默认震荡")
        return "shake"

    ma60 = calc_ma(index_kline, 60)
    # 需要 MA60 至少有2个有效值才能判断方向
    valid_ma = [v for v in ma60 if v is not None]
    if len(valid_ma) < 2:
        print("[大盘] 均线计算不足，默认震荡")
        return "shake"

    current_close = index_kline[-1]["close"]
    current_ma60 = valid_ma[-1]
    prev_ma60 = valid_ma[-2]
    ma60_rising = current_ma60 > prev_ma60

    if current_close > current_ma60 and ma60_rising:
        status = "up"
        desc = f"上行(收{current_close:.2f}>MA60={current_ma60:.2f},MA60向上)"
    elif current_close < current_ma60 and not ma60_rising:
        status = "down"
        desc = f"下行(收{current_close:.2f}<MA60={current_ma60:.2f},MA60向下)"
    else:
        status = "shake"
        desc = f"震荡(收{current_close:.2f},MA60={current_ma60:.2f})"

    print(f"[大盘] {desc}")
    return status


# ============================================================
# Layer 2: 择时买入信号
# ============================================================

def check_buy_signals(
    code: str, quote: Dict[str, Any], kline: List[Dict]
) -> List[Dict[str, Any]]:
    """检查买入信号

    三种买入信号：
    ① 跳涨加仓：开盘跳涨>0.3%
    ② 低位止跌：跳跌>0.5% + 距20日高>6% + 近5日≥3天跌
    ③ 恐慌洗盘：跳空低开>1% + 放量>1.2倍5日均量
    """
    signals = []

    if len(kline) < 6:
        return signals

    # 计算跳空幅度：用实时行情的开盘价 vs 昨收
    today_open = quote.get("open", 0)
    yesterday_close = quote.get("yesterday_close", 0)

    if yesterday_close <= 0 or today_open <= 0:
        return signals

    gap_pct = (today_open - yesterday_close) / yesterday_close
    current_price = quote["price"]

    # 信号①：跳涨加仓
    if gap_pct > GAP_UP_THRESHOLD:
        signals.append({
            "type": "gap_up",
            "strength": "medium",
            "reason": f"跳涨加仓：开盘跳涨{gap_pct*100:.2f}%>{GAP_UP_THRESHOLD*100:.1f}%",
        })

    # 信号②：低位止跌买入
    high_20 = get_period_high_close(kline, 20)
    dist_from_high = (high_20 - current_price) / high_20 if high_20 > 0 else 0
    down_days = count_down_days(kline, OVERSOLD_LOOKBACK)

    if (gap_pct < -OVERSOLD_GAP_DOWN
            and dist_from_high > OVERSOLD_FROM_HIGH
            and down_days >= OVERSOLD_DROP_DAYS):
        signals.append({
            "type": "oversold_bounce",
            "strength": "strong",
            "reason": (f"低位止跌：跳跌{gap_pct*100:.2f}%，"
                       f"距20日高-{dist_from_high*100:.1f}%，"
                       f"近{OVERSOLD_LOOKBACK}日{down_days}天跌"),
        })

    # 信号③：恐慌洗盘买入
    avg_vol_5 = calc_avg_volume(kline, 5)
    today_vol = quote.get("volume", 0)  # 实时行情成交量（手）
    vol_ratio = today_vol / avg_vol_5 if avg_vol_5 > 0 else 0

    if gap_pct < -PANIC_GAP_DOWN and vol_ratio > PANIC_VOL_RATIO:
        signals.append({
            "type": "panic_washout",
            "strength": "strong",
            "reason": f"恐慌洗盘：低开{gap_pct*100:.2f}%，放量{vol_ratio:.1f}倍",
        })

    return signals


# ============================================================
# Layer 3: 择时卖出信号
# ============================================================

def check_sell_signals(
    position: Dict[str, Any], quote: Dict[str, Any], kline: List[Dict]
) -> List[Dict[str, Any]]:
    """检查卖出信号

    四种卖出信号：
    ① 高位缩量新高：创20日高但量<0.7倍均量 → 减仓30%
    ② 单日暴跌>3% → 减仓20%
    ③ 跌破20日均线且20日均线向下 → 减半仓(卖出50%)
    ④ 跌破60日均线且60日均线向下 → 清仓（终极止损）
    + 止损逻辑(Layer 6联动)

    强持规则：信号①②③在持仓标的仍在60日线上方且近30日涨幅>10%时不触发
    """
    signals = []

    if len(kline) < 21:
        return signals

    current_price = quote["price"]
    current_vol = quote.get("volume", 0)  # 手
    avg_vol_5 = calc_avg_volume(kline, 5)

    # 强持规则判断：仍在60日线上方且近30日涨幅>10%
    stock_is_strong = False
    if len(kline) >= 62:
        ma60 = calc_ma(kline, 60)
        valid_ma60 = [v for v in ma60 if v is not None]
        if len(valid_ma60) >= 1:
            last_ma60 = valid_ma60[-1]
            price_30d_ago = kline[-30]["close"] if len(kline) >= 30 else kline[0]["close"]
            gain_30d = (current_price - price_30d_ago) / price_30d_ago if price_30d_ago > 0 else 0
            if current_price > last_ma60 and gain_30d > 0.10:
                stock_is_strong = True

    # 信号①：高位缩量新高
    high_20_close = get_period_high_close(kline, 20)
    # 如果今天收盘价接近20日新高（差距<0.5%）
    if current_price >= high_20_close * 0.995 and avg_vol_5 > 0:
        vol_ratio = current_vol / avg_vol_5 if avg_vol_5 > 0 else 1
        if vol_ratio < HIGH_VOL_SHRINK_RATIO:
            if stock_is_strong:
                print(f"  [强持跳过] {position['name']} 仍在60日线上方且30日涨幅>10%，跳过高位缩量信号")
            else:
                signals.append({
                    "type": "high_divergence",
                    "action": "reduce",
                    "reduce_pct": 0.30,
                    "reason": f"高位缩量：创20日新高但量仅{vol_ratio:.2f}倍均量",
                })

    # 信号②：单日暴跌
    change_pct = quote.get("change_pct", 0)
    if change_pct < -SINGLE_DAY_CRASH * 100:
        if stock_is_strong:
            print(f"  [强持跳过] {position['name']} 仍在60日线上方且30日涨幅>10%，跳过单日暴跌信号")
        else:
            signals.append({
                "type": "single_crash",
                "action": "reduce",
                "reduce_pct": 0.20,
                "reason": f"单日暴跌：跌幅{change_pct:.2f}%>{SINGLE_DAY_CRASH*100:.0f}%",
            })

    # 信号③：跌破20日均线且均线向下 → 减半仓(50%)
    ma20 = calc_ma(kline, 20)
    # 找到最近两个有效的MA20值
    valid_ma20 = [(i, v) for i, v in enumerate(ma20) if v is not None]
    if len(valid_ma20) >= 2:
        last_ma20_idx, last_ma20 = valid_ma20[-1]
        prev_ma20_idx, prev_ma20 = valid_ma20[-2]
        if current_price < last_ma20 and last_ma20 < prev_ma20:
            if stock_is_strong:
                print(f"  [强持跳过] {position['name']} 仍在60日线上方且30日涨幅>10%，跳过破20日线信号")
            else:
                signals.append({
                    "type": "ma_breakdown",
                    "action": "reduce",
                    "reduce_pct": 0.50,
                    "reason": f"跌破20日线：现价{current_price:.2f}<MA20={last_ma20:.2f}，MA20向下，减半仓",
                })

    # 信号④：跌破60日均线且60日均线向下 → 清仓（终极止损）
    if len(kline) >= 62:
        ma60 = calc_ma(kline, 60)
        valid_ma60 = [(i, v) for i, v in enumerate(ma60) if v is not None]
        if len(valid_ma60) >= 2:
            last_ma60_idx, last_ma60 = valid_ma60[-1]
            prev_ma60_idx, prev_ma60 = valid_ma60[-2]
            if current_price < last_ma60 and last_ma60 < prev_ma60:
                signals.append({
                    "type": "ma60_breakdown",
                    "action": "clear",
                    "reduce_pct": 0.00,
                    "reason": f"跌破60日线(终极止损)：现价{current_price:.2f}<MA60={last_ma60:.2f}，MA60向下",
                })

    # 止损检查（Layer 6 联动）
    cost = position["costPrice"]
    pnl_pct = (current_price - cost) / cost if cost > 0 else 0
    if pnl_pct < STOP_LOSS_PCT:
        is_trend = _is_trend_stock(position)
        if is_trend:
            # 趋势股：保留30%轻仓
            signals.append({
                "type": "stop_loss_trend",
                "action": "reduce",
                "reduce_pct": 1.0 - TREND_STOCK_KEEP_RATIO,  # 卖出70%
                "reason": f"趋势股止损：亏{pnl_pct*100:.1f}%，保留{TREND_STOCK_KEEP_RATIO*100:.0f}%",
            })
        else:
            # 非趋势股：全清
            signals.append({
                "type": "stop_loss_full",
                "action": "clear",
                "reduce_pct": 0.00,
                "reason": f"止损清仓：非趋势股亏{pnl_pct*100:.1f}%",
            })

    return signals


# ============================================================
# Layer 5: 资金轮动 - 寻找最强标的
# ============================================================

def find_strongest_stock(
    candidates: List[Dict[str, Any]],
    quotes: Dict[str, Dict],
    kline_data: Dict[str, List[Dict]],
    held_codes: set,
) -> Optional[Dict[str, Any]]:
    """寻找最强标的：近20日涨幅最大 + 量能放大

    综合评分 = 近20日涨幅 * 0.6 + 量能比 * 0.4
    """
    best = None
    best_score = -float('inf')

    for candidate in candidates:
        code = candidate["code"]
        if code in held_codes:
            continue
        if code not in quotes or code not in kline_data:
            continue

        kline = kline_data[code]
        if len(kline) < 20:
            continue

        # 近20日涨幅
        price_20d_ago = kline[-20]["close"]
        current_price = quotes[code]["price"]
        gain_20d = (current_price - price_20d_ago) / price_20d_ago if price_20d_ago > 0 else 0

        # 量能放大（今日量 vs 20日均量）
        avg_vol = calc_avg_volume(kline, 20)
        today_vol = quotes[code].get("volume", 0)
        vol_ratio = today_vol / avg_vol if avg_vol > 0 else 1

        # 综合评分
        score = gain_20d * 0.6 + (vol_ratio - 1) * 0.4

        if score > best_score:
            best_score = score
            best = candidate

    return best


# ============================================================
# Layer 6: 趋势股判断
# ============================================================

def _is_trend_stock(position: Dict[str, Any]) -> bool:
    """判断是否是趋势股（三维度任一满足即可）

    1. 赛道趋势：属于未来趋势行业
    2. 长期看好：标记为赛道龙头
    3. 技术趋势：曾有过浮盈>2%
    """
    # 维度1：赛道趋势
    sector = position.get("sector", "")
    if sector in FUTURE_TREND_SECTORS:
        return True

    # 维度2：长期看好的龙头
    if position.get("longTermBullish", False):
        return True

    # 维度3：技术趋势（曾有过浮盈>2%）
    highest = position.get("highestPrice", position["costPrice"])
    if highest > position["costPrice"] * 1.02:
        return True

    return False


def _guess_sector(code: str) -> str:
    """根据代码从候选池猜测板块"""
    for c in CANDIDATE_POOL:
        if c["code"] == code:
            return c.get("sector", "其他")
    return "其他"


# ============================================================
# GitHub API
# ============================================================

def read_portfolio_from_github(repo: str, token: str) -> Tuple[Dict[str, Any], str]:
    """从 GitHub 读取 portfolio.json，返回 (portfolio_dict, sha)"""
    url = GITHUB_API_BASE.format(repo=repo)
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    resp = HTTP.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    sha = data["sha"]
    content = base64.b64decode(data["content"]).decode("utf-8")
    return json.loads(content), sha


def push_portfolio_to_github(repo: str, token: str, portfolio: Dict[str, Any], sha: str) -> bool:
    """推送更新后的 portfolio.json 到 GitHub"""
    url = GITHUB_API_BASE.format(repo=repo)
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    content_str = json.dumps(portfolio, ensure_ascii=False, indent=2)
    content_b64 = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")

    now_str = datetime.now(CST).strftime("%Y-%m-%d %H:%M")
    payload = {
        "message": f"每日交易更新 {now_str}",
        "content": content_b64,
        "sha": sha,
    }
    resp = HTTP.put(url, headers=headers, json=payload, timeout=15)
    if resp.status_code in (200, 201):
        print("[GitHub] 推送成功")
        return True
    else:
        print(f"[GitHub] 推送失败: {resp.status_code} {resp.text[:200]}")
        return False


# ============================================================
# 基金 NAV 获取
# ============================================================

def fetch_fund_nav(fund_code: str) -> Optional[float]:
    """获取基金最新净值（腾讯基金API）"""
    if not fund_code or fund_code.startswith("FUND_"):
        return None
    url = f"https://qt.gtimg.cn/q=fu{fund_code}"
    try:
        resp = HTTP.get(url, timeout=10)
        resp.encoding = "gbk"
        text = resp.text.strip()
        if "~" not in text:
            return None
        parts = text.split("~")
        if len(parts) > 3 and parts[3]:
            return float(parts[3])
    except Exception as e:
        print(f"[基金] 获取净值失败 {fund_code}: {e}")
    return None


# ============================================================
# 主流程
# ============================================================

async def main() -> None:
    result_mode_raw = sys.argv[1] if len(sys.argv) > 1 else "auto"
    github_repo = sys.argv[2] if len(sys.argv) > 2 else "ceshi1986/stock-strategy"
    initial_capital = float(sys.argv[3]) if len(sys.argv) > 3 else 50000.0

    sdk = CodeActSDK()

    try:
        mode = (result_mode_raw or "auto").strip().lower()
        if mode not in {"auto", "display_only", "notify", "no_reply"}:
            raise ValueError("result_mode 只能是 auto / display_only / notify / no_reply")
        # 交易脚本有交易行为，始终 display_only
        actual_mode = "display_only" if mode == "auto" else mode

        print(f"[参数] result_mode={mode}→{actual_mode}, repo={github_repo}, initial_capital={initial_capital}")

        # ===== 读取 GitHub Token =====
        github_token = ""
        for sp in [
            "/app/data/SECRET.md",
            os.path.expanduser("~/SECRET.md"),
            "./SECRET.md",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "SECRET.md"),
        ]:
            if os.path.exists(sp):
                with open(sp, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line.startswith("GITHUB_TOKEN="):
                            github_token = line.split("=", 1)[1].strip()
                            break
            if github_token:
                break

        if not github_token:
            github_token = os.environ.get("GITHUB_TOKEN", "")
        if not github_token:
            raise ValueError("未找到 GitHub Token，请在 SECRET.md 中设置 GITHUB_TOKEN")

        print(f"[GitHub] Token 已获取，长度={len(github_token)}")

        # ===== 交易时段检查 =====
        now_cst = datetime.now(CST)
        weekday = now_cst.weekday()  # 0=Mon, 6=Sun
        hour = now_cst.hour
        minute = now_cst.minute
        time_val = hour * 60 + minute  # 分钟数

        if weekday >= 5:
            # 周末，跳过
            await sdk.submit_result(
                result_mode=actual_mode,
                status="skipped",
                message="今日非交易日（周末），跳过执行",
            )
            return

        # A股交易时段：9:15-11:30, 13:00-15:00
        morning_open = 9 * 60 + 15   # 9:15
        morning_close = 11 * 60 + 30  # 11:30
        afternoon_open = 13 * 60      # 13:00
        afternoon_close = 15 * 60     # 15:00

        in_trading = (morning_open <= time_val <= morning_close) or \
                     (afternoon_open <= time_val <= afternoon_close)

        if not in_trading:
            # 非交易时段，使用收盘数据做卖出检查，但跳过买入
            print(f"[交易时段] 当前时间 {now_cst.strftime('%H:%M')}，非交易时段")
            print("[交易时段] 仅执行卖出检查和净值更新，跳过买入操作")
        else:
            print(f"[交易时段] 当前时间 {now_cst.strftime('%H:%M')}，交易时段内")

        # 将是否在交易时段传递给后续逻辑
        _is_trading_hours = in_trading

        # ===== 步骤1: 从 GitHub 读取 portfolio.json =====
        print("\n===== 步骤1: 从 GitHub 读取 portfolio.json =====")
        portfolio, sha = read_portfolio_from_github(github_repo, github_token)
        state = portfolio.get("state", {})
        cash = state.get("cash", 0)
        positions = state.get("positions", [])
        funds = state.get("funds", [])  # 基金持仓
        trades = portfolio.get("trades", [])
        snapshots = portfolio.get("dailySnapshots", [])
        stats = portfolio.get("stats", {})
        meta = portfolio.get("meta", {})

        print(f"[状态] 现金={cash:.2f}, 股票持仓={len(positions)}, 基金持仓={len(funds)}")
        for p in positions:
            trend_mark = " [趋势股]" if _is_trend_stock(p) else ""
            print(f"  {p['name']}({p['code']}): {p['shares']}股 @ {p['costPrice']}{trend_mark}")
        for f in funds:
            print(f"  {f['name']}({f.get('code', 'N/A')}): {f['shares']}份 @ {f['costNav']}")

        # ===== 步骤2: 获取大盘K线 → Layer 1 仓位管理 =====
        print("\n===== 步骤2: 获取大盘K线数据 =====")
        index_kline = fetch_kline(MARKET_INDEX_CODE, count=80)
        print(f"[大盘] 获取到 {len(index_kline)} 日K线数据")

        market_status = determine_market_status(index_kline)

        # ===== 步骤3: 获取所有持仓+候选池实时行情 =====
        print("\n===== 步骤3: 获取实时行情 =====")
        all_codes = [p["code"] for p in positions]
        candidate_codes = [c["code"] for c in CANDIDATE_POOL]
        fetch_codes = list(dict.fromkeys(all_codes + candidate_codes + [MARKET_INDEX_CODE]))

        quotes = fetch_quotes(fetch_codes)
        print(f"[行情] 获取到 {len(quotes)} 只行情")
        for code, q in quotes.items():
            if code != MARKET_INDEX_CODE:
                print(f"  {q['name']}({code}): 现价={q['price']}, 涨幅={q['change_pct']}%, "
                      f"开盘={q['open']}, 昨收={q['yesterday_close']}")

        # ===== 步骤4: 获取个股K线数据 =====
        print("\n===== 步骤4: 获取个股K线数据 =====")
        kline_data = {}
        codes_need_kline = list(dict.fromkeys(all_codes + candidate_codes))
        for code in codes_need_kline:
            kline = fetch_kline(code, count=80)
            if kline:
                kline_data[code] = kline
                print(f"  {code}: {len(kline)}日K线")
            else:
                print(f"  {code}: K线获取失败")

        # ===== 步骤5: Layer 3 - 检查卖出信号 =====
        print("\n===== 步骤5: 检查卖出信号 =====")
        today_str = datetime.now(CST).strftime("%Y-%m-%d")
        now_time_str = datetime.now(CST).strftime("%H:%M:%S")
        sell_actions = []

        for pos in list(positions):
            code = pos["code"]
            if code not in quotes:
                print(f"  [警告] {pos['name']}({code}) 无行情，跳过")
                continue

            quote = quotes[code]
            current_price = quote["price"]

            # 更新最高价
            prev_highest = pos.get("highestPrice", pos["costPrice"])
            if current_price > prev_highest:
                pos["highestPrice"] = current_price
                print(f"  [更新] {pos['name']} 最高价: {prev_highest:.2f} → {current_price:.2f}")

            # 检查卖出信号
            kline = kline_data.get(code, [])
            sell_signals = check_sell_signals(pos, quote, kline)

            if sell_signals:
                # 信号优先级：MA60破位(终极止损) > 止损全清 > 趋势股止损(保留) > MA20破位(减半) > 暴跌 > 缩量
                # MA60破位是终极止损信号，优先级最高
                priority = {"ma60_breakdown": 0, "stop_loss_full": 1, "stop_loss_trend": 2,
                            "ma_breakdown": 3, "single_crash": 4, "high_divergence": 5}
                sell_signals.sort(key=lambda s: priority.get(s["type"], 99))
                sig = sell_signals[0]

                if sig["action"] == "clear":
                    sell_shares = pos["shares"]
                elif sig["action"] == "reduce":
                    if sig["type"] == "stop_loss_trend":
                        # 趋势股止损：保留30%，至少保留1手
                        keep_shares = int(pos["shares"] * TREND_STOCK_KEEP_RATIO) // LOT_SIZE * LOT_SIZE
                        if keep_shares < LOT_SIZE:
                            keep_shares = LOT_SIZE
                        sell_shares = pos["shares"] - keep_shares
                        # 如果可卖股数不足1手，则不执行卖出（保留全仓等企稳）
                        if sell_shares < LOT_SIZE:
                            print(f"  [跳过] {pos['name']} 趋势股止损但持仓过小({pos['shares']}股)，"
                                  f"无法卖出70%并保留整手，暂保留全仓等企稳")
                            continue
                    else:
                        sell_shares = int(pos["shares"] * sig["reduce_pct"]) // LOT_SIZE * LOT_SIZE
                        if sell_shares < LOT_SIZE:
                            sell_shares = pos["shares"] if pos["shares"] <= LOT_SIZE else LOT_SIZE
                else:
                    continue

                sell_actions.append((pos, sig, sell_shares, quote))
                print(f"  [卖出信号] {pos['name']}({code}): {sig['reason']}")

        # 执行卖出
        released_funds = 0.0
        for pos, sig, sell_shares, quote in sell_actions:
            actual_sell = min(sell_shares, pos["shares"])
            sell_price = quote["price"]
            sell_amount = actual_sell * sell_price
            pnl = (sell_price - pos["costPrice"]) * actual_sell

            trade = {
                "date": today_str,
                "time": now_time_str,
                "code": pos["code"],
                "name": pos["name"],
                "action": "sell",
                "price": sell_price,
                "shares": actual_sell,
                "amount": round(sell_amount, 2),
                "pnl": round(pnl, 2),
                "reason": sig["reason"],
            }
            trades.append(trade)

            if actual_sell >= pos["shares"]:
                positions = [p for p in positions if p["code"] != pos["code"]]
            else:
                for p in positions:
                    if p["code"] == pos["code"]:
                        p["shares"] -= actual_sell
                        break

            cash += sell_amount
            released_funds += sell_amount
            print(f"  [卖出执行] {pos['name']} {actual_sell}股@{sell_price}，"
                  f"金额={sell_amount:.2f}，盈亏={pnl:+.2f}")

        # ===== 步骤6: 计算当前仓位比例 =====
        print("\n===== 步骤6: 计算仓位比例 =====")
        positions_value = sum(
            p["shares"] * quotes[p["code"]]["price"]
            for p in positions if p["code"] in quotes
        )
        # 基金市值
        fund_value = 0.0
        for f in funds:
            nav = fetch_fund_nav(f.get("code", ""))
            if nav and nav > 0:
                fund_value += f["shares"] * nav
                f["currentNav"] = nav
            else:
                fund_value += f["shares"] * f["costNav"]
                f["currentNav"] = f["costNav"]

        total_value = cash + positions_value + fund_value
        current_position_ratio = (positions_value + fund_value) / total_value if total_value > 0 else 0

        print(f"[仓位] 总资产={total_value:.2f}，现金={cash:.2f}，"
              f"股票={positions_value:.2f}，基金={fund_value:.2f}")
        print(f"[仓位] 当前仓位{current_position_ratio*100:.1f}%（大盘{market_status}）")

        # ===== 步骤7: Layer 2 - 检查买入信号 + Layer 5 资金轮动 =====
        print("\n===== 步骤7: 检查买入信号 + 资金轮动 =====")
        buy_actions = []
        held_codes = {p["code"] for p in positions}

        if not _is_trading_hours:
            print("[交易时段] 非交易时段，跳过买入操作")

        # 买入条件：仓位低于上限 且 有可用资金 且 在交易时段内
        if _is_trading_hours and current_position_ratio < POSITION_MAX and cash >= LOT_SIZE * 10:
            # 7a. 检查候选池买入信号
            buy_candidates = []
            for candidate in CANDIDATE_POOL:
                code = candidate["code"]
                if code in held_codes:
                    # 已持仓标的：检查加仓信号
                    if code not in quotes or code not in kline_data:
                        continue
                    signals = check_buy_signals(code, quotes[code], kline_data[code])
                    # 已持仓只看跳涨加仓信号
                    gap_up_signals = [s for s in signals if s["type"] == "gap_up"]
                    if gap_up_signals:
                        buy_candidates.append({
                            "code": code,
                            "name": quotes[code]["name"],
                            "price": quotes[code]["price"],
                            "signals": gap_up_signals,
                            "score": 1,
                            "sector": candidate.get("sector", ""),
                            "is_add": True,  # 加仓标记
                        })
                else:
                    # 未持仓标的：检查所有买入信号
                    if code not in quotes or code not in kline_data:
                        continue
                    signals = check_buy_signals(code, quotes[code], kline_data[code])
                    if signals:
                        score = sum(2 if s["strength"] == "strong" else 1 for s in signals)
                        buy_candidates.append({
                            "code": code,
                            "name": quotes[code]["name"],
                            "price": quotes[code]["price"],
                            "signals": signals,
                            "score": score,
                            "sector": candidate.get("sector", ""),
                            "is_add": False,
                        })

            # 7b. Layer 5: 资金轮动 - 如果有释放资金，寻找最强标的
            if released_funds > 0:
                strongest = find_strongest_stock(CANDIDATE_POOL, quotes, kline_data, held_codes)
                if strongest and strongest["code"] not in held_codes:
                    code = strongest["code"]
                    if code in quotes and code in kline_data:
                        # 检查是否已在候选列表中
                        already_in = any(c["code"] == code for c in buy_candidates)
                        if not already_in:
                            # 资金轮动信号
                            signals = check_buy_signals(code, quotes[code], kline_data[code])
                            if not signals:
                                signals = [{
                                    "type": "rotation",
                                    "strength": "medium",
                                    "reason": f"资金轮动：释放资金{released_funds:.2f}元→最强标的",
                                }]
                            buy_candidates.append({
                                "code": code,
                                "name": quotes[code]["name"],
                                "price": quotes[code]["price"],
                                "signals": signals,
                                "score": 2,  # 资金轮动优先级较高
                                "sector": strongest.get("sector", ""),
                                "is_add": False,
                            })
                            print(f"  [资金轮动] 释放{released_funds:.2f}元→{quotes[code]['name']}({code})")

            # 按信号强度排序
            buy_candidates.sort(key=lambda x: x["score"], reverse=True)

            # 逐个买入
            for bc in buy_candidates:
                # 重新计算仓位
                positions_value_check = sum(
                    p["shares"] * quotes.get(p["code"], {}).get("price", p["costPrice"])
                    for p in positions
                )
                total_check = cash + positions_value_check + fund_value
                ratio_check = (positions_value_check + fund_value) / total_check if total_check > 0 else 0

                if ratio_check >= POSITION_MAX:
                    print(f"  [买入跳过] 仓位已达{ratio_check*100:.1f}%≥上限{POSITION_MAX*100:.0f}%")
                    break
                if cash < LOT_SIZE * bc["price"]:
                    print(f"  [买入跳过] 现金不足买入{bc['name']}1手")
                    break

                # 计算买入股数
                buy_amount = cash * BUY_CASH_RATIO
                shares = int(buy_amount / bc["price"]) // LOT_SIZE * LOT_SIZE
                if shares < LOT_SIZE:
                    continue

                actual_amount = shares * bc["price"]
                if actual_amount > cash:
                    shares = int(cash / bc["price"]) // LOT_SIZE * LOT_SIZE
                    actual_amount = shares * bc["price"]
                    if shares < LOT_SIZE:
                        continue

                reasons = "; ".join(s["reason"] for s in bc["signals"])
                buy_actions.append({
                    "code": bc["code"],
                    "name": bc["name"],
                    "price": bc["price"],
                    "shares": shares,
                    "amount": actual_amount,
                    "reason": reasons,
                    "sector": bc.get("sector", ""),
                    "is_add": bc.get("is_add", False),
                })
                cash -= actual_amount
                held_codes.add(bc["code"])
                print(f"  [买入计划] {bc['name']}({bc['code']}) {shares}股@{bc['price']} — {reasons}")

        # 执行买入
        for buy in buy_actions:
            trade = {
                "date": today_str,
                "time": now_time_str,
                "code": buy["code"],
                "name": buy["name"],
                "action": "buy",
                "price": buy["price"],
                "shares": buy["shares"],
                "amount": round(buy["amount"], 2),
                "pnl": 0,
                "reason": buy["reason"],
            }
            trades.append(trade)

            # 如果是加仓，合并到已有持仓
            existing_pos = None
            for p in positions:
                if p["code"] == buy["code"]:
                    existing_pos = p
                    break

            if existing_pos:
                # 加仓：更新成本和股数
                total_cost = existing_pos["costPrice"] * existing_pos["shares"] + buy["price"] * buy["shares"]
                total_shares = existing_pos["shares"] + buy["shares"]
                existing_pos["costPrice"] = round(total_cost / total_shares, 4)
                existing_pos["shares"] = total_shares
                if buy["price"] > existing_pos.get("highestPrice", buy["price"]):
                    existing_pos["highestPrice"] = buy["price"]
                print(f"  [加仓执行] {buy['name']}({buy['code']}) +{buy['shares']}股@{buy['price']}，"
                      f"总{total_shares}股，新成本{existing_pos['costPrice']}")
            else:
                # 新建持仓
                sector = buy.get("sector", "") or _guess_sector(buy["code"])
                new_pos = {
                    "code": buy["code"],
                    "name": buy["name"],
                    "shares": buy["shares"],
                    "costPrice": buy["price"],
                    "highestPrice": buy["price"],
                    "buyDate": today_str,
                    "sector": sector,
                    "longTermBullish": sector in FUTURE_TREND_SECTORS,
                }
                positions.append(new_pos)
                print(f"  [买入执行] {buy['name']}({buy['code']}) {buy['shares']}股@{buy['price']}，"
                      f"金额={buy['amount']:.2f}")

        # ===== 步骤8: 已移除（原大盘仓位调整，v4.0不再因大盘状态强制减仓）=====
        position_adjust_actions = []  # 保持兼容统计

        # ===== 步骤9: 资金不闲置检查 =====
        print("\n===== 步骤9: 资金闲置检查 =====")
        positions_value = sum(
            p["shares"] * quotes[p["code"]]["price"]
            for p in positions if p["code"] in quotes
        )
        total_value = cash + positions_value + fund_value
        current_position_ratio = (positions_value + fund_value) / total_value if total_value > 0 else 0

        idle_buy_actions = []
        if _is_trading_hours and cash >= LOT_SIZE * 10:
            print(f"  [资金闲置] 有可用现金{cash:.2f}元，寻找强势标的配置")
            strongest = find_strongest_stock(CANDIDATE_POOL, quotes, kline_data, held_codes)
            if strongest and strongest["code"] in quotes:
                code = strongest["code"]
                price = quotes[code]["price"]
                buy_amount = cash * BUY_CASH_RATIO
                shares = int(buy_amount / price) // LOT_SIZE * LOT_SIZE
                if shares >= LOT_SIZE:
                    actual_amount = shares * price
                    sector = strongest.get("sector", _guess_sector(code))
                    idle_buy_actions.append({
                        "code": code,
                        "name": quotes[code]["name"],
                        "price": price,
                        "shares": shares,
                        "amount": actual_amount,
                        "sector": sector,
                    })

        for buy in idle_buy_actions:
            trade = {
                "date": today_str,
                "time": now_time_str,
                "code": buy["code"],
                "name": buy["name"],
                "action": "buy",
                "price": buy["price"],
                "shares": buy["shares"],
                "amount": round(buy["amount"], 2),
                "pnl": 0,
                "reason": f"资金不闲置：有可用现金，买入最强标的",
            }
            trades.append(trade)
            new_pos = {
                "code": buy["code"],
                "name": buy["name"],
                "shares": buy["shares"],
                "costPrice": buy["price"],
                "highestPrice": buy["price"],
                "buyDate": today_str,
                "sector": buy.get("sector", _guess_sector(buy["code"])),
                "longTermBullish": buy.get("sector", "") in FUTURE_TREND_SECTORS,
            }
            positions.append(new_pos)
            cash -= buy["amount"]
            print(f"  [配置] {buy['name']}({buy['code']}) {buy['shares']}股@{buy['price']}")

        # ===== 步骤10: 记录今日快照 =====
        print("\n===== 步骤10: 记录今日快照 =====")
        positions_value = sum(
            p["shares"] * quotes[p["code"]]["price"]
            for p in positions if p["code"] in quotes
        )
        total_value = cash + positions_value + fund_value
        return_pct = (total_value - initial_capital) / initial_capital * 100
        current_position_ratio = (positions_value + fund_value) / total_value if total_value > 0 else 0

        snapshot = {
            "date": today_str,
            "totalValue": round(total_value, 2),
            "cash": round(cash, 2),
            "positionsValue": round(positions_value, 2),
            "fundValue": round(fund_value, 2),
            "return": round(return_pct, 2),
            "marketStatus": market_status,
            "positionRatio": round(current_position_ratio * 100, 1),
        }

        # 检查今天是否已有快照
        today_idx = None
        for i, snap in enumerate(snapshots):
            if snap.get("date") == today_str:
                today_idx = i
                break

        if today_idx is not None:
            snapshots[today_idx] = snapshot
        else:
            snapshots.append(snapshot)

        print(f"[快照] 总资产={total_value:.2f}，收益率={return_pct:+.2f}%，仓位={current_position_ratio*100:.1f}%")

        # ===== 步骤11: 更新 portfolio 并推送 GitHub =====
        print("\n===== 步骤11: 更新 portfolio.json 并推送 GitHub =====")
        portfolio["meta"]["strategyName"] = "投资人完整策略v4.0"
        portfolio["state"]["cash"] = round(cash, 2)
        portfolio["state"]["positions"] = positions
        portfolio["state"]["funds"] = funds
        portfolio["state"]["lastUpdate"] = today_str
        portfolio["trades"] = trades
        portfolio["dailySnapshots"] = snapshots

        # 更新统计
        total_sell = len(sell_actions)
        total_buy = len(buy_actions) + len(idle_buy_actions)
        total_trades = stats.get("totalTrades", 0) + total_sell + total_buy
        win_trades = stats.get("winTrades", 0)
        for pos, sig, sell_shares, quote in sell_actions:
            pnl = (quote["price"] - pos["costPrice"]) * min(sell_shares, pos["shares"])
            if pnl > 0:
                win_trades += 1
        loss_trades = total_trades - win_trades
        win_rate = round(win_trades / total_trades * 100, 2) if total_trades > 0 else 0

        # 最大回撤
        max_dd = stats.get("maxDrawdown", 0)
        if return_pct < max_dd:
            max_dd = round(return_pct, 2)

        portfolio["stats"] = {
            "totalTrades": total_trades,
            "winTrades": win_trades,
            "lossTrades": loss_trades,
            "winRate": win_rate,
            "maxDrawdown": max_dd,
            "currentStreak": stats.get("currentStreak", 0),
        }

        push_ok = push_portfolio_to_github(github_repo, github_token, portfolio, sha)
        if not push_ok:
            print("[GitHub] 推送失败，尝试重新获取 SHA...")
            try:
                _, new_sha = read_portfolio_from_github(github_repo, github_token)
                push_ok = push_portfolio_to_github(github_repo, github_token, portfolio, new_sha)
            except Exception as e:
                print(f"[GitHub] 重试也失败: {e}")

        # ===== 步骤12: 构建交易摘要 =====
        print("\n===== 步骤12: 构建交易摘要 =====")
        summary_lines = []
        summary_lines.append("📊 每日交易摘要（投资人完整策略v4.0 优化版）")
        summary_lines.append(f"日期：{today_str}")
        summary_lines.append("")

        # 大盘状态
        status_emoji = {"up": "🟢上行", "shake": "🟡震荡", "down": "🔴下行"}
        summary_lines.append(
            f"大盘状态：{status_emoji.get(market_status, market_status)}，"
            f"当前仓位{current_position_ratio*100:.1f}%"
        )
        summary_lines.append("")

        # 总资产
        summary_lines.append(
            f"💰 总资产：{total_value:,.2f} 元"
            f"（现金 {cash:,.2f} + 股票 {positions_value:,.2f} + 基金 {fund_value:,.2f}）"
        )
        summary_lines.append(f"📈 累计收益率：{return_pct:+.2f}%（初始资金 {initial_capital:,.0f}）")
        summary_lines.append("")

        # 卖出操作
        if sell_actions:
            summary_lines.append("🔴 卖出操作（择时信号）：")
            for pos, sig, sell_shares, quote in sell_actions:
                pnl = (quote["price"] - pos["costPrice"]) * sell_shares
                summary_lines.append(
                    f"  • {pos['name']}({pos['code']})：卖出 {sell_shares}股@{quote['price']}，"
                    f"盈亏 {pnl:+,.2f}元 — {sig['reason']}"
                )
            summary_lines.append("")

        # 买入操作
        if buy_actions:
            summary_lines.append("🟢 买入操作（择时信号）：")
            for buy in buy_actions:
                action_type = "加仓" if buy.get("is_add") else "买入"
                summary_lines.append(
                    f"  • {action_type} {buy['name']}({buy['code']})："
                    f"{buy['shares']}股@{buy['price']}，金额 {buy['amount']:,.2f}元 — {buy['reason']}"
                )
            summary_lines.append("")

        # 闲置资金配置
        if idle_buy_actions:
            summary_lines.append("🔄 闲置资金配置：")
            for buy in idle_buy_actions:
                summary_lines.append(
                    f"  • {buy['name']}({buy['code']})：{buy['shares']}股@{buy['price']}，"
                    f"金额 {buy['amount']:,.2f}元"
                )
            summary_lines.append("")

        if not sell_actions and not buy_actions and not idle_buy_actions:
            summary_lines.append("⏸️ 今日无交易操作")
            summary_lines.append("")

        # 当前持仓
        summary_lines.append("📋 当前持仓：")
        if positions:
            for pos in positions:
                q = quotes.get(pos["code"])
                if q:
                    cur_val = pos["shares"] * q["price"]
                    pnl = (q["price"] - pos["costPrice"]) * pos["shares"]
                    pnl_pct = (q["price"] - pos["costPrice"]) / pos["costPrice"] * 100
                    trend_mark = "🔥趋势" if _is_trend_stock(pos) else ""
                    summary_lines.append(
                        f"  • {pos['name']}({pos['code']})：{pos['shares']}股，"
                        f"成本{pos['costPrice']}→现价{q['price']}，"
                        f"市值{cur_val:,.2f}元，盈亏{pnl:+,.2f}元({pnl_pct:+.2f}%) {trend_mark}"
                    )
                else:
                    summary_lines.append(
                        f"  • {pos['name']}({pos['code']})：{pos['shares']}股@{pos['costPrice']}（无行情）"
                    )
        else:
            summary_lines.append("  空仓")

        # 基金持仓
        if funds:
            summary_lines.append("")
            summary_lines.append("💰 基金持仓：")
            for f in funds:
                current_nav = f.get("currentNav", f["costNav"])
                pnl = (current_nav - f["costNav"]) * f["shares"]
                pnl_pct = (current_nav - f["costNav"]) / f["costNav"] * 100 if f["costNav"] > 0 else 0
                summary_lines.append(
                    f"  • {f['name']}：{f['shares']}份，"
                    f"成本{f['costNav']}→现值{current_nav:.3f}，"
                    f"盈亏{pnl:+,.2f}元({pnl_pct:+.2f}%)"
                )

        # GitHub 推送状态
        push_status = "✅ 已推送" if push_ok else "❌ 推送失败"
        summary_lines.append(f"\nGitHub 推送：{push_status}")

        message = "\n".join(summary_lines)
        print(f"\n{message}")

        # 有交易操作时 @主人
        has_trades = sell_actions or buy_actions or idle_buy_actions
        if has_trades:
            message = f"[主人](at://owner) " + message

        await sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message=message,
            data={
                "total_value": round(total_value, 2),
                "cash": round(cash, 2),
                "positions_value": round(positions_value, 2),
                "fund_value": round(fund_value, 2),
                "return_pct": round(return_pct, 2),
                "market_status": market_status,
                "position_ratio": round(current_position_ratio * 100, 1),
                "sell_count": len(sell_actions),
                "buy_count": len(buy_actions) + len(idle_buy_actions),
                "github_pushed": push_ok,
                "positions": len(positions),
            },
        )

    except Exception as e:
        print(f"[错误] {e}\n{traceback.format_exc()}")
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"每日交易脚本执行失败：{e}",
        )


if __name__ == "__main__":
    asyncio.run(main())
