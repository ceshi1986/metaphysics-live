#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""每日自动交易脚本：更新策略网投资组合数据并推送 GitHub。

执行流程：
1. 从 GitHub 读取 portfolio.json
2. 通过腾讯行情 API 获取持仓 + 候选池实时价格（GBK 编码→UTF-8）
3. 检查止盈止损条件，执行卖出
4. 如有可用资金且持仓 < 3 只，扫描候选池寻找买入机会
5. 记录今日快照到 dailySnapshots
6. 更新 portfolio.json 并推送 GitHub

交易规则（投资人核心策略：1-3成总仓位控制）：
- 止损逻辑：
  - 趋势股 → 减仓70%保留30%（轻仓保留等企稳）
  - 非趋势股或刚买入就跌破止损 → 全部卖出（果断清仓等企稳）
- 趋势股定义（三维度任一满足）：
  - 技术趋势：历史最高价 > 成本价*1.02（曾有过浮盈）
  - 赛道趋势：属于未来趋势行业（新能源、半导体、AI等）
  - 龙头长期看好：买入时标记为赛道龙头，强者恒强
- 止盈线：持仓盈利 > 15% → 卖出一半
- 买入条件：涨幅 > 3% 且振幅 > 4% 且成交额 > 1 亿 → 用可用资金的 30% 买入
- 每次最多持 3 只股票
- 买入单位为 100 股整数倍
- 候选买入池：贵州茅台/宁德时代/中国平安/比亚迪/招商银行/长江电力/美的集团/中国中免

仓位管理原则：
- 趋势向下：保持1-3成轻仓，减仓但绝不清仓
- 趋势企稳：观察站稳后逐步加仓
- 趋势向上：顺势重仓

参数（codeact_args）：result_mode, github_repo, initial_capital
- result_mode: auto / display_only / notify / no_reply；auto 时始终 display_only
- github_repo: GitHub 仓库地址，默认 ceshi1986/stock-strategy
- initial_capital: 初始资金，默认 50000
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
    s.trust_env = False  # 不使用环境变量中的代理配置
    return s

HTTP = _make_session()

# ===== SDK 工具版本 =====
TOOL_SCHEMA_VERSIONS = {
    "codeact_fetch_web": "v1_2c8d0580b3f93a58",
    "codeact_search_web": "v1_5ac1b0eba8c26f2a",
}

# ===== 常量 =====
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={codes}"
GITHUB_API_BASE = "https://api.github.com/repos/{repo}/contents/portfolio.json"

# 候选买入池
CANDIDATE_POOL = [
    {"code": "sh600519", "name": "贵州茅台"},
    {"code": "sz300750", "name": "宁德时代"},
    {"code": "sh601318", "name": "中国平安"},
    {"code": "sz002594", "name": "比亚迪"},
    {"code": "sh600036", "name": "招商银行"},
    {"code": "sh600900", "name": "长江电力"},
    {"code": "sz000333", "name": "美的集团"},
    {"code": "sh601888", "name": "中国中免"},
]

# 交易参数
STOP_LOSS_PCT = -0.10       # 止损线：亏损 > 10%
TAKE_PROFIT_PCT = 0.15      # 止盈线：盈利 > 15%
BUY_CHANGE_PCT = 3.0        # 买入条件：涨幅 > 3%
BUY_AMPLITUDE_PCT = 4.0     # 买入条件：振幅 > 4%
BUY_TURNOVER_YI = 1.0       # 买入条件：成交额 > 1 亿
BUY_CASH_RATIO = 0.30       # 买入资金占比
MAX_POSITIONS = 3            # 最多持仓数
LOT_SIZE = 100               # 买入单位

CST = timezone(timedelta(hours=8))


# ===== 腾讯行情 API 解析 =====

def fetch_quotes(codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """批量获取腾讯行情数据，返回 {code: {price, name, change_pct, high, low, amplitude, turnover_yi, ...}}"""
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
            code_raw = parts[2]  # 000858
            name = parts[1].replace(" ", "")  # 五粮液
            current_price = float(parts[3])
            yesterday_close = float(parts[4])
            change_pct = float(parts[32]) if parts[32] else 0.0
            high = float(parts[33]) if parts[33] else current_price
            low = float(parts[34]) if parts[34] else current_price
            # 振幅字段
            amplitude = float(parts[43]) if len(parts) > 43 and parts[43] else 0.0
            # 成交额（万元）→ 亿元
            turnover_wan = float(parts[57]) if len(parts) > 57 and parts[57] else 0.0
            turnover_yi = turnover_wan / 10000.0

            # 推断带前缀的 code（prefix 已包含完整代码如 sz000858）
            full_code = line.split("=")[0].replace("v_", "").strip()
            if not full_code:
                full_code = code_raw

            result[full_code] = {
                "code": full_code,
                "name": name,
                "price": current_price,
                "yesterday_close": yesterday_close,
                "change_pct": change_pct,
                "high": high,
                "low": low,
                "amplitude": amplitude,
                "turnover_yi": turnover_yi,
                "raw_code": code_raw,
            }
        except (ValueError, IndexError) as e:
            print(f"[行情] 解析行失败: {e}, line={line[:80]}...")
            continue

    return result


# ===== GitHub API =====

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
    content_b64 = data["content"]
    content = base64.b64decode(content_b64).decode("utf-8")
    portfolio = json.loads(content)
    return portfolio, sha


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
        print(f"[GitHub] 推送成功")
        return True
    else:
        print(f"[GitHub] 推送失败: {resp.status_code} {resp.text[:200]}")
        return False


# ===== 交易逻辑 =====

def check_stop_loss(position: Dict[str, Any], quote: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """检查止损条件：
    - 趋势股（行业趋势 / 长期看好龙头 / 技术趋势）→ 轻仓保留30%
    - 非趋势股或刚买入就跌破止损 → 果断清仓，等企稳
    """
    cost = position["costPrice"]
    current = quote["price"]
    pnl_pct = (current - cost) / cost

    if pnl_pct < STOP_LOSS_PCT:
        # 判断是否是趋势股（三个维度任一满足即可）
        highest_price = position.get("highestPrice", cost)
        tech_trend = highest_price > cost * 1.02  # 技术趋势：曾有超过2%的浮盈
        sector_trend = _is_future_trend_sector(position.get("sector", ""))  # 行业趋势：未来趋势赛道
        company_trend = position.get("longTermBullish", False)  # 公司趋势：长期看好的龙头股

        if tech_trend or sector_trend or company_trend:
            # 趋势股短期回调 → 轻仓保留30%
            keep_shares = (position["shares"] * 0.3) // LOT_SIZE * LOT_SIZE
            if keep_shares < LOT_SIZE:
                keep_shares = LOT_SIZE
            if keep_shares >= position["shares"]:
                keep_shares = position["shares"]
            sell_shares = position["shares"] - keep_shares

            trend_type = []
            if tech_trend:
                trend_type.append(f"技术趋势(曾最高{highest_price:.2f},浮盈{(highest_price-cost)/cost*100:.1f}%)")
            if sector_trend:
                trend_type.append(f"赛道趋势({position.get('sector', '未知')}行业)")
            if company_trend:
                trend_type.append("龙头长期看好")

            return {
                "action": "sell",
                "shares": sell_shares,
                "reason": f"趋势股止损减仓：{' + '.join(trend_type)}，现跌{pnl_pct*100:.1f}%，保留{keep_shares}股",
                "pnl_pct": pnl_pct,
            }
        else:
            # 非趋势股或从未盈利就跌破止损 → 清仓或保留10%底仓（资金要灵活调动）
            # 如果持仓量足够（>=300股），保留10%底仓观察；否则全清
            if position["shares"] >= 300:
                keep_shares = int(position["shares"] * 0.1) // LOT_SIZE * LOT_SIZE
                if keep_shares >= LOT_SIZE:
                    sell_shares = position["shares"] - keep_shares
                    return {
                        "action": "sell",
                        "shares": sell_shares,
                        "reason": f"止损减仓：非趋势股，现亏{pnl_pct*100:.2f}%，保留{keep_shares}股底仓观察",
                        "pnl_pct": pnl_pct,
                    }
            # 持仓量小或计算后不足100股，全清
            sell_shares = position["shares"] // LOT_SIZE * LOT_SIZE
            if sell_shares < LOT_SIZE:
                sell_shares = position["shares"]
            return {
                "action": "sell",
                "shares": sell_shares,
                "reason": f"止损清仓：非趋势股，现亏{pnl_pct*100:.2f}% < {STOP_LOSS_PCT*100:.0f}%",
                "pnl_pct": pnl_pct,
            }
    return None


def check_take_profit(position: Dict[str, Any], quote: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """检查止盈条件：盈利 > 15% → 卖出一半"""
    cost = position["costPrice"]
    current = quote["price"]
    pnl_pct = (current - cost) / cost

    if pnl_pct > TAKE_PROFIT_PCT:
        sell_shares = position["shares"] // 2
        if sell_shares < LOT_SIZE:
            sell_shares = LOT_SIZE
        if sell_shares > position["shares"]:
            sell_shares = position["shares"]
        return {
            "action": "sell",
            "shares": sell_shares,
            "reason": f"止盈卖出：盈利{pnl_pct*100:.2f}% > {TAKE_PROFIT_PCT*100:.0f}%，卖出一半",
            "pnl_pct": pnl_pct,
        }
    return None


def check_buy_candidate(
    quote: Dict[str, Any], cash: float, current_positions: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """检查买入条件：涨幅 > 3% 且振幅 > 4% 且成交额 > 1 亿"""
    # 已持有的不买
    held_codes = {p["code"] for p in current_positions}
    if quote["code"] in held_codes:
        return None

    change_pct = quote["change_pct"]
    amplitude = quote["amplitude"]
    turnover_yi = quote["turnover_yi"]

    if change_pct <= BUY_CHANGE_PCT:
        return None
    if amplitude <= BUY_AMPLITUDE_PCT:
        return None
    if turnover_yi <= BUY_TURNOVER_YI:
        return None

    # 计算买入股数
    buy_amount = cash * BUY_CASH_RATIO
    price = quote["price"]
    shares = int(buy_amount / price) // LOT_SIZE * LOT_SIZE
    if shares < LOT_SIZE:
        return None  # 资金不够买 1 手

    return {
        "action": "buy",
        "code": quote["code"],
        "name": quote["name"],
        "price": price,
        "shares": shares,
        "reason": f"技术买入：涨幅{change_pct:.2f}%>{BUY_CHANGE_PCT}%，振幅{amplitude:.2f}%>{BUY_AMPLITUDE_PCT}%，成交额{turnover_yi:.2f}亿>{BUY_TURNOVER_YI}亿",
    }


# ===== 主流程 =====

async def main() -> None:
    result_mode_raw = sys.argv[1] if len(sys.argv) > 1 else "auto"
    github_repo = sys.argv[2] if len(sys.argv) > 2 else "ceshi1986/stock-strategy"
    initial_capital = float(sys.argv[3]) if len(sys.argv) > 3 else 50000.0

    sdk = CodeActSDK()
    try:
        mode = (result_mode_raw or "auto").strip().lower()
        if mode not in {"auto", "display_only", "notify", "no_reply"}:
            raise ValueError("result_mode 只能是 auto / display_only / notify / no_reply")

        # 交易脚本有交易行为，始终 display_only 通知用户
        actual_mode = "display_only" if mode == "auto" else mode

        print(f"[参数] result_mode={mode}→{actual_mode}, repo={github_repo}, initial_capital={initial_capital}")

        # 从 SECRET.md 读取 GitHub Token
        github_token = ""
        # 尝试多个可能的路径
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

        # 如果 SECRET.md 未找到 token，使用环境变量或参数
        if not github_token:
            github_token = os.environ.get("GITHUB_TOKEN", "")

        if not github_token:
            raise ValueError("未找到 GitHub Token，请在 SECRET.md 中设置 GITHUB_TOKEN 或通过环境变量传入")

        print(f"[GitHub] Token 已获取，长度={len(github_token)}")

        # ===== 1. 从 GitHub 读取 portfolio.json =====
        print("[步骤1] 从 GitHub 读取 portfolio.json...")
        portfolio, sha = read_portfolio_from_github(github_repo, github_token)
        state = portfolio.get("state", {})
        cash = state.get("cash", 0)
        positions = state.get("positions", [])
        trades = portfolio.get("trades", [])
        snapshots = portfolio.get("dailySnapshots", [])
        stats = portfolio.get("stats", {})
        meta = portfolio.get("meta", {})

        print(f"[状态] 现金={cash:.2f}, 持仓数={len(positions)}")
        for p in positions:
            print(f"  {p['name']}({p['code']}): {p['shares']}股 @ {p['costPrice']}")

        # ===== 2. 获取所有持仓 + 候选池的实时行情 =====
        print("[步骤2] 获取实时行情...")
        all_codes = [p["code"] for p in positions]
        candidate_codes = [c["code"] for c in CANDIDATE_POOL]
        # 合并去重
        fetch_codes = list(dict.fromkeys(all_codes + candidate_codes))

        quotes = fetch_quotes(fetch_codes)
        print(f"[行情] 获取到 {len(quotes)} 只股票行情")
        for code, q in quotes.items():
            print(f"  {q['name']}({code}): 现价={q['price']}, 涨幅={q['change_pct']}%, 振幅={q['amplitude']}%, 成交额={q['turnover_yi']:.2f}亿")

        # ===== 3. 检查止盈止损 =====
        print("[步骤3] 检查止盈止损...")
        today_str = datetime.now(CST).strftime("%Y-%m-%d")
        now_time_str = datetime.now(CST).strftime("%H:%M:%S")
        sell_actions = []

        for pos in list(positions):
            code = pos["code"]
            if code not in quotes:
                print(f"  [警告] {pos['name']}({code}) 无行情数据，跳过")
                continue

            quote = quotes[code]
            current_price = quote["price"]

            # 更新最高价（用于判断技术趋势）
            prev_highest = pos.get("highestPrice", pos["costPrice"])
            if current_price > prev_highest:
                pos["highestPrice"] = current_price
                print(f"  [更新] {pos['name']}({code}) 最高价: {prev_highest:.2f} → {current_price:.2f}")

            # 先检查止损
            stop_loss = check_stop_loss(pos, quote)
            if stop_loss:
                sell_actions.append((pos, stop_loss, quote))
                print(f"  [止损] {pos['name']}({code}): {stop_loss['reason']}")
                continue

            # 再检查止盈
            take_profit = check_take_profit(pos, quote)
            if take_profit:
                sell_actions.append((pos, take_profit, quote))
                print(f"  [止盈] {pos['name']}({code}): {take_profit['reason']}")

        # 执行卖出
        for pos, action, quote in sell_actions:
            sell_shares = min(action["shares"], pos["shares"])
            sell_price = quote["price"]
            sell_amount = sell_shares * sell_price
            pnl = (sell_price - pos["costPrice"]) * sell_shares

            # 记录交易
            trade = {
                "date": today_str,
                "time": now_time_str,
                "code": pos["code"],
                "name": pos["name"],
                "action": "sell",
                "price": sell_price,
                "shares": sell_shares,
                "amount": round(sell_amount, 2),
                "pnl": round(pnl, 2),
                "reason": action["reason"],
            }
            trades.append(trade)

            # 更新持仓
            if sell_shares >= pos["shares"]:
                # 全部卖出
                positions = [p for p in positions if p["code"] != pos["code"]]
                cash += sell_amount
                print(f"  [卖出] {pos['name']} 全部卖出 {sell_shares}股@{sell_price}，金额={sell_amount:.2f}，盈亏={pnl:.2f}")
            else:
                # 部分卖出
                for p in positions:
                    if p["code"] == pos["code"]:
                        p["shares"] -= sell_shares
                        break
                cash += sell_amount
                print(f"  [卖出] {pos['name']} 部分卖出 {sell_shares}股@{sell_price}，金额={sell_amount:.2f}，盈亏={pnl:.2f}")

        # ===== 4. 扫描候选池寻找买入机会 =====
        print("[步骤4] 扫描候选池寻找买入机会...")
        buy_actions = []

        if len(positions) < MAX_POSITIONS and cash >= LOT_SIZE * 10:  # 至少有买1手的钱
            # 按涨幅排序候选
            buy_candidates = []
            for candidate in CANDIDATE_POOL:
                code = candidate["code"]
                if code not in quotes:
                    continue
                quote = quotes[code]
                buy_check = check_buy_candidate(quote, cash, positions)
                if buy_check:
                    buy_candidates.append((buy_check, quote))

            # 按涨幅降序排列
            buy_candidates.sort(key=lambda x: x[1]["change_pct"], reverse=True)

            # 逐个买入，每次买入后更新现金
            for buy_action, quote in buy_candidates:
                if len(positions) >= MAX_POSITIONS:
                    break
                if cash < LOT_SIZE * quote["price"]:
                    break

                # 重新计算买入股数（基于当前现金）
                buy_amount = cash * BUY_CASH_RATIO
                shares = int(buy_amount / quote["price"]) // LOT_SIZE * LOT_SIZE
                if shares < LOT_SIZE:
                    continue

                actual_amount = shares * quote["price"]
                if actual_amount > cash:
                    shares = int(cash / quote["price"]) // LOT_SIZE * LOT_SIZE
                    actual_amount = shares * quote["price"]
                    if shares < LOT_SIZE:
                        continue

                buy_actions.append({
                    "code": quote["code"],
                    "name": quote["name"],
                    "price": quote["price"],
                    "shares": shares,
                    "amount": actual_amount,
                    "reason": buy_action["reason"],
                })
                # 预扣资金
                cash -= actual_amount

        # 执行买入
        for buy in buy_actions:
            # 记录交易
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

            # 新增持仓
            new_pos = {
                "code": buy["code"],
                "name": buy["name"],
                "shares": buy["shares"],
                "costPrice": buy["price"],
                "highestPrice": buy["price"],  # 初始最高价=买入价
                "buyDate": today_str,
                "sector": _guess_sector(buy["code"]),
            }
            positions.append(new_pos)
            print(f"  [买入] {buy['name']}({buy['code']}) {buy['shares']}股@{buy['price']}，金额={buy['amount']:.2f}")

        # ===== 5. 记录今日快照 =====
        print("[步骤5] 记录今日快照...")
        positions_value = 0.0
        for pos in positions:
            if pos["code"] in quotes:
                positions_value += pos["shares"] * quotes[pos["code"]]["price"]
            else:
                positions_value += pos["shares"] * pos["costPrice"]

        total_value = cash + positions_value
        return_pct = (total_value - initial_capital) / initial_capital * 100

        # 检查今天是否已有快照，有则更新
        today_snapshot_idx = None
        for i, snap in enumerate(snapshots):
            if snap.get("date") == today_str:
                today_snapshot_idx = i
                break

        snapshot = {
            "date": today_str,
            "totalValue": round(total_value, 2),
            "cash": round(cash, 2),
            "positionsValue": round(positions_value, 2),
            "return": round(return_pct, 2),
        }

        if today_snapshot_idx is not None:
            snapshots[today_snapshot_idx] = snapshot
        else:
            snapshots.append(snapshot)

        # ===== 6. 更新 portfolio 并推送 =====
        print("[步骤6] 更新 portfolio.json 并推送 GitHub...")
        portfolio["state"]["cash"] = round(cash, 2)
        portfolio["state"]["positions"] = positions
        portfolio["state"]["lastUpdate"] = today_str
        portfolio["trades"] = trades
        portfolio["dailySnapshots"] = snapshots

        # 更新统计
        total_trades = stats.get("totalTrades", 0) + len(sell_actions) + len(buy_actions)
        win_trades = stats.get("winTrades", 0)
        for pos, action, quote in sell_actions:
            if action["pnl_pct"] > 0:
                win_trades += 1
        loss_trades = len(sell_actions) - (win_trades - stats.get("winTrades", 0))
        win_rate = round(win_trades / total_trades * 100, 2) if total_trades > 0 else 0

        portfolio["stats"] = {
            "totalTrades": total_trades,
            "winTrades": win_trades,
            "lossTrades": loss_trades,
            "winRate": win_rate,
            "maxDrawdown": stats.get("maxDrawdown", 0),
            "currentStreak": stats.get("currentStreak", 0),
        }

        push_ok = push_portfolio_to_github(github_repo, github_token, portfolio, sha)

        if not push_ok:
            # GitHub 推送失败，尝试重新获取 SHA 后重试
            print("[GitHub] 推送失败，尝试重新获取 SHA...")
            try:
                _, new_sha = read_portfolio_from_github(github_repo, github_token)
                push_ok = push_portfolio_to_github(github_repo, github_token, portfolio, new_sha)
            except Exception as e:
                print(f"[GitHub] 重试也失败: {e}")

        # ===== 7. 构建交易摘要 =====
        summary_lines = []
        summary_lines.append("📊 每日交易摘要")
        summary_lines.append(f"日期：{today_str}")
        summary_lines.append("")

        # 总资产
        summary_lines.append(f"💰 总资产：{total_value:,.2f} 元（现金 {cash:,.2f} + 持仓 {positions_value:,.2f}）")
        summary_lines.append(f"📈 累计收益率：{return_pct:+.2f}%（初始资金 {initial_capital:,.0f}）")
        summary_lines.append("")

        # 卖出操作
        if sell_actions:
            summary_lines.append("🔴 卖出操作：")
            for pos, action, quote in sell_actions:
                sell_shares = min(action["shares"], pos["shares"])
                pnl = (quote["price"] - pos["costPrice"]) * sell_shares
                summary_lines.append(
                    f"  • {pos['name']}({pos['code']})：卖出 {sell_shares}股@{quote['price']}，"
                    f"盈亏 {pnl:+,.2f}元，{action['reason']}"
                )
            summary_lines.append("")

        # 买入操作
        if buy_actions:
            summary_lines.append("🟢 买入操作：")
            for buy in buy_actions:
                summary_lines.append(
                    f"  • {buy['name']}({buy['code']})：买入 {buy['shares']}股@{buy['price']}，"
                    f"金额 {buy['amount']:,.2f}元，{buy['reason']}"
                )
            summary_lines.append("")

        if not sell_actions and not buy_actions:
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
                    summary_lines.append(
                        f"  • {pos['name']}({pos['code']})：{pos['shares']}股，"
                        f"成本 {pos['costPrice']}→现价 {q['price']}，"
                        f"市值 {cur_val:,.2f}元，盈亏 {pnl:+,.2f}元({pnl_pct:+.2f}%)"
                    )
                else:
                    summary_lines.append(
                        f"  • {pos['name']}({pos['code']})：{pos['shares']}股@{pos['costPrice']}（无行情）"
                    )
        else:
            summary_lines.append("  空仓")

        # GitHub 推送状态
        summary_lines.append("")
        push_status = "✅ 已推送" if push_ok else "❌ 推送失败"
        summary_lines.append(f"GitHub 推送：{push_status}")

        message = "\n".join(summary_lines)
        print(f"\n{message}")

        # 交易有变化时 @主人
        if sell_actions or buy_actions:
            message = f"[主人](at://owner) " + message

        await sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message=message,
            data={
                "total_value": round(total_value, 2),
                "cash": round(cash, 2),
                "positions_value": round(positions_value, 2),
                "return_pct": round(return_pct, 2),
                "sell_count": len(sell_actions),
                "buy_count": len(buy_actions),
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


def _guess_sector(code: str) -> str:
    """根据代码猜测板块"""
    sector_map = {
        "sh600519": "消费", "sz300750": "新能源", "sh601318": "金融",
        "sz002594": "新能源", "sh600036": "金融", "sh600900": "电力",
        "sz000333": "家电", "sh601888": "消费",
    }
    return sector_map.get(code, "其他")


# 未来趋势行业（符合长期发展方向的板块）
FUTURE_TREND_SECTORS = {
    "新能源", "半导体", "芯片", "人工智能", "AI", "新能源车",
    "光伏", "储能", "生物医药", "创新药", "军工", "航空航天",
    "数字经济", "云计算", "5G", "物联网", "机器人",
}


def _is_future_trend_sector(sector: str) -> bool:
    """判断是否是未来趋势行业（长期看好的板块）"""
    return sector in FUTURE_TREND_SECTORS


if __name__ == "__main__":
    asyncio.run(main())
