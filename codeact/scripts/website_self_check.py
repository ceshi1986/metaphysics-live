#!/usr/bin/env python3
"""每日网站自检脚本：检查5个网站的可访问性和关键功能，发现问题立即报告"""

import json, sys, os
from datetime import datetime

# CodeAct SDK
from codeact import CodeAct

SCRIPT_NAME = "website_self_check"

SITES = {
    "足球预测网": {
        "url": "https://ceshi1986.github.io/football-predictions/index.html",
        "checks": [
            ("schedule.json", "https://ceshi1986.github.io/football-predictions/schedule.json", "json_matches"),
            ("ai-predictions.json", "https://ceshi1986.github.io/football-predictions/data/ai-predictions.json", "json"),
            ("odds_api_odds.json", "https://ceshi1986.github.io/football-predictions/data/odds_api_odds.json", "json"),
        ],
        "min_schedule_matches": 1,
        "key_js": ["_fetchRealOdds", "_calc", "fpUpdateLockState", "_matchRealOdds"],
    },
    "玄学直播网": {
        "url": "https://ceshi1986.github.io/metaphysics-live/index.html",
        "checks": [],
        "key_js": [],
    },
    "直播助手": {
        "url": "https://ceshi1986.github.io/live-assistant/index.html",
        "checks": [],
        "key_js": [],
    },
    "AI策略网": {
        "url": "https://ceshi1986.github.io/stock-strategy/index.html",
        "checks": [
            ("portfolio.json", "https://ceshi1986.github.io/stock-strategy/portfolio.json", "json"),
        ],
        "key_js": [],
    },
    "学习转化网": {
        "url": "https://ceshi1986.github.io/xuexizhuanhua/index.html",
        "checks": [],
        "key_js": [],
    },
}


def check_url(url, timeout=15):
    """检查URL是否可访问，返回(status_code, size_bytes, error)"""
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (SelfCheck/1.0)"})
        r = urllib.request.urlopen(req, timeout=timeout)
        body = r.read()
        return r.status, len(body), None
    except Exception as e:
        return 0, 0, str(e)


def check_json_data(url, timeout=15):
    """检查JSON数据是否有效，返回(data, error)"""
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (SelfCheck/1.0)"})
        r = urllib.request.urlopen(req, timeout=timeout)
        data = json.loads(r.read().decode("utf-8"))
        return data, None
    except json.JSONDecodeError as e:
        return None, f"JSON解析失败: {e}"
    except Exception as e:
        return None, str(e)


def check_js_functions(url, functions, timeout=15):
    """检查HTML中是否包含关键JS函数定义"""
    import urllib.request
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (SelfCheck/1.0)"})
        r = urllib.request.urlopen(req, timeout=timeout)
        content = r.read().decode("utf-8", errors="replace")
        missing = []
        for fn in functions:
            if f"function {fn}" not in content and f"{fn}(" not in content:
                missing.append(fn)
        return missing, None
    except Exception as e:
        return [], str(e)


def run_check():
    """执行全部检查，返回报告"""
    results = []
    issues = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    for site_name, config in SITES.items():
        site_result = {"name": site_name, "status": "ok", "details": []}

        # 1. 主页可访问性
        status, size, err = check_url(config["url"])
        if err:
            site_result["status"] = "error"
            site_result["details"].append(f"主页访问失败: {err}")
            issues.append(f"🔴 {site_name} 主页无法访问: {err}")
        elif status != 200:
            site_result["status"] = "warn"
            site_result["details"].append(f"主页HTTP {status}")
            issues.append(f"🟡 {site_name} 主页HTTP {status}")
        else:
            site_result["details"].append(f"主页正常 ({size} bytes)")

        # 2. 数据源检查
        for check_name, check_url, check_type in config.get("checks", []):
            if check_type == "json_matches":
                data, err = check_json_data(check_url)
                if err:
                    site_result["details"].append(f"{check_name}: {err}")
                    if "schedule" in check_name.lower():
                        issues.append(f"🔴 {site_name} {check_name}加载失败: {err}")
                else:
                    matches = data if isinstance(data, list) else data.get("matches", [])
                    count = len(matches) if isinstance(matches, list) else 0
                    site_result["details"].append(f"{check_name}: {count}条")
                    min_count = config.get("min_schedule_matches", 0)
                    if count < min_count:
                        issues.append(f" {site_name} {check_name}数据不足({count}<{min_count})")
            else:
                data, err = check_json_data(check_url)
                if err:
                    site_result["details"].append(f"{check_name}: {err}")
                    issues.append(f" {site_name} {check_name}: {err}")
                else:
                    count = len(data) if isinstance(data, list) else len(data.keys())
                    site_result["details"].append(f"{check_name}: {count}条")

        # 3. 关键JS函数检查
        key_js = config.get("key_js", [])
        if key_js:
            missing, err = check_js_functions(config["url"], key_js)
            if err:
                site_result["details"].append(f"JS检查失败: {err}")
            elif missing:
                site_result["status"] = "error" if site_result["status"] == "ok" else site_result["status"]
                site_result["details"].append(f"缺少JS函数: {','.join(missing)}")
                issues.append(f"🔴 {site_name} 缺少关键JS函数: {','.join(missing)}")
            else:
                site_result["details"].append(f"JS函数({len(key_js)}个)全部存在")

        results.append(site_result)

    # 4. 足球预测网专项：检查赔率数据源优先级
    fp_result = results[0]  # 足球预测网是第一个
    if fp_result["status"] == "ok":
        # 验证 schedule.json 中的赔率数据
        schedule_data, err = check_json_data("https://ceshi1986.github.io/football-predictions/schedule.json")
        if not err and schedule_data:
            matches = schedule_data if isinstance(schedule_data, list) else schedule_data.get("matches", [])
            odds_count = sum(1 for m in matches if m.get("odds"))
            fp_result["details"].append(f"schedule.json含赔率比赛: {odds_count}/{len(matches)}")
            if odds_count == 0 and len(matches) > 0:
                issues.append(f"🟡 足球预测网 schedule.json 无赔率数据（{len(matches)}场比赛均无odds）")

    # 汇总报告
    ok_count = sum(1 for r in results if r["status"] == "ok")
    total = len(results)

    report_lines = [f"📋 每日网站自检报告 ({now})"]
    report_lines.append(f"{'='*40}")
    for r in results:
        icon = "✅" if r["status"] == "ok" else ("️" if r["status"] == "warn" else "❌")
        report_lines.append(f"{icon} {r['name']} [{r['status'].upper()}]")
        for d in r["details"]:
            report_lines.append(f"   └ {d}")

    if issues:
        report_lines.append(f"\n{'='*40}")
        report_lines.append("⚠️ 发现问题：")
        for issue in issues:
            report_lines.append(f"  {issue}")
    else:
        report_lines.append(f"\n 全部 {total} 个网站检查通过")

    report = "\n".join(report_lines)
    return report, issues


def main():
    result_mode = sys.argv[1] if len(sys.argv) > 1 else "notify"
    report, issues = run_check()

    if not issues:
        # 无问题，安静返回
        print(report)
        if result_mode == "notify":
            CodeAct.notify("NO_REPLY")
        else:
            CodeAct.display(report)
    else:
        # 有问题，报告
        print(report)
        if result_mode == "notify":
            CodeAct.notify(report)
        else:
            CodeAct.display(report)


if __name__ == "__main__":
    main()
