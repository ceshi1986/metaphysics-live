# 共享状态与增量运行规则

当脚本会定时运行、重复运行、监控新增内容、去重、续跑或维护历史基线时加载本文件。

## 参考脚本

- 飞书文档更新监控：参考 `assets/script/feishu_doc_watch.py`。
  - 适用于文档更新、内容变更、重复运行监控、冷启动基线。
  - 重点参考：SQLite `doc_state`、token 主键、正文快照、首次基线、成功提交后写状态、失败不写状态。
- 定时提醒 scaffold：参考 `assets/script/reminder.py`。
  - 适用于喝水/吃药/会议/运动等周期提醒。
  - 重点参考：`auto` 归一到 `display_only`、消息开头 `[主人](at://owner)`、无外部状态的轻量提醒。
- 阈值监控分流：参考 `assets/script/gold_price_monitor.py`。
  - 适用于价格/指标达到阈值才提醒；未触发时 `no_reply`。
- 增量报告状态库：参考 `assets/script/moore_threads_daily_briefing.py`。
  - 适用于跨来源报告、跨次去重、历史基线、新增/延续判断。
  - 重点参考：来源表、信息单元表、报告快照表、运行记录表，以及“成功后写状态、失败不写状态”。
  - 模块化最佳实践见 `references/agentic-report-blocks.md`。
- baseline/增量双窗口 + 版本化缓存：参考 `assets/script/daily_stock_summary.py`。
  - 适用于每日重复运行、首跑补历史底料、之后只捞增量、并对昂贵中间结果做缓存的场景。
  - 重点参考：`_is_baseline_run` 冷启动判定、baseline 长窗口 vs 增量短窗口切换、`seen_sources` 去重、
    `runs` 运行记录、`DailyCache` 带 TTL 的版本化缓存（cache_key 绑定脚本/工具/prompt 版本，改版即失效）。

## 状态存储

- 使用 SQLite。
- 路径为 `./codeact/output/<业务域>_state.db`。
- 状态表必须有 `PRIMARY KEY` 或 `UNIQUE` 约束。
- 写入使用 `INSERT OR IGNORE` 或 `ON CONFLICT`。

SQLite 状态库模板：

```python
import sqlite3

DB_PATH = "./codeact/output/news_state.db"
conn = sqlite3.connect(DB_PATH)
conn.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        url TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
""")

existing = {row[0] for row in conn.execute("SELECT url FROM articles")}
new_items = [item for item in candidates if item["url"] not in existing]
conn.executemany(
    "INSERT OR IGNORE INTO articles (url, title, created_at) VALUES (?, ?, ?)",
    [(item["url"], item["title"], item["created_at"]) for item in new_items],
)
conn.commit()
conn.close()
```

## 读-执行-写闭环

- 执行前读取已有状态。
- 执行业务逻辑并核验结果。
- 成功后写入新状态。
- 失败时不要写入状态，避免漏处理。

## 入库不等于上屏

- 日报/监控类脚本中，历史条目、日期不明条目、已见条目可以入库去重。
- 但这些条目不能进入本次 `submit_result.message`。
- 只有“新近 + 未报告过 + 已核验”的条目才展示给用户。

## 冷启动基线

长期增量任务不应把第一次运行直接当作日报增量。

推荐模式：

- `auto`：默认模式。未完成基线时走 `init`，完成后走 `daily`。
- `init`：扩大搜索范围，构建历史状态库，只输出初始化完成摘要，不展示历史明细。
- `daily`：只展示新近增量。

状态库中使用 `meta` 表记录基线是否完成：

```python
conn.execute("""
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
""")

baseline_completed = get_meta("baseline_completed", "0") == "1"
actual_mode = "daily" if run_mode == "auto" and baseline_completed else "init"
```

完成初始化后写入：

```python
set_meta("baseline_completed", "1")
set_meta("baseline_completed_at", now.isoformat(timespec="seconds"))
```

## 状态契约

如果脚本依赖 state.db，需要在 `index.json` 的 description 中说明状态表名和去重口径。
