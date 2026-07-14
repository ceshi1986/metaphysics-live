# Skill 调用与 CLI 规则

当任务需要使用其他 Skill、Skill 提供的 CLI/本地脚本、飞书 lark-cli，或用户明确指定某个 Skill 时加载本文件。

## 参考脚本

- 飞书文档监控：参考 `assets/script/feishu_doc_watch.py`。
  - 适用于“读取飞书文档、监控飞书文档更新、生成文档变更总结”。
  - 重点参考：脚本内用 `subprocess.run(["lark-cli", ...], timeout=..., capture_output=True, text=True)`，先检查授权，CLI 失败统一上抛后 `notify`。
- 行情/数据 CLI + SDK 工具混合脚本：参考 `assets/script/daily_stock_summary.py`。
  - 适用于“既要用 Skill/本地 CLI 取结构化数据，又要用 SDK 工具搜索/抓取网页，并把两者融合成报告”。
  - 重点参考：`_run_cli` 统一封装 `subprocess.run(..., capture_output=True, text=True, timeout=...)` 调用 `westockdata` CLI，
    `_run_cli_async` 用 `asyncio.to_thread` 把同步 CLI 包成协程以便并发，CLI 返回非 JSON 时（如 Markdown 表格）单独写解析函数，
    单支失败降级为逐个重试。CLI 取到的行情走本文件规则，网页搜索/抓取则按 `references/web-data.md` 用 `sdk.call_tool()`——两类调用边界不要混用。
- 非 CLI 的 CodeAct SDK 工具脚本不要套用本文件模板；天气/金价类参考 `references/web-data.md` 和对应 `assets/script/weather_query.py`、`assets/script/gold_price_monitor.py`。

## Agent 侧与 CodeAct 脚本内的边界

Skill 的选择、加载、参考文档阅读、鉴权检查属于 Agent 侧准备工作。

Agent 侧负责：

- 用 `skill_load` 读取 `SKILL.md`。
- 按 Skill 指引读取必要的 `references/`。
- 如需鉴权，用 Agent 侧 bash 或 Skill 指令先确认状态。
- 明确 CodeAct 脚本运行时应该调用哪个 CLI、本地脚本、SDK 工具或 Python 库。

CodeAct 脚本内禁止调用 Agent 侧 Skill 工具：

- 禁止 `await sdk.call_tool("skill_load", ...)`
- 禁止 `await sdk.call_tool("skill_fill_variable", ...)`
- 禁止 `await sdk.call_tool("coze_skill_dynamic_expand", ...)`

CodeAct 脚本内只能用以下方式承接 Skill 能力：

- Skill 文档指定的 CLI：用 `subprocess.run([...], capture_output=True, text=True, timeout=N)`。
- Skill 文档指定的本地脚本：用 `subprocess.run()` 调用，优先要求 JSON 输出。
- CodeAct SDK schema 中存在的工具：用 `sdk.call_tool()` 并传对应 `schema_version`。
- 普通 Python 库：正常 import；必要时按依赖规则安装。

## Agent 侧准备示例

以下是写脚本前的 Agent 侧操作，不写进 CodeAct 脚本：

```text
1. skill_load("lark_cli")，阅读 SKILL.md，确认读取云文档应使用 lark-cli。
2. bash 执行 `lark-cli auth status --output json`，确认已授权。
3. 将脚本内运行时调用设计为 subprocess.run(["lark-cli", ...])。
```

## 脚本内调用 Skill CLI 的通用模板

```python
import json
import subprocess

def run_cli_json(cmd: list[str], timeout: int = 60) -> dict:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"CLI 执行失败: {err[:300]}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"CLI 未返回合法 JSON: {e}")

# 示例：Skill 文档要求使用某个 CLI，并支持 JSON 输出
data = run_cli_json(["some-skill-cli", "query", "--target", target, "--output", "json"])
```

## 脚本内调用飞书 lark-cli 的模板

```python
import json
import subprocess

def lark_cli(args: list[str], timeout: int = 60) -> dict:
    result = subprocess.run(
        ["lark-cli", *args, "--output", "json"],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "lark-cli 调用失败").strip()[:300])
    return json.loads(result.stdout)

# 编写脚本前，Agent 侧必须已用 bash 验证过 `lark-cli auth status`
doc = lark_cli(["doc", "get", "--url", doc_url])
```

## 完整 CodeAct 脚本示例

```python
import asyncio
import json
import subprocess
import sys
from codeact_sdk import CodeActSDK

result_mode = sys.argv[1] if len(sys.argv) > 1 else "display_only"
doc_url = sys.argv[2] if len(sys.argv) > 2 else ""

def run_json_cli(cmd: list[str], timeout: int = 60) -> dict:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        err = (result.stderr or result.stdout or "").strip()
        raise RuntimeError(f"命令执行失败: {err[:300]}")
    return json.loads(result.stdout)

async def main():
    sdk = CodeActSDK()
    try:
        # lark_cli Skill 要求使用 lark-cli；Agent 侧已在写脚本前确认授权
        doc = run_json_cli(["lark-cli", "doc", "get", "--url", doc_url, "--output", "json"])
        title = doc.get("title", "未命名文档")
        await sdk.submit_result(
            result_mode=result_mode,
            status="success",
            message=f"已读取文档：{title}",
            data={"title": title},
        )
    except Exception as e:
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"执行失败：{e}",
        )

asyncio.run(main())
```

## 飞书专项

- 任务涉及飞书相关操作时，优先加载 lark_cli Skill，除非它无法完成或任务明确指定其他飞书技能。
- lark_cli 的授权链接必须通过 bash 执行飞书 CLI 命令获得，禁止自行拼接字符串。
- 编写脚本前先用 bash 执行 `lark-cli auth status` 确认授权状态，已授权才在脚本中使用 lark-cli。
