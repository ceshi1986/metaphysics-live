# 核心规则

本文件适用于所有 CodeAct 脚本任务。

## 脚本结构

- 使用 `async def main()` 和 `asyncio.run(main())`。
- 在 `main()` 中实例化 `sdk = CodeActSDK()`。
- 所有可变业务值放在参数区，通过 `sys.argv` 提供默认值。
- 脚本应写入 `./codeact/scripts/`；产物写入 `./codeact/output/`。
- 如果功能接近本 Skill 的 `assets/script/` 参考脚本，优先复用其参数顺序、错误处理和提交口径。

基础脚手架：

```python
import asyncio
import sys
from codeact_sdk import CodeActSDK

async def main():
    result_mode = sys.argv[1] if len(sys.argv) > 1 else "display_only"
    param_a = sys.argv[2] if len(sys.argv) > 2 else "<默认值>"

    print(f"[参数] result_mode={result_mode}, param_a={param_a}")
    sdk = CodeActSDK()

    try:
        actual_mode = result_mode if result_mode != "auto" else "display_only"
        await sdk.submit_result(
            result_mode=actual_mode,
            status="success",
            message="结果描述",
        )
    except Exception as e:
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"执行失败: {e}",
        )

asyncio.run(main())
```

## 工具边界

- 脚本内只能通过 `sdk.call_tool()` 调用 CodeAct SDK 侧工具。
- 不得在脚本中调用 Agent 侧工具，例如 `bash`、`read_file`、`write_file`、`edit_file`、`search_web`、`fetch_web`。
- 需要命令行能力时使用 `subprocess.run()`，必须设置 `timeout`、`capture_output=True`、`text=True`。

## Schema Version

- 编写脚本前必须获取实际工具 schema。
- 每个工具使用自己的 schema version，写成顶部常量。

```python
TOOL_SCHEMA_VERSIONS = {
    "codeact_search_web": "v1_xxx",
    "codeact_fetch_web": "v1_yyy",
}

search = await sdk.call_tool(
    "codeact_search_web",
    # 时效性任务（最新/今日/近期/价格/榜单等）必须加时间过滤参数 publish_time，
    # 不要只传 query，否则会召回过期结果。完整时间窗口模板见 web-data.md「时效性」。
    {"query": query},
    schema_version=TOOL_SCHEMA_VERSIONS["codeact_search_web"],
)
```

## LLM 结构化输出

```python
from pydantic import BaseModel

class MyOutput(BaseModel):
    summary: str
    items: list[str]

result = await sdk.call_llm(
    messages=[{"role": "user", "content": "..."}],
    response_format=MyOutput,
)
```

## 结果提交

所有成功、失败、提前返回路径都必须调用 `await sdk.submit_result(...)`，不能只 `print()` 后退出。

### submit_result 结构

标准结构：

```python
await sdk.submit_result(
    result_mode=actual_mode,
    status="success",
    message=message,
    data={
        "key": "value",
    },
)
```

字段约束：

| 字段 | 必填 | 类型 | 允许值/要求 |
|---|---|---|---|
| `result_mode` | 是 | `str` | 只能是 `"display_only"`、`"notify"`、`"no_reply"`；用户传入的 `"auto"` 必须先映射，不能直接提交 |
| `status` | 是 | `str` | 只能是 `"success"` 或 `"error"` |
| `message` | 是 | `str` | 人类可读的结论、摘要、提醒、错误原因或文件入口；禁止原始 HTML、完整 JSON、错误栈、大段日志 |
| `data` | 否 | `dict` | 结构化元数据，值应可 JSON 序列化；放文件路径、URL、计数、命中状态、失败块号等机器可读信息 |

`result_mode` 语义：

| result_mode | 使用场景 | message 要求 |
|---|---|---|
| `display_only` | 查询结果、提醒、告警、用户需要直接看到的短结果 | 先结论后细节；定时提醒、阈值告警、状态变更通知必须以 `[主人](at://owner)` 开头 |
| `notify` | 需要主 Agent 继续处理、需要交付文件、或所有错误路径 | 简短说明结果或错误原因；错误时不能复用用户传入的 `result_mode` |
| `no_reply` | 监控正常、无新增、未触发阈值且无需打扰用户 | message 可使用 `"NO_REPLY"` 或极简内部说明；不要输出长内容 |

`status` 语义：

- `status="success"`：脚本按预期完成。即使部分 item 失败但已有容错结果，也可以 success，并在 `data` 里写 `failed_items`、`failed_chunks` 等字段。
- `status="error"`：脚本无法完成核心目标。必须配合 `result_mode="notify"`。

### message 内容规则

- `message` 面向用户或主 Agent，必须短、清楚、可直接展示。
- `display_only` message 先结论后细节。
- 纯查看型结果不需要 `[主人](at://owner)`。
- 定时提醒、阈值告警、状态变更通知必须以 `[主人](at://owner)` 开头。
- 长文、报告正文、原始数据、大表格应写入文件，并在 message 中给摘要和文件入口。
- 在 `message` 或回复消息中把本地文件、图片递给用户时，必须使用 `computer://` 协议的普通 Markdown 链接：`[文件名称](computer://文件绝对路径)`。
- `computer://` 后必须是文件或图片的绝对路径；不能写 `computer: \`https://...\``；不能使用图片嵌入语法 `![](computer://...)`。
- 平台会删除协议字符串，删除后剩余正文仍要通顺；不要把协议放在“请看：”这类删除后会残缺的位置。
- 网络资源（`http://` / `https://`）不使用 `computer://`，按渠道能力直接输出 URL 或 Markdown 链接。

不得在 `message` 中输出：

- 原始 HTML
- 完整 JSON 响应
- Python 错误栈
- 大段内部日志

### 错误提交

所有错误路径必须使用：

```python
await sdk.submit_result(
    result_mode="notify",
    status="error",
    message="执行失败：简短的人类可读原因",
    data={"error_type": type(e).__name__},
)
```

## 输出文件

- 脚本产物写入 `./codeact/output/`。
- 使用相对路径。
- 文件名简短有意义，只使用中文、英文、数字、下划线、短横线，避免空格。
- 不要把失败信息保存进结果文件。

生成文件时，在 `submit_result.data` 中返回结构化信息；如果 message 里需要给用户文件入口，则使用上面的 `computer://` 绝对路径链接：

```python
import os

abs_report_path = os.path.abspath(report_path)
summary_message = f"报告已生成：[完整报告](computer://{abs_report_path})"

await sdk.submit_result(
    result_mode="display_only",
    status="success",
    message=summary_message,
    data={"report_path": report_path, "report_url": report_url},
)
```


## 通用工程规则

- 处理用户上传文件时，先用 `head()`、`columns`、样例行等方式确认结构，再编写处理逻辑。
- API 链路中，前一步返回值必须显式保存并传递给下一步；遇到 429 或限流时使用指数退避重试。
- 超过 5 步的长流程，应在关键阶段把中间结果写入 `./codeact/output/`，便于失败后诊断和续跑。
- 批量操作用循环、列表推导或并发任务表达，不要重复写多段近似代码。
- 复杂数学运算必须通过代码完成，禁止直接输出未经计算的结果。
- 执行失败时不要将失败信息保存进结果文件。
- 如果代码中已经将结果保存为文件，不要重复保存。

## 参考脚本

- `assets/script/reminder.py`：最小提醒 scaffold。参考 `result_mode` 归一化、`display_only` 推送、`[主人](at://owner)` 固定提醒格式、安装时自定义文案区块。
- `assets/script/long_text_translate.py`：文件输入、输出落盘、长流程容错。参考 `submit_result.data` 返回文件路径、失败块信息只放 message/data、不污染结果文件。
- `assets/script/weather_query.py` / `assets/script/gold_price_monitor.py`：完整 CodeAct SDK 工具调用样例。参考 schema version 常量、ResponseFormat、错误路径 `notify`。
- `assets/script/feishu_doc_watch.py`：CLI 型脚本样例。参考 `subprocess.run(..., timeout=..., capture_output=True, text=True)`、授权失败上抛、状态成功后写入。
- `assets/script/daily_stock_summary.py`：大型脚本的组织样例。参考用 frozen dataclass（DomainConfig）把领域配置与 workflow 解耦、`WorkflowContext` + 分阶段 `WorkflowBlock` 串行编排、全局 try/except 兜底 `notify`、`auto` 按结果分流 `display_only/no_reply`。

## 验证

- 写完脚本后至少运行一次。
- 失败时根据 stdout、exit_code 和异常信息修复；同一问题最多重复 2 次，之后换策略。
