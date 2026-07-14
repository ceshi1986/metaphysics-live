#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Heartbeat 读取器 CodeAct 脚本

每30分钟由 Calendar 触发，读取工作目录下的 HEARTBEAT.md 文件，
将内容通过 submit_result 以 result_mode=notify 传递给主 agent。
替代系统内置 Heartbeat 功能，由用户通过 HEARTBEAT.md 文件控制通知内容。

参数顺序（codeact_args）：result_mode, heartbeat_path
- result_mode: 有内容时使用的提交模式，默认 "notify"
              （auto 映射为 notify；no_reply / display_only 按字面值使用）
- heartbeat_path: HEARTBEAT.md 文件路径，默认 "./HEARTBEAT.md"

逻辑：
  1. 读取 HEARTBEAT.md 文件内容（原样读取，仅去除首尾空白，不做任何清理）
  2. 文件不存在 / 内容为空 / 内容为 "NO_REPLY" → no_reply 静默
  3. 文件有内容 → 按参数 result_mode 提交，message 附带 Heartbeat 检查说明
  4. 读取失败 → notify，message 为错误信息
"""

import asyncio
import os
import sys

from codeact_sdk import CodeActSDK


def norm_mode(raw: str, default: str = "notify") -> str:
    """归一化 result_mode；auto 映射为 default，非法值回退 default。"""
    mode = (raw or default).strip().lower()
    if mode == "auto":
        return default
    return mode if mode in {"display_only", "notify", "no_reply"} else default


async def main():
    result_mode = norm_mode(sys.argv[1] if len(sys.argv) > 1 else "notify")
    heartbeat_path = sys.argv[2] if len(sys.argv) > 2 else "./HEARTBEAT.md"
    print(f"[参数] result_mode={result_mode}, heartbeat_path={heartbeat_path}")

    sdk = CodeActSDK()
    try:
        # --- 文件不存在 ---
        if not os.path.isfile(heartbeat_path):
            print(f"[Heartbeat] 文件不存在: {heartbeat_path}")
            await sdk.submit_result(
                result_mode="no_reply",
                status="success",
                message="NO_REPLY",
                data={"heartbeat_path": heartbeat_path, "reason": "file_not_found"},
            )
            return

        # --- 读取文件 ---
        with open(heartbeat_path, "r", encoding="utf-8-sig") as f:
            raw_content = f.read()

        content = raw_content.strip()

        # --- 内容为空或 NO_REPLY ---
        if not content or content.upper() == "NO_REPLY":
            reason = "no_reply_marker" if content else "empty_content"
            print(f"[Heartbeat] 内容为空或 NO_REPLY（reason={reason}），静默跳过")
            await sdk.submit_result(
                result_mode="no_reply",
                status="success",
                message="NO_REPLY",
                data={"heartbeat_path": heartbeat_path, "reason": reason},
            )
            return

        # --- 有内容 → 通知主 agent ---
        message = (
            "现在是一次心跳检查：以下是 HEARTBEAT.md 的内容，请严格遵循其内容完成检查。"
            "先忽略本对话中的其他历史对话。\n\n"
            "【HEARTBEAT.md 内容】\n"
            f"{content}\n\n"
            "【输出规则】\n"
            "- 检查完成后，无需通知用户：只输出 NO_REPLY 这一个词，严禁输出任何分析、总结或其他文字\n"
            "- 检查完成后，需要通知用户：正常回复即可，不要输出 NO_REPLY"
        )
        print(f"[Heartbeat] 发现内容，长度={len(content)} 字符")
        await sdk.submit_result(
            result_mode=result_mode,
            status="success",
            message=message,
            data={
                "heartbeat_path": heartbeat_path,
                "content_length": len(content),
            },
        )

    except Exception as e:
        print(f"[Heartbeat] 执行失败: {e}")
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message=f"Heartbeat 读取失败: {e}",
            data={"heartbeat_path": heartbeat_path, "error_type": type(e).__name__},
        )


if __name__ == "__main__":
    asyncio.run(main())
