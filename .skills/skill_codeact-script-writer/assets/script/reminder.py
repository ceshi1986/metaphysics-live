#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""定时提醒 CodeAct 脚本（scaffold 母版）

最轻量的一类脚本：不联网、不查数据、不调用 SDK 工具，到点把提醒推送给主人。
适用：喝水、吃药、久坐起身、运动、会议、起床等固定 / 周期提醒。

⚠️ 这是 scaffold（改写型）母版，不要原样复用。
   遇到具体提醒需求时：复制为带主题的新文件（如 reminder_wechat.py），
   把下方 CUSTOMIZE 区块的 VARIANTS 改写成贴合该主题的个性化文案后再注册、建日程。

参数顺序（codeact_args）：result_mode, reminder_text
- result_mode: display_only / notify / no_reply / auto
              （提醒应送达，默认 display_only；auto 也按 display_only 处理）
- reminder_text: 提醒主题/标签，如「喝水提醒」「吃药提醒」，默认「提醒」

个性化设计：
  为避免周期提醒每次措辞一样，提示语写在脚本内的 VARIANTS 里、随机抽取；
  reminder_text 只作主题标签，不承载整句文案。
"""

import asyncio
import random
import sys

from codeact_sdk import CodeActSDK

# === CUSTOMIZE:VARIANTS START ===  安装时按提醒主题改写为 3-5 条个性化文案（{text}=reminder_text，见 index.customize）
VARIANTS = [
    "⏰ {text}：时间到啦，记得完成哦。",
    "⏰ {text}：温馨提醒，别忘了这件小事。",
    "⏰ {text}：到点了，顺手做一下吧。",
]
# === CUSTOMIZE:VARIANTS END ===


def norm_mode(raw: str) -> str:
    mode = (raw or "display_only").strip().lower()
    if mode == "auto":
        return "display_only"
    return mode if mode in {"display_only", "notify", "no_reply"} else "display_only"


async def main():
    # 参数顺序：result_mode, reminder_text（result_mode 固定第一，平台约定）
    result_mode = norm_mode(sys.argv[1] if len(sys.argv) > 1 else "display_only")
    reminder_text = " ".join(sys.argv[2:]).strip() or "提醒"
    print(f"[参数] result_mode={result_mode}, reminder_text={reminder_text}")

    sdk = CodeActSDK()
    try:
        variant = random.choice(VARIANTS).format(text=reminder_text)
        # display_only 不经过主 agent，at://owner 必须在脚本内硬编码到 message 开头
        message = f"[主人](at://owner) {variant}"
        await sdk.submit_result(
            result_mode=result_mode, status="success",
            message=message, data={"reminder_text": reminder_text},
        )
    except Exception as e:
        await sdk.submit_result(
            result_mode="notify", status="error",
            message=f"定时提醒执行失败: {e}",
        )


if __name__ == "__main__":
    asyncio.run(main())
