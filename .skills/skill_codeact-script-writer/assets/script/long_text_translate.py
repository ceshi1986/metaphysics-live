#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""长文翻译 CodeAct 脚本（展示长文本处理、并发 LLM、失败容错）。

这个示例用于凸显 codeact-script-writer 中「批量/长文本任务」的推荐实现：
- 参数全部从 codeact_args 读取，并提供安全默认值；
- 长文先按 Markdown 结构切块，再用 LLM 并发翻译，最后按原始顺序合并；
- 多块长文注入前后文窗口；块数较多时再构建全局术语表，兼顾效率与术语一致性；
- 代码块不交给 LLM 翻译，避免模型改写代码；
- 单块失败后重试，最终失败只在输出中回填原文，失败详情放入 submit_result.data。

参数顺序（codeact_args）：result_mode, input_path, target_lang
- result_mode: 建议传 notify（译完交还主 agent 把文件交付用户）；auto/非法 → 按 notify。
- input_path: 待翻译的文本/Markdown 文件路径（如 ./用户上传/xxx.md）。
- target_lang: 目标语言，默认 中文。

要点：
- 动态分块：无代码块短文整篇一次译；长文按「目标块数」反推块大小并在自然边界切，控制块数与轮次。
- 短块优先结构化输出、长块纯文本输出，并兼容 JSON 兜底解析，降低模型输出解释性废话的概率。
- 全局术语表：预计翻译块较多时才先扫一遍抽关键术语，避免中等长度文档被前置 LLM 调用拖慢。
- 哨兵包裹 + 前后文参考：连贯且不重复翻译。
- 并发翻译（Semaphore 3）+ 容错（单块失败后最多重试 3 次，最终失败则在输出中回填原文，失败信息只放 message/data），所有路径都 submit_result。
- 代码块原样保留（不翻译），表格只翻文字。

说明：仅用 sdk.call_llm + pydantic response_format + 标准库文件操作，不调用 search/fetch 等 SDK 工具，无 schema_version 占位；
模板版与测试版正文完全一致。
"""

import asyncio
import json
import math
import os
import re
import sys
import traceback

from pydantic import BaseModel, Field
from codeact_sdk import CodeActSDK

# ===== 长文本翻译策略配置 =====
SINGLE_SHOT_CHARS = 6000        # 小于此长度整篇一次译，且不抽术语表
CHUNK_LO, CHUNK_HI = 3500, 8000  # 动态块大小的上下限（上限留输出余量，防截断）
TARGET_CHUNKS = 10              # 长文目标块数量级
CONCURRENCY = 3                 # LLM 并发上限（SP 建议 ≤3）
CONTEXT_CHARS = 400            # 前后文参考字符数
MAX_RETRY = 3                  # 单块失败后的最大重试次数，不含首次调用
STRUCTURED_OUTPUT_CHARS = 4500  # 短块使用结构化输出，长块避免 JSON 包装增加截断风险
GLOSSARY_MIN_CHUNKS = 3         # 预计至少 3 个翻译块才构建术语表，避免术语表成为中等文档的串行瓶颈
OUTPUT_DIR = "./codeact/output"
START, END = "===TRANSLATE_START===", "===TRANSLATE_END==="


class TranslatedChunk(BaseModel):
    """单块翻译结果。"""
    translated_text: str = Field(description="翻译后的文本，保持原 Markdown 格式")


def plan_chunk_size(total_chars: int) -> int:
    """按文档总长动态决定目标块大小：短文整篇；长文按目标块数反推并夹在 [LO, HI]。"""
    if total_chars <= SINGLE_SHOT_CHARS:
        return total_chars
    return max(CHUNK_LO, min(CHUNK_HI, math.ceil(total_chars / TARGET_CHUNKS)))


def split_markdown(text: str, chunk_size: int) -> list:
    """结构感知拆分：体现 CodeAct 长文本任务先分离结构化片段、再并发处理正文。

    代码块/表格整体保留，其余按段落/标题/空行/大小在自然边界切。
    返回 [{"type": "code|table|text", "content": str}, ...]。"""
    lines = text.split("\n")
    chunks, buf, btype = [], [], "text"
    in_code = in_table = False

    def flush():
        nonlocal buf, btype
        s = "\n".join(buf).rstrip("\n")
        if s.strip():
            chunks.append({"type": btype, "content": s})
        buf, btype = [], "text"

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):  # 代码围栏
            if in_code:
                buf.append(line); flush(); in_code = False
            else:
                flush(); in_code = True; btype = "code"; buf.append(line)
            continue
        if in_code:
            buf.append(line); continue

        is_table = stripped.startswith("|")
        if is_table and not in_table:
            flush(); in_table = True; btype = "table"
        elif not is_table and in_table:
            flush(); in_table = False
        if in_table:
            buf.append(line); continue

        if stripped.startswith("#"):   # 标题起新块（仍按 text 处理，避免类型泄漏）
            flush(); buf.append(line); continue
        if not stripped:               # 空行=段落边界
            flush(); continue
        buf.append(line)
        if len("\n".join(buf)) >= chunk_size:
            flush()
    flush()
    return chunks


def has_code_fence(text: str) -> bool:
    """检测是否存在围栏代码块；有代码块时不走整篇单次翻译，避免模型改写代码。"""
    return any(line.strip().startswith("```") for line in text.splitlines())


def build_chunks(text: str, total_chars: int) -> tuple[list, int]:
    """构建翻译块。无代码块短文整篇一次译；其他情况走结构感知分块。"""
    chunk_size = plan_chunk_size(total_chars)
    if total_chars <= SINGLE_SHOT_CHARS and not has_code_fence(text):
        return [{"type": "text", "content": text.strip("\n")}], chunk_size
    chunks = split_markdown(text, chunk_size)
    chunks = repack(chunks, chunk_size)
    return enforce_max(chunks, max(chunk_size, CHUNK_HI)), chunk_size


def repack(chunks: list, chunk_size: int) -> list:
    """把相邻的小 text 块贪心合并到 chunk_size，减少块数与调用轮次；code/table 作为天然边界。"""
    out, cur = [], None
    for c in chunks:
        if c["type"] == "text":
            if cur is not None and len(cur["content"]) + len(c["content"]) + 2 <= chunk_size:
                cur["content"] += "\n\n" + c["content"]
            else:
                cur = {"type": "text", "content": c["content"]}
                out.append(cur)
        else:
            out.append(c); cur = None
    return out


def hard_split(content: str, limit: int) -> list:
    """兜底：单个 text 块过长（如整段无换行）时，按句末/换行硬切，防输出截断丢内容。"""
    if len(content) <= limit:
        return [content]
    parts, cur = [], ""
    for seg in re.split(r"(?<=[。.!?！？\n])", content):
        if cur and len(cur) + len(seg) > limit:
            parts.append(cur); cur = seg
        else:
            cur += seg
    if cur:
        parts.append(cur)
    return parts


def enforce_max(chunks: list, limit: int) -> list:
    out = []
    for c in chunks:
        if c["type"] == "text" and len(c["content"]) > limit:
            out.extend({"type": "text", "content": p} for p in hard_split(c["content"], limit))
        else:
            out.append(c)
    return out


async def build_glossary(sdk: CodeActSDK, text: str, target_lang: str) -> str:
    """为多块长文建立全局术语表。

    这是一次串行的前置 LLM 调用：对很多块的长文能换来术语一致性；
    对只有一两块的中等文档则可能成为延迟瓶颈，所以调用方会按块数决定是否启用。
    """
    sample = text[:6000]
    if len(text) > 12000:
        mid = len(text) // 2
        sample += "\n...\n" + text[mid:mid + 2000]
    prompt = (
        f"从下面内容里提取需要在全文统一译法的关键专有名词（人名/公司名/产品名/专业术语），"
        f"给出建议的{target_lang}译法（广为人知或宜保留原文的就保留原文）。每行一个，格式：原词 => 译法。"
        f"最多 20 个；若没有明显专有名词，只回复“无”。\n\n{sample}"
    )
    try:
        out = await sdk.call_llm(messages=[{"role": "user", "content": prompt}])
        out = (out if isinstance(out, str) else str(out)).strip()
        return "" if not out or out[:3] == "无" else out
    except Exception as e:
        print(f"[术语表] 跳过：{e}")
        return ""


async def translate_chunk(sdk, chunk, target_lang, glossary, prev_ctx, next_ctx):
    """翻译单块（结构化/纯文本输出 + 哨兵包裹 + 前后文/术语表参考），带重试。"""
    if chunk["type"] == "code":
        return chunk["content"]

    g = f"\n【术语表（务必统一译法）】\n{glossary}\n" if glossary else ""
    table_hint = "（这是 Markdown 表格，保持表格结构与分隔符不变，只翻译单元格里的文字）" if chunk["type"] == "table" else ""
    heading_hint = "（当前块以 Markdown 标题开头，保持 # 标记与层级不变）" if chunk["content"].lstrip().startswith("#") else ""
    pc = f"\n【前文参考·仅理解用·不要翻译】\n{prev_ctx[-CONTEXT_CHARS:]}\n" if prev_ctx else ""
    nc = f"\n【后文参考·仅理解用·不要翻译】\n{next_ctx[:CONTEXT_CHARS]}\n" if next_ctx else ""

    system_prompt = (
        f"你是一位专业翻译，擅长将文档准确翻译为{target_lang}，覆盖金融、科技、法律和技术文档。\n\n"
        "翻译要求：\n"
        "1. 准确传达原文含义，不遗漏信息；\n"
        "2. 专有名词（人名、公司名、产品名）保持英文，除非有广为接受的译名；\n"
        "3. 金融与技术术语（如 SEC、NASDAQ、API、Form 10-K）保持原文；\n"
        "4. 数字、百分比、日期、代码、URL 原样保留；\n"
        "5. 保持 Markdown 格式不变，包括标题、列表、加粗、链接、表格与引用；\n"
        "6. 参考上下文和术语表，保证代词、术语、人名前后一致；\n"
        "7. 只输出译文本身，不要输出标记、上下文参考或任何解释。"
    )
    user_prompt = (
        f"请只翻译 {START} 与 {END} 之间的正文为{target_lang}。\n"
        f"{table_hint}{heading_hint}{g}{pc}{nc}\n{START}\n{chunk['content']}\n{END}"
    )
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    last = "未知错误"
    use_structured = len(chunk["content"]) <= STRUCTURED_OUTPUT_CHARS
    for attempt in range(MAX_RETRY + 1):
        try:
            kwargs = {"messages": messages}
            if use_structured and attempt == 0:
                kwargs["response_format"] = TranslatedChunk
            out = await sdk.call_llm(**kwargs)
            out = normalize_translation_output(out)
            if out:
                return out
            last = "空输出"
        except Exception as e:
            last = str(e)
    raise RuntimeError(last)


def normalize_translation_output(out) -> str:
    """兼容结构化结果、纯文本结果和模型误返回的 JSON 字符串。"""
    if isinstance(out, TranslatedChunk):
        text = out.translated_text
    else:
        text = out if isinstance(out, str) else str(out)

    text = text.replace(START, "").replace(END, "").strip()
    if text.startswith("{") and "translated_text" in text:
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict) and parsed.get("translated_text"):
                return str(parsed["translated_text"]).strip()
        except json.JSONDecodeError:
            pass
    return text


async def translate_all(sdk, chunks, target_lang, glossary):
    """按 Semaphore 控制 LLM 并发；gather 保持结果顺序，单块失败不拖垮整篇。"""
    sem = asyncio.Semaphore(CONCURRENCY)
    results = [None] * len(chunks)

    async def worker(i):
        prev = chunks[i - 1]["content"] if i > 0 else ""
        nxt = chunks[i + 1]["content"] if i < len(chunks) - 1 else ""
        async with sem:
            results[i] = await translate_chunk(sdk, chunks[i], target_lang, glossary, prev, nxt)
        print(f"  ✓ {i + 1}/{len(chunks)} ({chunks[i]['type']}, {len(chunks[i]['content'])}字)")

    print(f"[翻译] {len(chunks)} 块，并发 {CONCURRENCY}")
    outcomes = await asyncio.gather(*(worker(i) for i in range(len(chunks))), return_exceptions=True)

    failed = []
    for i, oc in enumerate(outcomes):
        if isinstance(oc, Exception) or results[i] is None:
            print(f"  ✗ 块 {i + 1} 失败：{oc}")
            results[i] = chunks[i]["content"]
            failed.append(i + 1)
    return results, failed


def assemble_output(results: list[str]) -> str:
    """按块顺序组装输出，避免 strip() 吞掉首尾空格；统一补一个文件末尾换行。"""
    return "\n\n".join(part.rstrip("\n") for part in results).strip("\n") + "\n"


def norm_mode(raw: str) -> str:
    mode = (raw or "notify").strip().lower()
    return mode if mode in {"display_only", "notify", "no_reply"} else "notify"


async def main():
    result_mode = norm_mode(sys.argv[1] if len(sys.argv) > 1 else "notify")
    input_path = (sys.argv[2] if len(sys.argv) > 2 else "").strip()
    target_lang = (sys.argv[3] if len(sys.argv) > 3 else "中文").strip() or "中文"
    print(f"[参数] result_mode={result_mode}, input={input_path or '未提供'}, target={target_lang}")

    sdk = CodeActSDK()
    try:
        if not input_path or not os.path.exists(input_path):
            await sdk.submit_result(result_mode="notify", status="error",
                                    message=f"待翻译文件不存在：{input_path or '未提供 input_path'}")
            return
        with open(input_path, "r", encoding="utf-8") as f:
            text = f.read()
        total = len(text)
        if not text.strip():
            await sdk.submit_result(result_mode="notify", status="error", message="文件为空，无可翻译内容。")
            return

        # 动态分块
        chunks, chunk_size = build_chunks(text, total)
        should_build_glossary = total > SINGLE_SHOT_CHARS and len(chunks) >= GLOSSARY_MIN_CHUNKS
        glossary = await build_glossary(sdk, text, target_lang) if should_build_glossary else ""
        print(f"[分块] {total} 字 → chunk_size≈{chunk_size}，共 {len(chunks)} 块"
              + (f"，术语表 {glossary.count(chr(10)) + 1} 条" if glossary else ""))

        # 并发翻译
        results, failed = await translate_all(sdk, chunks, target_lang, glossary)
        out_text = assemble_output(results)

        # 落盘 ./codeact/output/
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        base = re.sub(r"[^\w\-]", "_", os.path.splitext(os.path.basename(input_path))[0]) or "translation"
        out_path = os.path.join(OUTPUT_DIR, f"{base}_{target_lang}.md")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(out_text)

        abs_out_path = os.path.abspath(out_path)
        note = f"，其中 {len(failed)} 块翻译失败，输出中已回填对应原文（块 {failed}）" if failed else ""
        msg = (f"翻译完成：{os.path.basename(input_path)} → {target_lang}，"
               f"共 {len(chunks)} 块{note}。\n"
               f"输出文件：[查看译文](computer://{abs_out_path})")
        await sdk.submit_result(
            result_mode=result_mode, status="success", message=msg,
            data={"output_file": abs_out_path, "chunks": len(chunks), "failed_chunks": failed,
                  "target_lang": target_lang, "source_chars": total},
        )
    except Exception as e:
        print(traceback.format_exc())
        await sdk.submit_result(result_mode="notify", status="error", message=f"长文翻译执行失败：{e}")


if __name__ == "__main__":
    asyncio.run(main())
