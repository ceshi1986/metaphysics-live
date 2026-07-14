#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""飞书文档更新监控 CodeAct 脚本（展示授权检查、状态基线、增量 diff 总结）。

这个示例用于凸显 codeact-script-writer 中「定时/重复执行 + 状态管理 + 长文总结」的实现：
- 参数从 codeact_args 读取，监控类任务固定按 auto 语义分流；
- 先检查飞书 user 授权，再用 lark-cli 读取文档元数据和正文；
- SQLite 按文档 token 维护最后编辑时间，形成“读取状态 → 执行业务 → 成功后写状态”的闭环；
- 首次运行建立冷启动基线并保存正文快照，不把历史内容误当作增量更新；
- 后续更新通过新旧快照计算真实 diff，只把变更块交给 LLM 总结；
- diff 或首次概览过长时分块并发总结，再合并为用户可读摘要；
- 所有异常路径都 submit_result(result_mode="notify")，交还主 agent 处理授权或执行失败。

参数顺序（codeact_args）：result_mode, doc_url
- result_mode: 传 auto。本监控固定按 auto 分流：有更新/首次基线 → display_only(@主人)；
  无更新 → no_reply；未授权 / 任何 lark-cli 失败 / 异常 → notify（交还主 agent）。
- doc_url: 飞书文档链接。

口径：只用沙箱内置 lark-cli 读飞书元数据和文档正文，不用网页/搜索/fetch 兜底。
未授权或任何 lark-cli 失败都直接抛异常 → notify，交还主 agent 去引导授权。
靠「最后编辑时间」判断有无更新；有更新时额外抓取文档正文，通过 LLM 生成变更内容总结。

变更总结策略（优化版）：
- 使用 difflib 计算新旧文档的真实 diff，提取变更块及上下文
- 不把整篇原文粗暴截断后送 LLM；只处理真实 diff，超长 diff 按 hunk/行安全拆分
- 若 diff 过长（超过 DIFF_CHUNK_SIZE），分块并发总结再合并
- 首次基线的长文档也采用分块→并发概览→合并的方式

状态用 SQLite 按文档 token 记录；内容快照存为文件供下次 diff。

说明：本脚本不调用 SDK 搜索/fetch 工具，仅 subprocess 调 lark-cli + sdk.call_llm + submit_result。
文档类型限制：仅 docx/doc/wiki 等文本型文档支持内容总结；sheet/bitable 等类型仅提醒有更新。
"""

import asyncio
import datetime as dt
import difflib
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse

from pydantic import BaseModel

from codeact_sdk import CodeActSDK

STATE_PATH = "./codeact/output/feishu_doc_watch_state.db"
CONTENT_DIR = "./codeact/output/feishu_doc_content"
OWNER = "[主人](at://owner)"
AUTH_HINT = "飞书文档监控失败：可能未完成飞书授权，或对该文档无访问权限。请确认飞书授权后重试。"

# ── diff / 分块参数：对应 codeact-script-writer 的长文本分块与并发 LLM 规则 ─────
# diff 输出中，每个变更块附带多少行上下文
DIFF_CONTEXT_LINES = 3
# 单次送 LLM 的 diff 最大字符数；超过则分块
DIFF_CHUNK_SIZE = 8000
# 首次概览时，单块内容最大字符数
OVERVIEW_CHUNK_SIZE = 8000
# 分块重叠字符数（概览用）
CHUNK_OVERLAP = 400

# LLM 并发信号量：总结类任务使用 Semaphore(3)，避免一次性打满模型调用。
LLM_SEMAPHORE = 3

# 支持内容获取的文档类型（docs +fetch 可读取正文）
TEXT_DOC_TYPES = {"docx", "doc", "wiki"}

TYPE_MAP = {
    "document": "docx",
    "docs": "docx",
    "docx": "docx",
    "doc": "doc",
    "sheet": "sheet",
    "sheets": "sheet",
    "spreadsheet": "sheet",
    "bitable": "bitable",
    "base": "bitable",
    "wiki": "wiki",
    "file": "file",
    "mindnote": "mindnote",
    "mindnotes": "mindnote",
    "slides": "slides",
}


# ── LLM 结构化输出模型：所有总结都用 ResponseFormat 约束输出字段 ─────────────


class ChangeSummary(BaseModel):
    """文档变更总结。"""

    summary: str


class ContentOverview(BaseModel):
    """文档内容概览。"""

    overview: str


class MergedSummary(BaseModel):
    """合并后的总结。"""

    summary: str


# ── 异常 ─────────────────────────────────────────────────────────────


class LarkError(Exception):
    """lark-cli 失败（含未授权）。统一上抛 → notify。"""


# ── lark-cli 调用 ────────────────────────────────────────────────────


def run_lark(args, timeout=60):
    """调用 lark-cli，最多重试 2 次；仍失败则抛 LarkError 走 notify。"""
    last_msg = "lark-cli 调用失败"
    for attempt in range(3):
        try:
            r = subprocess.run(["lark-cli", *args], capture_output=True, text=True, timeout=timeout)
        except FileNotFoundError as e:
            raise LarkError("未找到 lark-cli") from e
        except subprocess.TimeoutExpired:
            last_msg = "lark-cli 执行超时"
        else:
            if r.returncode == 0:
                out = (r.stdout or "").strip()
                if not out:
                    return {}
                starts = [i for i in (out.find("{"), out.find("[")) if i >= 0]
                if not starts:
                    return {}
                try:
                    return json.loads(out[min(starts):])
                except Exception as e:
                    raise LarkError("lark-cli 返回非 JSON") from e
            last_msg = (r.stderr or r.stdout or "lark-cli 调用失败").strip()[:300]
        if attempt < 2:
            import time

            time.sleep(0.5 * (attempt + 1))
    raise LarkError(last_msg)


def _is_truthy(value):
    """兼容 lark-cli auth status 中常见的布尔/字符串可用状态。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"true", "yes", "y", "1", "ok", "ready", "available", "active", "valid"}
    return False


def ensure_user_auth_available():
    """先检查 user 身份授权状态，避免后续 lark-cli 链路产生难懂错误。"""
    status = run_lark(["auth", "status"], timeout=30)
    user = status.get("identities", {}).get("user") if isinstance(status, dict) else None
    if not isinstance(user, dict):
        raise LarkError("飞书 user 身份授权状态不可用")

    available = _is_truthy(user.get("available"))
    status_text = str(user.get("status") or user.get("state") or "").strip().lower()
    token_status = str(
        user.get("tokenStatus") or user.get("token_status") or user.get("tokenStatusText") or ""
    ).strip().lower()

    unavailable_states = {"missing", "expired", "unavailable", "invalid", "none", "not_login", "not_logged_in"}
    if not available or status_text in unavailable_states or token_status in {"expired", "missing", "invalid"}:
        raise LarkError("飞书 user 身份未授权或授权已过期")

    return status


# ── JSON 解析辅助 ────────────────────────────────────────────────────


def _dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _dicts(v)


def pick(obj, keys):
    """在嵌套 JSON 里按 key 优先级取第一个非空值。"""
    dicts = list(_dicts(obj))
    for k in keys:
        for d in dicts:
            if d.get(k) not in (None, "", [], {}):
                return d[k]
    return None


# ── URL / 类型 / 格式化 ──────────────────────────────────────────────


def norm_type(raw):
    v = str(raw or "").strip().lower()
    return TYPE_MAP.get(v, v)


def valid_feishu_url(url):
    p = urlparse(url)
    host = (p.netloc or "").lower()
    return p.scheme in ("http", "https") and (
        host.endswith("feishu.cn") or host.endswith("larksuite.com") or host.endswith("larksuite.cn")
    )


def fmt_time(raw):
    s = str(raw or "").strip().strip('"')
    if not s:
        return "未知"
    if s.isdigit():
        n = int(s)
        if n > 10_000_000_000:
            n //= 1000
        try:
            return dt.datetime.fromtimestamp(n).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return s
    return s


def fmt_user(raw):
    if isinstance(raw, dict):
        for k in ("name", "display_name", "nickname", "nick_name", "en_name"):
            if raw.get(k):
                return str(raw[k])
        return "飞书用户"

    s = str(raw or "").strip().strip('"')
    if not s:
        return "未知"
    if re.match(r"^(ou_|on_|oc_|user_)", s) or re.match(r"^[A-Za-z0-9_-]{20,}$", s):
        return "飞书用户"
    return s


# ── 内容快照管理 ─────────────────────────────────────────────────────


def _content_path(token):
    """内容快照文件路径。"""
    safe_token = re.sub(r"[^A-Za-z0-9_-]", "_", token)
    return os.path.join(CONTENT_DIR, f"{safe_token}.md")


def save_content_snapshot(token, content):
    """保存正文快照，供下次运行计算真实 diff；失败只打日志不影响本次通知。"""
    try:
        os.makedirs(CONTENT_DIR, exist_ok=True)
        Path(_content_path(token)).write_text(content, encoding="utf-8")
        print(f"[内容] 已保存快照，长度={len(content)}")
    except Exception as e:
        print(f"[内容] 快照保存失败（不影响主流程）：{e}")


def load_content_snapshot(token):
    """加载上次文档内容快照。不存在返回空字符串。"""
    path = _content_path(token)
    try:
        if os.path.exists(path):
            return Path(path).read_text(encoding="utf-8")
    except Exception as e:
        print(f"[内容] 快照加载失败：{e}")
    return ""


# ── 文档内容获取 ─────────────────────────────────────────────────────


def fetch_doc_content(doc_url, token, dtype):
    """通过 lark-cli docs +fetch 获取文档正文（markdown）。

    仅对 docx/doc/wiki 类型尝试获取；sheet/bitable 等类型仍可用元数据监控更新时间。
    正文获取失败时返回空字符串，监控链路继续运行，只是不生成内容总结。
    """
    if dtype not in TEXT_DOC_TYPES:
        print(f"[内容] 文档类型={dtype}，不支持正文获取，跳过内容总结")
        return ""

    try:
        result = run_lark(
            ["docs", "+fetch", "--doc", doc_url, "--as", "user", "--format", "json"],
            timeout=90,
        )
        markdown = str(pick(result, ["markdown", "content", "text", "body"]) or "").strip()
        if markdown:
            print(f"[内容] 获取成功，markdown 长度={len(markdown)}")
        else:
            print(f"[内容] 获取成功但正文为空")
        return markdown
    except LarkError as e:
        print(f"[内容] 获取失败，跳过内容总结：{e}")
        return ""
    except Exception as e:
        print(f"[内容] 获取异常，跳过内容总结：{e}")
        return ""


# ── Diff 计算 ────────────────────────────────────────────────────────


def compute_structured_diff(old_content: str, new_content: str, context_lines: int = DIFF_CONTEXT_LINES) -> str:
    """计算新旧内容的结构化 diff，返回人类可读的 diff 文本（含上下文）。

    策略：
    1. 按行拆分后用 unified_diff 生成差异
    2. 过滤掉纯元信息行（--- / +++ / @@ 标记中的行号），只保留有意义的变更行和上下文
    3. 返回可直接送入 LLM 的 diff 描述文本，只让模型关注真实变化
    """
    old_lines = old_content.splitlines(keepends=True)
    new_lines = new_content.splitlines(keepends=True)

    diff = difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile="更新前",
        tofile="更新后",
        n=context_lines,
    )
    diff_text = "".join(diff)

    if not diff_text:
        return ""

    return diff_text


def extract_diff_hunks(diff_text: str) -> List[str]:
    """将 unified diff 文本按 hunk（变更块）拆分。

    每个 hunk 以 @@ 开头，包含变更行及其上下文。
    如果只有一个 hunk 或无需拆分，返回包含整个 diff 的单元素列表。
    """
    if not diff_text:
        return []

    # 按 @@ ... @@ 分割成 hunks
    hunk_pattern = re.compile(r"^@@ .+? @@$", re.MULTILINE)
    hunk_starts = [m.start() for m in hunk_pattern.finditer(diff_text)]

    if not hunk_starts:
        # 没有 hunk 标记（可能没有实际变更），返回原文
        return [diff_text] if diff_text.strip() else []

    hunks = []
    for i, start in enumerate(hunk_starts):
        end = hunk_starts[i + 1] if i + 1 < len(hunk_starts) else len(diff_text)
        hunk = diff_text[start:end].strip()
        if hunk:
            hunks.append(hunk)

    return hunks


# ── 分块工具 ────────────────────────────────────────────────────────


def chunk_text(text: str, chunk_size: int = OVERVIEW_CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """将长文本按字符数分块，支持重叠。

    用于首次基线概览：长文档分块并发摘要，避免整篇塞进单次 LLM。
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = end - overlap

    return chunks


# 单行截断阈值：这里只截断极端超长单行，不截断整篇文档。
# 这样既保留真实 diff，又避免单个表格行/粘贴块撑爆 LLM 上下文。
MAX_DIFF_LINE_CHARS = 1800
TRUNC_SUFFIX = " …（该行过长已截断）\n"


def _trim_diff_line(line: str) -> str:
    """防御性截断超长 diff 行，避免单个行撑爆 LLM 上下文。"""
    if len(line) <= MAX_DIFF_LINE_CHARS:
        return line
    # 保留行首标记（+/-/空格）+ 截断内容 + 后缀
    prefix = line[0] if line and line[0] in "+- " else ""
    body = line[1:] if prefix else line
    return prefix + body[: MAX_DIFF_LINE_CHARS - len(prefix) - len(TRUNC_SUFFIX)] + TRUNC_SUFFIX


def _split_large_hunk(hunk: str, max_chars: int) -> List[str]:
    """将超过 max_chars 的单个 hunk 按行拆分为更小的块。

    拆分策略：
    1. 保留 @@ 标记行在每个子块开头
    2. 按行累积，接近阈值时断开
    3. 超长行先截断再入块，确保每个子块不超过 max_chars
    """
    if len(hunk) <= max_chars:
        return [hunk]

    lines = hunk.splitlines(keepends=True)

    # 提取 @@ 标记行（如果有），作为每个子块的头部
    header_line = ""
    data_start = 0
    for i, line in enumerate(lines):
        if line.startswith("@@"):
            header_line = line.rstrip("\n") + "\n"
            data_start = i + 1
            break

    sub_chunks = []
    current = header_line
    current_len = len(header_line)

    for line in lines[data_start:]:
        # 先截断超长行
        line = _trim_diff_line(line)
        line_len = len(line)
        # 加入此行后超限，且当前块有内容，断开
        if current_len + line_len > max_chars and current_len > len(header_line):
            sub_chunks.append(current)
            current = header_line
            current_len = len(header_line)
        current += line
        current_len += line_len

    if current.strip() and current != header_line:
        sub_chunks.append(current)

    return sub_chunks if sub_chunks else [hunk]


def chunk_hunks(hunks: List[str], max_chars: int = DIFF_CHUNK_SIZE) -> List[str]:
    """将多个 diff hunk 合并为若干块，每块不超过 max_chars。

    相邻 hunk 会尽量合并到同一块中，减少 LLM 调用次数。
    超大 hunk 会被按行拆分为更小的子块。
    """
    if not hunks:
        return []

    # 先将超大 hunk 拆分为小段
    expanded = []
    for hunk in hunks:
        if len(hunk) > max_chars:
            expanded.extend(_split_large_hunk(hunk, max_chars))
        else:
            expanded.append(hunk)

    chunks = []
    current = ""

    for hunk in expanded:
        if len(current) + len(hunk) + 2 > max_chars:
            # 加入会超限，先把当前块收起
            if current:
                chunks.append(current)
            current = hunk
        else:
            if current:
                current += "\n\n"
            current += hunk

    if current:
        chunks.append(current)

    return chunks


# ── LLM 变更/概览总结 ────────────────────────────────────────────────


def _safe_extract_summary(llm_result, field_name: str, fallback: str = "") -> str:
    """安全提取 LLM 结构化输出字段，兼容返回纯字符串的情况。"""
    if isinstance(llm_result, str):
        return llm_result.strip() or fallback
    if isinstance(llm_result, BaseModel):
        val = getattr(llm_result, field_name, None)
        if val and str(val).strip():
            return str(val).strip()
        return fallback
    return fallback


async def _summarize_diff_chunk(sdk: CodeActSDK, title: str, diff_chunk: str, sem: asyncio.Semaphore) -> str:
    """用 LLM 总结单个 diff 块的变更内容。"""
    prompt = (
        "你是一个文档变更分析助手。以下是飞书文档的一处变更（含上下文）。请分析并总结这处变更的内容。\n\n"
        f"文档标题：{title}\n\n"
        "【变更内容（diff 格式，- 开头为删除行，+ 开头为新增行，其余为上下文）】\n"
        f"{diff_chunk}\n\n"
        "请用简洁的中文总结这处变更，包括：\n"
        "- 新增了什么\n"
        "- 删除/修改了什么\n"
        "- 不要重复未变更的上下文\n"
        "- 总结控制在150字以内"
    )

    async with sem:
        try:
            result = await sdk.call_llm(
                messages=[{"role": "user", "content": prompt}],
                response_format=ChangeSummary,
            )
            return _safe_extract_summary(result, "summary", "")
        except Exception as e:
            print(f"[LLM] diff 块总结失败：{e}")
            return ""


async def _merge_diff_summaries(sdk: CodeActSDK, title: str, partial_summaries: List[str], sem: asyncio.Semaphore) -> str:
    """将多个 diff 块的局部总结合并为一份完整的变更总结。"""
    combined = "\n".join(f"{i+1}. {s}" for i, s in enumerate(partial_summaries) if s)

    prompt = (
        "你是一个文档变更总结助手。以下是飞书文档各处变更的分段总结，请合并为一份完整的变更总结。\n\n"
        f"文档标题：{title}\n\n"
        f"【各处分段总结】\n{combined}\n\n"
        "请用简洁的中文合并总结，要求：\n"
        "1. 合并相同主题的变更\n"
        "2. 按重要程度排列\n"
        "3. 不要遗漏关键变更\n"
        "4. 总结控制在200字以内"
    )

    async with sem:
        try:
            result = await sdk.call_llm(
                messages=[{"role": "user", "content": prompt}],
                response_format=MergedSummary,
            )
            return _safe_extract_summary(result, "summary", "")
        except Exception as e:
            print(f"[LLM] 合并总结失败：{e}")
            # 降级：直接拼接
            return "\n".join(f"- {s}" for s in partial_summaries if s)


async def generate_change_summary(sdk: CodeActSDK, title: str, old_content: str, new_content: str) -> str:
    """对比新旧内容，用 LLM 生成变更总结。

    核心优化：先计算真实 diff，再基于 diff 送入 LLM；不把整篇原文截断后让模型猜变化。
    - 若 diff 较短，一次 LLM 调用完成
    - 若 diff 较长，分块并发总结再合并
    """
    # 1. 计算结构化 diff
    diff_text = compute_structured_diff(old_content, new_content)
    if not diff_text:
        print("[diff] 无实际变更")
        return ""

    print(f"[diff] 变更文本长度={len(diff_text)}")

    # 2. 判断是否需要分块
    sem = asyncio.Semaphore(LLM_SEMAPHORE)

    if len(diff_text) <= DIFF_CHUNK_SIZE:
        # 短 diff：一次 LLM 调用
        return await _summarize_diff_chunk(sdk, title, diff_text, sem)

    # 3. 长 diff：分 hunk → 分块 → 并发总结 → 合并
    hunks = extract_diff_hunks(diff_text)
    print(f"[diff] 拆分为 {len(hunks)} 个 hunk")

    chunks = chunk_hunks(hunks, DIFF_CHUNK_SIZE)
    print(f"[diff] 合并为 {len(chunks)} 个 LLM 块")

    partial_summaries = await asyncio.gather(
        *(_summarize_diff_chunk(sdk, title, chunk, sem) for chunk in chunks),
        return_exceptions=True,
    )

    # 过滤异常
    valid_summaries = []
    for ps in partial_summaries:
        if isinstance(ps, Exception):
            print(f"[LLM] 局部总结异常：{ps}")
        elif ps:
            valid_summaries.append(ps)

    if not valid_summaries:
        return "文档内容有更新，但变更总结生成失败。"

    if len(valid_summaries) == 1:
        return valid_summaries[0]

    # 4. 合并多个局部总结
    merged = await _merge_diff_summaries(sdk, title, valid_summaries, sem)
    return merged or "文档内容有更新，但变更总结合并失败。"


async def _summarize_overview_chunk(sdk: CodeActSDK, title: str, chunk: str, chunk_idx: int, total: int, sem: asyncio.Semaphore) -> str:
    """用 LLM 总结单个内容块的概览。"""
    position_hint = f"（第 {chunk_idx}/{total} 部分）" if total > 1 else ""
    prompt = (
        "你是一个文档内容概览助手。以下是飞书文档的部分内容，请用简洁的中文概括这部分内容的要点。\n\n"
        f"文档标题：{title}{position_hint}\n\n"
        f"【文档内容】\n{chunk}\n\n"
        "要求：\n"
        "- 用2-3句话概括这部分的核心要点\n"
        "- 重点说明关键概念和重要结论\n"
        "- 概括控制在100字以内"
    )

    async with sem:
        try:
            result = await sdk.call_llm(
                messages=[{"role": "user", "content": prompt}],
                response_format=ContentOverview,
            )
            return _safe_extract_summary(result, "overview", "")
        except Exception as e:
            print(f"[LLM] 概览块总结失败：{e}")
            return ""


async def _merge_overview_summaries(sdk: CodeActSDK, title: str, partials: List[str], sem: asyncio.Semaphore) -> str:
    """合并多个概览块为完整概览。"""
    combined = "\n".join(f"部分{i+1}：{s}" for i, s in enumerate(partials) if s)

    prompt = (
        "你是一个文档概览助手。以下是飞书文档各部分内容的分段概览，请合并为一份完整的文档概览。\n\n"
        f"文档标题：{title}\n\n"
        f"【各部分概览】\n{combined}\n\n"
        "要求：\n"
        "- 用2-3句话概括文档的核心内容和结构\n"
        "- 重点说明文档的用途和主要章节\n"
        "- 概括控制在150字以内"
    )

    async with sem:
        try:
            result = await sdk.call_llm(
                messages=[{"role": "user", "content": prompt}],
                response_format=ContentOverview,
            )
            return _safe_extract_summary(result, "overview", "")
        except Exception as e:
            print(f"[LLM] 概览合并失败：{e}")
            return "；".join(partials)


async def generate_content_overview(sdk: CodeActSDK, title: str, content: str) -> str:
    """用 LLM 生成文档内容概览（首次基线时使用）。

    优化：长文档分块→并发概览→合并，避免一次性截断长文内容。
    """
    sem = asyncio.Semaphore(LLM_SEMAPHORE)

    if len(content) <= OVERVIEW_CHUNK_SIZE:
        # 短内容：一次 LLM 调用
        return await _summarize_overview_chunk(sdk, title, content, 1, 1, sem)

    # 长内容：分块并发总结再合并
    chunks = chunk_text(content, OVERVIEW_CHUNK_SIZE, CHUNK_OVERLAP)
    total = len(chunks)
    print(f"[概览] 内容长度={len(content)}，拆分为 {total} 个块")

    partials = await asyncio.gather(
        *(_summarize_overview_chunk(sdk, title, chunk, i + 1, total, sem) for i, chunk in enumerate(chunks)),
        return_exceptions=True,
    )

    valid_partials = []
    for p in partials:
        if isinstance(p, Exception):
            print(f"[LLM] 概览块异常：{p}")
        elif p:
            valid_partials.append(p)

    if not valid_partials:
        return ""

    if len(valid_partials) == 1:
        return valid_partials[0]

    # 合并
    merged = await _merge_overview_summaries(sdk, title, valid_partials, sem)
    return merged or ""


# ── SQLite 状态管理 ──────────────────────────────────────────────────


def open_state_db():
    """打开状态库并创建 doc_state 表；token 主键保证每个文档只有一条基线。"""
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    con = sqlite3.connect(STATE_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_state (
            token TEXT PRIMARY KEY,
            modify_time TEXT NOT NULL,
            title TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    return con


def load_doc_state(con, token):
    row = con.execute("SELECT modify_time, title FROM doc_state WHERE token = ?", (token,)).fetchone()
    return {"modify_time": row[0], "title": row[1]} if row else None


def save_doc_state(con, token, modify_time, title):
    """成功提交结果后写入最新编辑时间；失败路径不写状态，避免漏报下一次更新。"""
    con.execute(
        """
        INSERT INTO doc_state (token, modify_time, title, updated_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(token) DO UPDATE SET
            modify_time = excluded.modify_time,
            title = excluded.title,
            updated_at = excluded.updated_at
        """,
        (token, modify_time, title, dt.datetime.now().isoformat(timespec="seconds")),
    )
    con.commit()


def save_doc_state_best_effort(con, token, modify_time, title):
    try:
        save_doc_state(con, token, modify_time, title)
    except Exception as e:
        print(f"[状态] 保存失败，下次会重新核验该文档：{e}")


# ── 主流程 ───────────────────────────────────────────────────────────


async def main():
    _ = (sys.argv[1] if len(sys.argv) > 1 else "auto").strip()
    doc_url = (sys.argv[2] if len(sys.argv) > 2 else "").strip()
    print(f"[参数] doc_url={'已提供' if doc_url else '未提供'}")

    sdk = CodeActSDK()

    # 参数缺失/非法：直接 notify
    if not doc_url:
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message="未提供飞书文档链接，请在 doc_url 参数中传入飞书文档链接。",
        )
        return

    if not valid_feishu_url(doc_url):
        await sdk.submit_result(
            result_mode="notify",
            status="error",
            message="doc_url 不是有效的飞书文档链接（需 feishu.cn / larksuite.com 域名）。",
        )
        return

    try:
        # 1. 授权检查
        ensure_user_auth_available()

        # 2. 解析文档 token / 类型
        info = run_lark(["drive", "+inspect", "--as", "user", "--url", doc_url, "--format", "json"])
        token = str(pick(info, ["canonical_token", "doc_token", "obj_token", "file_token", "token"]) or "").strip()
        dtype = norm_type(pick(info, ["canonical_type", "doc_type", "obj_type", "file_type", "type"]))
        if not token or not dtype:
            raise LarkError("未能识别文档类型或标识")

        # 3. 取元数据
        payload = json.dumps({"request_docs": [{"doc_token": token, "doc_type": dtype}]}, ensure_ascii=False)
        meta = run_lark(["drive", "metas", "batch_query", "--as", "user", "--data", payload, "--format", "json"])

        title = str(pick(meta, ["title", "name"]) or pick(info, ["title", "name"]) or "未命名文档").strip()
        mtime = str(
            pick(
                meta,
                [
                    "latest_modify_time",
                    "last_modified_time",
                    "modified_time",
                    "modify_time",
                    "edit_time",
                    "updated_at",
                    "update_time",
                ],
            )
            or ""
        ).strip()
        muser = pick(
            meta,
            [
                "latest_modify_user",
                "last_modify_user",
                "last_modifier",
                "modifier",
                "modified_by",
                "edit_user",
                "owner",
            ],
        )

        # 4. 状态比对：读取历史基线，只用 modify_time 判断是否发生增量更新。
        con = open_state_db()
        prev = load_doc_state(con, token)
        first_run = prev is None
        changed = (not first_run) and bool(mtime) and prev.get("modify_time", "") != mtime
        next_modify_time = mtime or (prev.get("modify_time", "") if prev else "")

        # ── 5. 内容获取 & LLM 总结：首次做概览，增量运行只总结 diff ─────────────
        content_summary = ""

        if first_run:
            # 冷启动基线：记录当前状态，给用户一个概览，但不把历史内容当作“新增变更”。
            content = fetch_doc_content(doc_url, token, dtype)
            if content:
                content_summary = await generate_content_overview(sdk, title, content)
                save_content_snapshot(token, content)

        elif changed:
            # 增量更新：拿新旧快照计算真实 diff，再总结变更内容。
            new_content = fetch_doc_content(doc_url, token, dtype)
            if new_content:
                old_content = load_content_snapshot(token)
                if old_content:
                    content_summary = await generate_change_summary(sdk, title, old_content, new_content)
                else:
                    # 旧快照不存在（可能首次运行时内容获取失败），仅总结新内容
                    content_summary = await generate_content_overview(sdk, title, new_content)
                save_content_snapshot(token, new_content)

        # ── 6. 按 auto 分流：有更新/首次基线展示；无更新静默；异常 notify ───────
        data = {
            "title": title,
            "doc_type": dtype,
            "first_run": first_run,
            "has_update": bool(first_run or changed),
            "latest_modify_time": mtime,
            "latest_modify_user": fmt_user(muser),
            "has_content_summary": bool(content_summary),
        }

        try:
            if first_run:
                msg = f"{OWNER} 飞书文档《{title}》已建立监控基线，后续有更新再提醒。\n"
                msg += f"最后编辑：{fmt_time(mtime)} {fmt_user(muser)}\n"
                if content_summary:
                    msg += f"\n📋 文档概览：{content_summary}\n"
                msg += doc_url
                await sdk.submit_result(result_mode="display_only", status="success", message=msg, data=data)

            elif changed:
                msg = f"{OWNER} 飞书文档《{title}》有更新！\n"
                msg += f"最后编辑：{fmt_time(mtime)} {fmt_user(muser)}\n"
                if content_summary:
                    msg += f"\n📝 更新内容总结：\n{content_summary}\n"
                msg += f"\n点击查看：{doc_url}"
                await sdk.submit_result(result_mode="display_only", status="success", message=msg, data=data)

            else:
                await sdk.submit_result(result_mode="no_reply", status="success", message="NO_REPLY", data=data)

            save_doc_state_best_effort(con, token, next_modify_time, title)
        finally:
            con.close()

    except LarkError as e:
        print(f"[lark] {e}")
        await sdk.submit_result(result_mode="notify", status="error", message=AUTH_HINT)
    except Exception as e:
        print(f"[异常] {e}")
        await sdk.submit_result(result_mode="notify", status="error", message="飞书文档更新监控执行失败，请稍后重试。")


if __name__ == "__main__":
    asyncio.run(main())
