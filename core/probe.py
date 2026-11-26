"""预检：探测目标资源能力与链路基线。

只读探测，不写盘。产出 ProbeResult 供 planner / scheduler 决策。

要点：
1. HEAD 可能被服务器禁用（405），失败不致命，用 Range GET 兜底。
2. 强制 Accept-Encoding: identity —— 一旦服务器对内容做 gzip，字节偏移就失效，
   Range 分块会拼出损坏文件。
3. 用一小段 Range 请求同时完成两件事：确认是否支持 Range、量出单连接基线速度。
4. 拿不到大小时要能说清「为什么」——被跳登录页、403 无权限、还是返回了网页，
   这三种情况的处理方式完全不同，不能让用户对着「拿不到大小」瞎猜。
"""

from __future__ import annotations

import re
import time
import urllib.parse
from dataclasses import dataclass

import aiohttp

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_CONTENT_RANGE_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)")

# 命中这些片段说明请求被踢到了登录/鉴权页
_LOGIN_HINTS = (
    "passport",
    "/login",
    "signin",
    "sign_in",
    "/sso/",
    "oauth",
    "auth/login",
    "login.microsoftonline",
    "accounts.google.com",
    "login.live.com",
)


@dataclass
class ProbeResult:
    url: str
    final_url: str = ""
    status: int = 0
    size: int | None = None
    accept_ranges: bool = False
    etag: str | None = None
    last_modified: str | None = None
    content_type: str | None = None
    ttfb: float | None = None          # 首字节时间（秒）
    single_speed: float | None = None  # 单连接速度（bytes/s）
    server_checksum: str | None = None
    filename: str | None = None        # 从 Content-Disposition 解析出的真实文件名
    needs_auth: bool = False           # 判定为需要登录 / 无权限
    note: str = ""

    @property
    def can_parallel(self) -> bool:
        """能否多连接分块下载。"""
        return bool(self.accept_ranges and self.size and self.size > 0)

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "final_url": self.final_url,
            "status": self.status,
            "size": self.size,
            "accept_ranges": self.accept_ranges,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "content_type": self.content_type,
            "ttfb": self.ttfb,
            "single_speed": self.single_speed,
            "can_parallel": self.can_parallel,
            "filename": self.filename,
            "needs_auth": self.needs_auth,
            "note": self.note,
        }


def build_headers(user_headers: dict | None = None) -> dict:
    """构造请求头。用户头优先，但 Accept-Encoding 强制 identity。"""
    h: dict[str, str] = {"User-Agent": DEFAULT_UA}
    if user_headers:
        for k, v in user_headers.items():
            if v is None or v == "":
                continue
            h[str(k)] = str(v)
    # 必须在最后覆盖：压缩会破坏字节偏移语义
    h["Accept-Encoding"] = "identity"
    return h


def _looks_like_login(u: str) -> bool:
    ul = (u or "").lower()
    return any(k in ul for k in _LOGIN_HINTS)


def _parse_content_disposition(cd: str) -> str | None:
    """从 Content-Disposition 里抠文件名，兼容 RFC 5987 与 latin-1 乱码两种情况。"""
    if not cd:
        return None

    m = re.search(r"filename\*\s*=\s*([^;]+)", cd, re.I)
    if m:
        val = m.group(1).strip().strip('"')
        if "''" in val:
            charset, _, rest = val.partition("''")
            try:
                return urllib.parse.unquote(rest, encoding=charset or "utf-8")
            except Exception:  # noqa: BLE001
                return urllib.parse.unquote(rest)
        return urllib.parse.unquote(val)

    m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.I)
    if m:
        raw = m.group(1).strip()
        # 很多服务端把 UTF-8 字节直接塞进 latin-1 头里
        try:
            return raw.encode("latin-1").decode("utf-8")
        except Exception:  # noqa: BLE001
            return raw
    return None


def _fill_meta(res: ProbeResult, headers) -> None:
    ar = (headers.get("Accept-Ranges") or "").lower()
    if "bytes" in ar:
        res.accept_ranges = True
    cl = headers.get("Content-Length")
    if cl and cl.isdigit() and res.size is None:
        res.size = int(cl)
    res.etag = res.etag or headers.get("ETag")
    res.last_modified = res.last_modified or headers.get("Last-Modified")
    res.content_type = res.content_type or headers.get("Content-Type")
    res.filename = res.filename or _parse_content_disposition(
        headers.get("Content-Disposition") or ""
    )
    res.server_checksum = (
        res.server_checksum
        or headers.get("Content-MD5")
        or headers.get("x-amz-checksum-sha256")
        or headers.get("Digest")
    )


def _diagnose(res: ProbeResult) -> None:
    """拿不到大小时，把真实原因说清楚，而不是笼统报「无法获取文件大小」。"""
    ct = (res.content_type or "").lower()
    is_html = ct.startswith("text/html") or ct.startswith("application/xhtml")

    # 被重定向到登录页：此时拿到的「大小」是登录页 HTML 的大小，必须作废
    if _looks_like_login(res.final_url):
        res.size = None
        res.accept_ranges = False
        res.needs_auth = True
        res.note = (
            "链接需要登录：请求被重定向到了登录页。"
            "浏览器能下是因为带着你的登录 Cookie。"
            "请按 F12 → Network 找到该请求 → Copy as cURL，"
            "把 Cookie 整行填到面板「高级 · 自定义请求头」里再试。"
        )
        return

    if res.status in (401, 403):
        res.size = None
        res.needs_auth = True
        res.note = (
            f"无权限（HTTP {res.status}）：链接可能需要 Cookie 或 Referer，"
            "也可能签名已过期。可在「高级 · 自定义请求头」里补上再试。"
        )
        return

    if res.size:
        return

    if is_html:
        res.note = (
            "返回的是网页而不是文件：这个地址多半是分享页/详情页，"
            "不是文件直链。请在页面上右键文件选「复制下载链接」再试。"
        )
        return

    if res.status >= 400:
        res.note = f"服务器返回 HTTP {res.status}，拿不到文件。"
        return

    tail = f"（探测记录：{res.note}）" if res.note else ""
    res.note = f"服务器既没返回文件大小，也不支持 Range，无法确认长度。{tail}"


async def probe(
    session: aiohttp.ClientSession,
    url: str,
    user_headers: dict | None = None,
    probe_bytes: int = 256 * 1024,
    timeout: float = 20.0,
) -> ProbeResult:
    """探测 URL 的下载能力与基线速度。"""
    h = build_headers(user_headers)
    tmo = aiohttp.ClientTimeout(total=timeout, sock_connect=10.0)
    res = ProbeResult(url=url, final_url=url)

    # ---- 1) HEAD（可能不被支持） ----
    head_ok = False
    try:
        async with session.head(url, headers=h, timeout=tmo, allow_redirects=True) as r:
            res.status = r.status
            res.final_url = str(r.url)
            if r.status < 400:
                head_ok = True
                _fill_meta(res, r.headers)
            else:
                res.note = f"HEAD {r.status}"
    except Exception as e:  # noqa: BLE001
        res.note = f"HEAD 失败: {type(e).__name__}"

    # ---- 2) Range 探针：确认能力 + 量基线速度 ----
    try:
        rh = dict(h)
        rh["Range"] = f"bytes=0-{max(0, probe_bytes - 1)}"
        t0 = time.perf_counter()
        async with session.get(url, headers=rh, timeout=tmo, allow_redirects=True) as r:
            res.final_url = str(r.url)
            if r.status == 206:
                res.accept_ranges = True
                m = _CONTENT_RANGE_RE.search(r.headers.get("Content-Range") or "")
                if m and m.group(3) != "*":
                    res.size = int(m.group(3))  # Content-Range 的总长最权威
                _fill_meta(res, r.headers)
                res.ttfb = time.perf_counter() - t0
                got = 0
                async for data in r.content.iter_chunked(64 * 1024):
                    got += len(data)
                    if got >= probe_bytes:
                        break
                dt = time.perf_counter() - t0
                if dt > 0 and got > 0:
                    res.single_speed = got / dt
            elif r.status == 200:
                # 服务器忽略了 Range：只能单流下载
                res.accept_ranges = False
                res.ttfb = time.perf_counter() - t0
                if res.size is None:
                    cl = r.headers.get("Content-Length")
                    if cl and cl.isdigit():
                        res.size = int(cl)
                if not head_ok:
                    _fill_meta(res, r.headers)
                res.note = (res.note + "; " if res.note else "") + "服务器忽略 Range"
            else:
                res.status = r.status
                if not head_ok:
                    _fill_meta(res, r.headers)
                res.note = (res.note + "; " if res.note else "") + f"Range 探测 HTTP {r.status}"
    except Exception as e:  # noqa: BLE001
        res.note = (res.note + "; " if res.note else "") + f"Range 探测失败: {type(e).__name__}"

    # ---- 3) 诊断：拿不到大小时给出可执行的原因 ----
    _diagnose(res)
    return res
