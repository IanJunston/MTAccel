"""预检：探测目标资源能力与链路基线。

只读探测，不写盘。产出 ProbeResult 供 planner / scheduler 决策。

要点：
1. HEAD 可能被服务器禁用（405），失败不致命，用 Range GET 兜底。
2. 强制 Accept-Encoding: identity —— 一旦服务器对内容做 gzip，字节偏移就失效，
   Range 分块会拼出损坏文件。
3. 用一小段 Range 请求同时完成两件事：确认是否支持 Range、量出单连接基线速度。
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass

import aiohttp

DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_CONTENT_RANGE_RE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+|\*)")


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
    res.server_checksum = (
        res.server_checksum
        or headers.get("Content-MD5")
        or headers.get("x-amz-checksum-sha256")
        or headers.get("Digest")
    )


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
                res.note = (res.note + "; " if res.note else "") + f"Range 探测 HTTP {r.status}"
    except Exception as e:  # noqa: BLE001
        res.note = (res.note + "; " if res.note else "") + f"Range 探测失败: {type(e).__name__}"

    return res
