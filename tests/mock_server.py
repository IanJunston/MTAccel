"""本地 Range 测试服务器（用于验证自适应算法，不依赖外网）。

可以模拟三种真实世界的限制：
    --per-conn   单连接限速（模拟服务器对单 TCP 连接的限速）
    --conn-limit 最大并发连接数（超出返回 429，模拟单 IP 并发风控）
    --total      服务器总出口带宽（所有连接共享，模拟真实瓶颈）

典型用法（验证"爬坡能收敛到拐点"）：
    python -m tests.mock_server --size 200MB --per-conn 5MB --conn-limit 4 --total 20MB

预期：并发从 1 爬到 4 时速度线性增长（5/10/15/20），到 8 时因 conn-limit=4
      被拒/总带宽封顶，增益归零，算法应回退锁定 4。
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import re
import time

from aiohttp import web

RANGE_RE = re.compile(r"bytes=(\d+)-(\d*)")


def parse_size(s: str) -> int:
    s = s.strip().upper()
    mult = 1
    for suf, m in (("KB", 1024), ("MB", 1024**2), ("GB", 1024**3), ("K", 1024), ("M", 1024**2), ("G", 1024**3)):
        if s.endswith(suf):
            mult = m
            s = s[: -len(suf)]
            break
    return int(float(s) * mult)


class Bucket:
    """全局令牌桶：所有连接共享，用来模拟服务器总出口带宽。"""

    def __init__(self, rate: float | None):
        self.rate = rate
        self.tokens = float(rate or 0)
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def consume(self, n: int) -> None:
        if not self.rate:
            return
        while True:
            async with self.lock:
                now = time.monotonic()
                self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= n:
                    self.tokens -= n
                    return
                need = (n - self.tokens) / self.rate
            await asyncio.sleep(min(need, 0.5))


class MockServer:
    def __init__(
        self,
        size: int,
        per_conn: float | None,
        conn_limit: int | None,
        total: float | None,
        no_range: bool = False,
    ):
        self.size = size
        self.no_range = no_range
        self.per_conn = per_conn
        self.conn_limit = conn_limit
        self.bucket = Bucket(total)
        self.active = 0
        self.rejected = 0
        self.served = 0
        # 固定种子生成的确定性内容：任何实例、任何时刻同一偏移都是同一字节，
        # 这样测试脚本可以独立算出期望 hash 来校验下载结果。
        self.block = random.Random(20260101).randbytes(1024 * 1024)
        self.block_len = len(self.block)

    def _payload(self, pos: int, n: int) -> bytes:
        """按全局偏移生成稳定的内容（同偏移永远同一字节，便于 hash 校验）。"""
        out = bytearray()
        while len(out) < n:
            off = (pos + len(out)) % self.block_len
            take = min(n - len(out), self.block_len - off)
            out += self.block[off : off + take]
        return bytes(out)

    async def handle_head(self, request: web.Request) -> web.Response:
        headers = {
            "Content-Length": str(self.size),
            "Content-Type": "application/octet-stream",
            "ETag": f'"mock-{self.size}"',
        }
        if not self.no_range:
            headers["Accept-Ranges"] = "bytes"
        return web.Response(status=200, headers=headers)

    async def handle_get(self, request: web.Request) -> web.StreamResponse:
        if self.conn_limit is not None and self.active >= self.conn_limit:
            self.rejected += 1
            return web.Response(status=429, text="too many connections")

        self.active += 1
        try:
            rng = None if self.no_range else request.headers.get("Range")
            if rng:
                m = RANGE_RE.match(rng)
                if not m:
                    return web.Response(status=416, text="bad range")
                start = int(m.group(1))
                end = int(m.group(2)) if m.group(2) else self.size - 1
                if start >= self.size:
                    return web.Response(status=416, text="range not satisfiable")
                end = min(end, self.size - 1)
                status = 206
                headers = {
                    "Content-Range": f"bytes {start}-{end}/{self.size}",
                    "Content-Length": str(end - start + 1),
                    "Accept-Ranges": "bytes",
                    "ETag": f'"mock-{self.size}"',
                }
            else:
                start, end = 0, self.size - 1
                status = 200
                headers = {
                    "Content-Length": str(self.size),
                    "ETag": f'"mock-{self.size}"',
                }
                if not self.no_range:
                    headers["Accept-Ranges"] = "bytes"

            resp = web.StreamResponse(status=status, headers=headers)
            await resp.prepare(request)

            pos = start
            step = 256 * 1024
            t0 = time.monotonic()
            while pos <= end:
                n = min(step, end - pos + 1)
                chunk = self._payload(pos, n)
                await self.bucket.consume(n)
                if self.per_conn:
                    # 单连接限速：按已发送量校准时间
                    should = (pos - start + n) / self.per_conn
                    actual = time.monotonic() - t0
                    if should > actual:
                        await asyncio.sleep(should - actual)
                await resp.write(chunk)
                pos += n
                self.served += n
            return resp
        except (ConnectionResetError, asyncio.CancelledError):
            return web.Response(status=499)
        finally:
            self.active -= 1


def main() -> None:
    ap = argparse.ArgumentParser(description="本地 Range 测试服务器")
    ap.add_argument("--size", default="200MB")
    ap.add_argument("--per-conn", default=None, help="单连接限速，如 5MB")
    ap.add_argument("--conn-limit", type=int, default=None, help="最大并发连接数")
    ap.add_argument("--total", default=None, help="服务器总带宽，如 20MB")
    ap.add_argument("--no-range", action="store_true", help="模拟不支持 Range 的服务器")
    ap.add_argument("--port", type=int, default=8899)
    args = ap.parse_args()

    srv = MockServer(
        size=parse_size(args.size),
        per_conn=parse_size(args.per_conn) if args.per_conn else None,
        conn_limit=args.conn_limit,
        total=parse_size(args.total) if args.total else None,
        no_range=args.no_range,
    )

    app = web.Application()
    app.router.add_route("HEAD", "/file.bin", srv.handle_head)
    app.router.add_get("/file.bin", srv.handle_get, allow_head=False)

    print(f"mock server: http://127.0.0.1:{args.port}/file.bin")
    print(f"  size={srv.size}  per_conn={srv.per_conn}  conn_limit={srv.conn_limit}  total={srv.bucket.rate}")
    web.run_app(app, host="127.0.0.1", port=args.port, access_log=None)


if __name__ == "__main__":
    main()
