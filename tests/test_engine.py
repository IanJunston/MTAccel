"""引擎自测：完整性 + 断点续传 + 限速。

自包含：脚本内部起 mock 源站（8897），不依赖外部进程。

运行：
    python tests/test_engine.py
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
import time

from aiohttp import web

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tests"))

from core.engine import Downloader, Options  # noqa: E402
from mock_server import MockServer  # noqa: E402

PORT = 8897
URL = f"http://127.0.0.1:{PORT}/file.bin"
SIZE = 200 * 1024 * 1024
OUT = os.path.join(ROOT, "temp", "resume_test.bin")


def expected_digest() -> str:
    """按 mock server 的确定性内容算法，算出应有的 sha256。"""
    srv = MockServer(SIZE, None, None, None)
    h = hashlib.sha256()
    pos, step = 0, 4 * 1024 * 1024
    while pos < SIZE:
        n = min(step, SIZE - pos)
        h.update(srv._payload(pos, n))
        pos += n
    return h.hexdigest()


def file_digest(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(4 * 1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def cleanup() -> None:
    for p in (OUT, OUT + ".part", OUT + ".part.json", OUT + ".part.json.tmp"):
        if os.path.exists(p):
            os.remove(p)


async def run_once(stop_after: float | None = None, speed_limit: float | None = None):
    ev = {"plan": None, "done": None, "error": None, "strategy": []}

    async def emit(e: dict) -> None:
        t = e.get("type")
        if t in ("plan", "done", "error"):
            ev[t] = e
        elif t == "strategy":
            ev["strategy"].append(e)

    dl = Downloader(
        URL, OUT, None,
        Options(max_concurrency=8, ramp_window=1.0, speed_limit=speed_limit),
        emit=emit,
    )
    if stop_after:
        async def stopper():
            await asyncio.sleep(stop_after)
            dl.stop()
        asyncio.create_task(stopper())
    t0 = time.perf_counter()
    await dl.run()
    ev["wall"] = time.perf_counter() - t0
    return ev


async def _main_body() -> int:
    ok = True
    os.makedirs(os.path.join(ROOT, "temp"), exist_ok=True)
    cleanup()
    want = expected_digest()
    print(f"[期望] sha256={want[:16]}…")

    # ---- 1. 中断 + 续传 ----
    print("\n== 用例 1：跑到 3 秒后中断，再续传 ==")
    e1 = await run_once(stop_after=3.0)
    part = OUT + ".part"
    sidecar = OUT + ".part.json"
    if not (os.path.exists(part) and os.path.exists(sidecar)):
        print("  [FAIL] 中断后没有留下 .part / sidecar")
        ok = False
    else:
        print(f"  [OK] 中断留下 .part {os.path.getsize(part)}B 与 sidecar")

    e2 = await run_once()
    if not e2["done"]:
        print(f"  [FAIL] 续传未完成: {e2['error']}")
        return 1
    plan = e2["plan"]
    print(f"  续传 plan: resumed={plan['resumed']} 已完成块={plan['done_chunks']}/{plan['total_chunks']}")
    if not plan["resumed"] or plan["done_chunks"] == 0:
        print("  [FAIL] 没有走续传路径（应为 resumed=True 且已完成块 > 0）")
        ok = False
    else:
        print("  [OK] 走了续传路径")

    got = file_digest(OUT)
    if got != want:
        print(f"  [FAIL] hash 不符: {got[:16]}… != {want[:16]}…")
        ok = False
    else:
        print("  [OK] 完整性校验通过（续传后内容与源一致）")
    if os.path.exists(sidecar):
        print("  [FAIL] 完成后 sidecar 未清理")
        ok = False

    # ---- 2. 全量重下 ----
    print("\n== 用例 2：全量下载（对照）==")
    cleanup()
    e3 = await run_once()
    if not e3["done"]:
        print(f"  [FAIL] 下载失败: {e3['error']}")
        return 1
    print(f"  [OK] {e3['done']['size']}B / {e3['wall']:.2f}s / 平均 {e3['done']['avg_speed']/1048576:.2f}MB/s")
    print(f"  策略: " + " → ".join(f"{s['concurrency']}并发({s['speed']/1048576:.1f}MB/s)" for s in e3["strategy"]))

    # ---- 3. 限速 ----
    print("\n== 用例 3：限速 10MB/s ==")
    cleanup()
    e4 = await run_once(speed_limit=10 * 1024 * 1024)
    if not e4["done"]:
        print(f"  [FAIL] 下载失败: {e4['error']}")
        return 1
    avg = e4["done"]["avg_speed"] / 1048576
    print(f"  实测平均 {avg:.2f}MB/s（限速 10）")
    if avg > 12:
        print("  [FAIL] 限速未生效")
        ok = False
    else:
        print("  [OK] 限速生效")

    print("\n结果:", "全部通过" if ok else "存在失败项")
    cleanup()
    return 0 if ok else 1


async def main() -> int:
    """起内嵌源站后跑用例，结束时统一清理。"""
    src = MockServer(SIZE, 5 * 1024 * 1024, 4, 20 * 1024 * 1024)
    mock_app = web.Application()
    mock_app.router.add_route("HEAD", "/file.bin", src.handle_head)
    mock_app.router.add_get("/file.bin", src.handle_get, allow_head=False)
    runner = web.AppRunner(mock_app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", PORT).start()
    try:
        return await _main_body()
    finally:
        await runner.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
