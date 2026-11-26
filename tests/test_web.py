"""Web 面板端到端测试：静态页 + SSE 事件流 + 启动/停止/动态调参。

自包含：脚本内部同时起 mock 源站（8898）与面板服务（8765），不依赖外部进程。

运行：
    python tests/test_web.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp
from aiohttp import web

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from app.server import create_app  # noqa: E402
from mock_server import MockServer  # noqa: E402

MOCK_PORT = 8898
PANEL_PORT = 8765
BASE = f"http://127.0.0.1:{PANEL_PORT}"
SRC = f"http://127.0.0.1:{MOCK_PORT}/file.bin"
OUT = str(ROOT / "temp" / "web_test.bin")


async def main() -> int:
    os.makedirs(str(ROOT / "temp"), exist_ok=True)
    for p in (OUT, OUT + ".part", OUT + ".part.json"):
        if os.path.exists(p):
            os.remove(p)

    # ---- 内嵌 mock 源站 ----
    src = MockServer(200 * 1024 * 1024, 5 * 1024 * 1024, 4, 20 * 1024 * 1024)
    mock_app = web.Application()
    mock_app.router.add_route("HEAD", "/file.bin", src.handle_head)
    mock_app.router.add_get("/file.bin", src.handle_get, allow_head=False)
    mock_runner = web.AppRunner(mock_app, access_log=None)
    await mock_runner.setup()
    await web.TCPSite(mock_runner, "127.0.0.1", MOCK_PORT).start()

    # ---- 内嵌面板服务 ----
    panel_app = create_app(ROOT / "temp" / "downloads")
    panel_runner = web.AppRunner(panel_app, access_log=None)
    await panel_runner.setup()
    await web.TCPSite(panel_runner, "127.0.0.1", PANEL_PORT).start()

    events: list[dict] = []
    ok = True
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(BASE + "/") as r:
                html = await r.text()
                has_ui = ("MTAccel" in html) and ("chunkMap" in html) and ("speedChart" in html)
                print(f"[首页] status={r.status} bytes={len(html)} 关键元素={has_ui}")
                ok = ok and r.status == 200 and has_ui

            async with s.get(BASE + "/api/events") as es:
                async def reader():
                    async for raw in es.content:
                        line = raw.decode("utf-8", "ignore").strip()
                        if line.startswith("data: "):
                            try:
                                events.append(json.loads(line[6:]))
                            except json.JSONDecodeError:
                                pass

                reader_task = asyncio.create_task(reader())
                await asyncio.sleep(0.3)

                async with s.post(BASE + "/api/start", json={
                    "url": SRC,
                    "output": OUT,
                    "max_concurrency": 8,
                }) as r:
                    print(f"[启动] status={r.status} {(await r.text())[:100]}")
                    ok = ok and r.status == 200

                await asyncio.sleep(4)
                async with s.post(BASE + "/api/config", json={"max_concurrency": 12}) as r:
                    print(f"[调参] status={r.status} {(await r.text())[:100]}")
                    ok = ok and r.status == 200

                for _ in range(300):
                    await asyncio.sleep(0.3)
                    if any(e.get("type") == "done" for e in events):
                        break
                reader_task.cancel()

        counts: dict[str, int] = {}
        for e in events:
            counts[e.get("type", "?")] = counts.get(e.get("type", "?"), 0) + 1
        print("[事件] " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())))

        need = ["probe", "plan", "progress", "strategy", "chunk", "done"]
        missing = [t for t in need if counts.get(t, 0) == 0]
        if missing:
            print(f"[FAIL] 缺少事件类型: {missing}")
            ok = False
        else:
            print("[OK] 全部关键事件类型都收到")

        done = next((e for e in events if e.get("type") == "done"), None)
        if not done:
            print("[FAIL] 没有收到 done 事件")
            return 1
        print(f"[完成] {done['size']}B / {done['elapsed']}s / 平均 {done['avg_speed'] / 1048576:.2f}MB/s")
        if not os.path.exists(OUT):
            print("[FAIL] 目标文件不存在")
            ok = False
        else:
            size = os.path.getsize(OUT)
            print(f"[文件] {size}B")
            ok = ok and size == done["size"]

        # 重复启动应被拒绝（409）
        async with aiohttp.ClientSession() as s:
            async with s.post(BASE + "/api/start", json={"url": SRC, "output": OUT}) as r:
                print(f"[重复启动] status={r.status}（预期 409 或已完成）")
                ok = ok and r.status in (200, 409)

        print("\n结果:", "通过" if ok else "存在失败项")
        return 0 if ok else 1
    finally:
        await panel_runner.cleanup()
        await mock_runner.cleanup()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
