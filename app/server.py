"""本地 Web 面板服务：aiohttp + SSE。

为什么用 SSE 而不是 WebSocket：
    进度是单向推送（服务端 → 浏览器）。SSE 协议简单、浏览器自带 EventSource、
    断线自动重连，不需要任何前端库或打包工具。WebSocket 的双向能力这里用不上。

单任务模型（v0.1）：
    同一时刻只跑一个下载。任务状态保存在 AppState，页面刷新后通过事件历史回放
    重建界面，不会丢进度。
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import sys
import webbrowser
from pathlib import Path
from urllib.parse import unquote, urlparse

from aiohttp import web

# 两种运行方式都要能找到资源：
#   1) `python app/server.py` —— sys.path[0] 是 app/，得把项目根塞进 sys.path
#   2) 打包成 exe —— core 已在包内，静态资源在 PyInstaller 的解压目录 _MEIPASS
_FROZEN = getattr(sys, "frozen", False)
_ROOT = (
    Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    if _FROZEN
    else Path(__file__).resolve().parent.parent
)
if not _FROZEN and str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.engine import Downloader, Options  # noqa: E402

WEB_DIR = _ROOT / "app" / "web"
# exe 的默认下载目录放在 exe 旁边，免得落到系统临时目录
_DEFAULT_DL = str(Path(sys.executable).parent / "downloads") if _FROZEN else "./downloads"
HISTORY_LIMIT = 30000


def guess_name(url: str) -> str:
    path = urlparse(url).path
    name = unquote(os.path.basename(path)) or "download.bin"
    # 去掉可能的查询串残留
    return name.split("?")[0] or "download.bin"


class Broadcaster:
    """事件广播：多订阅者 + 历史回放。"""

    def __init__(self):
        self._queues: set[asyncio.Queue] = set()
        self.history: list[dict] = []

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=4000)
        self._queues.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._queues.discard(q)

    def publish(self, ev: dict) -> None:
        self.history.append(ev)
        if len(self.history) > HISTORY_LIMIT:
            del self.history[: len(self.history) - HISTORY_LIMIT]
        for q in list(self._queues):
            with contextlib.suppress(asyncio.QueueFull):
                q.put_nowait(ev)

    def reset(self) -> None:
        self.history.clear()


class AppState:
    def __init__(self, download_dir: Path):
        self.download_dir = download_dir
        self.downloader: Downloader | None = None
        self.task: asyncio.Task | None = None
        self.broadcaster = Broadcaster()
        self.status = "idle"
        self.last_progress: dict = {}

    @property
    def busy(self) -> bool:
        return self.task is not None and not self.task.done()


# ---------------- handlers ----------------


async def handle_index(request: web.Request) -> web.StreamResponse:
    p = WEB_DIR / "index.html"
    if not p.exists():
        return web.Response(text="index.html 缺失", status=500)
    return web.FileResponse(p, headers={"Cache-Control": "no-store"})


async def _sse(resp: web.StreamResponse, ev: dict) -> None:
    payload = json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
    await resp.write(f"data: {payload}\n\n".encode("utf-8"))


async def handle_events(request: web.Request) -> web.StreamResponse:
    st: AppState = request.app["state"]
    resp = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream; charset=utf-8",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    await resp.prepare(request)
    q = st.broadcaster.subscribe()
    try:
        for ev in list(st.broadcaster.history):
            await _sse(resp, ev)
        await resp.write(b": connected\n\n")
        while True:
            try:
                ev = await asyncio.wait_for(q.get(), timeout=15)
                await _sse(resp, ev)
            except asyncio.TimeoutError:
                await resp.write(b": ping\n\n")
    except (asyncio.CancelledError, ConnectionResetError, RuntimeError):
        pass
    finally:
        st.broadcaster.unsubscribe(q)
    return resp


async def handle_start(request: web.Request) -> web.Response:
    st: AppState = request.app["state"]
    try:
        data = await request.json()
    except Exception:  # noqa: BLE001
        raise web.HTTPBadRequest(text="请求体必须是 JSON")

    url = str(data.get("url") or "").strip()
    if not url.startswith(("http://", "https://")):
        raise web.HTTPBadRequest(text="只支持 http/https 直链")
    if st.busy:
        raise web.HTTPConflict(text="已有下载任务在进行，请先停止")

    out = str(data.get("output") or "").strip() or guess_name(url)
    if not os.path.isabs(out):
        st.download_dir.mkdir(parents=True, exist_ok=True)
        out = str(st.download_dir / out)

    headers = data.get("headers") or {}
    if isinstance(headers, list):
        headers = {str(k): str(v) for k, v in headers}
    if not isinstance(headers, dict):
        headers = {}

    try:
        max_c = int(data.get("max_concurrency") or 32)
    except (TypeError, ValueError):
        max_c = 32
    max_c = max(1, min(64, max_c))

    speed_limit = data.get("speed_limit")
    try:
        speed_limit = float(speed_limit) if speed_limit else None
    except (TypeError, ValueError):
        speed_limit = None

    verify_hash = str(data.get("verify_hash") or "").strip() or None
    algo = str(data.get("verify_algo") or "sha256").strip() or "sha256"

    opts = Options(
        max_concurrency=max_c,
        speed_limit=speed_limit,
        verify_hash=verify_hash,
        verify_algo=algo,
    )

    st.broadcaster.reset()
    st.status = "running"
    st.last_progress = {}

    async def emit(ev: dict) -> None:
        if ev.get("type") == "progress":
            st.last_progress = ev
        if ev.get("type") == "state":
            st.status = str(ev.get("state") or st.status)
        st.broadcaster.publish(ev)

    dl = Downloader(url, out, headers, opts, emit=emit)
    st.downloader = dl

    async def runner() -> None:
        try:
            await dl.run()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            st.broadcaster.publish({"type": "error", "message": f"{type(e).__name__}: {e}"})
        finally:
            if st.status == "running":
                st.status = "idle"

    st.task = asyncio.create_task(runner())
    return web.json_response({"ok": True, "output": out, "max_concurrency": max_c})


async def handle_stop(request: web.Request) -> web.Response:
    st: AppState = request.app["state"]
    if not st.busy or st.downloader is None:
        return web.json_response({"ok": False, "message": "没有进行中的任务"})
    st.downloader.stop()
    return web.json_response({"ok": True, "message": "已请求停止，已下分块保留"})


async def handle_config(request: web.Request) -> web.Response:
    """运行中动态调整并发上限 / 限速。"""
    st: AppState = request.app["state"]
    try:
        data = await request.json()
    except Exception:  # noqa: BLE001
        raise web.HTTPBadRequest(text="请求体必须是 JSON")

    dl = st.downloader
    applied: dict = {}
    if dl and st.busy:
        if "max_concurrency" in data:
            with contextlib.suppress(TypeError, ValueError):
                n = max(1, min(64, int(data["max_concurrency"])))
                dl.set_concurrency(n)
                applied["max_concurrency"] = n
        if "speed_limit" in data:
            v = data["speed_limit"]
            try:
                rate = float(v) if v else None
            except (TypeError, ValueError):
                rate = None
            dl.set_speed_limit(rate)
            applied["speed_limit"] = rate
    return web.json_response({"ok": True, "applied": applied, "running": bool(dl and st.busy)})


async def handle_state(request: web.Request) -> web.Response:
    st: AppState = request.app["state"]
    return web.json_response({
        "status": st.status,
        "busy": st.busy,
        "download_dir": str(st.download_dir),
        "progress": st.last_progress,
    })


def create_app(download_dir: Path) -> web.Application:
    app = web.Application(client_max_size=1024**2)
    app["state"] = AppState(download_dir)
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/events", handle_events)
    app.router.add_post("/api/start", handle_start)
    app.router.add_post("/api/stop", handle_stop)
    app.router.add_post("/api/config", handle_config)
    app.router.add_get("/api/state", handle_state)
    return app


async def serve(args) -> None:
    download_dir = Path(args.download_dir).resolve()
    download_dir.mkdir(parents=True, exist_ok=True)
    app = create_app(download_dir)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()

    port = args.port
    site = None
    for _ in range(30):
        try:
            site = web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            break
        except OSError:
            site = None
            port += 1
    if site is None:
        await runner.cleanup()
        raise SystemExit("找不到可用端口")

    # 打包成 exe 时，把黑窗口标题改得像个正经程序
    if _FROZEN:
        with contextlib.suppress(Exception):
            import ctypes

            ctypes.windll.kernel32.SetConsoleTitleW("多线程加速下载器")

    url = f"http://127.0.0.1:{port}/"
    print(f"面板已启动: {url}")
    print(f"下载目录: {download_dir}")
    print("按 Ctrl+C 退出")
    if not args.no_browser:
        with contextlib.suppress(Exception):
            webbrowser.open(url)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    with contextlib.suppress(NotImplementedError):
        loop.add_signal_handler(2, stop.set)  # SIGINT，Windows 不支持则忽略

    try:
        await stop.wait()
    except asyncio.CancelledError:
        pass
    finally:
        st: AppState = app["state"]
        if st.busy and st.downloader:
            st.downloader.stop()
        await runner.cleanup()


def main() -> None:
    ap = argparse.ArgumentParser(description="多线程下载加速器 · 本地面板")
    ap.add_argument("--port", type=int, default=8765, help="起始端口（被占用自动 +1）")
    ap.add_argument("--download-dir", default=_DEFAULT_DL, help="默认下载目录")
    ap.add_argument("--no-browser", action="store_true", help="不自动打开浏览器")
    args = ap.parse_args()

    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        print("\n已退出")
        sys.exit(0)


if __name__ == "__main__":
    main()
