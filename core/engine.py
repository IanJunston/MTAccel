"""下载引擎：worker 池 + 动态任务队列 + 断点续传 + 完整性校验。

对外只暴露两类东西：
    Downloader(...).run()      —— 跑完整个下载
    Downloader(...).stop()     —— 请求停止（保留 .part 与 sidecar，可续传）

事件协议（dict，通过 emit 回调异步推出）：
    {"type":"probe", ...}                    探测结果
    {"type":"plan", ...}                     分块方案 / 是否续传
    {"type":"chunk","i":n,"s":"active|done"} 单个分块状态变化
    {"type":"strategy", ...}                 爬坡与 AIMD 每一步
    {"type":"progress", ...}                 周期性进度
    {"type":"verify","stage":"..."}          校验阶段
    {"type":"state","state":"..."}           整体状态
    {"type":"error","message":"..."}         错误
    {"type":"done", ...}                     完成
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable

import aiohttp

from . import planner, probe as probe_mod, resume as resume_mod, verify as verify_mod
from . import writer as writer_mod
from .scheduler import AdaptiveGate, StrategyController


class StopRequested(Exception):
    """内部信号：收到停止请求，让 worker 干净退出。"""


class RateLimited(Exception):
    """服务器限流（429/503）：不算块失败，应立即降并发并退避重试。"""


@dataclass
class Options:
    chunk_size: int | None = None        # None = 自动
    max_concurrency: int = 32
    speed_limit: float | None = None     # bytes/s，None = 不限
    retries: int = 4
    verify_hash: str | None = None
    verify_algo: str = "sha256"
    ramp_window: float = 2.0
    min_gain: float = 0.15
    progress_interval: float = 0.2


class TokenBucket:
    """令牌桶限速。"""

    def __init__(self, rate: float):
        self.rate = float(rate)
        self.tokens = float(rate)
        self.last = time.monotonic()

    def set_rate(self, rate: float) -> None:
        self.rate = float(max(1.0, rate))
        self.tokens = min(self.tokens, self.rate)

    async def consume(self, n: int) -> None:
        while True:
            now = time.monotonic()
            self.tokens = min(self.rate, self.tokens + (now - self.last) * self.rate)
            self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            await asyncio.sleep(min((n - self.tokens) / self.rate, 0.2))


class Downloader:
    def __init__(
        self,
        url: str,
        out_path: str,
        headers: dict | None = None,
        options: Options | None = None,
        emit: Callable[[dict], Awaitable[None]] | None = None,
    ):
        self.url = url
        self.out_path = out_path
        self.headers = headers or {}
        self.opt = options or Options()
        self._emit_cb = emit

        self._stop = False
        self._t_start = 0.0

        # 运行时状态
        self.probe: probe_mod.ProbeResult | None = None
        self.total: int = 0
        self.chunk_size: int = 0
        self.chunks: list[tuple[int, int, int]] = []
        self.done: list[bool] = []
        self.done_bytes: int = 0
        self.bytes_now: int = 0
        self.errors: int = 0
        self.rate_limited: int = 0
        self.failed: list[int] = []
        self.peak_speed: float = 0.0

        self.queue: asyncio.Queue | None = None
        self.gate: AdaptiveGate | None = None
        self.bucket: TokenBucket | None = None
        self.controller: StrategyController | None = None

        self._samples: deque[tuple[float, int]] = deque()
        self._speed = 0.0
        self._last_sample_t = 0.0
        self._active: set[int] = set()
        self._writer: writer_mod.PreallocatedFile | None = None
        self._resume: resume_mod.ResumeState | None = None
        self._last_save = 0.0
        self._workers_n = 0

    # ---------- 事件与度量 ----------

    async def emit(self, ev: dict) -> None:
        if self._emit_cb:
            with contextlib.suppress(Exception):
                await self._emit_cb(ev)

    def speed_now(self) -> float:
        """滑动窗口瞬时速度（bytes/s），窗口 3 秒。"""
        now = time.monotonic()
        if now - self._last_sample_t < 0.05:
            return self._speed
        self._last_sample_t = now
        self._samples.append((now, self.bytes_now))
        while len(self._samples) > 2 and now - self._samples[0][0] > 3.0:
            self._samples.popleft()
        if len(self._samples) < 2:
            return self._speed
        t0, b0 = self._samples[0]
        t1, b1 = self._samples[-1]
        dt = t1 - t0
        if dt <= 0:
            return self._speed
        self._speed = max(0.0, (b1 - b0) / dt)
        self.peak_speed = max(self.peak_speed, self._speed)
        return self._speed

    def _all_done(self) -> bool:
        if self.queue is None:
            return True
        return self.queue.empty() and not self._active

    def stop(self) -> None:
        self._stop = True

    def set_concurrency(self, n: int) -> None:
        n = max(1, min(64, int(n)))
        if self.controller:
            self.controller.max_concurrency = n
            # 爬坡阶段只更新上限，不打断正在进行的探测；稳态下立即生效
            if self.controller.phase != "ramp" and self.gate:
                asyncio.create_task(self.gate.set_limit(n))
        elif self.gate:
            asyncio.create_task(self.gate.set_limit(n))

    def set_speed_limit(self, rate: float | None) -> None:
        if rate and rate > 0:
            if self.bucket is None:
                self.bucket = TokenBucket(rate)
            else:
                self.bucket.set_rate(rate)
        else:
            self.bucket = None

    # ---------- 主流程 ----------

    async def run(self) -> dict | None:
        self._t_start = time.perf_counter()
        connector = aiohttp.TCPConnector(limit=0, force_close=False)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=60)
        try:
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                await self.emit({"type": "state", "state": "probing"})
                pr = await probe_mod.probe(session, self.url, self.headers)
                self.probe = pr
                await self.emit({"type": "probe", **pr.to_dict()})

                if pr.status >= 400:
                    await self.emit({"type": "error", "message": f"HTTP {pr.status}，无法下载"})
                    return None

                if pr.can_parallel:
                    await self._run_parallel(session, pr)
                else:
                    await self._run_single(session, pr)

                if self._stop:
                    self._save_state(force=True)
                    await self.emit({"type": "state", "state": "paused"})
                return {"stopped": self._stop, "failed": list(self.failed)}
        except asyncio.CancelledError:
            self._save_state(force=True)
            await self.emit({"type": "state", "state": "paused"})
            raise
        except Exception as e:  # noqa: BLE001
            self._save_state(force=True)
            await self.emit({"type": "error", "message": f"{type(e).__name__}: {e}"})
            raise

    # ---------- 单流模式（服务器不支持 Range） ----------

    async def _run_single(self, session: aiohttp.ClientSession, pr: probe_mod.ProbeResult) -> None:
        part = self.out_path + ".part"
        total = pr.size or 0
        self.total = total
        await self.emit({
            "type": "plan", "mode": "single", "size": total,
            "chunk_size": 0, "total_chunks": 0, "resumed": False, "done_chunks": 0,
        })
        self._samples.clear()
        self._last_sample_t = 0.0
        self.bytes_now = 0
        got = 0
        h = probe_mod.build_headers(self.headers)
        last_emit = 0.0
        with open(part, "wb") as f:
            async with session.get(self.url, headers=h) as r:
                if r.status >= 400:
                    raise RuntimeError(f"HTTP {r.status}")
                if total == 0:
                    cl = r.headers.get("Content-Length")
                    if cl and cl.isdigit():
                        total = int(cl)
                        self.total = total
                async for data in r.content.iter_chunked(256 * 1024):
                    if self._stop:
                        raise StopRequested()
                    f.write(data)
                    got += len(data)
                    self.bytes_now += len(data)
                    if self.bucket:
                        await self.bucket.consume(len(data))
                    now = time.perf_counter()
                    if now - last_emit > 0.2:
                        last_emit = now
                        await self._emit_progress(got, total, 1)
        if total and got != total:
            raise RuntimeError(f"单流下载不完整: {got}/{total}")
        await self._finalize(pr, part)

    # ---------- 多连接分块模式 ----------

    async def _run_parallel(self, session: aiohttp.ClientSession, pr: probe_mod.ProbeResult) -> None:
        size = int(pr.size or 0)
        self.total = size
        cs = self.opt.chunk_size or planner.choose_chunk_size(size, self.opt.max_concurrency)
        self.chunk_size = cs
        chunks = planner.plan(size, cs)
        self.chunks = chunks
        total_chunks = len(chunks)
        part = self.out_path + ".part"

        rs = resume_mod.ResumeState.load(part)
        resumed = False
        if rs and rs.compatible(size, cs, total_chunks, pr.etag, pr.last_modified):
            done = rs.done_list()
            self._resume = rs
            resumed = True
        else:
            if rs:
                rs.remove()
            done = [False] * total_chunks
            self._resume = resume_mod.ResumeState(part, {}, "")
        self.done = done
        self.done_bytes = sum(
            planner.chunk_bytes(size, cs, i) for i, d in enumerate(done) if d
        )

        self._writer = writer_mod.PreallocatedFile(part, size)
        self._writer.prepare()

        self.queue = asyncio.Queue()
        for i, s, e in chunks:
            if not done[i]:
                self.queue.put_nowait((i, s, e, 0))
        pending = self.queue.qsize()

        await self.emit({
            "type": "plan", "mode": "parallel", "size": size,
            "chunk_size": cs, "total_chunks": total_chunks,
            "resumed": resumed, "done_chunks": total_chunks - pending,
        })
        if resumed and pending:
            for i, d in enumerate(done):
                if d:
                    await self.emit({"type": "chunk", "i": i, "s": "done"})

        if pending == 0:
            await self._finalize(pr, part)
            return

        if self.opt.speed_limit:
            self.bucket = TokenBucket(self.opt.speed_limit)

        self._workers_n = min(self.opt.max_concurrency, pending)
        self.gate = AdaptiveGate(1)
        self.bytes_now = self.done_bytes
        self._samples.clear()
        self._last_sample_t = 0.0

        self.controller = StrategyController(
            gate=self.gate,
            speed_provider=self.speed_now,
            error_provider=lambda: self.errors,
            max_concurrency=self._workers_n,
            ramp_window=self.opt.ramp_window,
            min_gain=self.opt.min_gain,
            emit=self.emit,
            remaining_provider=lambda: (self.queue.qsize() + len(self._active)),
        )

        workers = [
            asyncio.create_task(self._worker(i, session, pr)) for i in range(self._workers_n)
        ]
        ctrl_task = asyncio.create_task(
            self.controller.run(lambda: self._stop or self._all_done())
        )
        prog_task = asyncio.create_task(self._progress_loop())

        try:
            await asyncio.gather(*workers)
        finally:
            for t in (ctrl_task, prog_task):
                t.cancel()
            for t in (ctrl_task, prog_task):
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await t

        self._save_state(force=True)

        if self._stop:
            return
        if self.failed:
            await self.emit({
                "type": "error",
                "message": f"{len(self.failed)} 个分块重试耗尽，未完成下载",
            })
            return

        await self._finalize(pr, part)

    # ---------- worker ----------

    async def _worker(self, wid: int, session: aiohttp.ClientSession, pr: probe_mod.ProbeResult) -> None:
        assert self.queue is not None and self.gate is not None and self._writer is not None
        fh = self._writer.open_writer()
        try:
            while not self._stop:
                try:
                    idx, start, end, attempt = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                await self.gate.acquire()
                self._active.add(idx)
                await self.emit({"type": "chunk", "i": idx, "s": "active", "w": wid})
                ok = False
                retry_delay = 0.0
                requeue_free = False
                try:
                    ok = await self._fetch(session, pr, idx, start, end, fh)
                except StopRequested:
                    requeue_free = True
                except RateLimited:  # noqa: BLE001
                    # 服务器限流：这不是块的错，是并发开太大了。
                    # 立即降档，但下限锁在已验证的安全并发 best，避免连锁降档把并发压到 1。
                    self.rate_limited += 1
                    self.errors += 1
                    floor = self.controller.best if self.controller else 1
                    with contextlib.suppress(Exception):
                        await self.gate.set_limit(max(max(1, floor), self.gate.limit - 1))
                    if self.rate_limited < 300:
                        requeue_free = True
                        retry_delay = min(0.4 + 0.12 * self.rate_limited, 4.0)
                    else:
                        self.failed.append(idx)
                        await self.emit({"type": "chunk", "i": idx, "s": "failed"})
                except Exception:  # noqa: BLE001
                    self.errors += 1
                finally:
                    self._active.discard(idx)
                    await self.gate.release()

                if ok:
                    self.done[idx] = True
                    nbytes = end - start + 1
                    self.done_bytes += nbytes
                    await self.emit({"type": "chunk", "i": idx, "s": "done"})
                    self._save_state()
                    continue

                if requeue_free:
                    if retry_delay:
                        await asyncio.sleep(retry_delay)
                    if not self._stop:
                        self.queue.put_nowait((idx, start, end, attempt))
                    continue

                if attempt + 1 < self.opt.retries:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    if not self._stop:
                        self.queue.put_nowait((idx, start, end, attempt + 1))
                else:
                    self.failed.append(idx)
                    await self.emit({"type": "chunk", "i": idx, "s": "failed"})
        finally:
            fh.close()

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        pr: probe_mod.ProbeResult,
        idx: int,
        start: int,
        end: int,
        fh,
    ) -> bool:
        expected = end - start + 1
        got = 0
        h = probe_mod.build_headers(self.headers)
        h["Range"] = f"bytes={start}-{end}"
        try:
            async with session.get(self.url, headers=h) as r:
                if r.status in (429, 503):
                    raise RateLimited(f"HTTP {r.status}")
                if r.status == 200:
                    raise RuntimeError("服务器对 Range 请求返回 200，分块下载不可用")
                if r.status != 206:
                    raise RuntimeError(f"HTTP {r.status}")
                fh.seek(start)
                async for data in r.content.iter_chunked(256 * 1024):
                    if self._stop:
                        raise StopRequested()
                    fh.write(data)
                    n = len(data)
                    got += n
                    self.bytes_now += n
                    if self.bucket:
                        await self.bucket.consume(n)
            if got != expected:
                raise RuntimeError(f"分块长度不足 {got}/{expected}")
            return True
        except StopRequested:
            self.bytes_now -= got
            raise
        except Exception:
            self.bytes_now -= got
            raise

    # ---------- 进度 ----------

    async def _progress_loop(self) -> None:
        while True:
            await asyncio.sleep(self.opt.progress_interval)
            active = self.gate.active if self.gate else 0
            await self._emit_progress(self.done_bytes, self.total, active)
            if self._all_done():
                return

    async def _emit_progress(self, confirmed: int, total: int, active: int) -> None:
        sp = self.speed_now()
        elapsed = time.perf_counter() - self._t_start
        avg = confirmed / elapsed if elapsed > 0 else 0.0
        percent = (confirmed / total * 100.0) if total else 0.0
        remain = max(0, total - confirmed)
        eta = (remain / sp) if sp > 1 else None
        await self.emit({
            "type": "progress",
            "downloaded": self.bytes_now,
            "confirmed": confirmed,
            "total": total,
            "percent": round(min(100.0, percent), 2),
            "speed": round(sp, 1),
            "avg_speed": round(avg, 1),
            "peak_speed": round(self.peak_speed, 1),
            "eta": eta,
            "active": active,
            "concurrency": self.gate.limit if self.gate else 1,
            "errors": self.errors,
            "elapsed": round(elapsed, 2),
        })

    # ---------- 状态持久化 ----------

    def _save_state(self, force: bool = False) -> None:
        if not self._resume or not self.chunks or not self.probe:
            return
        now = time.monotonic()
        if not force and now - self._last_save < 2.0:
            return
        self._last_save = now
        meta = {
            "size": self.total,
            "chunk_size": self.chunk_size,
            "total_chunks": len(self.chunks),
            "etag": self.probe.etag,
            "last_modified": self.probe.last_modified,
            "final_url": self.probe.final_url,
            "saved_at": time.time(),
        }
        with contextlib.suppress(Exception):
            self._resume.save(meta, self.done)

    # ---------- 收尾 ----------

    async def _finalize(self, pr: probe_mod.ProbeResult, part: str) -> None:
        await self.emit({"type": "verify", "stage": "size"})
        ok, msg = verify_mod.verify_size(part, self.total or None)
        if not ok:
            await self.emit({"type": "error", "message": f"校验失败：{msg}"})
            return

        if self.opt.verify_hash:
            await self.emit({"type": "verify", "stage": "hash"})
            loop = asyncio.get_running_loop()
            last = [0.0]

            def _cb(done: int, total: int) -> None:
                now = time.monotonic()
                if now - last[0] < 0.5:
                    return
                last[0] = now
                fut = loop.create_task(self.emit({
                    "type": "verify", "stage": "hash",
                    "done": done, "total": total,
                    "percent": round(done / total * 100, 2) if total else 0,
                }))
                fut.add_done_callback(lambda f: f.exception())

            ok, msg = await asyncio.to_thread(
                verify_mod.verify_hash, part, self.opt.verify_hash, self.opt.verify_algo, _cb
            )
            if not ok:
                await self.emit({"type": "error", "message": f"校验失败：{msg}"})
                return

        await asyncio.to_thread(os.replace, part, self.out_path)
        if self._resume:
            self._resume.remove()

        elapsed = time.perf_counter() - self._t_start
        await self.emit({
            "type": "done",
            "path": self.out_path,
            "size": self.total,
            "elapsed": round(elapsed, 2),
            "avg_speed": round(self.total / elapsed, 1) if elapsed > 0 else 0,
            "peak_speed": round(self.peak_speed, 1),
            "strategy": self.controller.history if self.controller else [],
        })
        await self.emit({"type": "state", "state": "done"})
