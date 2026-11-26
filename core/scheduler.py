"""自适应并发调度：爬坡探测 + AIMD 稳态微调。

为什么不用固定线程数：
    最佳并发取决于三个事先未知的变量——本机带宽、服务器单连接限速、服务器单 IP
    并发上限。三者不同站点差异巨大。固定值必然在某些站点过小（跑不满）或过大
    （被限速 / 429）。所以让程序自己试出来。

算法：
    阶段一 爬坡（Ramp）：1 → 2 → 4 → 8 → 16 → 32，每档跑 ramp_window 秒，测这一档
        的吞吐并算边际增益。增益低于 min_gain 就回退到上一档并锁定。这个拐点就是
        "当前链路的真实上限"，比任何固定值都更贴近实际。
    阶段二 稳态（AIMD）：每秒采样。速度明显下滑且伴随错误 → 并发 -1（拥塞响应）；
        长期稳定无错误 → 每 5 秒尝试 +1（继续探测更高档）。

两个防误判设计：
    - 尾部不调档：剩余块数少于并发数的 2 倍时停止调整，否则"快下完了速度自然下降"
      会被误判成拥塞，把并发降到 1，反而拖慢收尾。
    - 速度样本用滑动窗口（engine 侧），不用累计平均，避免历史拉平瞬时变化。
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable


class AdaptiveGate:
    """可动态调整上限的并发闸门（asyncio.Condition 实现，无忙等）。"""

    def __init__(self, limit: int = 1):
        self._limit = max(1, int(limit))
        self._active = 0
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def active(self) -> int:
        return self._active

    async def acquire(self) -> None:
        async with self._cond:
            await self._cond.wait_for(lambda: self._active < self._limit)
            self._active += 1

    async def release(self) -> None:
        async with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()

    async def set_limit(self, n: int) -> None:
        n = max(1, int(n))
        async with self._cond:
            if n != self._limit:
                self._limit = n
                self._cond.notify_all()


class StrategyController:
    def __init__(
        self,
        gate: AdaptiveGate,
        speed_provider: Callable[[], float],
        error_provider: Callable[[], int],
        max_concurrency: int = 32,
        ramp_window: float = 2.0,
        min_gain: float = 0.15,
        emit: Callable[[dict], Awaitable[None]] | None = None,
        remaining_provider: Callable[[], int] | None = None,
    ):
        self.gate = gate
        self.speed_provider = speed_provider
        self.error_provider = error_provider
        self.max_concurrency = max(1, int(max_concurrency))
        self.ramp_window = ramp_window
        self.min_gain = min_gain
        self._emit = emit
        self.remaining_provider = remaining_provider
        self.history: list[dict] = []
        self.phase = "init"
        self.best = 1
        # 限流惩罚期：一旦撞到 429/超时，窗口内不再试探升档，避免来回震荡
        self.penalty_until = 0.0
        self.penalty_window = 30.0
        self.probe_up_interval = 5.0

    async def _log(self, phase: str, concurrency: int, speed: float, gain=None, reason: str = "") -> None:
        entry = {
            "phase": phase,
            "concurrency": concurrency,
            "speed": round(speed, 1),
            "gain": None if gain is None else round(gain, 4),
            "reason": reason,
            "t": time.time(),
        }
        self.history.append(entry)
        if self._emit:
            await self._emit({"type": "strategy", **entry})

    async def run(self, should_stop: Callable[[], bool]) -> None:
        await self._ramp(should_stop)
        if not should_stop():
            await self._steady(should_stop)

    # ---------- 阶段一：爬坡 ----------

    async def _ramp(self, should_stop: Callable[[], bool]) -> None:
        self.phase = "ramp"
        level = 1
        await self.gate.set_limit(1)
        await asyncio.sleep(self.ramp_window)
        if should_stop():
            return
        prev = self.speed_provider()
        err_base = self.error_provider()
        await self._log("ramp", 1, prev, None, "单连接基线")
        self.best = 1

        while level < self.max_concurrency and not should_stop():
            nxt = min(level * 2, self.max_concurrency)
            await self.gate.set_limit(nxt)
            await asyncio.sleep(self.ramp_window)
            if should_stop():
                return
            cur = self.speed_provider()

            # 错误优先于速度：这一档触发限流/超时，说明已经越过服务器的并发容忍度
            err_now = self.error_provider()
            if err_now > err_base:
                await self.gate.set_limit(level)
                self.best = level
                self.penalty_until = time.monotonic() + self.penalty_window
                await self._log(
                    "ramp", nxt, cur, None,
                    f"该档触发 {err_now - err_base} 次错误（限流/超时），回退锁定 {level} 并发",
                )
                return

            gain = (cur - prev) / prev if prev > 0 else 1.0
            if prev > 0 and gain < self.min_gain:
                await self.gate.set_limit(level)
                self.best = level
                await self._log(
                    "ramp", nxt, cur, gain,
                    f"边际增益 {gain:.0%} < {self.min_gain:.0%}，回退并锁定 {level} 并发",
                )
                return
            await self._log("ramp", nxt, cur, gain, "增益达标，继续爬坡")
            level, prev = nxt, cur
            self.best = level

        await self._log("ramp", level, prev, None, "已达并发上限")

    # ---------- 阶段二：稳态 AIMD ----------

    async def _steady(self, should_stop: Callable[[], bool]) -> None:
        self.phase = "steady"
        prev = self.speed_provider()
        last_err = self.error_provider()
        stable_since = time.monotonic()

        while not should_stop():
            await asyncio.sleep(1.0)
            if should_stop():
                break
            now = time.monotonic()
            cur = self.speed_provider()
            err = self.error_provider()
            new_err = err > last_err
            last_err = err
            limit = self.gate.limit

            if new_err:
                # 出现限流/超时：进入惩罚期，期间不再试探升档
                self.penalty_until = now + self.penalty_window
                if limit > self.best:
                    await self.gate.set_limit(self.best)
                    await self._log("aimd", self.best, cur, None, "遇到限流，直接退回已验证的安全并发")
                    stable_since = now
                    prev = cur
                    continue

            remaining = self.remaining_provider() if self.remaining_provider else 10**9
            if remaining <= limit * 2:
                # 尾部保护：不再调档
                prev = cur
                continue

            if prev > 0 and cur < prev * 0.85 and self.gate.active >= limit and limit > 1:
                await self.gate.set_limit(limit - 1)
                await self._log("aimd", limit - 1, cur, None, "速度下滑且连接打满，降档")
                stable_since = now
            elif (
                not new_err
                and now > self.penalty_until
                and (now - stable_since) > self.probe_up_interval
                and limit < self.max_concurrency
            ):
                await self.gate.set_limit(limit + 1)
                await self._log("aimd", limit + 1, cur, None, "稳定，试探升档")
                stable_since = now
            prev = cur
