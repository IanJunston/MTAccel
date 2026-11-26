"""基准测试：直接驱动引擎，把策略与吞吐打到 stdout。

用途：
    1. 验证自适应算法在受控服务器上的收敛行为（配合 tests/mock_server.py）
    2. 对真实外网大文件测各并发档的实际吞吐，产出可引用的对比数据

用法：
    python tests/run_bench.py http://127.0.0.1:8899/file.bin --threads 32
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.engine import Downloader, Options  # noqa: E402


def mb(x: float | None) -> str:
    return "   nan" if x is None else f"{x / 1024 / 1024:6.2f}"


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("url")
    ap.add_argument("-o", "--output", default="./temp/bench.bin")
    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--limit", type=float, default=None, help="bytes/s")
    ap.add_argument("--chunk", type=int, default=None)
    args = ap.parse_args()

    out = os.path.abspath(args.output)
    os.makedirs(os.path.dirname(out), exist_ok=True)

    strategies: list[dict] = []
    state = {"done": None, "error": None}

    async def emit(ev: dict) -> None:
        t = ev.get("type")
        if t == "strategy":
            strategies.append(ev)
            gain = ev.get("gain")
            print(
                f"[strategy] {ev['phase']:5s} 并发={ev['concurrency']:2d} "
                f"速度={mb(ev.get('speed'))}MB/s "
                f"增益={'  --' if gain is None else f'{gain * 100:5.1f}%'}  {ev.get('reason', '')}"
            )
        elif t == "probe":
            print(
                f"[probe]    size={ev.get('size')} range={ev.get('accept_ranges')} "
                f"单连接={mb(ev.get('single_speed'))}MB/s note={ev.get('note', '')}"
            )
        elif t == "plan":
            print(
                f"[plan]     mode={ev.get('mode')} chunks={ev.get('total_chunks')} "
                f"chunk={ev.get('chunk_size')} resumed={ev.get('resumed')}"
            )
        elif t == "done":
            state["done"] = ev
            print(
                f"[done]     {ev['path']} {ev['size']}B in {ev['elapsed']}s "
                f"avg={mb(ev['avg_speed'])}MB/s peak={mb(ev['peak_speed'])}MB/s"
            )
        elif t == "error":
            state["error"] = ev.get("message")
            print(f"[error]    {ev.get('message')}")

    opts = Options(max_concurrency=args.threads, chunk_size=args.chunk, speed_limit=args.limit)
    dl = Downloader(args.url, out, None, opts, emit=emit)

    t0 = time.perf_counter()
    await dl.run()
    wall = time.perf_counter() - t0
    print(f"[wall]     {wall:.2f}s")

    if strategies:
        best = max(strategies, key=lambda s: s.get("speed") or 0)
        print(
            f"[best]     并发={best['concurrency']} "
            f"速度={mb(best.get('speed'))}MB/s（{best.get('phase')}）"
        )

    return 0 if state["done"] else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
