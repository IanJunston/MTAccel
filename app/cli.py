"""命令行入口：终端里看进度与策略。

用法：
    python -m app.cli <URL> [-o 文件名] [--threads 32] [--limit 5M] [-H "Cookie: xxx"]
                          [--hash sha256:xxxx] [--chunk 8M]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from rich.text import Text

# 允许直接 `python app/cli.py` 运行
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from core.engine import Downloader, Options  # noqa: E402
from core.writer import human_eta, human_size, human_speed  # noqa: E402


def guess_name(url: str) -> str:
    path = urlparse(url).path
    name = unquote(os.path.basename(path)) or "download.bin"
    return name


def parse_size(s: str | None) -> float | None:
    if not s:
        return None
    s = s.strip().upper()
    mult = 1
    for suf, m in (("KB", 1024), ("MB", 1024**2), ("GB", 1024**3), ("K", 1024), ("M", 1024**2), ("G", 1024**3)):
        if s.endswith(suf):
            mult = m
            s = s[: -len(suf)]
            break
    return float(s) * mult


def parse_headers(items: list[str] | None) -> dict:
    out = {}
    for it in items or []:
        if ":" in it:
            k, v = it.split(":", 1)
            out[k.strip()] = v.strip()
    return out


async def run(args) -> int:
    console = Console()
    out_path = args.output or guess_name(args.url)
    if not os.path.isabs(out_path):
        out_path = os.path.abspath(out_path)

    verify_hash = None
    verify_algo = "sha256"
    if args.hash:
        if ":" in args.hash:
            verify_algo, verify_hash = args.hash.split(":", 1)
        else:
            verify_hash = args.hash

    opts = Options(
        chunk_size=int(parse_size(args.chunk)) if args.chunk else None,
        max_concurrency=args.threads,
        speed_limit=parse_size(args.limit),
        verify_hash=verify_hash,
        verify_algo=verify_algo,
        insecure=bool(getattr(args, 'insecure', False)),
    )

    state: dict = {"progress": {}, "probe": {}, "strategy": [], "state": "init", "done": None, "error": None}

    async def emit(ev: dict) -> None:
        t = ev.get("type")
        if t == "strategy":
            state["strategy"].append(ev)
            del state["strategy"][:-6]
        elif t in ("progress", "probe", "state", "done", "error", "plan"):
            state[t] = ev

    dl = Downloader(args.url, out_path, parse_headers(args.header), opts, emit=emit)

    progress = Progress(
        TextColumn("[bold cyan]{task.description}"),
        BarColumn(bar_width=None),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    )
    task_id = progress.add_task("连接中…", total=None)

    def render():
        p = state["progress"]
        probe = state["probe"]
        if p:
            progress.update(
                task_id,
                total=p.get("total") or None,
                completed=p.get("confirmed", 0),
                description=f"下载 {human_size(p.get('confirmed'))}/{human_size(p.get('total'))}",
            )

        grid = Table.grid(padding=(0, 2))
        grid.add_column(justify="left", style="dim")
        grid.add_column(justify="left", style="bold")
        grid.add_column(justify="left", style="dim")
        grid.add_column(justify="left", style="bold")

        if probe:
            grid.add_row(
                "大小", human_size(probe.get("size")),
                "Range", "[green]支持[/]" if probe.get("accept_ranges") else "[red]不支持[/]",
            )
        if p:
            eta = p.get("eta")
            grid.add_row(
                "瞬时", human_speed(p.get("speed")),
                "ETA", human_eta(eta),
            )
            grid.add_row(
                "并发", f"{p.get('concurrency', '-')} (活跃 {p.get('active', 0)})",
                "重试", str(p.get("errors", 0)),
            )

        strat_lines = []
        for s in state["strategy"][-3:]:
            phase = "爬坡" if s.get("phase") == "ramp" else "稳态"
            gain = s.get("gain")
            gtxt = f" 增益 {gain * 100:.0f}%" if isinstance(gain, (int, float)) else ""
            strat_lines.append(
                f"[dim]{phase}[/] [bold]{s.get('concurrency')} 并发[/] "
                f"[cyan]{human_speed(s.get('speed'))}[/]{gtxt} [dim]{s.get('reason', '')}[/]"
            )
        strat_block = Text.from_markup("\n".join(strat_lines)) if strat_lines else Text("策略探测中…", style="dim")

        return Group(progress, grid, Panel(strat_block, title="策略", border_style="magenta", padding=(0, 1)))

    with Live(render(), console=console, refresh_per_second=8) as live:
        dl_task = asyncio.create_task(dl.run())
        try:
            while not dl_task.done():
                live.update(render())
                await asyncio.sleep(0.15)
            await dl_task
        except KeyboardInterrupt:
            dl.stop()
            console.print("[yellow]正在停止，已下分块会保留以便续传…[/]")
            await dl_task
        except Exception as e:  # noqa: BLE001
            console.print(f"[red]失败：{e}[/]")
            return 1
        live.update(render())

    done = state.get("done")
    err = state.get("error")
    if err:
        console.print(f"[red]{err.get('message')}[/]")
        return 1
    if done:
        console.print(
            f"\n[green]完成[/] {done.get('path')}\n"
            f"耗时 {done.get('elapsed')}s  平均 {human_speed(done.get('avg_speed'))}  "
            f"峰值 {human_speed(done.get('peak_speed'))}"
        )
        return 0
    console.print("[yellow]已暂停（.part 与状态文件保留，重新运行同一命令即可续传）[/]")
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="多线程下载加速器（自适应并发）")
    ap.add_argument("url", help="下载地址")
    ap.add_argument("-o", "--output", help="输出文件名或路径")
    ap.add_argument("--threads", type=int, default=32, help="最大并发连接数（默认 32）")
    ap.add_argument("--chunk", help="分块大小，如 8M（默认自动）")
    ap.add_argument("--limit", help="限速，如 5M")
    ap.add_argument("-H", "--header", action="append", help='自定义请求头，如 -H "Referer: xxx"')
    ap.add_argument("--hash", help="校验 hash，如 sha256:abc123 或 md5:xxx")
    ap.add_argument(
        "--insecure",
        action="store_true",
        help="跳过 HTTPS 证书校验（本地代理拦截、自签证书站点用）",
    )
    args = ap.parse_args()

    try:
        code = asyncio.run(run(args))
    except KeyboardInterrupt:
        code = 130
    sys.exit(code)


if __name__ == "__main__":
    main()
