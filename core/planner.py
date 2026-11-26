"""分块规划。

块大小的两个约束：
    1. 块要够多 —— 至少让每个 worker 手里有 4 块可抢，否则最后会剩几个 worker
       干看着别人跑，形成长尾。
    2. 块不能太多 —— 每块都有一次 HTTP 请求开销（握手/首字节），块太小会被
       请求开销吃掉吞吐。同时块数过多会让前端热力图和 sidecar 变得臃肿。
"""

from __future__ import annotations

import math

MIN_CHUNK = 4 * 1024 * 1024        # 4 MB：再小就会被每块的请求开销吃掉吞吐
MAX_CHUNK = 64 * 1024 * 1024       # 64 MB
DEFAULT_CHUNK = 8 * 1024 * 1024    # 8 MB
MAX_CHUNKS = 4096
FLOOR_CHUNK = 1 * 1024 * 1024      # 1 MB：仅小文件时允许降到这里


def choose_chunk_size(size: int | None, max_concurrency: int, preferred: int = DEFAULT_CHUNK) -> int:
    if not size or size <= 0:
        return preferred
    # 目标块数：并发数的 4 倍（留出动态领取的余量），限制在 [8, MAX_CHUNKS]
    target = max(8, min(MAX_CHUNKS, max_concurrency * 4))
    ideal = size / target
    cs = int(max(MIN_CHUNK, min(MAX_CHUNK, ideal)))
    # 文件特别小：允许降到 1MB，保证至少 4 块
    while cs > MIN_CHUNK and math.ceil(size / cs) < 4:
        cs //= 2
    # 块数超过上限：加倍
    while math.ceil(size / cs) > MAX_CHUNKS:
        cs *= 2
    return int(max(FLOOR_CHUNK, cs))


def plan(size: int, chunk_size: int) -> list[tuple[int, int, int]]:
    """返回 [(索引, 起始字节, 结束字节), ...]，结束字节为闭区间。"""
    n = math.ceil(size / chunk_size)
    chunks: list[tuple[int, int, int]] = []
    for i in range(n):
        start = i * chunk_size
        end = min(size, start + chunk_size) - 1
        chunks.append((i, start, end))
    return chunks


def chunk_bytes(size: int, chunk_size: int, index: int) -> int:
    start = index * chunk_size
    return min(size, start + chunk_size) - start
