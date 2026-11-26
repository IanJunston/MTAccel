"""完整性校验。

诚实说明一个限制：
    整体 hash（MD5/SHA256）**无法并行**——hash 是链式的，第 N 块的摘要依赖第 N-1 块。
    所以这里只能顺序读一遍文件计算。好在顺序读能跑满磁盘带宽，瓶颈通常在 hash 的
    单核 CPU 上（约 500MB/s ~ 1.5GB/s），10GB 文件大约 10~20 秒，可接受。

    真正能"并行"的是**分块独立校验**（每块单独算 hash），但那需要服务器提供每块的
    期望值，常规 HTTP 下载没有这个信息。所以不做虚假的并行优化。

校验策略：
    1. 长度校验（必做，几乎零成本）—— 能挡住 90% 的截断类错误
    2. 整体 hash（可选，需用户提供期望值，或服务器给了 Content-MD5）
"""

from __future__ import annotations

import hashlib
import os


def verify_size(path: str, expected: int | None) -> tuple[bool, str]:
    if expected is None:
        return True, ""
    try:
        actual = os.path.getsize(path)
    except OSError as e:
        return False, f"无法读取文件: {e}"
    if actual != expected:
        return False, f"大小不符: 实际 {actual} / 期望 {expected}"
    return True, ""


def hash_file(
    path: str,
    algo: str = "sha256",
    chunk: int = 8 * 1024 * 1024,
    on_progress=None,
) -> str:
    h = hashlib.new(algo)
    total = os.path.getsize(path)
    done = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
            done += len(b)
            if on_progress:
                on_progress(done, total)
    return h.hexdigest()


def verify_hash(path: str, expected: str, algo: str = "sha256", on_progress=None) -> tuple[bool, str]:
    actual = hash_file(path, algo=algo, on_progress=on_progress)
    if actual.lower() != expected.strip().lower():
        return False, f"{algo} 不符: 实际 {actual}"
    return True, f"{algo} 校验通过"
