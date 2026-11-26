"""写盘层：预分配 + 偏移写。

关键设计（Windows 兼容）：
    os.pwrite 是 Unix 专有，Windows 上不可用。
    这里让**每个 worker 持有独立的文件句柄**，各自 seek 到自己负责的块偏移再写。
    因为每个句柄的游标互相独立，天然无锁，也不会互相踩到对方的写入位置。

预分配：先用 truncate 把文件撑到目标大小，避免下载过程中反复扩文件造成碎片。
"""

from __future__ import annotations

import os


class PreallocatedFile:
    """目标文件的预分配器，同时作为 worker 句柄工厂。"""

    def __init__(self, path: str, size: int):
        self.path = path
        self.size = size

    def prepare(self) -> bool:
        """预分配文件。返回 True 表示新建，False 表示复用已有同尺寸文件。"""
        if os.path.exists(self.path) and os.path.getsize(self.path) == self.size:
            return False
        # 先删除旧的异常尺寸文件，避免 truncate 后残留脏数据被误认为已完成
        if os.path.exists(self.path):
            os.remove(self.path)
        with open(self.path, "wb") as f:
            f.truncate(self.size)
        return True

    def open_writer(self):
        """每个 worker 调用一次，拿到独立游标的可写句柄。"""
        return open(self.path, "r+b")


def human_size(n: float | None) -> str:
    if n is None:
        return "-"
    units = ["B", "KB", "MB", "GB", "TB"]
    v = float(n)
    i = 0
    while v >= 1024 and i < len(units) - 1:
        v /= 1024.0
        i += 1
    return f"{v:.2f} {units[i]}" if i else f"{int(v)} B"


def human_speed(n: float | None) -> str:
    return "-" if n is None else human_size(n) + "/s"


def human_eta(seconds: float | None) -> str:
    if seconds is None or seconds < 0 or seconds != seconds:  # NaN
        return "--:--"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"
