"""断点续传：sidecar 状态文件 + 一致性校验。

为什么需要一致性校验：
    多线程下载是"按偏移往预分配文件里填洞"。如果源文件在中途变了（重新上传、
    CDN 回源更新），已下的块和新下的块来自两个版本，拼出来的文件不会报错，
    但内容是坏的。所以续传前必须比对 ETag / Last-Modified / 大小，任何一项变化
    就整体作废重下。
"""

from __future__ import annotations

import json
import os

SIDECAR_SUFFIX = ".json"
STATE_VERSION = 1


class ResumeState:
    def __init__(self, part_path: str, meta: dict, done_bits: str):
        self.part_path = part_path
        self.meta = meta or {}
        self.done_bits = done_bits or ""

    # ---------- 读写 ----------

    @classmethod
    def load(cls, part_path: str) -> "ResumeState | None":
        sidecar = part_path + SIDECAR_SUFFIX
        if not (os.path.exists(part_path) and os.path.exists(sidecar)):
            return None
        try:
            with open(sidecar, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:  # noqa: BLE001
            return None
        if data.get("version") != STATE_VERSION:
            return None
        return cls(part_path, data.get("meta", {}), data.get("done", ""))

    def save(self, meta: dict, done: list[bool]) -> None:
        self.meta = dict(meta)
        self.done_bits = "".join("1" if d else "0" for d in done)
        sidecar = self.part_path + SIDECAR_SUFFIX
        tmp = sidecar + ".tmp"
        payload = {
            "version": STATE_VERSION,
            "meta": self.meta,
            "done": self.done_bits,
        }
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp, sidecar)

    def remove(self) -> None:
        for p in (self.part_path + SIDECAR_SUFFIX, self.part_path + SIDECAR_SUFFIX + ".tmp"):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass

    # ---------- 校验 ----------

    def compatible(
        self,
        size: int,
        chunk_size: int,
        total_chunks: int,
        etag: str | None,
        last_modified: str | None,
    ) -> bool:
        m = self.meta
        if m.get("size") != size:
            return False
        if m.get("chunk_size") != chunk_size:
            return False
        if len(self.done_bits) != total_chunks:
            return False
        # ETag 优先；没有 ETag 时退到 Last-Modified；两者都缺则只信大小
        if m.get("etag") and etag and m["etag"] != etag:
            return False
        if not m.get("etag") and m.get("last_modified") and last_modified:
            if m["last_modified"] != last_modified:
                return False
        return True

    def done_list(self) -> list[bool]:
        return [c == "1" for c in self.done_bits]

    @property
    def completed_count(self) -> int:
        return self.done_bits.count("1")
