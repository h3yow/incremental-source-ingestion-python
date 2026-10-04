"""按数据源持久化增量位点，拒绝回退与不连续。"""

from __future__ import annotations

import json
import os
import threading

from ingestion.errors import CursorGapError, CursorRegressionError
from ingestion.models import INITIAL_CURSOR


class CursorStore:
    """文件持久化的位点存储。

    位点语义：cursor = 下一条待采记录的序号（从 0 开始）。
    commit 采用 compare-and-swap：
    - new_cursor < stored  -> CursorRegressionError（位点回退，拒绝）
    - expected != stored   -> CursorGapError（并发下位点不连续，拒绝）
    - new_cursor == stored -> 幂等重放，视为无操作
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _read_all(self) -> dict[str, int]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        return {k: int(v) for k, v in raw.items()}

    def _write_all(self, data: dict[str, int]) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)  # 原子替换，避免中断留下半个文件

    def load(self, source_id: str) -> int:
        with self._lock:
            return self._read_all().get(source_id, INITIAL_CURSOR)

    def commit(self, source_id: str, cursor: int, *, expected: int) -> None:
        with self._lock:
            data = self._read_all()
            stored = data.get(source_id, INITIAL_CURSOR)
            if cursor < stored:
                raise CursorRegressionError(
                    f"cursor regression: new={cursor} < stored={stored}",
                    source_id=source_id,
                )
            if expected != stored:
                raise CursorGapError(
                    f"cursor gap: expected={expected} != stored={stored}",
                    source_id=source_id,
                )
            if cursor == stored:
                return  # 幂等重放：不推进也不报错
            data[source_id] = cursor
            self._write_all(data)

    def reset(self, source_id: str, cursor: int) -> None:
        """受控重置位点：只允许经人工确认、已留痕的重新同步调用。

        与 commit 不同，不做连续性/回退校验——重新同步本身就可能把位点
        向前推过丢失区间（跳过确认）或向后退到源端重建后的新起点。
        调用方（Pipeline.resync）必须持有该源的互斥锁并已完成审计留痕。
        """
        with self._lock:
            data = self._read_all()
            data[source_id] = cursor
            self._write_all(data)
