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

    def reset(self, source_id: str, cursor: int) -> int:
        """受控重新同步专用：把位点强制推进到新起点，返回原位点。

        只允许向前（cursor > stored），拒绝回退；调用方（resync 流程）
        负责在调用前完成人工确认与审计留痕。正常采集路径不得使用。
        """
        with self._lock:
            data = self._read_all()
            stored = data.get(source_id, INITIAL_CURSOR)
            if cursor <= stored:
                raise CursorRegressionError(
                    f"resync reset must move forward: new={cursor} <= stored={stored}",
                    source_id=source_id,
                )
            data[source_id] = cursor
            self._write_all(data)
            return stored
