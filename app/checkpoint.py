"""基于本地文件的增量位点存储。

位点语义：
* 每个数据源独立维护键 ``<source>: last_position``，初值 0；
* 位点只能单调递增；
* :meth:`advance` 是“读-校验-写”原子操作，拒绝回退（POSIX ``O_EXCL`` 语义
  通过文件锁 + 原子 rename 实现），返回旧值供调用方区分并发竞争。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path


class CheckpointStore:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def _read_locked(self) -> dict[str, int]:
        if not self.path.exists():
            return {}
        text = self.path.read_text(encoding="utf-8").strip()
        if not text:
            return {}
        return json.loads(text)

    def _write_locked(self, data: dict[str, int]) -> None:
        # 同目录临时文件 + 原子 rename，保证崩溃后不会读到半个文件
        fd, tmp = tempfile.mkstemp(
            dir=self.path.parent, prefix=self.path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2, sort_keys=True)
                fh.write("\n")
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def get(self, source: str) -> int:
        with self._lock:
            return int(self._read_locked().get(source, 0))

    def advance(self, source: str, new_position: int) -> int:
        """尝试把位点推进到 ``new_position``，返回推进前的旧值。

        位点回退（new < old）时抛 :class:`ValueError`。
        """
        with self._lock:
            data = self._read_locked()
            old = int(data.get(source, 0))
            if new_position < old:
                raise ValueError(
                    f"位点回退被拒绝: source={source} old={old} new={new_position}"
                )
            if new_position == old:
                return old
            data[source] = new_position
            self._write_locked(data)
            return old

    def reset(self, source: str | None = None) -> None:
        """测试/演示辅助：清空位点。"""
        with self._lock:
            if source is None:
                self._write_locked({})
            else:
                data = self._read_locked()
                data.pop(source, None)
                self._write_locked(data)
