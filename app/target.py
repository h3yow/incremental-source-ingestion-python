"""本地文件投递目标端。

存储为单个 JSON 文件：``{"<source>:": [记录, ...]}``，每条记录带
``position`` 与 ``record_id``。写入采用原子 rename。

幂等规则：去重键为 ``(source, record_id)``。重复投递返回 ``False``
且文件内容不变，因此重放/重试不会产生重复写入。
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any

from app.errors import FailureCode, IngestionError
from app.models import Record


class FileTarget:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        if not self.path.exists():
            self._write({})

    def _read(self) -> dict[str, list[dict[str, Any]]]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _write(self, data: dict[str, list[dict[str, Any]]]) -> None:
        fd, tmp = tempfile.mkstemp(
            dir=self.path.parent, prefix=self.path.name + ".", suffix=".tmp"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=2)
                fh.write("\n")
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
            raise

    def deliver(self, record: Record) -> bool:
        """投递一条记录，返回 True 表示新写入，False 表示幂等跳过。"""
        item = {
            "position": record.position,
            "record_id": record.record_id,
            "payload": record.payload,
        }
        with self._lock:
            try:
                data = self._read()
            except (OSError, json.JSONDecodeError) as exc:
                raise IngestionError(
                    FailureCode.TARGET_ERROR,
                    "目标端文件无法读取",
                    source=record.source,
                    position=record.position,
                    cause_type=type(exc).__name__,
                ) from None
            rows = data.setdefault(record.source, [])
            if any(r["record_id"] == record.record_id for r in rows):
                return False
            rows.append(item)
            try:
                self._write(data)
            except OSError as exc:
                raise IngestionError(
                    FailureCode.TARGET_ERROR,
                    "目标端文件写入失败",
                    source=record.source,
                    position=record.position,
                    cause_type=type(exc).__name__,
                ) from None
            return True

    def delivered_count(self, source: str | None = None) -> int:
        with self._lock:
            data = self._read()
        if source is not None:
            return len(data.get(source, []))
        return sum(len(rows) for rows in data.values())

    def records(self, source: str) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._read().get(source, []))
