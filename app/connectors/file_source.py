"""本地 JSONL 文件数据源连接器（数据源形态一）。

文件每行是一条记录，位点 = 非空行的物理行号（从 1 开始）。
* 文件不可读等底层 I/O 异常 -> SOURCE_UNAVAILABLE（不透出原始异常）
* 单行 JSON 解析失败       -> RECORD_MALFORMED（在 normalize 阶段报错）
* 字段缺失/类型错误        -> MISSING_FIELD / TYPE_MISMATCH
"""

from __future__ import annotations

import json
from collections.abc import Iterable

from app.connectors.base import BaseConnector
from app.errors import FailureCode, IngestionError
from app.models import Record, SourceEvent
from app.normalizer import normalize_raw


class FileSourceConnector(BaseConnector):
    def __init__(self, name: str, path: str) -> None:
        self.name = name
        self.path = path

    def fetch(self, after_position: int) -> Iterable[SourceEvent]:
        try:
            with open(self.path, encoding="utf-8") as fh:
                lines = fh.readlines()
        except OSError as exc:
            raise IngestionError(
                FailureCode.SOURCE_UNAVAILABLE,
                f"文件数据源不可读: {self.path}",
                source=self.name,
                cause_type=type(exc).__name__,
            ) from None
        for lineno, line in enumerate(lines, start=1):
            if not line.strip() or lineno <= after_position:
                continue
            yield SourceEvent(position=lineno, raw=line.rstrip("\n"))

    def normalize(self, event: SourceEvent) -> Record:
        raw = event.raw
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise IngestionError(
                    FailureCode.RECORD_MALFORMED,
                    f"记录不是合法 JSON: {exc.msg}",
                    source=self.name,
                    position=event.position,
                    cause_type=type(exc).__name__,
                ) from None
        return normalize_raw(self.name, event.position, raw)
