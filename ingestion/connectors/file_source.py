"""JSONL 文件数据源连接器。"""

from __future__ import annotations

import json
from typing import Any, Iterator

from ingestion.connectors.base import Connector
from ingestion.errors import SourceError, SourceMovedError
from ingestion.models import RawRecord


class FileConnector(Connector):
    """从本地 JSONL 文件按行增量读取。

    序号规则：行内带整数 "seq" 字段时以它为准（模拟换存储后序号空间变化），
    否则用行号（0 起）。文件形态：{"id": ..., "payload": ...}。
    对齐检测：本地进度超出文件当前能提供的最大序号（文件被清库/截断/重建）
    时抛 SourceMovedError，而不是静默返回空。
    底层错误（文件缺失、JSON 损坏、非对象行）一律包装为 SourceError，
    只暴露脱敏后的原因，不透出原始异常。
    """

    def __init__(self, source_id: str, path: str) -> None:
        self.source_id = source_id
        self.path = path

    def _read_rows(self) -> list[RawRecord]:
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                lines = list(f)
        except OSError:
            raise SourceError(
                "source file unavailable", source_id=self.source_id
            ) from None
        rows: list[RawRecord] = []
        for line_no, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                raise SourceError(
                    f"corrupt record at seq={line_no}", source_id=self.source_id
                ) from None
            if not isinstance(data, dict):
                raise SourceError(
                    f"non-object record at seq={line_no}", source_id=self.source_id
                )
            seq = data.get("seq", line_no)
            if not isinstance(seq, int):
                raise SourceError(
                    f"invalid seq at line={line_no}", source_id=self.source_id
                )
            rows.append(RawRecord(seq=seq, data=data))
        return rows

    def fetch(self, after: int) -> Iterator[RawRecord]:
        rows = self._read_rows()
        max_seq = rows[-1].seq if rows else -1
        if after > max_seq + 1:
            # 本地已确认进度超出源端当前可提供的范围：源端被清库/截断/重建，
            # 必须判异常并停下，不能静默当成"没有新数据"。
            raise SourceMovedError(
                "source cannot align with local cursor "
                f"(local={after}, source_max={max_seq})",
                source_id=self.source_id,
                gap_from=max_seq + 1,
                gap_to=after,
            )
        for raw in rows:
            if raw.seq >= after:
                yield raw

    def normalize(self, raw: RawRecord) -> dict[str, Any]:
        # 文件源字段名与统一模型一致，直接透传所需字段
        return {"id": raw.data.get("id"), "payload": raw.data.get("payload")}
