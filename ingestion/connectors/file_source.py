"""JSONL 文件数据源连接器。"""

from __future__ import annotations

import json
from typing import Any, Iterator

from ingestion.connectors.base import Connector
from ingestion.errors import SourceError, SourceGapError
from ingestion.models import RawRecord


class FileConnector(Connector):
    """从本地 JSONL 文件按行增量读取，行号（0 起）即序号。

    文件形态：{"id": ..., "payload": ...}
    底层错误（文件缺失、JSON 损坏、非对象行）一律包装为 SourceError，
    只暴露脱敏后的原因，不透出原始异常。

    对齐检测：文件只增不减。若文件总行数小于本地已确认位点，说明源端
    被重建/清库/截断，当前文件内容（[0, total)）从未被采集过，抛出
    SourceGapError，由人工确认后才允许把位点重置到文件当前末尾。
    """

    def __init__(self, source_id: str, path: str) -> None:
        self.source_id = source_id
        self.path = path

    def fetch(self, after: int) -> Iterator[RawRecord]:
        try:
            f = open(self.path, "r", encoding="utf-8")
        except OSError:
            raise SourceError(
                "source file unavailable", source_id=self.source_id
            ) from None
        with f:
            total = sum(1 for _ in f)
        if after > total:
            raise SourceGapError(
                f"source rebuilt or truncated: {total} line(s) available, "
                f"local cursor is {after}",
                source_id=self.source_id,
                gap_from=0,
                gap_to=total,
            )
        with open(self.path, "r", encoding="utf-8") as f:
            for seq, line in enumerate(f):
                if seq < after:
                    continue
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    raise SourceError(
                        f"corrupt record at seq={seq}", source_id=self.source_id
                    ) from None
                if not isinstance(data, dict):
                    raise SourceError(
                        f"non-object record at seq={seq}", source_id=self.source_id
                    )
                yield RawRecord(seq=seq, data=data)

    def normalize(self, raw: RawRecord) -> dict[str, Any]:
        # 文件源字段名与统一模型一致，直接透传所需字段
        return {"id": raw.data.get("id"), "payload": raw.data.get("payload")}
