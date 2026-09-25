"""JSONL 文件数据源连接器。"""

from __future__ import annotations

import json
from typing import Any, Iterator

from ingestion.connectors.base import Connector
from ingestion.errors import SourceError
from ingestion.models import RawRecord


class FileConnector(Connector):
    """从本地 JSONL 文件按行增量读取，行号（0 起）即序号。

    文件形态：{"id": ..., "payload": ...}
    底层错误（文件缺失、JSON 损坏、非对象行）一律包装为 SourceError，
    只暴露脱敏后的原因，不透出原始异常。
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
