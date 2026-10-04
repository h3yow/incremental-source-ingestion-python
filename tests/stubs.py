"""测试用内存连接器：可控数据、可控报错、可控位点行为。"""

from __future__ import annotations

from typing import Any, Iterator

from ingestion.connectors.base import Connector
from ingestion.errors import SourceError
from ingestion.models import RawRecord


class InMemoryConnector(Connector):
    def __init__(self, source_id: str, rows: list[dict[str, Any]] | None = None,
                 fail_on_fetch: bool = False, start_seq: int = 0) -> None:
        self.source_id = source_id
        self.rows = rows if rows is not None else []
        self.fail_on_fetch = fail_on_fetch
        # 源端记录的起始序号：>0 模拟源端重建/清库后从更高位置继续
        self.start_seq = start_seq

    def fetch(self, after: int) -> Iterator[RawRecord]:
        if self.fail_on_fetch:
            # 模拟底层抛出任意异常，连接器必须脱敏包装
            raise RuntimeError("DB connection lost: secret-db-host:3306") from None
        for i, data in enumerate(self.rows):
            seq = self.start_seq + i
            if seq >= after:
                yield RawRecord(seq=seq, data=dict(data))

    def normalize(self, raw: RawRecord) -> dict[str, Any]:
        return {"id": raw.data.get("id"), "payload": raw.data.get("payload")}


class MaskedErrorConnector(InMemoryConnector):
    def fetch(self, after: int) -> Iterator[RawRecord]:
        if self.fail_on_fetch:
            raise SourceError("source unavailable", source_id=self.source_id)
        yield from super().fetch(after)


class ReplayFromZeroConnector(InMemoryConnector):
    """忽略传入位点，始终从头返回（模拟数据源回退），用于验证位点回退被拒绝。"""

    def fetch(self, after: int) -> Iterator[RawRecord]:
        for seq, data in enumerate(self.rows):
            yield RawRecord(seq=seq, data=dict(data))
