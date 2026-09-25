"""统一内部模型与结果模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

REQUIRED_FIELDS: dict[str, type] = {
    "id": str,
    "payload": str,
}

# 位点从 0 开始：下一条待采记录的序号
INITIAL_CURSOR = 0


@dataclass(frozen=True)
class RawRecord:
    """连接器读出的原始记录：序号 + 原始字典。"""

    seq: int
    data: dict[str, Any]


@dataclass(frozen=True)
class UnifiedRecord:
    """归一后的内部模型。"""

    source_id: str
    record_id: str
    seq: int
    payload: str

    @property
    def dedup_key(self) -> str:
        return f"{self.source_id}:{self.record_id}"


@dataclass
class RecordOutcome:
    """单条记录的处理结果。"""

    seq: int
    status: str  # "delivered" | "duplicate" | "failed"
    record_id: str | None = None
    failure: dict | None = None


@dataclass
class SourceRunResult:
    """单个数据源一次采集的结果。"""

    source_id: str
    status: str  # "ok" | "failed"
    cursor_before: int = INITIAL_CURSOR
    cursor_after: int = INITIAL_CURSOR
    outcomes: list[RecordOutcome] = field(default_factory=list)
    failure: dict | None = None
