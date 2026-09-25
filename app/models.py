"""统一内部模型。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.errors import IngestionError


@dataclass(frozen=True)
class SourceEvent:
    """连接器返回的原始事件（尚未归一化）。"""

    position: int
    raw: Any


@dataclass(frozen=True)
class Record:
    """归一化后的统一内部模型。

    :param source: 数据源名（用于多源隔离与去重）
    :param position: 数据源内严格递增、连续的整数位点（从 1 开始）
    :param record_id: 业务主键，参与目标端幂等去重
    :param payload: 业务字段
    """

    source: str
    position: int
    record_id: str
    payload: dict[str, Any]


@dataclass
class RecordFailure:
    """单条记录级别的失败描述。"""

    position: int
    code: str
    message: str
    record_id: str | None = None

    def to_dict(self) -> dict:
        return {
            "position": self.position,
            "code": self.code,
            "message": self.message,
            "record_id": self.record_id,
        }


@dataclass
class RunReport:
    """一次采集运行的结果报告。"""

    source: str
    started_at: str
    finished_at: str
    last_position_before: int
    last_position_after: int
    delivered: int = 0
    skipped_replayed: int = 0
    record_failures: list[RecordFailure] = field(default_factory=list)
    fatal: IngestionError | None = None

    @property
    def ok(self) -> bool:
        return self.fatal is None and not self.record_failures

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "last_position_before": self.last_position_before,
            "last_position_after": self.last_position_after,
            "delivered": self.delivered,
            "skipped_replayed": self.skipped_replayed,
            "record_failures": [f.to_dict() for f in self.record_failures],
            "fatal": self.fatal.to_dict() if self.fatal else None,
        }
