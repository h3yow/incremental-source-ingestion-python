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


@dataclass
class SourceState:
    """单个数据源的运行状态（持久化于 SourceStateStore）。

    - status: "ok" | "failed" | "pending_resync"
    - last_failure: 最近一次失败的脱敏字典（含 reason，跨段时含 gap）
    - pending_skip: 待人工确认的跳过区间 {"from","to","detected_at","reason"}
    - resync_history: 重新同步审计记录（谁/何时/跳过区间/依据）
    """

    source_id: str
    status: str = "ok"
    last_failure: dict | None = None
    pending_skip: dict | None = None
    resync_history: list[dict] = field(default_factory=list)

    @property
    def healthy(self) -> bool:
        return self.status == "ok" and self.pending_skip is None

    def to_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "status": self.status,
            "healthy": self.healthy,
            "last_failure": self.last_failure,
            "pending_skip": self.pending_skip,
            "resync_history": list(self.resync_history),
        }

    @classmethod
    def from_dict(cls, source_id: str, data: dict) -> "SourceState":
        return cls(
            source_id=source_id,
            status=data.get("status", "ok"),
            last_failure=data.get("last_failure"),
            pending_skip=data.get("pending_skip"),
            resync_history=list(data.get("resync_history", [])),
        )
