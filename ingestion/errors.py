"""错误分类：所有对外暴露的失败都必须能区分原因，且不透出底层异常细节。"""

from __future__ import annotations

from enum import Enum


class FailureReason(str, Enum):
    """可区分的失败原因。"""

    SOURCE_ERROR = "source_error"            # 数据源自身报错（已脱敏）
    MISSING_FIELD = "missing_field"          # 必填字段缺失
    TYPE_MISMATCH = "type_mismatch"          # 字段类型不匹配
    CURSOR_REGRESSION = "cursor_regression"  # 位点回退，拒绝推进
    CURSOR_GAP = "cursor_gap"                # 位点不连续，拒绝推进
    SINK_ERROR = "sink_error"                # 目标端写入失败
    SOURCE_MOVED = "source_moved"            # 源端跨过未处理区间/被重建，与本地进度对不上
    RESYNC_REQUIRED = "resync_required"      # 存在待确认的跳过区间，确认前拒绝采集


class IngestionError(Exception):
    """所有受控失败的基类，携带可区分的原因与脱敏后的信息。"""

    reason: FailureReason = FailureReason.SOURCE_ERROR

    def __init__(self, message: str, *, source_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.source_id = source_id

    def to_dict(self) -> dict:
        return {
            "reason": self.reason.value,
            "message": self.message,
            "source_id": self.source_id,
        }


class SourceError(IngestionError):
    """数据源底层异常的脱敏包装：不携带原始异常对象与堆栈。"""

    reason = FailureReason.SOURCE_ERROR


class RecordValidationError(IngestionError):
    """记录校验失败：必填字段缺失或类型不匹配。"""

    def __init__(self, reason: FailureReason, message: str, *, source_id: str | None = None) -> None:
        assert reason in (FailureReason.MISSING_FIELD, FailureReason.TYPE_MISMATCH)
        super().__init__(message, source_id=source_id)
        self.reason = reason


class CursorRegressionError(IngestionError):
    """位点回退：新位点小于已提交位点。"""

    reason = FailureReason.CURSOR_REGRESSION


class CursorGapError(IngestionError):
    """位点不连续：新位点跳过了未处理的区间。"""

    reason = FailureReason.CURSOR_GAP


class SinkError(IngestionError):
    """目标端写入失败。"""

    reason = FailureReason.SINK_ERROR


class SourceMovedError(IngestionError):
    """源端实际能提供的位置与本地已确认进度对不上。

    gap = {"from": x, "to": y} 表示源端跨过了本地尚未处理的区间 [x, y)，
    或本地进度超出源端当前可提供的范围（源端被重建/清库）。
    该异常触发后本次运行对该源判为异常并停止，本地位点保持不动。
    """

    reason = FailureReason.SOURCE_MOVED

    def __init__(
        self,
        message: str,
        *,
        source_id: str | None = None,
        gap_from: int,
        gap_to: int,
    ) -> None:
        super().__init__(message, source_id=source_id)
        self.gap = {"from": gap_from, "to": gap_to}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["gap"] = dict(self.gap)
        return d


class ResyncRequiredError(IngestionError):
    """存在待人工确认的跳过区间：确认重新同步之前拒绝继续采集。"""

    reason = FailureReason.RESYNC_REQUIRED

    def __init__(
        self,
        message: str,
        *,
        source_id: str | None = None,
        gap: dict | None = None,
    ) -> None:
        super().__init__(message, source_id=source_id)
        self.gap = dict(gap) if gap else None

    def to_dict(self) -> dict:
        d = super().to_dict()
        if self.gap is not None:
            d["gap"] = dict(self.gap)
        return d
