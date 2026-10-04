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
    SOURCE_GAP = "source_gap"                # 源端跨过一段未处理数据，对齐失败
    RESYNC_REQUIRED = "resync_required"      # 存在待确认跳过区间，采集被拒绝
    NO_PENDING_GAP = "no_pending_gap"        # 没有待确认区间，重新同步被拒绝
    INVALID_CONFIRMATION = "invalid_confirmation"  # 重新同步确认信息不完整


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


class SourceGapError(IngestionError):
    """源端实际可提供的位置与本地已确认进度对不上：

    源端跨过 [gap_from, gap_to) 这段未被处理的数据。本地位点不被推进，
    该区间被记为待确认，必须经人工重新同步确认后才允许跳过。
    resume_from 为确认后允许续采的新起点（即 gap_to）。
    """

    reason = FailureReason.SOURCE_GAP

    def __init__(
        self,
        message: str,
        *,
        source_id: str | None = None,
        gap_from: int,
        gap_to: int,
    ) -> None:
        super().__init__(message, source_id=source_id)
        self.gap_from = gap_from
        self.gap_to = gap_to

    @property
    def resume_from(self) -> int:
        return self.gap_to

    def gap_dict(self) -> dict:
        return {"from": self.gap_from, "to": self.gap_to, "resume_from": self.resume_from}

    def to_dict(self) -> dict:
        d = super().to_dict()
        d["gap"] = self.gap_dict()
        return d


class ResyncError(IngestionError):
    """重新同步请求被拒绝：没有待确认区间，或确认信息（操作人/依据）不完整。"""

    def __init__(self, reason: FailureReason, message: str, *, source_id: str | None = None) -> None:
        assert reason in (FailureReason.NO_PENDING_GAP, FailureReason.INVALID_CONFIRMATION)
        super().__init__(message, source_id=source_id)
        self.reason = reason
