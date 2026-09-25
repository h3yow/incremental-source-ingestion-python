"""统一对外错误模型。

数据源底层异常不允许原样透出到对外接口，统一包装为 :class:`IngestionError`。
"""

from __future__ import annotations

from enum import Enum


class FailureCode(str, Enum):
    """可区分的失败原因编码。"""

    SOURCE_UNAVAILABLE = "source_unavailable"
    SOURCE_PROTOCOL = "source_protocol"
    RECORD_MALFORMED = "record_malformed"
    MISSING_FIELD = "missing_field"
    TYPE_MISMATCH = "type_mismatch"
    POSITION_REGRESSED = "position_regressed"
    POSITION_GAP = "position_gap"
    TARGET_ERROR = "target_error"
    SOURCE_BUSY = "source_busy"
    SOURCE_NOT_FOUND = "source_not_found"


class IngestionError(Exception):
    """对外统一错误。

    :param code: 标准化失败原因
    :param message: 可读说明
    :param source: 数据源名称
    :param position: 可选的记录位点
    :param cause_type: 被包装的底层异常类型名（仅记录类型，不透出原始异常）
    """

    def __init__(
        self,
        code: FailureCode,
        message: str,
        *,
        source: str | None = None,
        position: int | None = None,
        cause_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.source = source
        self.position = position
        self.cause_type = cause_type

    def to_dict(self) -> dict:
        return {
            "code": self.code.value,
            "message": self.message,
            "source": self.source,
            "position": self.position,
            "cause_type": self.cause_type,
        }
