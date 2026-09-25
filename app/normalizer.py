"""通用归一化逻辑：把原始 JSON 对象校验为 Record。

校验规则（均产生可区分的失败原因，不静默跳过）：
* 原始对象不是 JSON 对象      -> RECORD_MALFORMED
* 缺少必填字段 ``id``         -> MISSING_FIELD
* ``id`` 不是字符串            -> TYPE_MISMATCH
* ``data`` 存在但不是对象      -> TYPE_MISMATCH
"""

from __future__ import annotations

from typing import Any

from app.errors import FailureCode, IngestionError
from app.models import Record

REQUIRED_FIELDS = ("id",)


def normalize_raw(source: str, position: int, raw: Any) -> Record:
    if not isinstance(raw, dict):
        raise IngestionError(
            FailureCode.RECORD_MALFORMED,
            f"记录必须是 JSON 对象，实际为 {type(raw).__name__}",
            source=source,
            position=position,
        )
    for key in REQUIRED_FIELDS:
        if key not in raw or raw[key] is None:
            raise IngestionError(
                FailureCode.MISSING_FIELD,
                f"缺少必填字段 '{key}'",
                source=source,
                position=position,
            )
    rid = raw["id"]
    if not isinstance(rid, str):
        raise IngestionError(
            FailureCode.TYPE_MISMATCH,
            f"字段 'id' 应为字符串，实际为 {type(rid).__name__}",
            source=source,
            position=position,
        )
    data = raw.get("data", {})
    if not isinstance(data, dict):
        raise IngestionError(
            FailureCode.TYPE_MISMATCH,
            f"字段 'data' 应为对象，实际为 {type(data).__name__}",
            source=source,
            position=position,
        )
    return Record(source=source, position=position, record_id=rid, payload=data)
