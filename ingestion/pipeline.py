"""采集编排：校验、位点推进、并发调度。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from ingestion.connectors.base import Connector
from ingestion.cursor_store import CursorStore
from ingestion.errors import (
    FailureReason,
    IngestionError,
    RecordValidationError,
)
from ingestion.models import (
    REQUIRED_FIELDS,
    SourceRunResult,
    UnifiedRecord,
    RecordOutcome,
)
from ingestion.sink import LocalSink

log = logging.getLogger("ingestion")


class Pipeline:
    """按数据源串行、跨数据源并发的增量采集管线。

    - 同一数据源在进程内有锁，并发触发只会串行执行；第二次触发读到已推进位点，
      通常无新记录（重放时 sink 幂等），不会重复写入。
    - 跨数据源按 max_workers 并发；任一数据源失败只标记自身结果，不影响其它源。
    - 校验失败/数据源错误采用 fail-fast：位点只提交到失败记录之前，
      失败原因区分 missing_field / type_mismatch / source_error 等。
    """

    def __init__(self, cursors: CursorStore, sink: LocalSink, max_workers: int = 4) -> None:
        self.cursors = cursors
        self.sink = sink
        self.max_workers = max_workers
        self._source_locks: dict[str, object] = {}
        self._locks_guard = __import__("threading").Lock()

    def _source_lock(self, source_id: str):
        with self._locks_guard:
            lock = self._source_locks.get(source_id)
            if lock is None:
                import threading

                lock = threading.Lock()
                self._source_locks[source_id] = lock
            return lock

    def _validate(self, source_id: str, seq: int, fields: dict) -> UnifiedRecord:
        for name, expected_type in REQUIRED_FIELDS.items():
            if name not in fields or fields[name] is None:
                raise RecordValidationError(
                    FailureReason.MISSING_FIELD,
                    f"missing required field '{name}' at seq={seq}",
                    source_id=source_id,
                )
            if not isinstance(fields[name], expected_type):
                raise RecordValidationError(
                    FailureReason.TYPE_MISMATCH,
                    f"field '{name}' expected {expected_type.__name__}, "
                    f"got {type(fields[name]).__name__} at seq={seq}",
                    source_id=source_id,
                )
        return UnifiedRecord(
            source_id=source_id,
            record_id=fields["id"],
            seq=seq,
            payload=fields["payload"],
        )

    def run_source(self, connector: Connector) -> SourceRunResult:
        source_id = connector.source_id
        with self._source_lock(source_id):
            cursor = self.cursors.load(source_id)
            result = SourceRunResult(
                source_id=source_id,
                status="ok",
                cursor_before=cursor,
                cursor_after=cursor,
            )
            log.info("[%s] run start, cursor=%d", source_id, cursor)
            try:
                for raw in connector.fetch(cursor):
                    try:
                        fields = connector.normalize(raw)
                        record = self._validate(source_id, raw.seq, fields)
                    except IngestionError as exc:
                        if isinstance(exc, RecordValidationError):
                            outcome = RecordOutcome(
                                seq=raw.seq, status="failed", failure=exc.to_dict()
                            )
                            result.outcomes.append(outcome)
                            log.info(
                                "[%s] seq=%d FAILED reason=%s detail=%s",
                                source_id, raw.seq, exc.reason.value, exc.message,
                            )
                        raise
                    delivered = self.sink.write(record)
                    # compare-and-swap 提交位点；期望位点为当前已处理边界
                    self.cursors.commit(
                        source_id, raw.seq + 1, expected=cursor
                    )
                    cursor = raw.seq + 1
                    result.cursor_after = cursor
                    status = "delivered" if delivered else "duplicate"
                    result.outcomes.append(
                        RecordOutcome(
                            seq=raw.seq,
                            status=status,
                            record_id=record.record_id,
                        )
                    )
                    log.info(
                        "[%s] seq=%d id=%s %s, cursor -> %d",
                        source_id, raw.seq, record.record_id, status, cursor,
                    )
            except IngestionError as exc:
                result.status = "failed"
                result.failure = exc.to_dict()
                result.cursor_after = cursor
                log.info(
                    "[%s] run FAILED reason=%s detail=%s, cursor stays %d",
                    source_id, exc.reason.value, exc.message, cursor,
                )
                return result
            except Exception:
                # 最后防线：连接器/目标端漏网的底层异常一律脱敏，绝不原样透出
                from ingestion.errors import SourceError

                exc = SourceError("connector failure", source_id=source_id)
                result.status = "failed"
                result.failure = exc.to_dict()
                result.cursor_after = cursor
                log.info(
                    "[%s] run FAILED reason=%s, cursor stays %d",
                    source_id, exc.reason.value, cursor,
                )
                return result
            log.info("[%s] run ok, cursor=%d", source_id, cursor)
            return result

    def run_all(self, connectors: list[Connector]) -> list[SourceRunResult]:
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            return list(pool.map(self.run_source, connectors))
