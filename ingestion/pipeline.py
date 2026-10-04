"""采集编排：校验、位点推进、并发调度、源端对齐与受控重新同步。"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from ingestion.connectors.base import Connector
from ingestion.cursor_store import CursorStore
from ingestion.errors import (
    FailureReason,
    IngestionError,
    RecordValidationError,
    SourceGapError,
)
from ingestion.models import (
    REQUIRED_FIELDS,
    ResyncResult,
    SourceRunResult,
    UnifiedRecord,
    RecordOutcome,
)
from ingestion.sink import LocalSink
from ingestion.source_state import SourceStateStore

log = logging.getLogger("ingestion")


class Pipeline:
    """按数据源串行、跨数据源并发的增量采集管线。

    - 同一数据源在进程内有锁，并发触发只会串行执行；第二次触发读到已推进位点，
      通常无新记录（重放时 sink 幂等），不会重复写入。
    - 跨数据源按 max_workers 并发；任一数据源失败只标记自身结果，不影响其它源。
    - 校验失败/数据源错误采用 fail-fast：位点只提交到失败记录之前，
      失败原因区分 missing_field / type_mismatch / source_error 等。
    - 源端对齐：每条记录的 seq 必须与本地期望位置连续；源端跨过一段未处理
      数据（seq 跳跃）即判 source_gap 异常并停下，跳过区间登记为待确认，
      确认前该源拒绝继续采集（resync_required），不影响其它源。
    - 重新同步：resync() 与 run_source() 共用同一把源锁，并发结果确定——
      要么同步先生效、采集从新起点继续，要么采集先跑完、同步基于最新状态判断。
    """

    def __init__(
        self,
        cursors: CursorStore,
        sink: LocalSink,
        max_workers: int = 4,
        state: SourceStateStore | None = None,
    ) -> None:
        self.cursors = cursors
        self.sink = sink
        self.max_workers = max_workers
        self.state = state if state is not None else SourceStateStore(None)
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
            # 有待确认的跳过区间：拒绝继续采集，必须先人工确认重新同步
            pending = self.state.pending_gap(source_id)
            if pending is not None:
                result.status = "failed"
                result.failure = {
                    "reason": FailureReason.RESYNC_REQUIRED.value,
                    "message": "source has a skipped interval awaiting confirmation",
                    "source_id": source_id,
                    "gap": {
                        "from": pending["from"],
                        "to": pending["to"],
                        "resume_from": pending["resume_from"],
                    },
                }
                log.info(
                    "[%s] run BLOCKED reason=resync_required pending gap [%d, %d)",
                    source_id, pending["from"], pending["to"],
                )
                return result
            log.info("[%s] run start, cursor=%d", source_id, cursor)
            expected = cursor  # 本地已确认进度：下一条记录的 seq 必须等于它
            try:
                for raw in connector.fetch(cursor):
                    if raw.seq > expected:
                        # 源端跨过 [expected, raw.seq) 这段未处理数据：判异常、停下，
                        # 不认下源端位置，也不把这段当成不存在
                        raise SourceGapError(
                            f"source skipped unprocessed interval "
                            f"[{expected}, {raw.seq})",
                            source_id=source_id,
                            gap_from=expected,
                            gap_to=raw.seq,
                        )
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
                    expected = cursor
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
                self.state.record_failure(source_id, result.failure)
                if isinstance(exc, SourceGapError):
                    log.info(
                        "[%s] run FAILED reason=%s gap=[%d, %d) resume_from=%d, "
                        "cursor stays %d",
                        source_id, exc.reason.value, exc.gap_from, exc.gap_to,
                        exc.resume_from, cursor,
                    )
                else:
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
                self.state.record_failure(source_id, result.failure)
                log.info(
                    "[%s] run FAILED reason=%s, cursor stays %d",
                    source_id, exc.reason.value, cursor,
                )
                return result
            self.state.record_ok(source_id)
            log.info("[%s] run ok, cursor=%d", source_id, cursor)
            return result

    def resync(
        self, source_id: str, *, operator: str, justification: str
    ) -> ResyncResult:
        """受控重新同步：确认源端跳过区间永久丢失后，把位点推到新起点。

        与 run_source 共用同一把源锁：并发触发时二者串行，结果确定。
        确认信息（操作人、依据）必须非空；全程留痕，可审计。
        """
        with self._source_lock(source_id):
            cursor_before = self.cursors.load(source_id)
            gap = self.state.pending_gap(source_id)
            if gap is None:
                failure = {
                    "reason": FailureReason.NO_PENDING_GAP.value,
                    "message": "no skipped interval awaiting confirmation",
                    "source_id": source_id,
                }
                log.info("[%s] resync REJECTED reason=no_pending_gap", source_id)
                return ResyncResult(
                    source_id=source_id,
                    status="rejected",
                    cursor_before=cursor_before,
                    cursor_after=cursor_before,
                    failure=failure,
                )
            if not (operator and operator.strip()) or not (
                justification and justification.strip()
            ):
                failure = {
                    "reason": FailureReason.INVALID_CONFIRMATION.value,
                    "message": "operator and justification are required",
                    "source_id": source_id,
                }
                log.info(
                    "[%s] resync REJECTED reason=invalid_confirmation", source_id
                )
                return ResyncResult(
                    source_id=source_id,
                    status="rejected",
                    cursor_before=cursor_before,
                    cursor_after=cursor_before,
                    gap={
                        "from": gap["from"],
                        "to": gap["to"],
                        "resume_from": gap["resume_from"],
                    },
                    failure=failure,
                )
            new_cursor = gap["resume_from"]
            self.cursors.reset(source_id, new_cursor)
            audit = self.state.record_resync(
                source_id,
                operator=operator.strip(),
                justification=justification.strip(),
                gap=gap,
                cursor_before=cursor_before,
                cursor_after=new_cursor,
            )
            log.info(
                "[%s] resync ok by=%s skipped=[%d, %d) cursor %d -> %d",
                source_id, audit["operator"], gap["from"], gap["to"],
                cursor_before, new_cursor,
            )
            return ResyncResult(
                source_id=source_id,
                status="ok",
                cursor_before=cursor_before,
                cursor_after=new_cursor,
                gap={
                    "from": gap["from"],
                    "to": gap["to"],
                    "resume_from": gap["resume_from"],
                },
                audit=audit,
            )

    def status(self, source_id: str) -> dict:
        """按数据源查状态：当前位点、健康度、最近异常、待确认区间、同步留痕。"""
        s = self.state.status(source_id)
        return {
            "source_id": source_id,
            "cursor": self.cursors.load(source_id),
            "status": s["status"],
            "last_failure": s["last_failure"],
            "pending_gap": s["pending_gap"],
            "resyncs": s["resyncs"],
        }

    def run_all(
        self, connectors: list[Connector], max_workers: int | None = None
    ) -> list[SourceRunResult]:
        workers = max_workers or self.max_workers
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(self.run_source, connectors))
