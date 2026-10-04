"""采集编排：校验、位点推进、异常对齐、受控重新同步、并发调度。"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor

from ingestion.connectors.base import Connector
from ingestion.cursor_store import CursorStore
from ingestion.errors import (
    FailureReason,
    IngestionError,
    RecordValidationError,
    ResyncRequiredError,
    SourceMovedError,
)
from ingestion.models import (
    REQUIRED_FIELDS,
    SourceRunResult,
    UnifiedRecord,
    RecordOutcome,
)
from ingestion.sink import LocalSink
from ingestion.state_store import SourceStateStore

log = logging.getLogger("ingestion")


class Pipeline:
    """按数据源串行、跨数据源并发的增量采集管线。

    - 同一数据源在进程内有锁，并发触发只会串行执行；第二次触发读到已推进位点，
      通常无新记录（重放时 sink 幂等），不会重复写入。
    - 跨数据源按 max_workers 并发；任一数据源失败只标记自身结果，不影响其它源。
    - 校验失败/数据源错误采用 fail-fast：位点只提交到失败记录之前，
      失败原因区分 missing_field / type_mismatch / source_error 等。
    - 异常对齐：源端实际能提供的位置越过本地已确认进度（跨段/清库/重建）时，
      本次运行判为 source_moved 并停止，登记待确认跳过区间；
      确认前再次触发一律以 resync_required 拒绝。
    - 受控重新同步：resync() 需显式 confirm，留痕后把位点推到新起点；
      与采集共用每源互斥锁，并发下结果确定。
    """

    def __init__(
        self,
        cursors: CursorStore,
        sink: LocalSink,
        max_workers: int = 4,
        state_store: SourceStateStore | None = None,
    ) -> None:
        self.cursors = cursors
        self.sink = sink
        self.max_workers = max_workers
        if state_store is None:
            # 默认与位点文件同目录，便于本地验证与重启恢复
            state_store = SourceStateStore(
                os.path.join(os.path.dirname(cursors.path) or ".", "source_state.json")
            )
        self.states = state_store
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
            # 有待人工确认的跳过区间：确认重新同步之前，拒绝继续采集该源
            pending = self.states.get(source_id).pending_skip
            if pending is not None:
                exc = ResyncRequiredError(
                    "pending skipped range awaiting resync confirmation",
                    source_id=source_id,
                    gap={"from": pending["from"], "to": pending["to"]},
                )
                result.status = "failed"
                result.failure = exc.to_dict()
                log.info(
                    "[%s] run REJECTED reason=%s gap=[%d,%d), cursor stays %d",
                    source_id, exc.reason.value, pending["from"], pending["to"], cursor,
                )
                return result
            log.info("[%s] run start, cursor=%d", source_id, cursor)
            try:
                first = True
                for raw in connector.fetch(cursor):
                    if first and raw.seq > cursor:
                        # 源端跨过了一段本地未处理的数据：判异常并停下，
                        # 既不把本地进度直接认到源端新起点，也不当作没这段。
                        raise SourceMovedError(
                            "source skipped unprocessed range "
                            f"[{cursor}, {raw.seq})",
                            source_id=source_id,
                            gap_from=cursor,
                            gap_to=raw.seq,
                        )
                    first = False
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
                self.states.mark_failure(source_id, exc.to_dict())
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
                self.states.mark_failure(source_id, exc.to_dict())
                result.status = "failed"
                result.failure = exc.to_dict()
                result.cursor_after = cursor
                log.info(
                    "[%s] run FAILED reason=%s, cursor stays %d",
                    source_id, exc.reason.value, cursor,
                )
                return result
            self.states.mark_ok(source_id)
            log.info("[%s] run ok, cursor=%d", source_id, cursor)
            return result

    def resync(
        self,
        source_id: str,
        new_cursor: int,
        *,
        operator: str,
        reason: str,
        confirm: bool,
    ) -> dict:
        """受控重新同步：确认跳过区间永久丢失后，把该源位点推到新起点。

        - 必须显式 confirm=True（确认源端那段数据确实永久丢失、可以跳过）；
        - 只允许向前推进，跳过区间 [旧位点, new_cursor) 留痕（谁/何时/依据）；
        - 与采集共用每源互斥锁：并发下要么同步先生效、采集从新起点继续，
          要么采集先跑完、同步基于最新位点判断，不会进度错乱或重复投递。
        """
        if not confirm:
            raise ValueError(
                "resync requires explicit confirmation that the skipped "
                "range is permanently lost"
            )
        if not operator or not reason:
            raise ValueError("resync requires operator and reason for audit")
        with self._source_lock(source_id):
            stored = self.cursors.load(source_id)
            if new_cursor <= stored:
                from ingestion.errors import CursorRegressionError

                raise CursorRegressionError(
                    f"resync must move forward: new={new_cursor} <= stored={stored}",
                    source_id=source_id,
                )
            # 全部校验通过后：先留痕清状态再推位点。若中途失败，下次采集会
            # 重新检测出跨段并再次进入待确认，状态可自愈，不会静默丢区间。
            self.states.record_resync(
                source_id,
                skipped_from=stored,
                skipped_to=new_cursor,
                operator=operator,
                reason=reason,
            )
            self.cursors.reset(source_id, new_cursor)
            log.info(
                "[%s] resync confirmed by %s: cursor %d -> %d, skipped [%d,%d), reason=%s",
                source_id, operator, stored, new_cursor, stored, new_cursor, reason,
            )
            return {
                "source_id": source_id,
                "status": "ok",
                "cursor_before": stored,
                "cursor_after": new_cursor,
                "skipped": {"from": stored, "to": new_cursor},
                "operator": operator,
                "reason": reason,
            }

    def run_all(
        self, connectors: list[Connector], max_workers: int | None = None
    ) -> list[SourceRunResult]:
        workers = max_workers or self.max_workers
        with ThreadPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(self.run_source, connectors))
