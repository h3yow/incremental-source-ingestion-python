"""采集引擎：位点推进、投递、并发控制与失败隔离。

核心规则
========
1. 投递先于位点推进：每条记录先幂等投递成功，再把位点推进到该记录。
2. 位点必须连续：期望位点 = last_position + 1。
   * 实际位点 < 期望 -> POSITION_REGRESSED（数据源回放了旧数据，拒绝推进）
   * 实际位点 > 期望 -> POSITION_GAP（位点不连续，拒绝推进，避免丢数不可见）
3. 遇到畸形记录或数据源致命错误时立即停止该源本次运行；
   之前已投递的前缀与位点保留（at-least-once 投递 + 幂等目标端 => 不重不丢）。
4. 每个数据源一把互斥锁：同一源并发触发时，第二个请求得到 SOURCE_BUSY，
   不会并行推进同一源；不同源互不影响。
5. 并发采集用有界线程池，单个源的失败（报告 fatal）不影响其它源的提交结果。
"""

from __future__ import annotations

import threading
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

from app.checkpoint import CheckpointStore
from app.connectors.base import Connector
from app.errors import FailureCode, IngestionError
from app.models import RecordFailure, RunReport, SourceEvent
from app.target import FileTarget


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


class IngestionEngine:
    def __init__(
        self,
        connectors: dict[str, Connector],
        checkpoints: CheckpointStore,
        target: FileTarget,
        *,
        max_workers: int = 4,
        verbose: bool = True,
    ) -> None:
        self.connectors = connectors
        self.checkpoints = checkpoints
        self.target = target
        self.max_workers = max_workers
        self.verbose = verbose
        self._source_locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock_for(self, source: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._source_locks.get(source)
            if lock is None:
                lock = threading.Lock()
                self._source_locks[source] = lock
            return lock

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(msg, flush=True)

    def run_source(self, source: str) -> RunReport:
        connector = self.connectors.get(source)
        if connector is None:
            raise IngestionError(
                FailureCode.SOURCE_NOT_FOUND,
                f"未注册的数据源: {source}",
                source=source,
            )
        lock = self._lock_for(source)
        if not lock.acquire(blocking=False):
            busy = IngestionError(
                FailureCode.SOURCE_BUSY,
                "同一数据源正在采集中，本次触发被拒绝",
                source=source,
            )
            self._log(f"[{source}] 触发被拒绝: source_busy")
            return RunReport(
                source=source,
                started_at=_now(),
                finished_at=_now(),
                last_position_before=self.checkpoints.get(source),
                last_position_after=self.checkpoints.get(source),
                fatal=busy,
            )

        started = _now()
        try:
            return self._run_locked(connector, source, started)
        finally:
            lock.release()

    def _run_locked(self, connector: Connector, source: str, started: str) -> RunReport:
        last = self.checkpoints.get(source)
        report = RunReport(
            source=source,
            started_at=started,
            finished_at=started,
            last_position_before=last,
            last_position_after=last,
        )
        self._log(f"[{source}] 开始采集，起始位点 {last}")

        events: Iterable[SourceEvent]
        try:
            events = connector.fetch(last)
        except IngestionError as exc:
            report.fatal = exc
            report.finished_at = _now()
            self._log(f"[{source}] 数据源致命错误: {exc.code.value} - {exc.message}")
            return report

        delivered = 0
        skipped = 0
        try:
            for event in events:
                expected = last + 1
                if event.position < expected:
                    raise IngestionError(
                        FailureCode.POSITION_REGRESSED,
                        f"位点回退: 期望 {expected}，实际 {event.position}",
                        source=source,
                        position=event.position,
                    )
                if event.position > expected:
                    raise IngestionError(
                        FailureCode.POSITION_GAP,
                        f"位点不连续: 期望 {expected}，实际 {event.position}",
                        source=source,
                        position=event.position,
                    )

                try:
                    record = connector.normalize(event)
                except IngestionError as exc:
                    failure = RecordFailure(
                        position=event.position,
                        code=exc.code.value,
                        message=exc.message,
                    )
                    report.record_failures.append(failure)
                    self._log(
                        f"[{source}] 记录失败 pos={event.position} "
                        f"code={exc.code.value} msg={exc.message}"
                    )
                    raise

                is_new = self.target.deliver(record)
                # 投递与位点推进之间不会有其它线程介入（同源锁保护）
                old = self.checkpoints.advance(source, record.position)
                if old != last:
                    # 理论上不会发生（锁保护），显式拒绝任何意外的交叉推进
                    raise IngestionError(
                        FailureCode.POSITION_REGRESSED,
                        f"位点被外部改动: 期望旧值 {last}，实际 {old}",
                        source=source,
                        position=record.position,
                    )
                last = record.position
                report.last_position_after = last
                if is_new:
                    delivered += 1
                    self._log(
                        f"[{source}] 投递 pos={record.position} id={record.record_id} "
                        f"-> 位点推进至 {last}"
                    )
                else:
                    skipped += 1
                    self._log(
                        f"[{source}] 幂等跳过 pos={record.position} id={record.record_id}"
                    )
        except IngestionError as exc:
            report.fatal = exc
            self._log(f"[{source}] 运行中止于位点 {last}: {exc.code.value}")

        report.delivered = delivered
        report.skipped_replayed = skipped
        report.finished_at = _now()
        self._log(
            f"[{source}] 结束采集，位点 {report.last_position_before} -> "
            f"{report.last_position_after}，新投递 {delivered}，幂等跳过 {skipped}"
        )
        return report

    def run_all(self) -> dict[str, RunReport]:
        reports: dict[str, RunReport] = {}
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self.run_source, name): name
                for name in self.connectors
            }
            for fut in as_completed(futures):
                name = futures[fut]
                reports[name] = fut.result()
        return reports
