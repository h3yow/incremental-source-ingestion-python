"""按数据源持久化运行状态：健康度、最近异常、待确认跳过区间、重新同步审计。"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone

from ingestion.errors import FailureReason
from ingestion.models import SourceState


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SourceStateStore:
    """文件持久化的数据源状态存储（与位点存储分离，互不污染）。

    - 每次采集结束由管线写入 ok/failed 状态与最近失败原因；
    - 检测到源端跨段（source_moved）时进入 pending_resync，
      并记录待确认的跳过区间，确认前管线拒绝继续采集该源；
    - record_resync 留痕：操作人、时间、跳过区间、确认依据。
    进程内锁 + 临时文件原子 rename，保证并发读写安全。
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _read_all(self) -> dict[str, dict]:
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_all(self, data: dict[str, dict]) -> None:
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True)
        os.replace(tmp, self.path)

    def get(self, source_id: str) -> SourceState:
        with self._lock:
            return SourceState.from_dict(source_id, self._read_all().get(source_id, {}))

    def list_all(self) -> dict[str, SourceState]:
        with self._lock:
            return {
                sid: SourceState.from_dict(sid, raw)
                for sid, raw in self._read_all().items()
            }

    def _update(self, source_id: str, mutate) -> SourceState:
        with self._lock:
            data = self._read_all()
            state = SourceState.from_dict(source_id, data.get(source_id, {}))
            mutate(state)
            data[source_id] = state.to_dict()
            self._write_all(data)
            return state

    def mark_ok(self, source_id: str) -> SourceState:
        def _m(state: SourceState) -> None:
            state.status = "ok"
            state.last_failure = None
        return self._update(source_id, _m)

    def mark_failure(self, source_id: str, failure: dict) -> SourceState:
        """记录失败；source_moved 失败会同时登记待确认跳过区间。"""

        def _m(state: SourceState) -> None:
            state.last_failure = dict(failure)
            gap = failure.get("gap")
            if failure.get("reason") == FailureReason.SOURCE_MOVED.value and gap:
                state.status = "pending_resync"
                state.pending_skip = {
                    "from": gap["from"],
                    "to": gap["to"],
                    "detected_at": _now_iso(),
                    "reason": failure.get("message"),
                }
            else:
                state.status = "failed"
        return self._update(source_id, _m)

    def record_resync(
        self,
        source_id: str,
        *,
        skipped_from: int,
        skipped_to: int,
        operator: str,
        reason: str,
    ) -> SourceState:
        """确认重新同步：留痕并清除待确认状态，使该源恢复可采集。"""

        def _m(state: SourceState) -> None:
            state.resync_history.append(
                {
                    "operator": operator,
                    "at": _now_iso(),
                    "skipped_from": skipped_from,
                    "skipped_to": skipped_to,
                    "reason": reason,
                }
            )
            state.pending_skip = None
            state.last_failure = None
            state.status = "ok"
        return self._update(source_id, _m)
