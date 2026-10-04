"""按数据源记录对齐状态：健康度、最近异常、待确认跳过区间、重新同步留痕。"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class SourceStateStore:
    """文件持久化的数据源状态存储。

    每个数据源一条状态：
    - last_failure: 最近一次采集失败的可区分原因（成功时清空）
    - pending_gap:  待人工确认的跳过区间 {"from", "to", "resume_from", "detected_at", "detail"}
    - resyncs:      重新同步审计记录（谁、何时、跳过哪段、依据、位点变化），只增不删

    健康度由状态推导（不落盘）：
    - pending_gap 存在      -> "pending_confirmation"（采集被拒绝，等待确认）
    - last_failure 存在     -> "anomaly"
    - 否则                  -> "healthy"

    path=None 时为纯内存实现（测试用），不落盘。
    """

    def __init__(self, path: str | None) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._mem: dict[str, dict] = {}

    # ---------- 持久化 ----------

    def _read_all(self) -> dict[str, dict]:
        if self.path is None:
            return {k: dict(v) for k, v in self._mem.items()}
        if not os.path.exists(self.path):
            return {}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _write_all(self, data: dict[str, dict]) -> None:
        if self.path is None:
            self._mem = {k: dict(v) for k, v in data.items()}
            return
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, sort_keys=True, ensure_ascii=False)
        os.replace(tmp, self.path)  # 原子替换

    def _entry(self, data: dict[str, dict], source_id: str) -> dict:
        return data.setdefault(
            source_id, {"last_failure": None, "pending_gap": None, "resyncs": []}
        )

    # ---------- 采集侧状态流转 ----------

    def record_ok(self, source_id: str) -> None:
        """采集成功：清除最近异常（待确认区间存在时采集本就不会发生）。"""
        with self._lock:
            data = self._read_all()
            entry = self._entry(data, source_id)
            entry["last_failure"] = None
            self._write_all(data)

    def record_failure(self, source_id: str, failure: dict) -> None:
        """采集失败：记录可区分原因；source_gap 同时登记待确认区间。"""
        with self._lock:
            data = self._read_all()
            entry = self._entry(data, source_id)
            entry["last_failure"] = failure
            gap = failure.get("gap")
            if failure.get("reason") == "source_gap" and gap:
                entry["pending_gap"] = {
                    "from": gap["from"],
                    "to": gap["to"],
                    "resume_from": gap["resume_from"],
                    "detected_at": _now_iso(),
                    "detail": failure.get("message"),
                }
            self._write_all(data)

    # ---------- 重新同步 ----------

    def pending_gap(self, source_id: str) -> dict | None:
        with self._lock:
            return self._read_all().get(source_id, {}).get("pending_gap")

    def record_resync(
        self,
        source_id: str,
        *,
        operator: str,
        justification: str,
        gap: dict,
        cursor_before: int,
        cursor_after: int,
    ) -> dict:
        """确认跳过：追加审计记录并清除待确认区间与异常标记。返回审计条目。"""
        audit = {
            "operator": operator,
            "confirmed_at": _now_iso(),
            "justification": justification,
            "skipped": {"from": gap["from"], "to": gap["to"]},
            "cursor_before": cursor_before,
            "cursor_after": cursor_after,
        }
        with self._lock:
            data = self._read_all()
            entry = self._entry(data, source_id)
            entry["resyncs"].append(audit)
            entry["pending_gap"] = None
            entry["last_failure"] = None
            self._write_all(data)
        return audit

    # ---------- 查询 ----------

    def status(self, source_id: str) -> dict:
        with self._lock:
            entry = self._read_all().get(
                source_id, {"last_failure": None, "pending_gap": None, "resyncs": []}
            )
        if entry.get("pending_gap"):
            health = "pending_confirmation"
        elif entry.get("last_failure"):
            health = "anomaly"
        else:
            health = "healthy"
        return {
            "source_id": source_id,
            "status": health,
            "last_failure": entry.get("last_failure"),
            "pending_gap": entry.get("pending_gap"),
            "resyncs": list(entry.get("resyncs", [])),
        }
