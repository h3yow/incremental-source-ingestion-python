"""目标端：幂等写入契约，文件实现与本地回环 HTTP 实现。"""

from __future__ import annotations

import json
import os
import threading
import urllib.error
import urllib.request
from typing import Protocol

from ingestion.errors import SinkError
from ingestion.models import UnifiedRecord


class Sink(Protocol):
    """目标端契约：按 dedup_key 幂等，重复写入返回 False。"""

    def write(self, record: UnifiedRecord) -> bool: ...


class LocalSink:
    """文件持久化 + 去重键幂等的目标端。

    每行一条 JSON：{"dedup_key", "source_id", "record_id", "seq", "payload"}。
    同一 dedup_key 重复写入（重放/并发重复触发）直接返回 False，不产生重复行。
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()

    def _load_keys(self) -> set[str]:
        keys: set[str] = set()
        if not os.path.exists(self.path):
            return keys
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    keys.add(json.loads(line)["dedup_key"])
        return keys

    def write(self, record: UnifiedRecord) -> bool:
        """写入成功返回 True；已存在（重放）返回 False，不重复写入。"""
        with self._lock:
            if record.dedup_key in self._load_keys():
                return False
            row = {
                "dedup_key": record.dedup_key,
                "source_id": record.source_id,
                "record_id": record.record_id,
                "seq": record.seq,
                "payload": record.payload,
            }
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
                f.flush()
                os.fsync(f.fileno())
            return True

    def count(self) -> int:
        with self._lock:
            return len(self._load_keys())

    def keys(self) -> set[str]:
        with self._lock:
            return self._load_keys()


class HttpSink:
    """投递到本地回环接口的目标端；接收方负责去重。

    POST {url}  body 为记录 JSON，返回 {"duplicate": bool}。
    网络/协议错误包装为 SinkError，不透出底层异常。
    """

    def __init__(self, url: str, timeout: float = 5.0) -> None:
        self.url = url
        self.timeout = timeout

    def write(self, record: UnifiedRecord) -> bool:
        body = json.dumps(
            {
                "dedup_key": record.dedup_key,
                "source_id": record.source_id,
                "record_id": record.record_id,
                "seq": record.seq,
                "payload": record.payload,
            }
        ).encode("utf-8")
        req = urllib.request.Request(
            self.url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                result = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            raise SinkError("http sink request failed", source_id=record.source_id) from None
        duplicate = result.get("duplicate")
        if not isinstance(duplicate, bool):
            raise SinkError("malformed sink response", source_id=record.source_id)
        return not duplicate
