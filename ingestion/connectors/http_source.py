"""本地回环 HTTP 数据源连接器。"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import Any, Iterator

from ingestion.connectors.base import Connector
from ingestion.errors import SourceError, SourceMovedError
from ingestion.models import RawRecord

PAGE_SIZE = 100


class HttpConnector(Connector):
    """从本地回环接口分页拉取记录。

    接口契约：GET {base_url}/records?after={seq}&limit={n}
    返回 {"records": [{"seq": int, "recordId": str, "text": str}, ...]}
    网络/协议错误一律包装为 SourceError，不透出底层异常。
    """

    def __init__(self, source_id: str, base_url: str, timeout: float = 5.0) -> None:
        self.source_id = source_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _get_page(self, after: int) -> tuple[list[dict[str, Any]], int | None]:
        url = f"{self.base_url}/records?after={after}&limit={PAGE_SIZE}"
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            raise SourceError(
                "http source request failed", source_id=self.source_id
            ) from None
        records = body.get("records")
        if not isinstance(records, list):
            raise SourceError("malformed source response", source_id=self.source_id)
        min_seq = body.get("min_seq")  # 源端声明的当前最早可提供位置（可选）
        if min_seq is not None and not isinstance(min_seq, int):
            raise SourceError("malformed source response", source_id=self.source_id)
        return records, min_seq

    def fetch(self, after: int) -> Iterator[RawRecord]:
        cursor = after
        first_page = True
        while True:
            page, min_seq = self._get_page(cursor)
            if first_page and min_seq is not None and min_seq > after:
                # 源端最早可提供位置已越过本地进度：中间一段从未被采集，
                # 判异常并停下，绝不把本地进度直接认到源端新起点。
                raise SourceMovedError(
                    "source skipped unprocessed range "
                    f"[{after}, {min_seq})",
                    source_id=self.source_id,
                    gap_from=after,
                    gap_to=min_seq,
                )
            first_page = False
            if not page:
                return
            for item in page:
                seq = item.get("seq")
                if not isinstance(seq, int):
                    raise SourceError(
                        "record missing seq in source response",
                        source_id=self.source_id,
                    )
                yield RawRecord(seq=seq, data=item)
                cursor = seq + 1
            if len(page) < PAGE_SIZE:
                return

    def normalize(self, raw: RawRecord) -> dict[str, Any]:
        # HTTP 源字段名为 recordId/text，归一到统一模型的 id/payload
        return {"id": raw.data.get("recordId"), "payload": raw.data.get("text")}
