"""本地回环 HTTP 数据源连接器（数据源形态二）。

通过 ``GET /events?after=<position>`` 分页拉取，``next`` 为 null 或不增长时结束。

失败分类（底层 urllib 异常一律不透出）：
* 连接失败 / 超时 / HTTP 5xx      -> SOURCE_UNAVAILABLE
* 响应体非法 JSON / 结构不符合契约 -> SOURCE_PROTOCOL
* 记录字段缺失 / 类型错误         -> MISSING_FIELD / TYPE_MISMATCH
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Iterable

from app.connectors.base import BaseConnector
from app.errors import FailureCode, IngestionError
from app.models import Record, SourceEvent
from app.normalizer import normalize_raw


class HttpSourceConnector(BaseConnector):
    def __init__(
        self,
        name: str,
        base_url: str,
        *,
        timeout: float = 5.0,
        force_fail: bool = False,
    ) -> None:
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.force_fail = force_fail

    def _get_page(self, after: int) -> dict:
        suffix = "&fail=1" if self.force_fail else ""
        url = f"{self.base_url}/events?after={after}{suffix}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            raise IngestionError(
                FailureCode.SOURCE_UNAVAILABLE,
                f"HTTP 数据源返回错误状态: {exc.code}",
                source=self.name,
                cause_type=type(exc).__name__,
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise IngestionError(
                FailureCode.SOURCE_UNAVAILABLE,
                "HTTP 数据源不可达或超时",
                source=self.name,
                cause_type=type(exc).__name__,
            ) from None
        try:
            data = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise IngestionError(
                FailureCode.SOURCE_PROTOCOL,
                "HTTP 数据源返回了无法解析的响应体",
                source=self.name,
                cause_type=type(exc).__name__,
            ) from None
        if not isinstance(data, dict) or not isinstance(data.get("events"), list):
            raise IngestionError(
                FailureCode.SOURCE_PROTOCOL,
                "HTTP 响应缺少 events 数组",
                source=self.name,
            )
        return data

    def fetch(self, after_position: int) -> Iterable[SourceEvent]:
        after = after_position
        while True:
            page = self._get_page(after)
            events = page["events"]
            next_after = page.get("next")
            yielded = 0
            for item in events:
                if not isinstance(item, dict) or "position" not in item or "record" not in item:
                    raise IngestionError(
                        FailureCode.SOURCE_PROTOCOL,
                        "分页事件缺少 position/record 字段",
                        source=self.name,
                    )
                position = item["position"]
                if not isinstance(position, int) or isinstance(position, bool):
                    raise IngestionError(
                        FailureCode.SOURCE_PROTOCOL,
                        "position 必须为整数",
                        source=self.name,
                    )
                if position <= after:
                    raise IngestionError(
                        FailureCode.SOURCE_PROTOCOL,
                        f"分页位点不前进: position={position} after={after}",
                        source=self.name,
                    )
                yield SourceEvent(position=position, raw=item["record"])
                yielded += 1
            # 终止条件：空页、next 为空，或 next 未增长（防御异常服务端导致死循环）
            if not events or next_after is None or next_after <= after:
                break
            after = next_after

    def normalize(self, event: SourceEvent) -> Record:
        return normalize_raw(self.name, event.position, event.raw)
