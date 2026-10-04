"""统一连接器契约。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterator

from ingestion.models import RawRecord


class Connector(ABC):
    """连接器契约：

    - fetch(after): 返回序号严格大于等于 after 的原始记录，按 seq 升序、连续无空洞。
    - 对齐检测：若源端当前实际能提供的位置与 after 对不上（源端跨过
      未处理区间、被清库/重建），实现方必须抛 SourceMovedError 并携带
      gap 区间，不得静默返回空或直接从新起点给数。
    - 底层异常必须由实现方包装为 SourceError（脱敏），不得原样透出。
    """

    source_id: str

    @abstractmethod
    def fetch(self, after: int) -> Iterator[RawRecord]:
        raise NotImplementedError

    @abstractmethod
    def normalize(self, raw: RawRecord) -> dict[str, Any]:
        """把原始记录映射为统一字段字典（校验在 pipeline 层做）。"""
        raise NotImplementedError
