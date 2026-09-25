"""统一连接器契约。

所有数据源连接器实现 :class:`Connector` 协议：
* :meth:`fetch` 按位点返回 :class:`SourceEvent` 序列；
* :meth:`normalize` 把原始事件归一为 :class:`Record`，
  数据质量问题必须抛出 :class:`IngestionError`，不得静默跳过。
"""

from __future__ import annotations

from typing import Iterable, Protocol, runtime_checkable

from app.models import Record, SourceEvent


@runtime_checkable
class Connector(Protocol):
    name: str

    def fetch(self, after_position: int) -> Iterable[SourceEvent]:
        """返回位点严格大于 ``after_position`` 的事件序列。"""
        ...

    def normalize(self, event: SourceEvent) -> Record:
        """把单个原始事件归一化；异常数据抛出 IngestionError。"""
        ...


class BaseConnector:
    """连接器公共基类：提供通用字段校验。"""

    name: str

    def fetch(self, after_position: int) -> Iterable[SourceEvent]:
        raise NotImplementedError

    def normalize(self, event: SourceEvent) -> Record:
        raise NotImplementedError
