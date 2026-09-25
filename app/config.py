"""本地演示装配：数据目录、模拟数据、连接器与引擎构建。"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from app.checkpoint import CheckpointStore
from app.connectors.file_source import FileSourceConnector
from app.connectors.http_source import HttpSourceConnector
from app.engine import IngestionEngine
from app.target import FileTarget


@dataclass(frozen=True)
class DemoPaths:
    root: Path
    file_source: Path
    http_source: Path
    checkpoints: Path
    target: Path


def build_paths(root: str | os.PathLike[str] | None = None) -> DemoPaths:
    base = Path(root) if root else Path(os.environ.get("INGEST_HOME", ".runtime"))
    data = base / "data"
    data.mkdir(parents=True, exist_ok=True)
    return DemoPaths(
        root=base,
        file_source=data / "file_source.jsonl",
        http_source=data / "http_source.jsonl",
        checkpoints=base / "checkpoints.json",
        target=base / "target.json",
    )


def seed_demo_data(paths: DemoPaths, *, reset: bool = True) -> None:
    """写入演示数据。默认覆盖，保证本地重复运行的初始状态确定。"""
    if reset:
        for p in (paths.file_source, paths.http_source, paths.checkpoints, paths.target):
            p.unlink(missing_ok=True)
    if not paths.file_source.exists():
        rows = [
            {"id": "f-001", "data": {"name": "alpha", "amount": 10}},
            {"id": "f-002", "data": {"name": "beta", "amount": 20}},
            {"id": "f-003", "data": {"name": "gamma", "amount": 30}},
        ]
        paths.file_source.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
            encoding="utf-8",
        )
    if not paths.http_source.exists():
        rows = [
            {"id": "h-001", "data": {"city": "sh", "qty": 1}},
            {"id": "h-002", "data": {"city": "bj", "qty": 2}},
            {"id": "h-003", "data": {"city": "gz", "qty": 3}},
        ]
        paths.http_source.write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
            encoding="utf-8",
        )


def build_engine(paths: DemoPaths, *, http_base_url: str, max_workers: int = 4) -> IngestionEngine:
    connectors = {
        "file_source": FileSourceConnector("file_source", str(paths.file_source)),
        "http_source": HttpSourceConnector("http_source", http_base_url),
    }
    return IngestionEngine(
        connectors,
        CheckpointStore(str(paths.checkpoints)),
        FileTarget(str(paths.target)),
        max_workers=max_workers,
    )
