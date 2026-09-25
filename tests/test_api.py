"""对外 API 测试（FastAPI TestClient）。"""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.api import create_api_app
from app.checkpoint import CheckpointStore
from app.connectors.file_source import FileSourceConnector
from app.engine import IngestionEngine
from app.errors import FailureCode
from app.target import FileTarget
from tests.conftest import jrow, write_jsonl


def _client(tmp_path: Path) -> TestClient:
    src = write_jsonl(tmp_path / "fs.jsonl", [jrow("a"), jrow("b")])
    eng = IngestionEngine(
        {"fs": FileSourceConnector("fs", str(src))},
        CheckpointStore(str(tmp_path / "cp.json")),
        FileTarget(str(tmp_path / "target.json")),
        verbose=False,
    )
    return TestClient(create_api_app(eng))


def test_api_run_and_checkpoints(tmp_path: Path) -> None:
    client = _client(tmp_path)
    resp = client.post("/sources/fs/run")
    assert resp.status_code == 200
    body = resp.json()["report"]
    assert body["delivered"] == 2
    assert body["last_position_after"] == 2

    assert client.get("/checkpoints/fs").json() == {
        "source": "fs",
        "last_position": 2,
    }
    records = client.get("/target/fs").json()["records"]
    assert [r["record_id"] for r in records] == ["a", "b"]

    # 重复触发
    again = client.post("/sources/fs/run").json()["report"]
    assert again["delivered"] == 0 and again["skipped_replayed"] == 0


def test_api_unknown_source_structured_error(tmp_path: Path) -> None:
    client = _client(tmp_path)
    resp = client.post("/sources/ghost/run")
    assert resp.status_code == 404
    error = resp.json()["error"]
    assert error["code"] == FailureCode.SOURCE_NOT_FOUND.value
    assert error["source"] == "ghost"
