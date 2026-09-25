"""本地可运行的增量采集演示服务。

一个进程内同时提供三类回环接口：
1. GET  /mock/{source_id}/records  模拟 HTTP 数据源（recordId/text 形态）
2. POST /sink/deliver              模拟目标端接收（按 dedup_key 幂等去重）
3. POST /ingest/run                触发采集（文件源 + 回环 HTTP 源并发）

运行：uv run python main.py   （默认监听 127.0.0.1:8765）
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from ingestion.connectors.file_source import FileConnector
from ingestion.connectors.http_source import HttpConnector
from ingestion.cursor_store import CursorStore
from ingestion.models import UnifiedRecord
from ingestion.pipeline import Pipeline
from ingestion.sink import HttpSink, LocalSink

DATA_DIR = os.environ.get("INGEST_DATA_DIR", os.path.join(os.path.dirname(__file__), "data"))
LISTEN_HOST = os.environ.get("INGEST_HOST", "127.0.0.1")
LISTEN_PORT = int(os.environ.get("INGEST_PORT", "8765"))


def _ensure_demo_data(data_dir: str) -> None:
    """首次运行时写入示例配置与数据，便于直接本地验证。"""
    os.makedirs(data_dir, exist_ok=True)
    orders = os.path.join(data_dir, "orders.jsonl")
    if not os.path.exists(orders):
        with open(orders, "w", encoding="utf-8") as f:
            for i in range(5):
                f.write(json.dumps({"id": f"ord-{i}", "payload": f"order {i}"}) + "\n")
    http_records = os.path.join(data_dir, "events.json")
    if not os.path.exists(http_records):
        records = [
            {"seq": i, "recordId": f"evt-{i}", "text": f"event {i}"} for i in range(5)
        ]
        with open(http_records, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
    config = os.path.join(data_dir, "sources.json")
    if not os.path.exists(config):
        with open(config, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "max_workers": 4,
                    "sources": [
                        {"type": "file", "id": "orders", "path": orders},
                        {"type": "http", "id": "events", "base_url": "http://{host}:{port}/mock/events"},
                    ],
                },
                f,
                indent=2,
            )


def _load_mock_records(data_dir: str) -> dict[str, list[dict[str, Any]]]:
    path = os.path.join(data_dir, "events.json")
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return {"events": json.load(f)}


class DeliverBody(BaseModel):
    dedup_key: str
    source_id: str
    record_id: str
    seq: int
    payload: str


def create_app(
    data_dir: str = DATA_DIR,
    *,
    use_http_sink: bool = True,
    host: str = LISTEN_HOST,
    port: int = LISTEN_PORT,
) -> FastAPI:
    _ensure_demo_data(data_dir)
    config_path = os.path.join(data_dir, "sources.json")
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    cursors = CursorStore(os.path.join(data_dir, "cursors.json"))
    local_sink = LocalSink(os.path.join(data_dir, "sink.jsonl"))
    mock_records = _load_mock_records(data_dir)
    sink_lock = threading.Lock()

    def build_sink() -> HttpSink | LocalSink:
        if use_http_sink:
            return HttpSink(f"http://{host}:{port}/sink/deliver")
        return local_sink

    def build_connectors():
        connectors = []
        for item in config["sources"]:
            if item["type"] == "file":
                connectors.append(FileConnector(item["id"], item["path"]))
            elif item["type"] == "http":
                base_url = item["base_url"].format(host=host, port=port)
                connectors.append(HttpConnector(item["id"], base_url))
            else:
                raise ValueError(f"unknown source type: {item['type']}")
        return connectors

    app = FastAPI(title="incremental-source-ingestion")

    @app.get("/")
    def root():
        return {"service": "incremental-source-ingestion", "data_dir": data_dir}

    @app.get("/mock/{source_id}/records")
    def mock_source(source_id: str, after: int = 0, limit: int = 100):
        if source_id not in mock_records:
            raise HTTPException(status_code=503, detail="source unavailable")
        page = [r for r in mock_records[source_id] if r["seq"] >= after][:limit]
        return {"records": page}

    @app.post("/sink/deliver")
    def deliver(body: DeliverBody):
        with sink_lock:
            record = UnifiedRecord(
                source_id=body.source_id,
                record_id=body.record_id,
                seq=body.seq,
                payload=body.payload,
            )
            existed = not local_sink.write(record)
        return {"duplicate": existed}

    @app.get("/sink/records")
    def sink_records():
        rows = []
        if os.path.exists(local_sink.path):
            with open(local_sink.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        return {"count": len(rows), "records": rows}

    @app.post("/ingest/run")
    def run_ingestion(max_workers: int | None = None):
        workers = max_workers or int(config.get("max_workers", 4))
        pipeline = Pipeline(cursors, build_sink(), max_workers=workers)
        results = pipeline.run_all(build_connectors())
        return {
            "results": [
                {
                    "source_id": r.source_id,
                    "status": r.status,
                    "cursor_before": r.cursor_before,
                    "cursor_after": r.cursor_after,
                    "failure": r.failure,
                    "outcomes": [vars(o) for o in r.outcomes],
                }
                for r in results
            ]
        }

    @app.get("/cursors")
    def get_cursors():
        cursors_path = os.path.join(data_dir, "cursors.json")
        if not os.path.exists(cursors_path):
            return {"cursors": {}}
        with open(cursors_path, "r", encoding="utf-8") as f:
            return {"cursors": json.load(f)}

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(app, host=LISTEN_HOST, port=LISTEN_PORT)
