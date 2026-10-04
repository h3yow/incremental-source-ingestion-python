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
from ingestion.errors import IngestionError
from ingestion.models import SourceState, UnifiedRecord
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


class ResyncBody(BaseModel):
    new_cursor: int
    operator: str
    reason: str
    confirm: bool = False


class RebuildBody(BaseModel):
    start_seq: int
    count: int = 5


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

    # 管线单例：每源互斥锁跨请求共享，保证"采集与重新同步并发时结果确定"
    pipeline = Pipeline(
        cursors, build_sink(), max_workers=int(config.get("max_workers", 4))
    )

    app = FastAPI(title="incremental-source-ingestion")

    @app.get("/")
    def root():
        return {"service": "incremental-source-ingestion", "data_dir": data_dir}

    @app.get("/mock/{source_id}/records")
    def mock_source(source_id: str, after: int = 0, limit: int = 100):
        if source_id not in mock_records:
            raise HTTPException(status_code=503, detail="source unavailable")
        records = mock_records[source_id]
        page = [r for r in records if r["seq"] >= after][:limit]
        # min_seq：源端当前最早可提供的位置，供连接器做异常对齐检测
        min_seq = min((r["seq"] for r in records), default=None)
        return {"records": page, "min_seq": min_seq}

    @app.post("/mock/{source_id}/rebuild")
    def mock_rebuild(source_id: str, body: RebuildBody):
        """演示用：模拟源端被重建/换存储，数据从新的 seq 起点重新灌入。"""
        mock_records[source_id] = [
            {
                "seq": body.start_seq + i,
                "recordId": f"{source_id}-r{body.start_seq + i}",
                "text": f"rebuilt {body.start_seq + i}",
            }
            for i in range(body.count)
        ]
        return {
            "source_id": source_id,
            "min_seq": body.start_seq,
            "count": body.count,
        }

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
        results = pipeline.run_all(build_connectors(), max_workers=max_workers)
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

    @app.get("/sources/status")
    def sources_status():
        """按数据源查状态：当前进度、健康度、最近异常、待确认跳过区间。"""
        states = pipeline.states.list_all()
        source_ids = sorted(
            {item["id"] for item in config["sources"]} | set(states)
        )
        return {
            "sources": {
                sid: {
                    "cursor": cursors.load(sid),
                    **states.get(sid, SourceState(sid)).to_dict(),
                }
                for sid in source_ids
            }
        }

    @app.get("/sources/{source_id}/status")
    def source_status(source_id: str):
        return {
            "cursor": cursors.load(source_id),
            **pipeline.states.get(source_id).to_dict(),
        }

    @app.post("/sources/{source_id}/resync")
    def resync_source(source_id: str, body: ResyncBody):
        """受控重新同步：确认跳过区间永久丢失后，把该源进度推到新起点。"""
        try:
            return pipeline.resync(
                source_id,
                body.new_cursor,
                operator=body.operator,
                reason=body.reason,
                confirm=body.confirm,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        except IngestionError as exc:
            raise HTTPException(status_code=409, detail=exc.to_dict()) from None

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
