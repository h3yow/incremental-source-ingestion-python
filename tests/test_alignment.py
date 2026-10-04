"""异常对齐：源端跨段/清库被识别为 source_moved，区间可见，确认前拒绝续采。"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from ingestion.connectors.file_source import FileConnector
from ingestion.connectors.http_source import HttpConnector
from ingestion.cursor_store import CursorStore
from ingestion.pipeline import Pipeline
from ingestion.sink import LocalSink
from tests.stubs import InMemoryConnector


def _new_pipeline(tmp_path, workers=2):
    cursors = CursorStore(str(tmp_path / "cursors.json"))
    sink = LocalSink(str(tmp_path / "sink.jsonl"))
    return cursors, sink, Pipeline(cursors, sink, max_workers=workers)


def _print_result(title, result):
    print(f"\n== {title}: source={result.source_id} status={result.status} "
          f"cursor {result.cursor_before}->{result.cursor_after} failure={result.failure}")
    for o in result.outcomes:
        print(f"   seq={o.seq} status={o.status} id={o.record_id}")


def test_source_moved_detected_with_gap_range(tmp_path):
    """源端换存储后 seq 从 100 重新开始：本地进度 5 对不上，判异常并给出区间。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    r1 = pipeline.run_source(InMemoryConnector("s1", rows))
    assert r1.status == "ok" and r1.cursor_after == 5
    assert sink.count() == 5

    # 源端被重建：同样的记录流，但序号空间从 100 开始
    moved = InMemoryConnector("s1", rows, start_seq=100)
    r2 = pipeline.run_source(moved)
    _print_result("source moved", r2)
    assert r2.status == "failed"
    assert r2.failure["reason"] == "source_moved"
    assert r2.failure["gap"] == {"from": 5, "to": 100}
    # 本地进度不被源端新起点覆盖，跳过区间也没有被采
    assert cursors.load("s1") == 5 and sink.count() == 5
    # 状态：不健康 + 待确认跳过区间
    st = pipeline.states.get("s1")
    print("state:", st.to_dict())
    assert st.status == "pending_resync" and not st.healthy
    assert st.pending_skip["from"] == 5 and st.pending_skip["to"] == 100


def test_file_source_detects_rebuild_gap(tmp_path):
    """文件源换存储：新文件行内显式 seq 从 50 起，本地进度 3 被判跨段。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "".join(json.dumps({"id": f"o{i}", "payload": f"p{i}"}) + "\n"
                for i in range(3)),
        encoding="utf-8",
    )
    r1 = pipeline.run_source(FileConnector("orders", str(path)))
    assert r1.status == "ok" and r1.cursor_after == 3

    # 源端重建：新存储的序号从 50 开始
    path.write_text(
        "".join(json.dumps({"seq": 50 + i, "id": f"n{i}", "payload": f"q{i}"}) + "\n"
                for i in range(3)),
        encoding="utf-8",
    )
    r2 = pipeline.run_source(FileConnector("orders", str(path)))
    _print_result("file source rebuilt", r2)
    assert r2.status == "failed"
    assert r2.failure["reason"] == "source_moved"
    assert r2.failure["gap"] == {"from": 3, "to": 50}
    assert cursors.load("orders") == 3 and sink.count() == 3


def test_file_source_detects_truncation(tmp_path):
    """文件被清库/截断：本地进度 5，文件只剩 2 行，无法对齐，判异常。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "".join(json.dumps({"id": f"o{i}", "payload": f"p{i}"}) + "\n"
                for i in range(5)),
        encoding="utf-8",
    )
    r1 = pipeline.run_source(FileConnector("orders", str(path)))
    assert r1.status == "ok" and r1.cursor_after == 5

    # 源端清库：文件被截断到 2 行
    path.write_text(
        "".join(json.dumps({"id": f"x{i}", "payload": f"w{i}"}) + "\n"
                for i in range(2)),
        encoding="utf-8",
    )
    r2 = pipeline.run_source(FileConnector("orders", str(path)))
    _print_result("file source truncated", r2)
    assert r2.status == "failed"
    assert r2.failure["reason"] == "source_moved"
    assert r2.failure["gap"] == {"from": 2, "to": 5}
    assert cursors.load("orders") == 5 and sink.count() == 5


@pytest.fixture
def moved_http_server():
    """回环 HTTP 源：数据被重建，seq 从 100 起，响应声明 min_seq。"""
    records = [
        {"seq": 100 + i, "recordId": f"e{100 + i}", "text": f"t{100 + i}"}
        for i in range(3)
    ]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            after = 0
            for kv in qs.split("&"):
                if kv.startswith("after="):
                    after = int(kv.split("=")[1])
            page = [r for r in records if r["seq"] >= after]
            body = json.dumps({
                "records": page,
                "min_seq": min(r["seq"] for r in records),
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_http_source_detects_min_seq_gap(tmp_path, moved_http_server):
    """回环接口形态：本地进度 5，源端 min_seq=100，判异常并给出区间。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    # 先把本地进度推到 5（源端重建前的正常采集）
    cursors.commit("h", 5, expected=0)

    r = pipeline.run_source(HttpConnector("h", moved_http_server))
    _print_result("http source moved", r)
    assert r.status == "failed"
    assert r.failure["reason"] == "source_moved"
    assert r.failure["gap"] == {"from": 5, "to": 100}
    assert cursors.load("h") == 5 and sink.count() == 0
    st = pipeline.states.get("h")
    assert st.status == "pending_resync"
    assert st.pending_skip["from"] == 5 and st.pending_skip["to"] == 100


def test_pending_resync_rejects_further_runs(tmp_path):
    """跨段判异常后，未确认前再次触发一律 resync_required 拒绝，不碰源端。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    pipeline.run_source(InMemoryConnector("s1", rows))
    pipeline.run_source(InMemoryConnector("s1", rows, start_seq=100))

    r = pipeline.run_source(InMemoryConnector("s1", rows, start_seq=100))
    _print_result("rejected while pending", r)
    assert r.status == "failed"
    assert r.failure["reason"] == "resync_required"
    assert r.failure["gap"] == {"from": 5, "to": 100}
    assert cursors.load("s1") == 5 and sink.count() == 5
