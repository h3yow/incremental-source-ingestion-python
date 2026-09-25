"""连接器契约：增量读取、归一化、数据源报错脱敏（不原样透出底层异常）。"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from ingestion.connectors.file_source import FileConnector
from ingestion.connectors.http_source import HttpConnector
from ingestion.errors import SourceError


def test_file_connector_incremental(tmp_path):
    path = tmp_path / "data.jsonl"
    path.write_text(
        json.dumps({"id": "a", "payload": "x"}) + "\n"
        + json.dumps({"id": "b", "payload": "y"}) + "\n",
        encoding="utf-8",
    )
    conn = FileConnector("f", str(path))
    first = list(conn.fetch(0))
    assert [r.seq for r in first] == [0, 1]
    assert conn.normalize(first[1]) == {"id": "b", "payload": "y"}
    # 从位点续采
    more = list(conn.fetch(2))
    assert more == []


def test_file_connector_missing_file_masked(tmp_path):
    conn = FileConnector("f", str(tmp_path / "nope.jsonl"))
    with pytest.raises(SourceError) as exc:
        list(conn.fetch(0))
    d = exc.value.to_dict()
    print("failure:", d)
    assert d["reason"] == "source_error"
    # 不把本地路径/底层异常透出
    assert "nope.jsonl" not in d["message"]
    assert exc.value.__cause__ is None


def test_file_connector_corrupt_line_masked(tmp_path):
    path = tmp_path / "data.jsonl"
    path.write_text('{"id": "a"}\n{not-json}\n', encoding="utf-8")
    conn = FileConnector("f", str(path))
    with pytest.raises(SourceError) as exc:
        list(conn.fetch(0))
    d = exc.value.to_dict()
    print("failure:", d)
    assert d["reason"] == "source_error"
    assert "seq=1" in d["message"]
    assert exc.value.__cause__ is None


@pytest.fixture
def mock_http_server():
    records = [{"seq": i, "recordId": f"e{i}", "text": f"t{i}"} for i in range(3)]

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if not self.path.startswith("/records"):
                self.send_response(404)
                self.end_headers()
                return
            qs = self.path.split("?", 1)[1] if "?" in self.path else ""
            after = 0
            for kv in qs.split("&"):
                if kv.startswith("after="):
                    after = int(kv.split("=")[1])
            page = [r for r in records if r["seq"] >= after]
            body = json.dumps({"records": page}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def test_http_connector_incremental_and_normalize(mock_http_server):
    conn = HttpConnector("h", mock_http_server)
    first = list(conn.fetch(0))
    assert [r.seq for r in first] == [0, 1, 2]
    assert conn.normalize(first[0]) == {"id": "e0", "payload": "t0"}
    assert list(conn.fetch(3)) == []


def test_http_connector_connection_error_masked():
    conn = HttpConnector("h", "http://127.0.0.1:1", timeout=0.5)
    with pytest.raises(SourceError) as exc:
        list(conn.fetch(0))
    d = exc.value.to_dict()
    print("failure:", d)
    assert d["reason"] == "source_error"
    assert d["source_id"] == "h"
    # 不透出底层异常（errno/堆栈不挂在 __cause__ 上）
    assert exc.value.__cause__ is None
