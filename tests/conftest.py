"""测试夹具：临时数据目录 + 真实本地回环 HTTP 数据源。"""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import uvicorn

from app.http_source_app import create_http_source_app


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture()
def tmp_workspace(tmp_path: Path) -> Path:
    return tmp_path


def write_jsonl(path: Path, rows: list[str]) -> Path:
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def jrow(record_id: str | int | None = None, **data) -> str:
    return json.dumps({"id": record_id, "data": data}, ensure_ascii=False)


@pytest.fixture()
def http_server() -> Iterator:
    """启动一个可更换数据文件的回环 HTTP 源，返回控制句柄。"""
    started: dict[str, object] = {}

    def _start(data_path: Path, *, page_size: int = 2) -> str:
        port = _free_port()
        app = create_http_source_app(str(data_path), page_size=page_size)
        config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
        server = uvicorn.Server(config)
        thread = threading.Thread(target=server.run, daemon=True)
        thread.start()
        # 等待服务就绪
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        base_url = f"http://127.0.0.1:{port}"
        started["base_url"] = base_url
        started["server"] = server
        return base_url

    handle = type("Handle", (), {"start": staticmethod(_start)})()
    yield handle

    server = started.get("server")
    if isinstance(server, uvicorn.Server):
        server.should_exit = True
