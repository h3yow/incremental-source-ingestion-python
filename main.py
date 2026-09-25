"""本地演示入口。

启动两个本地回环服务：
* 模拟 HTTP 数据源: http://127.0.0.1:18081
* 采集控制 API:     http://127.0.0.1:18080

用法：
    uv run python main.py            # 重置演示数据并启动
    uv run python main.py --keep     # 保留既有数据/位点，便于验证续采
"""

from __future__ import annotations

import sys
import threading

import uvicorn

from app.api import create_api_app
from app.config import build_engine, build_paths, seed_demo_data
from app.http_source_app import create_http_source_app

HTTP_SOURCE_PORT = 18081
API_PORT = 18080


def serve_in_thread(app, port: int, name: str) -> threading.Thread:
    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=port,
        log_level="warning",
        timeout_graceful_shutdown=1,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name=name, daemon=True)
    thread.start()
    return thread


def main() -> None:
    keep = "--keep" in sys.argv
    paths = build_paths()
    seed_demo_data(paths, reset=not keep)

    http_app = create_http_source_app(str(paths.http_source), page_size=2)
    serve_in_thread(http_app, HTTP_SOURCE_PORT, "http-source")

    engine = build_engine(
        paths, http_base_url=f"http://127.0.0.1:{HTTP_SOURCE_PORT}"
    )
    api_app = create_api_app(engine)

    print(f"模拟 HTTP 数据源: http://127.0.0.1:{HTTP_SOURCE_PORT}/events")
    print(f"采集控制 API:     http://127.0.0.1:{API_PORT}")
    print("触发全量并发采集: curl -X POST http://127.0.0.1:18080/run")
    uvicorn.run(api_app, host="127.0.0.1", port=API_PORT, log_level="info")


if __name__ == "__main__":
    main()
