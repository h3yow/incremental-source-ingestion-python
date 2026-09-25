"""本地端到端演示（无需手工起服务）。

场景序列：
1. 文件源 + HTTP 源首次并发采集
2. 重复触发（无新数据，无重复）
3. 追加数据后续采
4. 制造畸形记录 -> 可区分失败，前缀进度保留
5. 同一源并发触发 -> busy 拒绝
6. 模拟位点回退重放 -> 幂等不重复

运行：uv run python demo.py
"""

from __future__ import annotations

import json
import threading
import time

from app.config import build_engine, build_paths, seed_demo_data
from app.target import FileTarget
from app.http_source_app import create_http_source_app

import socket
import uvicorn


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _start_http_source(data_path: str) -> str:
    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(
            create_http_source_app(data_path, page_size=2),
            host="127.0.0.1",
            port=port,
            log_level="error",
        )
    )
    threading.Thread(target=server.run, daemon=True).start()
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            time.sleep(0.05)
    return f"http://127.0.0.1:{port}"


def main() -> None:
    paths = build_paths(".runtime/demo")
    seed_demo_data(paths, reset=True)
    http_url = _start_http_source(str(paths.http_source))
    engine = build_engine(paths, http_base_url=http_url, max_workers=4)

    print("\n##### 1) 首次并发采集全部数据源 #####")
    reports = engine.run_all()
    assert all(r.ok for r in reports.values())

    print("\n##### 2) 重复触发（幂等，无新数据） #####")
    reports = engine.run_all()
    assert all(r.delivered == 0 for r in reports.values())

    print("\n##### 3) 追加数据后续采 #####")
    with open(paths.file_source, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"id": "f-004", "data": {"name": "delta"}}) + "\n")
    with open(paths.http_source, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"id": "h-004", "data": {"city": "sz"}}) + "\n")
    reports = engine.run_all()
    assert reports["file_source"].delivered == 1
    assert reports["http_source"].delivered == 1

    print("\n##### 4) 制造畸形记录（缺失必填字段） #####")
    with open(paths.file_source, "a", encoding="utf-8") as fh:
        fh.write(json.dumps({"data": {"oops": True}}) + "\n")
        fh.write(json.dumps({"id": "f-006"}) + "\n")
    report = engine.run_source("file_source")
    assert report.fatal is not None and report.fatal.code.value == "missing_field"
    assert report.last_position_after == 4  # 畸形行之前的进度保留

    print("\n##### 5) 同一数据源并发触发 #####")
    out: list = []

    def trigger() -> None:
        out.append(engine.run_source("http_source"))

    threads = [threading.Thread(target=trigger) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    busy = sum(1 for r in out if r.fatal and r.fatal.code.value == "source_busy")
    print(f"3 次并发触发中 {busy} 次被 busy 拒绝")
    assert busy == 2

    print("\n##### 6) 模拟位点回退重放 #####")
    target = FileTarget(str(paths.target))
    before = target.delivered_count("http_source")
    data = json.loads(paths.checkpoints.read_text())
    data["http_source"] = 3  # 从 4 回退到 3
    paths.checkpoints.write_text(json.dumps(data))
    report = engine.run_source("http_source")
    after = target.delivered_count("http_source")
    print(f"重放 h-004: 幂等跳过={report.skipped_replayed}, 目标端数量 {before} -> {after}")
    assert report.skipped_replayed == 1 and before == after

    print("\n最终目标端记录数:", {
        name: target.delivered_count(name) for name in engine.connectors
    })
    print("\n演示完成：未发生重复或丢失，所有失败均可解释。")


if __name__ == "__main__":
    main()
