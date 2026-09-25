"""HTTP 数据源连接器测试（真实本地回环服务）。"""

from __future__ import annotations

from pathlib import Path

from app.checkpoint import CheckpointStore
from app.connectors.http_source import HttpSourceConnector
from app.engine import IngestionEngine
from app.errors import FailureCode
from app.target import FileTarget
from tests.conftest import jrow, write_jsonl


def _engine(tmp_path: Path, conn):
    cps = CheckpointStore(str(tmp_path / "cp.json"))
    tgt = FileTarget(str(tmp_path / "target.json"))
    return IngestionEngine({"hs": conn}, cps, tgt, verbose=True), cps, tgt


def test_http_source_pagination_and_resume(
    tmp_path: Path, http_server
) -> None:
    data = write_jsonl(
        tmp_path / "http.jsonl", [jrow("h1", x=1), jrow("h2", x=2),
                                  jrow("h3", x=3), jrow("h4", x=4),
                                  jrow("h5", x=5)]
    )
    base = http_server.start(data, page_size=2)
    eng, cps, tgt = _engine(tmp_path, HttpSourceConnector("hs", base))

    rep = eng.run_source("hs")
    print(f"\n首次 HTTP 采集: 位点 {rep.last_position_before}->{rep.last_position_after}")
    assert rep.ok and rep.delivered == 5
    assert [r["record_id"] for r in tgt.records("hs")] == ["h1", "h2", "h3", "h4", "h5"]

    # 重复触发：HTTP 源端无新数据
    rep2 = eng.run_source("hs")
    assert rep2.ok and rep2.delivered == 0


def test_http_source_500_wrapped_as_unavailable(
    tmp_path: Path, http_server
) -> None:
    data = write_jsonl(tmp_path / "http.jsonl", [jrow("h1")])
    base = http_server.start(data)
    conn = HttpSourceConnector("hs", base, force_fail=True)
    eng, cps, tgt = _engine(tmp_path, conn)
    rep = eng.run_source("hs")
    print(f"\nHTTP 500 场景: code={rep.fatal.code.value} cause={rep.fatal.cause_type}")
    assert rep.fatal.code is FailureCode.SOURCE_UNAVAILABLE
    assert cps.get("hs") == 0 and tgt.delivered_count("hs") == 0


def test_http_source_malformed_body_wrapped_as_protocol(
    tmp_path: Path, http_server
) -> None:
    # 页大小 5，坏行落在第一页内，整页返回非法 JSON
    data = write_jsonl(tmp_path / "http.jsonl", [jrow("h1"), "__HTTP_BAD__"])
    base = http_server.start(data, page_size=5)
    eng, cps, tgt = _engine(tmp_path, HttpSourceConnector("hs", base))
    rep = eng.run_source("hs")
    print(f"\nHTTP 协议错误场景: code={rep.fatal.code.value} cause={rep.fatal.cause_type}")
    assert rep.fatal.code is FailureCode.SOURCE_PROTOCOL
    assert rep.fatal.cause_type == "JSONDecodeError"
    assert cps.get("hs") == 0


def test_http_source_connection_refused(tmp_path: Path) -> None:
    eng, _cps, _tgt = _engine(
        tmp_path, HttpSourceConnector("hs", "http://127.0.0.1:1", timeout=0.5)
    )
    rep = eng.run_source("hs")
    assert rep.fatal.code is FailureCode.SOURCE_UNAVAILABLE
    # 不泄露 urllib 原始文案
    assert "refused" not in rep.fatal.message.lower()
