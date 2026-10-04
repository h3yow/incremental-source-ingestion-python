"""异常对齐 + 受控重新同步：跨段拒绝、显式确认后续采不重复、并发结果确定。"""

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from ingestion.connectors.file_source import FileConnector
from ingestion.connectors.http_source import HttpConnector
from ingestion.cursor_store import CursorStore
from ingestion.pipeline import Pipeline
from ingestion.sink import LocalSink
from ingestion.source_state import SourceStateStore
from tests.stubs import InMemoryConnector


def _new_pipeline(tmp_path, workers=4):
    cursors = CursorStore(str(tmp_path / "cursors.json"))
    sink = LocalSink(str(tmp_path / "sink.jsonl"))
    state = SourceStateStore(str(tmp_path / "source_state.json"))
    return cursors, sink, state, Pipeline(cursors, sink, max_workers=workers, state=state)


def _print_result(title, result):
    print(f"\n== {title}: source={result.source_id} status={result.status} "
          f"cursor {result.cursor_before}->{result.cursor_after} failure={result.failure}")
    for o in result.outcomes:
        print(f"   seq={o.seq} status={o.status} id={o.record_id}")


def _print_status(title, status):
    print(f"\n== {title}: source={status['source_id']} cursor={status['cursor']} "
          f"status={status['status']} pending_gap={status['pending_gap']}")
    print(f"   last_failure={status['last_failure']}")
    for a in status["resyncs"]:
        print(f"   resync: by={a['operator']} at={a['confirmed_at']} "
              f"skipped=[{a['skipped']['from']}, {a['skipped']['to']}) "
              f"cursor {a['cursor_before']}->{a['cursor_after']} "
              f"why={a['justification']}")


def test_source_gap_detected_blocked_and_visible(tmp_path):
    """源端跨过 [5,10) 未处理数据：判异常、停下、区间可见，确认前拒绝继续。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    r1 = pipeline.run_source(InMemoryConnector("s1", rows))
    assert r1.status == "ok" and r1.cursor_after == 5

    # 源端重建：旧数据清库，新数据从 seq=10 开始
    rebuilt = InMemoryConnector(
        "s1", [{"id": f"n{i}", "payload": f"q{i}"} for i in range(3)], start_seq=10
    )
    r2 = pipeline.run_source(rebuilt)
    _print_result("gap detected", r2)
    assert r2.status == "failed"
    assert r2.failure["reason"] == "source_gap"
    assert r2.failure["gap"] == {"from": 5, "to": 10, "resume_from": 10}
    assert r2.cursor_after == 5  # 位点不被推进，源端位置不被认下
    assert cursors.load("s1") == 5
    assert sink.count() == 5  # 跳过区间没有被当成不存在，也没有写入任何新记录

    st = pipeline.status("s1")
    _print_status("after gap", st)
    assert st["status"] == "pending_confirmation"
    assert st["pending_gap"]["from"] == 5 and st["pending_gap"]["to"] == 10
    assert st["last_failure"]["reason"] == "source_gap"

    # 确认之前：拒绝往下走，原因可区分
    r3 = pipeline.run_source(rebuilt)
    _print_result("blocked before confirmation", r3)
    assert r3.status == "failed"
    assert r3.failure["reason"] == "resync_required"
    assert r3.failure["gap"] == {"from": 5, "to": 10, "resume_from": 10}
    assert cursors.load("s1") == 5 and sink.count() == 5


def test_resync_rejected_without_pending_gap(tmp_path):
    """没有待确认区间时不允许重新同步。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    pipeline.run_source(InMemoryConnector("s1", [{"id": "r0", "payload": "p"}]))
    res = pipeline.resync("s1", operator="ops", justification="whatever")
    print("resync rejected:", res.failure)
    assert res.status == "rejected"
    assert res.failure["reason"] == "no_pending_gap"
    assert cursors.load("s1") == 1


def test_resync_rejected_without_operator_or_justification(tmp_path):
    """确认信息（操作人/依据）不完整时不允许重新同步。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    pipeline.run_source(InMemoryConnector("s1", [{"id": f"r{i}", "payload": "p"} for i in range(2)]))
    pipeline.run_source(InMemoryConnector("s1", [{"id": "n0", "payload": "q"}], start_seq=9))

    for operator, justification in [("", "ticket-123"), ("ops", "  "), ("  ", "")]:
        res = pipeline.resync("s1", operator=operator, justification=justification)
        print(f"resync rejected (operator={operator!r}):", res.failure)
        assert res.status == "rejected"
        assert res.failure["reason"] == "invalid_confirmation"
    # 拒绝不影响待确认状态，仍然阻断采集
    assert cursors.load("s1") == 2
    assert pipeline.status("s1")["status"] == "pending_confirmation"


def test_resync_confirmed_then_resume_no_dup_no_backfill(tmp_path):
    """显式确认后：位点推到新起点、留痕完整、续采不重复、跳过区间不补写。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    old_rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    pipeline.run_source(InMemoryConnector("s1", old_rows))
    assert sink.count() == 5

    rebuilt = InMemoryConnector(
        "s1", [{"id": f"n{i}", "payload": f"q{i}"} for i in range(3)], start_seq=10
    )
    pipeline.run_source(rebuilt)  # 检出 gap [5,10)

    res = pipeline.resync(
        "s1", operator="ops-chen", justification="INC-42: source rebuilt, [5,10) lost"
    )
    print("\n== resync:", res.status, res.gap)
    assert res.status == "ok"
    assert res.cursor_before == 5 and res.cursor_after == 10
    assert res.audit["operator"] == "ops-chen"
    assert res.audit["skipped"] == {"from": 5, "to": 10}
    assert res.audit["justification"].startswith("INC-42")
    assert res.audit["confirmed_at"]
    assert cursors.load("s1") == 10

    st = pipeline.status("s1")
    _print_status("after resync", st)
    assert st["status"] == "healthy"
    assert st["pending_gap"] is None
    assert len(st["resyncs"]) == 1

    # 重新同步后再采集：从新起点继续，只采 10,11,12
    r = pipeline.run_source(rebuilt)
    _print_result("resume after resync", r)
    assert r.status == "ok"
    assert r.cursor_before == 10 and r.cursor_after == 13
    assert [(o.seq, o.status) for o in r.outcomes] == [
        (10, "delivered"), (11, "delivered"), (12, "delivered"),
    ]
    # 之前投递过的 5 条没有重写；跳过区间 [5,10) 没有被悄悄补上
    assert sink.count() == 8
    assert sink.keys() == {f"s1:r{i}" for i in range(5)} | {f"s1:n{i}" for i in range(3)}

    # 再触发一次：无新数据，全部幂等
    r2 = pipeline.run_source(rebuilt)
    assert r2.status == "ok" and r2.outcomes == []
    assert sink.count() == 8


def test_file_source_rebuild_detected_and_resynced(tmp_path):
    """文件源被重建变短：检出对齐异常，确认后位点受控回退到文件末尾。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    path = tmp_path / "orders.jsonl"
    path.write_text(
        "".join(json.dumps({"id": f"ord-{i}", "payload": f"p{i}"}) + "\n" for i in range(5)),
        encoding="utf-8",
    )
    r1 = pipeline.run_source(FileConnector("orders", str(path)))
    assert r1.status == "ok" and r1.cursor_after == 5

    # 源端重建：文件被替换为只有 2 行的新文件
    path.write_text(
        "".join(json.dumps({"id": f"new-{i}", "payload": f"x{i}"}) + "\n" for i in range(2)),
        encoding="utf-8",
    )
    r2 = pipeline.run_source(FileConnector("orders", str(path)))
    _print_result("file rebuilt shorter", r2)
    assert r2.status == "failed"
    assert r2.failure["reason"] == "source_gap"
    assert r2.failure["gap"] == {"from": 0, "to": 2, "resume_from": 2}
    assert cursors.load("orders") == 5

    res = pipeline.resync("orders", operator="ops-li", justification="file store replaced")
    print("resync:", res.status, res.audit)
    assert res.status == "ok"
    assert res.cursor_before == 5 and res.cursor_after == 2  # 受控回退
    assert cursors.load("orders") == 2

    # 新文件追加一行后，从 seq=2 续采；新文件前两行被确认跳过、不补写
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps({"id": "new-2", "payload": "x2"}) + "\n")
    r3 = pipeline.run_source(FileConnector("orders", str(path)))
    _print_result("resume on rebuilt file", r3)
    assert r3.status == "ok"
    assert [(o.seq, o.record_id) for o in r3.outcomes] == [(2, "new-2")]
    assert sink.count() == 6  # 5 旧 + 1 新，被跳过的新文件前 2 行不补写
    assert "orders:new-0" not in sink.keys() and "orders:new-1" not in sink.keys()


@pytest.fixture
def rebuilt_http_server():
    """模拟清库后从 seq=10 继续的 HTTP 源。"""
    records = [{"seq": 10 + i, "recordId": f"e{10 + i}", "text": f"t{10 + i}"} for i in range(3)]

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


def test_http_source_gap_detected_and_resynced(tmp_path, rebuilt_http_server):
    """回环 HTTP 源：本地进度 5，源端最早只能提供 10 -> 检出跨段并受控同步。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    cursors.commit("h", 5, expected=0)  # 本地已确认进度到 5

    conn = HttpConnector("h", rebuilt_http_server)
    r1 = pipeline.run_source(conn)
    _print_result("http gap detected", r1)
    assert r1.status == "failed"
    assert r1.failure["reason"] == "source_gap"
    assert r1.failure["gap"] == {"from": 5, "to": 10, "resume_from": 10}
    assert cursors.load("h") == 5

    res = pipeline.resync("h", operator="ops-wang", justification="source DB rebuilt")
    assert res.status == "ok" and res.cursor_after == 10

    r2 = pipeline.run_source(conn)
    _print_result("http resume after resync", r2)
    assert r2.status == "ok" and r2.cursor_after == 13
    assert [o.record_id for o in r2.outcomes] == ["e10", "e11", "e12"]
    assert sink.count() == 3


def test_concurrent_resync_and_run_deterministic(tmp_path):
    """重新同步与正常采集并发：结果确定——恰一次同步生效，位点不乱、不重复。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    old_rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    pipeline.run_source(InMemoryConnector("hot", old_rows))
    rebuilt = InMemoryConnector(
        "hot", [{"id": f"n{i}", "payload": f"q{i}"} for i in range(3)], start_seq=10
    )
    pipeline.run_source(rebuilt)  # 检出 gap [5,10)，进入待确认

    def do_run(_):
        return pipeline.run_source(rebuilt)

    def do_resync(i):
        return pipeline.resync("hot", operator=f"ops-{i}", justification="INC-42")

    with ThreadPoolExecutor(max_workers=12) as pool:
        run_results = list(pool.map(do_run, range(8)))
        resync_results = list(pool.map(do_resync, range(4)))

    ok_resyncs = [r for r in resync_results if r.status == "ok"]
    rejected = [r for r in resync_results if r.status == "rejected"]
    print(f"\n== resyncs: ok={len(ok_resyncs)} rejected={len(rejected)}")
    for r in run_results:
        _print_result("concurrent run", r)

    # 恰一次重新同步生效，其余因待确认区间已消费而被拒绝
    assert len(ok_resyncs) == 1
    assert all(r.failure["reason"] == "no_pending_gap" for r in rejected)
    assert len(pipeline.status("hot")["resyncs"]) == 1

    # 并发期间的采集：要么被阻断（同步前），要么从新起点继续（同步后）
    assert all(
        r.status == "failed" and r.failure["reason"] == "resync_required"
        or r.status == "ok" and r.cursor_before == 10
        for r in run_results
    )

    # 收尾再跑一次，状态完全确定：位点 13，5 旧 + 3 新，无重复
    final = pipeline.run_source(rebuilt)
    _print_result("settling run", final)
    assert final.status == "ok"
    assert cursors.load("hot") == 13
    assert sink.count() == 8
    assert sink.keys() == {f"hot:r{i}" for i in range(5)} | {f"hot:n{i}" for i in range(3)}


def test_anomaly_source_does_not_affect_others(tmp_path):
    """某源处于待确认状态时，其它源照常采集、位点照常提交。"""
    cursors, sink, state, pipeline = _new_pipeline(tmp_path)
    pipeline.run_source(InMemoryConnector("bad", [{"id": f"b{i}", "payload": "p"} for i in range(3)]))
    pipeline.run_source(InMemoryConnector("ok1", [{"id": f"g{i}", "payload": "p"} for i in range(2)]))

    # bad 源重建跨段；ok1 源正常追加
    results = {r.source_id: r for r in pipeline.run_all([
        InMemoryConnector("bad", [{"id": "bn0", "payload": "q"}], start_seq=9),
        InMemoryConnector("ok1", [{"id": f"g{i}", "payload": "p"} for i in range(3)]),
    ])}
    for r in results.values():
        _print_result("mixed run", r)
    assert results["bad"].status == "failed"
    assert results["bad"].failure["reason"] == "source_gap"
    assert results["ok1"].status == "ok" and results["ok1"].cursor_after == 3
    assert cursors.load("bad") == 3 and cursors.load("ok1") == 3
    assert sink.count() == 3 + 3  # bad 源 3 条旧记录 + ok1 源 3 条

    # bad 源确认后恢复，两源都健康
    pipeline.resync("bad", operator="ops", justification="confirmed lost")
    assert pipeline.status("bad")["status"] == "healthy"
    assert pipeline.status("ok1")["status"] == "healthy"
