"""并发：多源并发、局部失败隔离、同源并发触发一致不重复、并发度可配置。"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from ingestion.cursor_store import CursorStore
from ingestion.pipeline import Pipeline
from ingestion.models import RawRecord
from ingestion.sink import LocalSink
from tests.stubs import InMemoryConnector, MaskedErrorConnector


def _new_pipeline(tmp_path, workers=4):
    cursors = CursorStore(str(tmp_path / "cursors.json"))
    sink = LocalSink(str(tmp_path / "sink.jsonl"))
    return cursors, sink, Pipeline(cursors, sink, max_workers=workers)


def test_concurrent_sources_all_commit(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    connectors = [
        InMemoryConnector(f"s{i}", [{"id": f"s{i}-r{j}", "payload": "x"} for j in range(3)])
        for i in range(4)
    ]
    results = pipeline.run_all(connectors)
    assert sorted(r.source_id for r in results) == [f"s{i}" for i in range(4)]
    assert all(r.status == "ok" and r.cursor_after == 3 for r in results)
    assert sink.count() == 12
    for r in results:
        print(f"{r.source_id}: {r.cursor_before}->{r.cursor_after}, "
              f"{[ (o.seq, o.status) for o in r.outcomes]}")
    # 位点互不干扰
    assert {s: cursors.load(s) for s in [f"s{i}" for i in range(4)]} == {
        f"s{i}": 3 for i in range(4)
    }


def test_concurrent_partial_failure_isolation(tmp_path):
    """坏源（数据源错误 + 畸形记录）与好源并发，好源进度必须正常提交。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    good1 = InMemoryConnector("good1", [{"id": f"g-{i}", "payload": "p"} for i in range(4)])
    broken_fetch = MaskedErrorConnector("broken1", fail_on_fetch=True)
    bad_record = InMemoryConnector(
        "broken2",
        [{"id": "ok", "payload": "p"}, {"id": "no-payload"}],
    )
    good2 = InMemoryConnector("good2", [{"id": "h-0", "payload": "p"}])

    results = {r.source_id: r for r in pipeline.run_all(
        [good1, broken_fetch, bad_record, good2]
    )}
    for sid in ["good1", "broken1", "broken2", "good2"]:
        r = results[sid]
        print(f"{sid}: status={r.status} {r.cursor_before}->{r.cursor_after} "
              f"failure={r.failure}")

    assert results["good1"].status == "ok" and results["good1"].cursor_after == 4
    assert results["good2"].status == "ok" and results["good2"].cursor_after == 1
    assert results["broken1"].status == "failed"
    assert results["broken1"].failure["reason"] == "source_error"
    assert results["broken1"].cursor_after == 0
    assert results["broken2"].status == "failed"
    assert results["broken2"].failure["reason"] == "missing_field"
    assert results["broken2"].cursor_after == 1

    # 好源的记录完整落库，坏源只落了失败前的 1 条，不重不丢
    keys = sink.keys()
    assert keys == {f"good1:g-{i}" for i in range(4)} | {"good2:h-0", "broken2:ok"}
    assert sink.count() == 6
    # 坏源不影响好源位点
    assert cursors.load("good1") == 4 and cursors.load("good2") == 1
    assert cursors.load("broken1") == 0 and cursors.load("broken2") == 1


def test_same_source_concurrent_triggers_no_duplicate(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(6)]
    connectors = [InMemoryConnector("hot", rows) for _ in range(6)]

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(pipeline.run_source, connectors))

    ok_runs = [r for r in results if r.status == "ok"]
    delivered = sum(
        sum(1 for o in r.outcomes if o.status == "delivered") for r in ok_runs
    )
    duplicates = sum(
        sum(1 for o in r.outcomes if o.status == "duplicate") for r in ok_runs
    )
    print(f"runs={len(results)}, delivered={delivered}, duplicates={duplicates}, "
          f"sink={sink.count()}, cursor={cursors.load('hot')}")
    for r in results:
        print(f"run: {r.status} {r.cursor_before}->{r.cursor_after} "
              f"{[(o.seq, o.status) for o in r.outcomes]}")

    assert all(r.status == "ok" for r in results)
    assert cursors.load("hot") == 6
    # 恰好一次首投，其余全是幂等 duplicate，总数等于 6 个不同 key
    assert delivered == 6 and sink.count() == 6
    assert sink.keys() == {f"hot:r{i}" for i in range(6)}
    # 每次运行看到的位点变化是单调、连续的 0/6 两态之一
    assert all((r.cursor_before, r.cursor_after) in [(0, 6), (6, 6)] for r in results)


def test_concurrency_degree_configurable(tmp_path):
    """max_workers=1 时运行必然串行，可用并发执行标记验证。"""
    _, sink, serial = _new_pipeline(tmp_path, workers=1)

    running = threading.Event()
    overlap = threading.Event()

    class CheckConnector(InMemoryConnector):
        def fetch(self, after):
            if running.is_set():
                overlap.set()
            running.set()
            time.sleep(0.05)
            yield RawRecord(seq=after, data={"id": f"{self.source_id}-0", "payload": "p"})
            running.clear()

    connectors = [CheckConnector(f"s{i}") for i in range(3)]
    results = serial.run_all(connectors)
    assert all(r.status == "ok" for r in results)
    assert not overlap.is_set()
    print("max_workers=1 -> no overlap; sink count =", sink.count())


def test_raw_connector_exception_is_masked(tmp_path):
    """连接器直接抛出底层异常（模拟契约实现漏网），对外仍须脱敏为 source_error。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    r = pipeline.run_source(InMemoryConnector("leaky", fail_on_fetch=True))
    print("result:", r.status, r.failure)
    assert r.status == "failed"
    assert r.failure["reason"] == "source_error"
    assert "secret-db-host" not in json.dumps(r.failure)
    assert cursors.load("leaky") == 0
