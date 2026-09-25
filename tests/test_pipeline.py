"""管线：归一化、畸形记录可区分失败、重放不重不丢、位点回退拒绝、中断续采。"""

import json

from ingestion.cursor_store import CursorStore
from ingestion.pipeline import Pipeline
from ingestion.sink import LocalSink
from tests.stubs import InMemoryConnector, ReplayFromZeroConnector


def _new_pipeline(tmp_path):
    cursors = CursorStore(str(tmp_path / "cursors.json"))
    sink = LocalSink(str(tmp_path / "sink.jsonl"))
    return cursors, sink, Pipeline(cursors, sink, max_workers=2)


def _print_result(title, result):
    print(f"\n== {title}: source={result.source_id} status={result.status} "
          f"cursor {result.cursor_before}->{result.cursor_after} failure={result.failure}")
    for o in result.outcomes:
        print(f"   seq={o.seq} status={o.status} id={o.record_id} failure={o.failure}")


def test_happy_path_incremental_and_resume(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(3)]
    conn = InMemoryConnector("s1", rows)

    r1 = pipeline.run_source(conn)
    _print_result("first run", r1)
    assert r1.status == "ok" and r1.cursor_after == 3
    assert sink.count() == 3

    # 模拟中断重启：全新 Pipeline 对象，位点从磁盘恢复，无新记录不重复投递
    pipeline2 = Pipeline(CursorStore(str(tmp_path / "cursors.json")),
                         LocalSink(str(tmp_path / "sink.jsonl")))
    r2 = pipeline2.run_source(InMemoryConnector("s1", rows))
    _print_result("after restart (no new data)", r2)
    assert r2.status == "ok" and r2.cursor_before == 3 and r2.outcomes == []
    assert sink.count() == 3

    # 源端追加新记录，只采新的
    rows.append({"id": "r3", "payload": "p3"})
    r3 = pipeline2.run_source(InMemoryConnector("s1", rows))
    _print_result("new record appended", r3)
    assert r3.cursor_after == 4
    assert [o.record_id for o in r3.outcomes] == ["r3"]
    assert sink.count() == 4


def test_replay_is_idempotent_no_dup_no_loss(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(4)]

    pipeline.run_source(InMemoryConnector("s1", rows))
    # 人为把位点回退到 2 后重放：源重新给 seq 2,3，sink 幂等标记 duplicate，
    # 位点以 compare-and-swap 重提（expected=2），最终不重不丢
    cursors.commit("s1", 2, expected=4) if False else None
    with open(tmp_path / "cursors.json", "w", encoding="utf-8") as f:
        json.dump({"s1": 2}, f)

    r = pipeline.run_source(InMemoryConnector("s1", rows))
    _print_result("replay after cursor rollback to 2", r)
    assert r.status == "ok"
    assert [(o.seq, o.status) for o in r.outcomes] == [(2, "duplicate"), (3, "duplicate")]
    assert r.cursor_after == 4
    assert sink.count() == 4
    assert sink.keys() == {f"s1:r{i}" for i in range(4)}


def test_missing_field_is_distinguishable_and_stops_cursor(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [
        {"id": "ok-0", "payload": "a"},
        {"id": "missing-payload"},          # seq=1 缺 payload
        {"id": "ok-2", "payload": "c"},
    ]
    r = pipeline.run_source(InMemoryConnector("s1", rows))
    _print_result("missing field", r)
    assert r.status == "failed"
    assert r.failure["reason"] == "missing_field"
    assert "payload" in r.failure["message"]
    assert r.cursor_after == 1  # 失败记录之前已提交，之后不推进
    assert sink.count() == 1
    assert cursors.load("s1") == 1


def test_type_mismatch_is_distinguishable(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [
        {"id": "ok-0", "payload": "a"},
        {"id": 123, "payload": "b"},        # seq=1 id 类型错误
    ]
    r = pipeline.run_source(InMemoryConnector("s1", rows))
    _print_result("type mismatch", r)
    assert r.status == "failed"
    assert r.failure["reason"] == "type_mismatch"
    assert "expected str" in r.failure["message"]
    assert r.cursor_after == 1


def test_fix_bad_record_then_resume_no_dup_no_loss(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    bad_rows = [
        {"id": "ok-0", "payload": "a"},
        {"id": "ok-1"},                      # 畸形
    ]
    r1 = pipeline.run_source(InMemoryConnector("s1", bad_rows))
    assert r1.status == "failed" and r1.failure["reason"] == "missing_field"

    # 修复畸形数据并追加一条，从位点 1 续采：旧记录不重复
    fixed_rows = [
        {"id": "ok-0", "payload": "a"},
        {"id": "ok-1", "payload": "fixed"},
        {"id": "ok-2", "payload": "c"},
    ]
    r2 = pipeline.run_source(InMemoryConnector("s1", fixed_rows))
    _print_result("resume after fix", r2)
    assert r2.status == "ok" and r2.cursor_after == 3
    assert [o.record_id for o in r2.outcomes] == ["ok-1", "ok-2"]
    assert sink.count() == 3
    assert sink.keys() == {"s1:ok-0", "s1:ok-1", "s1:ok-2"}


def test_source_cursor_regression_rejected(tmp_path):
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(3)]
    pipeline.run_source(InMemoryConnector("s1", rows))
    assert cursors.load("s1") == 3

    # 数据源异常地从更早位置返回旧序号，且已提交位点更高 -> 回退必须被拒绝
    r = pipeline.run_source(ReplayFromZeroConnector("s1", rows))
    _print_result("source returns stale seqs", r)
    assert r.status == "failed"
    assert r.failure["reason"] == "cursor_regression"
    assert cursors.load("s1") == 3  # 拒绝推进
