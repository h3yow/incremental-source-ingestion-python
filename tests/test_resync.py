"""受控重新同步：显式确认、审计留痕、新起点续采不重复、并发确定、故障隔离。"""

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from ingestion.cursor_store import CursorStore
from ingestion.errors import CursorRegressionError
from ingestion.pipeline import Pipeline
from ingestion.sink import LocalSink
from tests.stubs import InMemoryConnector


def _new_pipeline(tmp_path, workers=4):
    cursors = CursorStore(str(tmp_path / "cursors.json"))
    sink = LocalSink(str(tmp_path / "sink.jsonl"))
    return cursors, sink, Pipeline(cursors, sink, max_workers=workers)


def _print_result(title, result):
    print(f"\n== {title}: source={result.source_id} status={result.status} "
          f"cursor {result.cursor_before}->{result.cursor_after} failure={result.failure}")
    for o in result.outcomes:
        print(f"   seq={o.seq} status={o.status} id={o.record_id}")


def _moved_source(tmp_path):
    """构造一个已判 source_moved 的源：本地进度 5，源端新起点 100。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    pipeline.run_source(InMemoryConnector("s1", rows))
    new_rows = [{"id": f"n{i}", "payload": f"q{i}"} for i in range(3)]
    pipeline.run_source(InMemoryConnector("s1", new_rows, start_seq=100))
    assert pipeline.states.get("s1").status == "pending_resync"
    return cursors, sink, pipeline, new_rows


def test_resync_requires_explicit_confirmation(tmp_path):
    cursors, sink, pipeline, _ = _moved_source(tmp_path)
    # 未显式确认 / 缺审计信息：都拒绝，位点不动
    with pytest.raises(ValueError):
        pipeline.resync("s1", 100, operator="op", reason="lost", confirm=False)
    with pytest.raises(ValueError):
        pipeline.resync("s1", 100, operator="", reason="", confirm=True)
    # 回退或原地不动也拒绝
    with pytest.raises(CursorRegressionError):
        pipeline.resync("s1", 3, operator="op", reason="lost", confirm=True)
    assert cursors.load("s1") == 5
    assert pipeline.states.get("s1").status == "pending_resync"
    print("unconfirmed resync rejected, cursor stays 5")


def test_resync_then_resume_no_duplicate_no_replay_of_skipped(tmp_path):
    cursors, sink, pipeline, new_rows = _moved_source(tmp_path)

    out = pipeline.resync(
        "s1", 100, operator="op-zhang",
        reason="vendor confirmed seq 5-99 permanently lost", confirm=True,
    )
    print("resync:", out)
    assert out["cursor_before"] == 5 and out["cursor_after"] == 100
    assert out["skipped"] == {"from": 5, "to": 100}
    assert cursors.load("s1") == 100

    # 审计留痕：谁、何时、跳过哪段、依据
    st = pipeline.states.get("s1")
    assert st.healthy and st.pending_skip is None
    audit = st.resync_history[-1]
    print("audit:", audit)
    assert audit["operator"] == "op-zhang"
    assert audit["skipped_from"] == 5 and audit["skipped_to"] == 100
    assert "permanently lost" in audit["reason"] and audit["at"]

    # 从新起点续采：跳过区间 [5,100) 不会被补采，旧记录不重写
    r = pipeline.run_source(InMemoryConnector("s1", new_rows, start_seq=100))
    _print_result("resume after resync", r)
    assert r.status == "ok" and r.cursor_before == 100 and r.cursor_after == 103
    assert [o.record_id for o in r.outcomes] == ["n0", "n1", "n2"]
    assert all(o.status == "delivered" for o in r.outcomes)
    assert sink.count() == 5 + 3  # 旧 5 条 + 新 3 条，无重复
    assert sink.keys() == {f"s1:r{i}" for i in range(5)} | {f"s1:n{i}" for i in range(3)}

    # 源端如果把旧记录重放一遍（同 record_id），sink 幂等挡掉，不产生重复行
    old_rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(5)]
    r2 = pipeline.run_source(InMemoryConnector("s1", old_rows, start_seq=103))
    _print_result("replay of old ids", r2)
    assert all(o.status == "duplicate" for o in r2.outcomes)
    assert sink.count() == 8


def test_concurrent_run_and_resync_deterministic(tmp_path):
    """并发确定性（异常分支）：resync 与采集撞车时，resync 抛回退也算确定结果。"""
    cursors, sink, pipeline, new_rows = _moved_source(tmp_path)
    pipeline.resync("s1", 100, operator="op", reason="lost", confirm=True)
    big_rows = [{"id": f"n{i}", "payload": f"q{i}"} for i in range(50)]

    outcomes = []

    def do_run():
        outcomes.append(("run", pipeline.run_source(
            InMemoryConnector("s1", big_rows, start_seq=100))))

    def do_resync():
        try:
            outcomes.append(("resync", pipeline.resync(
                "s1", 120, operator="op2", reason="confirmed", confirm=True)))
        except CursorRegressionError as exc:
            outcomes.append(("resync_rejected", exc))

    with ThreadPoolExecutor(max_workers=2) as pool:
        f1 = pool.submit(do_run)
        f2 = pool.submit(do_resync)
        f1.result(); f2.result()

    kinds = sorted(k for k, _ in outcomes)
    print("outcome kinds:", kinds)
    run = next(v for k, v in outcomes if k == "run")
    _print_result("race run", run)
    # 确定态一：resync 先生效（run 从 120 起跑）；确定态二：run 先跑完，resync 回退被拒
    assert kinds in (["resync", "run"], ["resync_rejected", "run"])
    if "resync" in kinds:
        assert run.cursor_before == 120 and run.cursor_after == 150
        assert sink.count() == 5 + 30
    else:
        assert run.cursor_after == 150
        assert sink.count() == 5 + 50
    assert cursors.load("s1") == 150
    assert len(sink.keys()) == sink.count()


def test_unhealthy_source_does_not_block_others(tmp_path):
    """s1 处于待确认状态，其他源照常采集、提交进度，并发度不受影响。"""
    cursors, sink, pipeline = _new_pipeline(tmp_path)
    rows = [{"id": f"r{i}", "payload": f"p{i}"} for i in range(3)]
    pipeline.run_source(InMemoryConnector("s1", rows))
    pipeline.run_source(InMemoryConnector("s1", rows, start_seq=100))
    assert pipeline.states.get("s1").status == "pending_resync"

    good = InMemoryConnector("s2", [{"id": f"g{i}", "payload": "p"} for i in range(4)])
    results = {r.source_id: r for r in pipeline.run_all(
        [InMemoryConnector("s1", rows, start_seq=100), good])}
    for r in results.values():
        _print_result("mixed run", r)
    assert results["s1"].status == "failed"
    assert results["s1"].failure["reason"] == "resync_required"
    assert results["s2"].status == "ok" and results["s2"].cursor_after == 4
    assert cursors.load("s1") == 3 and cursors.load("s2") == 4
    assert sink.count() == 3 + 4
