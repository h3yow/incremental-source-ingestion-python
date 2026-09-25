"""增量采集主流程测试。

覆盖：正常续采、重放幂等、位点回退/缺口拒绝、畸形记录、
多源并发局部失败、同源并发触发、底层异常不透出。

每个测试均打印位点变化与记录处理流水（见 RunReport 输出）。
"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from app.checkpoint import CheckpointStore
from app.connectors.file_source import FileSourceConnector
from app.connectors.http_source import HttpSourceConnector
from app.engine import IngestionEngine
from app.errors import FailureCode, IngestionError
from app.models import SourceEvent
from app.target import FileTarget
from tests.conftest import jrow, write_jsonl


def _print_reports(title: str, reports) -> None:
    print(f"\n===== {title} =====")
    items = reports.items() if isinstance(reports, dict) else [("run", reports)]
    for name, rep in items:
        d = rep.to_dict()
        print(
            f"[{name}] 位点 {d['last_position_before']} -> {d['last_position_after']}"
            f" | 新投递={d['delivered']} 幂等跳过={d['skipped_replayed']}"
            f" | fatal={d['fatal']['code'] if d['fatal'] else None}"
        )
        for fail in d["record_failures"]:
            print(f"    记录失败 pos={fail['position']} code={fail['code']}: {fail['message']}")


def _engine(tmp_path: Path, connectors, *, workers: int = 4, verbose: bool = False):
    cps = CheckpointStore(str(tmp_path / "cp.json"))
    tgt = FileTarget(str(tmp_path / "target.json"))
    return IngestionEngine(connectors, cps, tgt, max_workers=workers, verbose=verbose), cps, tgt


# ---------- 1. 正常采集 + 增量续采 + 重复触发 ----------

def test_incremental_resume_and_repeat_trigger(tmp_path: Path) -> None:
    src = write_jsonl(tmp_path / "fs.jsonl", [jrow("a", v=1), jrow("b", v=2)])
    eng, cps, tgt = _engine(tmp_path, {"fs": FileSourceConnector("fs", str(src))})

    r1 = eng.run_source("fs")
    _print_reports("首次采集", {"fs": r1})
    assert r1.ok and r1.delivered == 2 and cps.get("fs") == 2
    assert [r["record_id"] for r in tgt.records("fs")] == ["a", "b"]

    # 重复触发：无新数据，目标端无变化
    r2 = eng.run_source("fs")
    _print_reports("重复触发（无新数据）", {"fs": r2})
    assert r2.ok and r2.delivered == 0 and r2.last_position_after == 2
    assert tgt.delivered_count("fs") == 2

    # 追加新数据后续采，只处理新增部分
    with open(src, "a", encoding="utf-8") as fh:
        fh.write(jrow("c", v=3) + "\n")
    r3 = eng.run_source("fs")
    _print_reports("追加后续采", {"fs": r3})
    assert r3.delivered == 1 and r3.last_position_before == 2
    assert cps.get("fs") == 3
    assert [r["record_id"] for r in tgt.records("fs")] == ["a", "b", "c"]


# ---------- 2. 重放（崩溃窗口：已投递但位点回退）不产生重复 ----------

def test_replay_after_checkpoint_rollback_is_idempotent(tmp_path: Path) -> None:
    src = write_jsonl(tmp_path / "fs.jsonl", [jrow("a"), jrow("b"), jrow("c")])
    eng, cps, tgt = _engine(tmp_path, {"fs": FileSourceConnector("fs", str(src))})
    eng.run_source("fs")
    assert cps.get("fs") == 3

    # 模拟崩溃：目标端已有 3 条，位点文件回退到 1
    (tmp_path / "cp.json").write_text(json.dumps({"fs": 1}))

    rep = eng.run_source("fs")
    _print_reports("位点回退重放（目标端已存在）", {"fs": rep})
    assert rep.delivered == 0
    assert rep.skipped_replayed == 2  # pos2、pos3 被幂等去重
    assert cps.get("fs") == 3
    assert tgt.delivered_count("fs") == 3
    assert [r["record_id"] for r in tgt.records("fs")] == ["a", "b", "c"]


# ---------- 3. 位点缺口必须被拒绝 ----------

def test_position_gap_is_rejected(tmp_path: Path) -> None:
    class GapConnector(FileSourceConnector):
        def __init__(self) -> None:
            self.name = "fs"

        def fetch(self, after_position: int):
            # 已提交到 2，下一条直接跳到 4（缺 3）
            if after_position < 4:
                yield SourceEvent(4, {"id": "x4"})

    eng, cps, tgt = _engine(tmp_path, {"fs": GapConnector()})
    cps.advance("fs", 2)
    rep = eng.run_source("fs")
    _print_reports("位点缺口", {"fs": rep})
    assert rep.fatal is not None
    assert rep.fatal.code is FailureCode.POSITION_GAP
    assert cps.get("fs") == 2
    assert tgt.delivered_count("fs") == 0


# ---------- 4. 位点回退（数据源重放旧位点）必须被拒绝 ----------

def test_position_regression_is_rejected(tmp_path: Path) -> None:
    class RegressConnector(FileSourceConnector):
        def __init__(self) -> None:
            self.name = "fs"

        def fetch(self, after_position: int):
            yield SourceEvent(2, {"id": "old"})  # 已提交到 3 却回放 2
            yield SourceEvent(4, {"id": "new"})

    eng, cps, tgt = _engine(tmp_path, {"fs": RegressConnector()})
    cps.advance("fs", 3)
    rep = eng.run_source("fs")
    _print_reports("位点回退", {"fs": rep})
    assert rep.fatal is not None
    assert rep.fatal.code is FailureCode.POSITION_REGRESSED
    assert cps.get("fs") == 3
    assert tgt.delivered_count("fs") == 0

    with pytest.raises(ValueError):
        cps.advance("fs", 1)


# ---------- 5. 畸形记录：三种失败原因可区分，且不静默跳过 ----------

@pytest.mark.parametrize(
    ("row", "expected_code"),
    [
        ("not-a-json-line", FailureCode.RECORD_MALFORMED),
        ('{"data": {"v": 1}}', FailureCode.MISSING_FIELD),
        ('{"id": 123}', FailureCode.TYPE_MISMATCH),
        ('{"id": "ok", "data": [1,2]}', FailureCode.TYPE_MISMATCH),
    ],
)
def test_malformed_records_distinguishable(tmp_path: Path, row, expected_code) -> None:
    src = write_jsonl(tmp_path / "fs.jsonl", [jrow("good-before"), row, jrow("good-after")])
    eng, cps, tgt = _engine(tmp_path, {"fs": FileSourceConnector("fs", str(src))})
    rep = eng.run_source("fs")
    _print_reports(f"畸形记录 {expected_code.value}", {"fs": rep})

    assert rep.fatal is not None
    assert rep.fatal.code is expected_code
    assert len(rep.record_failures) == 1
    assert rep.record_failures[0].position == 2
    # 仅 pos1 前缀被提交；pos2 失败，pos3 不被越过（不丢数不静默）
    assert cps.get("fs") == 1
    assert tgt.delivered_count("fs") == 1

    # 修复 pos2 后，pos2/pos3 被继续处理
    write_jsonl(src, [jrow("good-before"), jrow("fixed"), jrow("good-after")])
    rep2 = eng.run_source("fs")
    _print_reports("修复后续采", {"fs": rep2})
    assert rep2.ok and rep2.delivered == 2
    assert cps.get("fs") == 3
    assert [r["record_id"] for r in tgt.records("fs")] == [
        "good-before",
        "fixed",
        "good-after",
    ]


# ---------- 6. 多源并发，局部失败互不影响 ----------

def test_concurrent_partial_failure_isolation(tmp_path: Path) -> None:
    g1 = write_jsonl(tmp_path / "g1.jsonl", [jrow("g1-1"), jrow("g1-2"), jrow("g1-3")])
    g2 = write_jsonl(tmp_path / "g2.jsonl", [jrow("g2-1"), jrow("g2-2"), jrow("g2-3"), jrow("g2-4")])
    bad = write_jsonl(tmp_path / "bad.jsonl", [jrow("bad-1"), jrow("bad-2"), "garbage", jrow("bad-4")])

    connectors = {
        "g1": FileSourceConnector("g1", str(g1)),
        "g2": FileSourceConnector("g2", str(g2)),
        "bad": FileSourceConnector("bad", str(bad)),
    }
    eng, cps, tgt = _engine(tmp_path, connectors, workers=3)
    reports = eng.run_all()
    _print_reports("三源并发（bad 源含畸形行）", reports)

    assert reports["g1"].ok and reports["g1"].delivered == 3
    assert reports["g2"].ok and reports["g2"].delivered == 4
    assert reports["bad"].fatal.code is FailureCode.RECORD_MALFORMED
    assert reports["bad"].delivered == 2

    # 各源位点独立，好源进度不受坏源影响
    assert cps.get("g1") == 3 and cps.get("g2") == 4 and cps.get("bad") == 2
    assert tgt.delivered_count("g1") == 3
    assert tgt.delivered_count("g2") == 4
    assert tgt.delivered_count("bad") == 2

    # 修复坏源后再次并发采集，仅坏源补齐
    write_jsonl(bad, [jrow("bad-1"), jrow("bad-2"), jrow("bad-3"), jrow("bad-4")])
    reports2 = eng.run_all()
    _print_reports("修复后再次并发", reports2)
    assert reports2["g1"].delivered == 0 and reports2["g2"].delivered == 0
    assert reports2["bad"].ok and reports2["bad"].delivered == 2
    assert tgt.delivered_count("bad") == 4


# ---------- 7. 同一数据源并发触发：busy 拒绝，无重复写入 ----------

def test_same_source_concurrent_trigger_is_serialized(tmp_path: Path) -> None:
    src = write_jsonl(tmp_path / "fs.jsonl", [jrow(f"r{i}") for i in range(1, 6)])

    class SlowConnector(FileSourceConnector):
        def normalize(self, event):
            import time

            time.sleep(0.05)
            return super().normalize(event)

    eng, cps, tgt = _engine(tmp_path, {"fs": SlowConnector("fs", str(src))})
    reports: list = []
    threads = [
        threading.Thread(target=lambda: reports.append(eng.run_source("fs")))
        for _ in range(5)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    busy = [r for r in reports if r.fatal and r.fatal.code is FailureCode.SOURCE_BUSY]
    ran = [r for r in reports if not (r.fatal and r.fatal.code is FailureCode.SOURCE_BUSY)]
    _print_reports("同源 5 并发触发", {"fs": ran[0]})
    print(f"    被 busy 拒绝的触发数: {len(busy)}")
    assert len(busy) == 4
    assert len(ran) == 1 and ran[0].delivered == 5
    assert cps.get("fs") == 5
    assert tgt.delivered_count("fs") == 5


# ---------- 8. 底层异常不透出对外接口 ----------

def test_source_io_error_is_wrapped(tmp_path: Path) -> None:
    eng, _cps, _tgt = _engine(
        tmp_path, {"fs": FileSourceConnector("fs", str(tmp_path / "missing.jsonl"))}
    )
    rep = eng.run_source("fs")
    assert rep.fatal is not None
    assert rep.fatal.code is FailureCode.SOURCE_UNAVAILABLE
    assert isinstance(rep.fatal, IngestionError)
    # 原始异常类型仅以名字记录，消息是经过包装的对外文案
    assert "FileNotFoundError" not in rep.fatal.message
    assert rep.fatal.cause_type == "FileNotFoundError"


def test_unknown_source_returns_structured_error(tmp_path: Path) -> None:
    eng, _cps, _tgt = _engine(tmp_path, {})
    with pytest.raises(IngestionError) as exc:
        eng.run_source("nope")
    assert exc.value.code is FailureCode.SOURCE_NOT_FOUND
