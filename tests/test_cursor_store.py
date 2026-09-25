"""位点语义：初始值、正常推进、幂等重放、回退拒绝、不连续拒绝、中断恢复。"""

import json

import pytest

from ingestion.cursor_store import CursorStore
from ingestion.errors import CursorGapError, CursorRegressionError
from ingestion.models import INITIAL_CURSOR


def _dump(path):
    with open(path, encoding="utf-8") as f:
        print("cursors.json ->", f.read())


def test_initial_cursor_is_zero(tmp_path):
    store = CursorStore(str(tmp_path / "cursors.json"))
    assert store.load("src") == INITIAL_CURSOR


def test_commit_and_resume(tmp_path):
    path = str(tmp_path / "cursors.json")
    store = CursorStore(path)
    store.commit("src", 3, expected=0)
    _dump(path)

    # 模拟中断重启：重新创建 store，从磁盘恢复位点
    restarted = CursorStore(path)
    assert restarted.load("src") == 3
    restarted.commit("src", 5, expected=3)
    assert restarted.load("src") == 5
    _dump(path)


def test_idempotent_replay_commit(tmp_path):
    store = CursorStore(str(tmp_path / "cursors.json"))
    store.commit("src", 3, expected=0)
    # 重放已提交位点：无操作，不报错
    store.commit("src", 3, expected=3)
    assert store.load("src") == 3


def test_regression_rejected(tmp_path):
    store = CursorStore(str(tmp_path / "cursors.json"))
    store.commit("src", 5, expected=0)
    with pytest.raises(CursorRegressionError) as exc:
        store.commit("src", 2, expected=5)
    assert exc.value.reason.value == "cursor_regression"
    assert exc.value.to_dict()["source_id"] == "src"
    print("failure:", exc.value.to_dict())
    # 拒绝后位点保持不变
    assert store.load("src") == 5
    _dump(store.path)


def test_gap_rejected(tmp_path):
    store = CursorStore(str(tmp_path / "cursors.json"))
    store.commit("src", 3, expected=0)
    # expected 与已存位点不一致（模拟丢更新/跳采），必须拒绝
    with pytest.raises(CursorGapError) as exc:
        store.commit("src", 6, expected=0)
    assert exc.value.reason.value == "cursor_gap"
    print("failure:", exc.value.to_dict())
    assert store.load("src") == 3


def test_sources_isolated(tmp_path):
    store = CursorStore(str(tmp_path / "cursors.json"))
    store.commit("a", 2, expected=0)
    store.commit("b", 4, expected=0)
    assert store.load("a") == 2
    assert store.load("b") == 4
    _dump(store.path)
