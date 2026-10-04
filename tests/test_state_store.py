"""状态存储：失败登记、待确认跳过区间、重新同步审计留痕、持久化恢复。"""

from ingestion.state_store import SourceStateStore


def test_failure_with_gap_enters_pending_resync(tmp_path):
    store = SourceStateStore(str(tmp_path / "state.json"))
    store.mark_failure("s1", {
        "reason": "source_moved",
        "message": "source skipped unprocessed range",
        "source_id": "s1",
        "gap": {"from": 5, "to": 100},
    })
    st = store.get("s1")
    print("state after source_moved:", st.to_dict())
    assert st.status == "pending_resync" and not st.healthy
    assert st.pending_skip["from"] == 5 and st.pending_skip["to"] == 100
    assert st.pending_skip["detected_at"]

    # 普通失败不进入待确认
    store.mark_failure("s2", {"reason": "source_error", "message": "x", "source_id": "s2"})
    st2 = store.get("s2")
    assert st2.status == "failed" and st2.pending_skip is None


def test_resync_audit_trail_and_recovery(tmp_path):
    path = str(tmp_path / "state.json")
    store = SourceStateStore(path)
    store.mark_failure("s1", {
        "reason": "source_moved", "message": "m", "source_id": "s1",
        "gap": {"from": 5, "to": 100},
    })
    store.record_resync(
        "s1", skipped_from=5, skipped_to=100,
        operator="op-zhang", reason="vendor confirmed data lost",
    )
    st = store.get("s1")
    print("state after resync:", st.to_dict())
    assert st.status == "ok" and st.healthy and st.pending_skip is None
    assert len(st.resync_history) == 1
    audit = st.resync_history[0]
    assert audit["operator"] == "op-zhang"
    assert audit["skipped_from"] == 5 and audit["skipped_to"] == 100
    assert audit["reason"] == "vendor confirmed data lost" and audit["at"]

    # 重启后状态仍在
    store2 = SourceStateStore(path)
    st2 = store2.get("s1")
    assert st2.resync_history == st.resync_history and st2.status == "ok"
    print("reloaded:", store2.list_all().keys())
