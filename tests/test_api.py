"""端到端集成：FastAPI 回环模拟源 + 回环目标端，覆盖重放、局部失败。"""

import json
import socket
import threading
import time
import urllib.error
import urllib.request

import uvicorn

from main import create_app


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, tmp_path):
        self.port = _free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        self.app = create_app(str(tmp_path), use_http_sink=True, port=self.port)
        self.thread = threading.Thread(
            target=uvicorn.run,
            args=(self.app,),
            kwargs={"host": "127.0.0.1", "port": self.port, "log_level": "error"},
            daemon=True,
        )

    def __enter__(self):
        self.thread.start()
        for _ in range(50):
            try:
                urllib.request.urlopen(self.base + "/", timeout=1).read()
                return self
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def __exit__(self, *exc):
        pass

    def get(self, path):
        return json.loads(urllib.request.urlopen(self.base + path, timeout=10).read())

    def post(self, path, body=None):
        data = json.dumps(body or {}).encode()
        req = urllib.request.Request(
            self.base + path, data=data, method="POST",
            headers={"Content-Type": "application/json"},
        )
        return json.loads(urllib.request.urlopen(req, timeout=10).read())


def test_api_full_run_replay_and_cursors(tmp_path):
    with Server(tmp_path) as srv:
        r1 = srv.post("/ingest/run")
        cursors1 = srv.get("/cursors")
        print("run1:", json.dumps(r1, ensure_ascii=False))
        print("cursors:", cursors1)

        results = {r["source_id"]: r for r in r1["results"]}
        assert results["orders"]["status"] == "ok"
        assert results["events"]["status"] == "ok"
        assert cursors1 == {"cursors": {"events": 5, "orders": 5}}

        # 重复触发：全部幂等，不产生重复
        r2 = srv.post("/ingest/run")
        print("run2 outcomes len:",
              {r["source_id"]: len(r["outcomes"]) for r in r2["results"]})
        assert all(r["status"] == "ok" for r in r2["results"])
        assert all(len(r["outcomes"]) == 0 for r in r2["results"])

        sink = srv.get("/sink/records")
        keys = [r["dedup_key"] for r in sink["records"]]
        assert sink["count"] == 10 and len(keys) == len(set(keys))

        # 未知模拟源 -> 数据源错误脱敏，且不影响文件源
        events_file = tmp_path / "events.json"
        records = json.loads(events_file.read_text(encoding="utf-8"))
        events_file.write_text(json.dumps(records + [
            {"seq": 5, "recordId": "evt-5", "text": "new"},
        ]), encoding="utf-8")
        # 用一个会失败的方式：直接请求不存在的模拟源应返回 503
        try:
            urllib.request.urlopen(srv.base + "/mock/unknown/records", timeout=5)
            assert False, "expected 503"
        except urllib.error.HTTPError as e:
            detail = json.loads(e.read())["detail"]
            print("unknown source ->", e.code, detail)
            assert e.code == 503 and detail == "source unavailable"


def test_api_restart_resumes_from_persisted_cursor(tmp_path):
    with Server(tmp_path) as srv:
        srv.post("/ingest/run")
    # 进程重启：新建 app，位点与目标端数据均在磁盘上
    port = _free_port()
    app = create_app(str(tmp_path), use_http_sink=False, port=port)
    t = threading.Thread(
        target=uvicorn.run,
        args=(app,),
        kwargs={"host": "127.0.0.1", "port": port, "log_level": "error"},
        daemon=True,
    )
    t.start()
    base = f"http://127.0.0.1:{port}"
    for _ in range(50):
        try:
            urllib.request.urlopen(base + "/", timeout=1).read()
            break
        except OSError:
            time.sleep(0.1)
    req = urllib.request.Request(base + "/ingest/run", data=b"{}", method="POST")
    r = json.loads(urllib.request.urlopen(req, timeout=10).read())
    print("post-restart run:",
          {x["source_id"]: (x["status"], x["cursor_before"], x["cursor_after"])
           for x in r["results"]})
    assert all(x["status"] == "ok" and not x["outcomes"] for x in r["results"])


def test_api_source_moved_resync_flow(tmp_path):
    """端到端：源端重建 -> 跨段判异常 -> 状态可查 -> 未确认拒绝 -> 确认后恢复。"""
    with Server(tmp_path) as srv:
        r1 = srv.post("/ingest/run")
        assert all(r["status"] == "ok" for r in r1["results"])

        # 源端被重建：events 数据换存储，seq 从 100 重新开始
        srv.post("/mock/events/rebuild", {"start_seq": 100, "count": 3})

        r2 = srv.post("/ingest/run")
        results2 = {r["source_id"]: r for r in r2["results"]}
        print("run after rebuild:",
              {k: (v["status"], v["failure"]) for k, v in results2.items()})
        # events 源判异常并给出跳过区间；orders 源不受影响
        assert results2["events"]["status"] == "failed"
        assert results2["events"]["failure"]["reason"] == "source_moved"
        assert results2["events"]["failure"]["gap"] == {"from": 5, "to": 100}
        assert results2["orders"]["status"] == "ok"

        # 按源查状态：进度、健康度、最近异常、待确认区间
        st = srv.get("/sources/events/status")
        print("events status:", json.dumps(st, ensure_ascii=False))
        assert st["cursor"] == 5 and st["healthy"] is False
        assert st["status"] == "pending_resync"
        assert st["pending_skip"]["from"] == 5 and st["pending_skip"]["to"] == 100
        all_st = srv.get("/sources/status")["sources"]
        assert all_st["orders"]["healthy"] is True

        # 未确认前再次触发：拒绝往下走
        r3 = srv.post("/ingest/run")
        events3 = {r["source_id"]: r for r in r3["results"]}["events"]
        assert events3["failure"]["reason"] == "resync_required"

        # 未显式确认的 resync 请求被拒绝
        try:
            srv.post("/sources/events/resync",
                     {"new_cursor": 100, "operator": "op", "reason": "x"})
            assert False, "expected 400"
        except urllib.error.HTTPError as e:
            print("unconfirmed resync ->", e.code)
            assert e.code == 400

        # 显式确认后重新同步：位点推到新起点，留痕可查
        out = srv.post("/sources/events/resync", {
            "new_cursor": 100, "operator": "op-zhang",
            "reason": "vendor confirmed seq 5-99 permanently lost",
            "confirm": True,
        })
        print("resync:", out)
        assert out["cursor_before"] == 5 and out["cursor_after"] == 100
        st2 = srv.get("/sources/events/status")
        assert st2["healthy"] is True and st2["pending_skip"] is None
        audit = st2["resync_history"][-1]
        assert audit["operator"] == "op-zhang"
        assert audit["skipped_from"] == 5 and audit["skipped_to"] == 100

        # 恢复采集：只采新起点的 3 条，已投递记录不重复
        r4 = srv.post("/ingest/run")
        results4 = {r["source_id"]: r for r in r4["results"]}
        print("run after resync:",
              {k: (v["status"], v["cursor_before"], v["cursor_after"],
                   [(o["seq"], o["status"]) for o in v["outcomes"]])
               for k, v in results4.items()})
        assert results4["events"]["status"] == "ok"
        assert results4["events"]["cursor_before"] == 100
        assert results4["events"]["cursor_after"] == 103
        sink = srv.get("/sink/records")
        keys = [r["dedup_key"] for r in sink["records"]]
        # 5 orders + 5 旧 events + 3 新 events，无重复
        assert sink["count"] == 13 and len(keys) == len(set(keys))
