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

    def post(self, path):
        req = urllib.request.Request(self.base + path, data=b"{}", method="POST")
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


def test_api_gap_status_and_resync_endpoints(tmp_path):
    """HTTP 源清库跨段：接口级检出异常、查状态、确认同步、续采不重复。"""
    # 预置：events 源重建后只能从 seq=5 提供（本地进度为 0，跨过 [0,5)）
    (tmp_path / "events.json").write_text(
        json.dumps([{"seq": 5 + i, "recordId": f"evt-{5 + i}", "text": f"e{5 + i}"}
                    for i in range(3)]),
        encoding="utf-8",
    )
    with Server(tmp_path) as srv:
        r1 = srv.post("/ingest/run")
        results = {r["source_id"]: r for r in r1["results"]}
        print("run1:", json.dumps(r1, ensure_ascii=False))
        assert results["orders"]["status"] == "ok"  # 文件源不受影响
        assert results["events"]["status"] == "failed"
        assert results["events"]["failure"]["reason"] == "source_gap"
        assert results["events"]["failure"]["gap"] == {"from": 0, "to": 5, "resume_from": 5}

        # 状态查询：events 待确认，orders 健康
        status = {s["source_id"]: s for s in srv.get("/ingest/status")["sources"]}
        print("status:", json.dumps(status, ensure_ascii=False))
        assert status["events"]["status"] == "pending_confirmation"
        assert status["events"]["pending_gap"]["from"] == 0
        assert status["events"]["pending_gap"]["to"] == 5
        assert status["orders"]["status"] == "healthy"
        assert status["orders"]["cursor"] == 5

        # 确认前再触发：events 被拒绝，原因可区分
        r2 = srv.post("/ingest/run")
        events2 = {r["source_id"]: r for r in r2["results"]}["events"]
        assert events2["status"] == "failed"
        assert events2["failure"]["reason"] == "resync_required"

        # 确认信息不完整 -> 409
        body = json.dumps({"source_id": "events", "operator": "", "justification": "x"}).encode()
        req = urllib.request.Request(
            srv.base + "/ingest/resync", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        try:
            urllib.request.urlopen(req, timeout=10)
            assert False, "expected 409"
        except urllib.error.HTTPError as e:
            detail = json.loads(e.read())["detail"]
            print("invalid confirmation ->", e.code, detail)
            assert e.code == 409
            assert detail["failure"]["reason"] == "invalid_confirmation"

        # 显式确认：留痕并把位点推到新起点
        body = json.dumps({
            "source_id": "events",
            "operator": "ops-chen",
            "justification": "INC-42: events store rebuilt, [0,5) confirmed lost",
        }).encode()
        req = urllib.request.Request(
            srv.base + "/ingest/resync", data=body,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        res = json.loads(urllib.request.urlopen(req, timeout=10).read())
        print("resync:", json.dumps(res, ensure_ascii=False))
        assert res["status"] == "ok"
        assert res["cursor_before"] == 0 and res["cursor_after"] == 5
        assert res["audit"]["operator"] == "ops-chen"
        assert res["audit"]["skipped"] == {"from": 0, "to": 5}

        # 同步后采集：从新起点继续，不重复、不补写跳过区间
        r3 = srv.post("/ingest/run")
        results3 = {r["source_id"]: r for r in r3["results"]}
        print("run3 events outcomes:", results3["events"]["outcomes"])
        assert results3["events"]["status"] == "ok"
        assert results3["events"]["cursor_before"] == 5
        assert results3["events"]["cursor_after"] == 8
        assert len(results3["events"]["outcomes"]) == 3

        sink = srv.get("/sink/records")
        keys = [r["dedup_key"] for r in sink["records"]]
        assert len(keys) == len(set(keys))
        assert sink["count"] == 5 + 3  # orders 5 条 + events 新起点后 3 条

        status2 = {s["source_id"]: s for s in srv.get("/ingest/status")["sources"]}
        assert status2["events"]["status"] == "healthy"
        assert status2["events"]["cursor"] == 8
        assert len(status2["events"]["resyncs"]) == 1
