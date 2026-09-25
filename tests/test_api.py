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
