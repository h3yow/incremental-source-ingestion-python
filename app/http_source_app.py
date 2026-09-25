"""本地回环 HTTP 数据源模拟服务（数据源形态二的服务端）。

从 JSONL 文件读取事件，通过分页接口提供：

    GET /events?after=<position>&limit=<n>

返回 ``{"events": [{"position": int, "record": <raw-json>}], "next": int|null}``。

故障开关（模拟数据源自身异常）：
* ``?fail=1``  -> HTTP 500（服务不可用）
* 数据文件某行写成 ``__HTTP_BAD__`` -> 该页返回非法 JSON 响应体（协议错误）
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

BAD_LINE_MARKER = "__HTTP_BAD__"


def create_http_source_app(data_path: str, *, page_size: int = 2) -> FastAPI:
    app = FastAPI(title="simulated-http-source")
    path = Path(data_path)

    def _load() -> list[tuple[int, str]]:
        return [
            (idx, line)
            for idx, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), start=1
            )
            if line.strip()
        ]

    @app.get("/events")
    def events(
        request: Request,
        after: int = 0,
        limit: int | None = None,
    ):
        if request.query_params.get("fail"):
            return JSONResponse(
                status_code=500, content={"error": "simulated server failure"}
            )
        size = limit if limit is not None else page_size
        rows = _load()
        page = [(p, line) for p, line in rows if p > after][:size]
        if any(line.strip() == BAD_LINE_MARKER for _, line in page):
            # 模拟服务端返回了无法解析的响应体（协议层错误）
            return PlainTextResponse("{oops, this is not valid json", media_type="application/json")
        payload_events = [
            {"position": position, "record": json.loads(line)} for position, line in page
        ]
        next_after = page[-1][0] if page else None
        return JSONResponse({"events": payload_events, "next": next_after})

    @app.get("/health")
    def health() -> PlainTextResponse:
        return PlainTextResponse("ok")

    return app
