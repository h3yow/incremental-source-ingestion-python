"""对外 HTTP 接口：触发采集、查看位点与结果。

* ``POST /sources/{source}/run`` 触发单个数据源采集
* ``POST /run``                    并发采集全部数据源
* ``GET  /checkpoints/{source}``   查看某源位点
* ``GET  /checkpoints``            查看全部位点
* ``GET  /target/{source}``        查看目标端记录

底层异常不会透出：错误统一以结构化 JSON 返回（code/message/source/position）。
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.engine import IngestionEngine
from app.errors import FailureCode, IngestionError


def create_api_app(engine: IngestionEngine) -> FastAPI:
    app = FastAPI(title="incremental-ingestion")

    @app.exception_handler(IngestionError)
    async def ingestion_error_handler(request: Request, exc: IngestionError) -> JSONResponse:
        status = 404 if exc.code.value == "source_not_found" else 422
        return JSONResponse(status_code=status, content={"error": exc.to_dict()})

    @app.post("/sources/{source}/run")
    def run_source(source: str) -> dict:
        report = engine.run_source(source)
        return {"report": report.to_dict()}

    @app.post("/run")
    def run_all() -> dict:
        reports = engine.run_all()
        return {"reports": {name: rep.to_dict() for name, rep in reports.items()}}

    @app.get("/checkpoints")
    def checkpoints_all() -> dict:
        return {
            "checkpoints": {
                name: engine.checkpoints.get(name) for name in engine.connectors
            }
        }

    @app.get("/checkpoints/{source}")
    def get_checkpoint(source: str) -> dict:
        if source not in engine.connectors:
            raise IngestionError(
                FailureCode.SOURCE_NOT_FOUND,
                f"未注册的数据源: {source}",
                source=source,
            )
        return {"source": source, "last_position": engine.checkpoints.get(source)}

    @app.get("/target/{source}")
    def target_records(source: str) -> dict:
        return {"source": source, "records": engine.target.records(source)}

    return app
