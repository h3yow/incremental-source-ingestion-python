# 多数据源增量采集与投递（本地可运行）

用本地 JSONL 文件模拟一类数据源，用本地回环 HTTP 接口模拟另一类数据源与目标端，
实现统一契约的增量采集、位点续采、幂等投递与并发编排。重复执行、数据异常、
并发触发下的最终状态确定：**不重、不丢、失败原因可区分**。

## 目录结构

```
ingestion/
  errors.py                 # 失败原因枚举与受控异常（脱敏，不透出底层异常）
  models.py                 # RawRecord / UnifiedRecord / 结果模型
  connectors/base.py        # 统一连接器契约 Connector
  connectors/file_source.py # JSONL 文件源（id/payload 形态）
  connectors/http_source.py # 回环 HTTP 源（recordId/text 形态）
  cursor_store.py           # 文件持久化位点，compare-and-swap 拒绝回退/不连续
  sink.py                   # Sink 契约；LocalSink（文件幂等）/ HttpSink（回环投递）
  pipeline.py               # 校验、fail-fast、每源串行锁、跨源线程池并发
main.py                     # FastAPI：模拟源 / 模拟目标端 / 触发接口
tests/                      # 24 个用例（pytest -s 可见位点与处理流水）
```

## 统一连接器契约

`Connector` 抽象类（`ingestion/connectors/base.py`）：

- `source_id`：数据源标识，同时作为位点与去重键的命名空间。
- `fetch(after) -> Iterator[RawRecord]`：返回序号 `seq >= after` 的原始记录，
  按 `seq` 升序、连续无空洞；`seq` 是数据源内的稳定位置。
- `normalize(raw) -> {"id", "payload"}`：把数据源自身字段映射为统一字段。
  文件源原样映射，HTTP 源把 `recordId/text` 归一为 `id/payload`。

归一后的统一内部模型 `UnifiedRecord(source_id, record_id, seq, payload)`，
去重键为 `source_id:record_id`。

### 失败不静默、原因可区分、底层异常不外泄

管线在归一后做统一校验，任何失败都进入 `SourceRunResult.failure`，
带枚举原因（不是日志吞掉、不是空跳过）：

| reason             | 触发条件                                   |
|--------------------|--------------------------------------------|
| `missing_field`    | 必填字段 `id`/`payload` 缺失或为 null      |
| `type_mismatch`    | 必填字段类型不是 `str`（含实际类型说明）   |
| `source_error`     | 数据源不可用、行损坏、协议错误等           |
| `cursor_regression`| 新位点小于已提交位点                       |
| `cursor_gap`       | compare-and-swap 发现期望位点与已存不一致  |
| `sink_error`       | 目标端投递失败                             |

连接器内部捕获所有 I/O/协议异常后 `raise ... from None`，对外只给脱敏信息
（不包含本地路径、主机名、errno、堆栈）；管线还有一层兜底捕获，
即使连接器实现漏网，对外也只呈现 `source_error: connector failure`。

## 位点语义

- 位点 `cursor = 下一条待采记录的 seq`，初始为 `0`，按数据源独立持久化
  （默认 `data/cursors.json`，临时文件 + 原子 rename 落盘）。
- 每条记录 **先投递成功，再 compare-and-swap 提交** `seq+1`；
  提交后再处理下一条。进程中断后重启，从已提交位点续采，已投递未提交的
  记录重放时由目标端幂等去重，因此不丢不重。
- `commit(source_id, new, expected=stored)` 的判定：
  - `new < stored` → `cursor_regression`，拒绝；
  - `expected != stored` → `cursor_gap`（并发丢更新/跳采），拒绝；
  - `new == stored` → 幂等重放，无操作。
- 畸形/错误记录采取 **fail-fast**：位点只提交到失败记录之前（at-least-once），
  本次运行标记失败原因并停止该源；修复数据后重新触发即可从断点续采，
  不会跳过坏记录之后的数据，也不会重复写入已投递记录。

## 目标端幂等

- `LocalSink`：JSONL 文件，写入持进程锁，按 `dedup_key` 判重，重复写入
  返回 `duplicate` 而不追加行。
- `HttpSink`：POST 到本地回环接口 `/sink/deliver`，接收端按 `dedup_key`
  去重并返回 `{"duplicate": bool}`。

## 并发与失败隔离

- `Pipeline(max_workers=N)`：跨数据源用线程池并发，**N 可配置**
  （`data/sources.json` 的 `max_workers` 或 `POST /ingest/run?max_workers=N`）。
- 同一数据源在进程内有专用互斥锁：并发触发同一源只会串行执行，
  后到的运行读到已推进位点，结合 sink 幂等，位点推进与写入保持一致、
  零重复（见 `test_same_source_concurrent_triggers_no_duplicate`）。
- 单个源失败只体现在它自己的结果里，其他源已提交的位点与投递不受影响
  （见 `test_concurrent_partial_failure_isolation`）。

## 本地验证

### 1. 一键跑测试

```bash
uv sync
uv run pytest -s          # -s 可打印位点变化与每条记录的处理流水
```

测试覆盖：初始位点与中断恢复、幂等重放、位点回退/不连续拒绝、
必填缺失与类型不匹配、数据源报错脱敏、修复后续采、
多源并发全部提交、并发局部失败隔离、同源并发触发不重复、
以及走真实回环 HTTP 的 FastAPI 端到端用例。

### 2. 启动本地服务

```bash
uv run python main.py     # 默认 127.0.0.1:8765，首次运行自动生成示例数据
```

另开终端：

```bash
curl -s -X POST http://127.0.0.1:8765/ingest/run            # 触发两源并发采集
curl -s http://127.0.0.1:8765/cursors                       # 查看位点
curl -s http://127.0.0.1:8765/sink/records | python -m json.tool
curl -s -X POST 'http://127.0.0.1:8765/ingest/run?max_workers=1'  # 调整并发度
```

重复执行 `/ingest/run`：第二次 outcomes 为空、目标端记录数不变。
运行状态都在 `data/`（`cursors.json`、`sink.jsonl`），删除该目录即可重置。

也可编辑 `data/orders.jsonl` 插入缺字段或类型错误的行，再触发采集，
观察返回结果中可区分的 `missing_field` / `type_mismatch` 失败原因与
停住的位点。
