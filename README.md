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
  connectors/file_source.py # JSONL 文件源（id/payload 形态，含重建检测）
  connectors/http_source.py # 回环 HTTP 源（recordId/text 形态）
  cursor_store.py           # 文件持久化位点，compare-and-swap 拒绝回退/不连续
  source_state.py           # 按源记录健康度、待确认跳过区间、重新同步留痕
  sink.py                   # Sink 契约；LocalSink（文件幂等）/ HttpSink（回环投递）
  pipeline.py               # 校验、fail-fast、源端对齐、受控重新同步、并发编排
main.py                     # FastAPI：模拟源 / 模拟目标端 / 触发 / 状态 / 重新同步接口
tests/                      # 33 个用例（pytest -s 可见位点、跳过区间与处理流水）
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
| `source_gap`       | 源端跨过一段未处理数据，对齐失败（带区间） |
| `resync_required`  | 有待确认跳过区间，采集被拒绝               |
| `no_pending_gap`   | 没有待确认区间，重新同步被拒绝             |
| `invalid_confirmation` | 重新同步缺少操作人或依据，被拒绝       |

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

## 源端对齐与受控重新同步

数据源被重建、清库或换存储后，源端实际能提供的位置可能与本地已确认进度
对不上。系统在采集时做 **对齐检查**，并提供 **人工确认后才能生效的重新同步**。

### 对齐检查（采集时自动进行）

管线为每条记录维护「本地期望位置」`expected`（初始为已提交位点，每处理一条
推进为 `seq+1`）。源端返回记录的 `seq > expected` 即判定源端跨过了
`[expected, seq)` 这段未处理数据：

- 本次对该源的采集立即判为异常并停止，失败原因 `source_gap`，
  返回中携带跳过区间 `gap: {"from", "to", "resume_from"}`；
- **本地位点不推进**（不认下源端位置），跳过区间不写入、也不被当成不存在；
- 该区间登记为 **待确认跳过区间**（`data/source_state.json`），确认之前
  该源的采集一律被拒绝（`resync_required`），其它源不受影响。

两种源形态都能发现：

- **回环 HTTP 源**：源端清库后最早只能提供更高的 seq（如本地进度 5、
  源端首条 seq=10），由管线比对首条/每条记录 seq 检出，区间为 `[5, 10)`。
- **文件源**：文件只增不减；若文件总行数小于本地已确认位点，说明文件被
  重建/截断，连接器直接抛出 `source_gap`，区间为 `[0, 当前行数)`，
  新起点为文件当前末尾。

### 受控重新同步（人工确认）

`Pipeline.resync(source_id, operator=..., justification=...)` /
`POST /ingest/resync`：

- **能确认**：该源存在待确认跳过区间，且 `operator`（谁）与
  `justification`（依据，如工单号）均非空。确认后位点被推到
  `resume_from`（HTTP 跨段场景为向前跳过丢失区间；文件重建场景为
  受控回退到文件当前末尾），并追加一条审计记录：操作人、确认时间、
  跳过区间、位点变化、依据（按源留痕，只增不删）。
- **不能确认**：没有待确认区间（`no_pending_gap`）；操作人或依据为空
  （`invalid_confirmation`）。被拒绝时不改动任何位点与状态。

同步完成后：再采集从新起点继续；目标端按 `dedup_key` 幂等，**之前投递过的
记录不会重写**；被跳过的区间**不会被补写**，但会永久留在审计记录与状态
查询中，不会被悄悄当成成功。

### 与位点语义的兼容

- 正常采集的位点推进规则完全不变：仍是「先投递、再 compare-and-swap 提交
  `seq+1`」，`cursor_regression` / `cursor_gap` 拒绝逻辑不变。
- `reset()` 是唯一的受控例外：只能由 `resync()` 在持有该源互斥锁、完成
  审计留痕后调用，可前移（跳过确认丢失的区间）也可回退（源端重建后的
  新起点）。普通提交路径无法触发。
- `resync()` 与 `run_source()` 共用同一把每源互斥锁：并发触发时结果确定——
  要么同步先生效、采集从新起点继续，要么采集先跑完、同步基于最新状态判断；
  不会出现位点错乱或重复投递（见
  `test_concurrent_resync_and_run_deterministic`）。

### 按数据源查状态

`Pipeline.status(source_id)` / `GET /ingest/status`：

```json
{
  "source_id": "events",
  "cursor": 8,
  "status": "healthy | anomaly | pending_confirmation",
  "last_failure": {"reason": "...", "message": "...", "gap": {...}} ,
  "pending_gap": {"from": 5, "to": 10, "resume_from": 10, "detected_at": "..."},
  "resyncs": [{"operator": "...", "confirmed_at": "...", "justification": "...",
               "skipped": {"from": 5, "to": 10},
               "cursor_before": 5, "cursor_after": 10}]
}
```

健康度推导：有待确认区间 → `pending_confirmation`；最近一次采集失败 →
`anomaly`；否则 `healthy`。某源处于异常/待确认状态不影响其它源的采集与
已提交位点，跨源并发度仍可配置。

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
源端跨段检出与阻断（文件源重建 + 回环 HTTP 清库两种形态）、
显式确认后重新同步并续采不重复、重新同步与采集并发结果确定、
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
运行状态都在 `data/`（`cursors.json`、`sink.jsonl`、`source_state.json`），
删除该目录即可重置。

也可编辑 `data/orders.jsonl` 插入缺字段或类型错误的行，再触发采集，
观察返回结果中可区分的 `missing_field` / `type_mismatch` 失败原因与
停住的位点。

### 3. 复现「源端重建 → 对齐异常 → 确认重新同步」

```bash
uv run python main.py
curl -s -X POST http://127.0.0.1:8765/ingest/run          # 正常采到 cursor=5

# 模拟源端重建：把 events 换成从 seq=10 开始的清库后数据
python - << 'PY'
import json
recs = [{"seq": 10+i, "recordId": f"evt-{10+i}", "text": f"new {i}"} for i in range(3)]
json.dump(recs, open("data/events.json", "w"), indent=2)
PY
# 重启服务（模拟源在启动时加载），再触发：
curl -s -X POST http://127.0.0.1:8765/ingest/run
#   -> events 失败：reason=source_gap, gap={"from":5,"to":10,"resume_from":10}
curl -s http://127.0.0.1:8765/ingest/status | python -m json.tool
#   -> events: pending_confirmation，pending_gap 可见；orders 不受影响

curl -s -X POST http://127.0.0.1:8765/ingest/run          # 确认前拒绝：resync_required

# 显式确认（操作人 + 依据），位点推到新起点并留痕
curl -s -X POST http://127.0.0.1:8765/ingest/resync \
  -H 'Content-Type: application/json' \
  -d '{"source_id":"events","operator":"ops-chen","justification":"INC-42: store rebuilt, [5,10) lost"}'

curl -s -X POST http://127.0.0.1:8765/ingest/run          # 从 seq=10 续采，旧记录不重复
curl -s http://127.0.0.1:8765/ingest/status | python -m json.tool
#   -> events: healthy，resyncs 中可查谁、何时、跳过哪段、依据
```

文件源形态：把 `data/orders.jsonl` 换成行数更少的文件再触发采集，
同样得到 `source_gap`（区间为 `[0, 当前行数)`），确认后位点受控回退到
文件当前末尾续采。
