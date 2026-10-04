# 多数据源增量采集与投递（本地可运行）

用本地 JSONL 文件模拟一类数据源，用本地回环 HTTP 接口模拟另一类数据源与目标端，
实现统一契约的增量采集、位点续采、幂等投递与并发编排。重复执行、数据异常、
并发触发下的最终状态确定：**不重、不丢、失败原因可区分**。

## 目录结构

```
ingestion/
  errors.py                 # 失败原因枚举与受控异常（脱敏，不透出底层异常）
  models.py                 # RawRecord / UnifiedRecord / 结果与状态模型
  connectors/base.py        # 统一连接器契约 Connector
  connectors/file_source.py # JSONL 文件源（id/payload 形态，支持行内显式 seq）
  connectors/http_source.py # 回环 HTTP 源（recordId/text 形态，识别 min_seq）
  cursor_store.py           # 文件持久化位点，compare-and-swap 拒绝回退/不连续
  state_store.py            # 数据源状态：健康度、最近异常、待确认区间、重同步审计
  sink.py                   # Sink 契约；LocalSink（文件幂等）/ HttpSink（回环投递）
  pipeline.py               # 校验、fail-fast、异常对齐、受控重同步、并发编排
main.py                     # FastAPI：模拟源 / 模拟目标端 / 触发 / 状态 / 重同步接口
tests/                      # 37 个用例（pytest -s 可见位点与处理流水）
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
| `source_moved`     | 源端跨过未处理区间/被重建，与本地进度对不上 |
| `resync_required`  | 有待确认的跳过区间，确认前拒绝采集         |

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

## 异常对齐与受控重新同步

数据源被重建、清库或换存储后，源端实际能提供的位置可能跟本地已确认的
进度对不上。系统在采集入口做 **对齐检查**，对不上的该源本次运行判为
`source_moved` 并停止，本地位点保持不动，失败结果里带具体跨段区间
`gap: {"from", "to"}`。

### 检测规则（两种源形态都覆盖）

- **通用规则（管线层）**：采集流第一条记录的 `seq` 大于本地进度，
  说明源端跨过了 `[cursor, first_seq)` 这段从未处理的数据 → 判异常。
  不会把本地进度直接认到源端新起点，也不会把这段当成不存在。
- **文件源**：行内可带显式整数 `seq` 字段（缺省用行号），用于表达换存储后
  序号空间变化；此外若本地进度超出文件当前最大序号（文件被清库/截断/
  重建变短），同样判 `source_moved`，而不是静默当成"没有新数据"。
- **回环 HTTP 源**：响应可带 `min_seq`（源端声明的当前最早可提供位置），
  `min_seq > after` 即判 `source_moved`，区间 `[after, min_seq)`。

判异常后该源进入 `pending_resync` 状态并登记 **待确认跳过区间**
（`data/source_state.json`）。此状态下再次触发采集一律以
`resync_required` 拒绝，不再访问源端；其他源的采集与已提交进度不受影响。

### 重新同步（人工确认后才允许跳过）

```
POST /sources/{source_id}/resync
{"new_cursor": 100, "operator": "op-zhang",
 "reason": "vendor confirmed seq 5-99 permanently lost", "confirm": true}
```

- **能确认跳过的情形**：运营已与数据源方核实 `[旧位点, new_cursor)` 这段
  数据 **永久丢失、不可恢复、业务上允许跳过**（如源端清库重建且无备份）。
  必须显式 `confirm: true` 并给出 `operator` 与 `reason`，缺一不可（400）。
- **不能确认的情形**：源端只是临时不可用、数据可能恢复、或区间内容未核实
  —— 此时应保持 `pending_resync`，等源端恢复或把 `new_cursor` 指到源端
  恢复后真正能提供的位置；任何未确认的触发都只得到 `resync_required`。
- 重新同步只允许 **向前** 推进（`new_cursor > 当前位点`），回退/原地不动
  被拒绝（`cursor_regression`，409）。每次确认都留痕：
  谁、什么时候、跳过了哪段、依据是什么（`resync_history`，持久化可查）。
- 确认后从新起点续采：被跳过的区间不会被补采，也不会被当成成功补上；
  之前投递过的记录若被源端重放，由目标端幂等去重，不会重复写入。

### 与位点语义的兼容

- 正常采集路径的位点语义完全不变：先投递后 CAS 提交、拒绝回退/不连续、
  幂等重放。`reset()` 是重同步专用的强制推进入口，只被确认流程调用，
  且同样只允许向前。
- 重同步与正常采集 **共用每源互斥锁**：并发发生时结果确定——要么同步
  先生效、采集从新起点继续；要么采集先跑完、同步基于最新位点判断
  （若变成回退则被拒绝）。不会出现进度错乱或重复投递。
- 留痕与位点更新的顺序保证可自愈：先写审计并清除待确认状态，再推位点；
  若中途崩溃，下次采集会重新检测出跨段并再次进入待确认，不会静默丢区间。

### 按源查状态

```
GET /sources/status            # 全部源
GET /sources/{source_id}/status
```

返回当前位点 `cursor`、`healthy`、`status`（`ok`/`failed`/`pending_resync`）、
最近一次异常 `last_failure`（含 reason 与 gap）、待确认区间 `pending_skip`、
重同步审计 `resync_history`。

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
源端跨段/清库判异常（文件源与回环接口两种形态）、待确认期间拒绝采集、
显式确认后重同步从新起点续采且不重复、采集与重同步并发结果确定、
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
运行状态都在 `data/`（`cursors.json`、`source_state.json`、`sink.jsonl`），
删除该目录即可重置。

也可编辑 `data/orders.jsonl` 插入缺字段或类型错误的行，再触发采集，
观察返回结果中可区分的 `missing_field` / `type_mismatch` 失败原因与
停住的位点。

### 3. 复现「源端重建 → 异常对齐 → 确认重同步」

```bash
# 1) 正常采集一轮（events 位点推进到 5）
curl -s -X POST http://127.0.0.1:8765/ingest/run

# 2) 模拟源端被重建/换存储：events 数据从 seq=100 重新灌入
curl -s -X POST http://127.0.0.1:8765/mock/events/rebuild \
  -H 'Content-Type: application/json' -d '{"start_seq": 100, "count": 3}'

# 3) 再触发：events 判 source_moved，gap=[5,100)；orders 不受影响
curl -s -X POST http://127.0.0.1:8765/ingest/run | python -m json.tool
curl -s http://127.0.0.1:8765/sources/events/status | python -m json.tool

# 4) 未确认前再触发：events 得到 resync_required，拒绝往下走
curl -s -X POST http://127.0.0.1:8765/ingest/run | python -m json.tool

# 5) 未带 confirm 的重同步请求被拒绝（400）
curl -s -X POST http://127.0.0.1:8765/sources/events/resync \
  -H 'Content-Type: application/json' \
  -d '{"new_cursor": 100, "operator": "op-zhang", "reason": "x"}'

# 6) 显式确认后重同步：位点 5 -> 100，留痕可查
curl -s -X POST http://127.0.0.1:8765/sources/events/resync \
  -H 'Content-Type: application/json' \
  -d '{"new_cursor": 100, "operator": "op-zhang",
       "reason": "vendor confirmed seq 5-99 permanently lost",
       "confirm": true}'

# 7) 恢复采集：只采 seq>=100 的新记录，已投递记录不重复
curl -s -X POST http://127.0.0.1:8765/ingest/run | python -m json.tool
curl -s http://127.0.0.1:8765/sources/status | python -m json.tool
```

文件源形态同样可复现：把 `data/orders.jsonl` 截断到少于已提交位点的行数，
或改写为行内 `"seq"` 从更大值开始的记录，再触发采集即可看到
`source_moved` 与具体区间。
