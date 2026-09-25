# 多数据源增量采集与投递（本地可运行）

用本地 JSONL 文件和本地回环 HTTP 接口模拟两种形态的数据源，用本地 JSON 文件模拟
幂等目标端，实现一套**重复执行、数据异常、并发采集下最终状态确定且可解释**的
增量采集流程。无外部中间件依赖（仅 FastAPI/uvicorn + 标准库）。

## 1. 架构

```
main.py / demo.py                  装配与启动
app/
├── connectors/
│   ├── base.py                    连接器契约（Connector Protocol）
│   ├── file_source.py             形态一：本地 JSONL 文件数据源
│   └── http_source.py             形态二：本地回环 HTTP 分页数据源
├── http_source_app.py             回环 HTTP 数据源模拟服务（可制造 500/坏响应）
├── normalizer.py                  原始 JSON -> 统一模型的校验式归一化
├── models.py                      Record / SourceEvent / RunReport
├── errors.py                      IngestionError + FailureCode（统一对外错误）
├── checkpoint.py                  每源增量位点存储（原子写 + 回退保护）
├── target.py                      幂等文件目标端（source + record_id 去重）
├── engine.py                      采集引擎（位点语义、并发控制、失败隔离）
├── api.py                         对外 HTTP 控制接口
└── config.py                      本地演示装配与数据准备
```

## 2. 连接器契约

所有数据源实现同一协议（`app/connectors/base.py`）：

```python
class Connector(Protocol):
    name: str
    def fetch(self, after_position: int) -> Iterable[SourceEvent]: ...
    def normalize(self, event: SourceEvent) -> Record: ...
```

- `fetch(after_position)` 返回位点 **严格大于** `after_position` 的原始事件流；
  事件 `SourceEvent(position: int, raw: Any)` 的位点由数据源给出。
- `normalize(event)` 把原始事件校验、归一为统一模型 `Record`。
- 已提供两种实现：
  - **文件源** `FileSourceConnector`：JSONL，位点 = 非空行物理行号（从 1 开始）。
  - **HTTP 源** `HttpSourceConnector`：`GET /events?after=<n>` 分页拉取，
    标准库 `urllib` 发起真实回环 HTTP 请求。

统一内部模型：

```python
Record(source: str, position: int, record_id: str, payload: dict)
```

### 失败原因（可区分，禁止静默跳过）

任何必填字段缺失、类型不匹配、数据源自身报错都会产生带标准编码的
`IngestionError(code, message, source, position, cause_type)`，不允许跳过：

| 编码 | 触发条件 |
| --- | --- |
| `source_unavailable` | 文件不可读、HTTP 连接失败/超时、HTTP 5xx |
| `source_protocol`    | HTTP 响应体非法 JSON、分页结构/位点字段不符合契约 |
| `record_malformed`   | 单行不是合法 JSON，或记录不是 JSON 对象 |
| `missing_field`      | 缺少必填字段 `id`（或值为 null） |
| `type_mismatch`      | `id` 非字符串，或 `data` 非对象 |
| `position_regressed` | 位点回退（数据源回放旧位点 / 位点被外部改小） |
| `position_gap`       | 位点不连续（跳过了期望位点） |
| `target_error`       | 目标端文件读写失败 |
| `source_busy`        | 同一数据源被并发触发 |
| `source_not_found`   | 触发了未注册的数据源 |

**底层异常不透出**：`OSError`、`urllib.error.URLError`、`json.JSONDecodeError`
等原始异常被捕获后转换为对外文案，原始异常类型只以字符串记录在 `cause_type`
中（便于排障），原始堆栈与消息不会出现在对外接口。

## 3. 位点语义

- 每个数据源独立维护一个位点 `last_position`（初值 0），持久化在
  `checkpoints.json`，写入采用「同目录临时文件 + 原子 rename」。
- 位点是**数据源内严格递增、连续的整数**。期望下一位点 = `last_position + 1`：
  - 实际位点 `< 期望` → `position_regressed`，拒绝继续；
  - 实际位点 `> 期望` → `position_gap`，拒绝继续（宁可停下，不可静默丢数）。
- 存储层对位点回退有第二道保护：`advance` 不接受比当前值小的新位点。

### 投递与位点的顺序（at-least-once + 幂等）

每条记录严格按顺序执行：**先幂等投递，成功后再推进位点**。

- 正常运行：位点之后的重复触发不会再拉到旧数据。
- 崩溃窗口（已投递、位点未落盘）：重启后重放该记录，目标端按
  `(source, record_id)` 去重，计为 `skipped_replayed`，不会重复写入，
  随后位点补齐。
- 遇到畸形记录或数据源致命错误时**立即停止该源本次运行**，之前已投递的
  前缀与其位点保留；修复数据后再次触发即可从停止位置续采，不重不丢。

## 4. 并发与失败处理

- `run_all()` 使用有界线程池（`max_workers` 可配置），多个数据源**并发采集**。
- 每个数据源一把互斥锁：
  - 同一数据源被并发触发时，只有一个请求执行，其余立即得到
    `source_busy` 报告（不排队、不并行推进同一源），因此位点推进与投递
    结果保持一致、无重复写入；
  - 不同数据源互不阻塞。
- 单个数据源失败只体现在它自己的 `RunReport.fatal` 中，**不影响其它数据源
  已提交的位点与投递**。
- 目标端文件写入带进程内锁，多源并发写同一文件不会丢更新。

每次运行返回结构化 `RunReport`，并在服务端打印处理流水：

```
[fs] 开始采集，起始位点 2
[fs] 投递 pos=3 id=r3 -> 位点推进至 3
[fs] 记录失败 pos=4 code=record_malformed msg=记录不是合法 JSON: ...
[fs] 运行中止于位点 3: record_malformed
[fs] 结束采集，位点 2 -> 3，新投递 1，幂等跳过 0
```

## 5. 对外 HTTP 接口

控制 API（默认 `http://127.0.0.1:18080`）：

| 方法/路径 | 说明 |
| --- | --- |
| `POST /run` | 并发采集全部数据源 |
| `POST /sources/{name}/run` | 触发单个数据源 |
| `GET  /checkpoints` / `GET /checkpoints/{name}` | 查看位点 |
| `GET  /target/{name}` | 查看目标端记录 |

错误统一为 `{"error": {"code", "message", "source", "position", "cause_type"}}`，
未知数据源返回 404，其余业务拒绝返回 422。

模拟 HTTP 数据源（默认 `http://127.0.0.1:18081`）：

- `GET /events?after=<n>&limit=<m>` 分页返回
  `{"events": [{"position", "record"}], "next"}`；
- 连接器参数 `force_fail=True` 或请求附加 `fail=1` → HTTP 500；
- 数据行写成 `__HTTP_BAD__` → 所在页返回非法 JSON 响应体（协议错误）。

## 6. 本地验证方法

要求 Python 3.14+，使用 [uv](https://docs.astral.sh/uv/)：

```bash
uv sync

# 方式一：端到端演示脚本（自动起回环 HTTP 服务，跑完全部场景）
uv run python demo.py

# 方式二：启动服务手工调用
uv run python main.py            # 重置 .runtime/ 下演示数据并启动
uv run python main.py --keep     # 保留数据/位点，验证中断重启续采
# 另开终端：
curl -X POST http://127.0.0.1:18080/run
curl http://127.0.0.1:18080/checkpoints
curl http://127.0.0.1:18080/target/file_source

# 方式三：测试套件（含位点/流水打印，-s 查看）
uv run pytest -s
```

测试覆盖：

1. 首次采集、重复触发、追加续采；
2. 崩溃窗口位点回退重放（幂等、不重不丢）；
3. 位点缺口拒绝、位点回退拒绝；
4. 四类畸形记录的失败原因可区分，修复后前缀续采；
5. 多源并发下的局部失败隔离；
6. 同一数据源并发触发的 busy 拒绝与无重复写入；
7. HTTP 500 / 连接失败（`source_unavailable`）、非法响应体（`source_protocol`）；
8. 底层异常不透出、未知数据源结构化报错。

运行期数据位于 `.runtime/`（git 忽略）：`data/*.jsonl`、`checkpoints.json`、
`target.json`。
