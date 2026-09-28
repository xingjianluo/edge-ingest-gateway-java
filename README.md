# 边缘数据采集网关（断线续传演示）

用**本地文件存储**模拟设备上报的采集缓冲与上报位点，目标是在**并发、乱序、断连与进程重启**
情况下，结果仍然一致、可解释：不重复、不丢失、拒绝必有明确原因。

## 1. 总体流程

```
设备 HTTP 上报(JSON)
        │
        ▼
 IngestService
   1. 解析 JSON（非法 → MALFORMED_JSON）
   2. 按格式选择 Normalizer 归一化（无法识别 → UNSUPPORTED_FORMAT）
   3. 字段校验（缺字段/时间戳非法/指标非数值 → 对应拒绝原因）
   4. 时间范围校验（非正数/过远未来 → TIMESTAMP_OUT_OF_RANGE）
   5. LocalFileEventStore.append
        ├─ 幂等键已存在 → accepted=true, duplicate=true（返回原 seq，不再落盘）
        ├─ 缓冲已满     → BUFFER_FULL（背压拒绝）
        ├─ 锁等待超时   → WRITE_CONGESTION（背压拒绝）
        └─ 成功         → 分配单调 seq，spool + dedup 顺序 fsync 落盘
        │
        ▼（断连期间数据留在本地，不影响采集）
 RelayService 后台轮询
   loadPending() 按业务时间 eventTime 升序（同时间按 seq）
   → UpstreamClient.publish（云端按幂等键去重，at-least-once 安全）
   → 连续确认前缀推进 checkpoint（frontier 只进不退，回退明确拒绝）
```

## 2. 统一上报模型

三种报文格式归一到同一个 `UnifiedEvent`：

| 统一字段 | 含义 |
|---|---|
| `idempotencyKey` | 幂等键，去重与云端幂等的唯一依据 |
| `deviceId` | 设备标识 |
| `eventTimeMillis` | **业务时间**（设备采样时间），补发排序依据 |
| `reportId` | 设备侧报文号，可空 |
| `metrics` | `指标名 -> 数值`，至少一个 |
| `attributes` | 附加属性（不参与去重指纹） |

### 受支持格式

**标准格式**
```json
{"deviceId":"dev-001","ts":1727500000000,"reportId":"r-1",
 "metrics":{"temp":23.5},"attributes":{"site":"A"}}
```
- 设备字段：`deviceId` / `device`；时间字段：`ts` / `timestamp`
- 时间戳支持 **epoch 毫秒数字**、**纯数字字符串**、**ISO-8601 字符串**（如 `2024-09-28T12:26:40.000Z`）

**扁平格式**（保留字段外的顶层数值全部视为指标）
```json
{"device":"dev-002","time":"2024-09-28T12:26:40.000Z","temp":24.1,"rpm":1200,"msgId":"f-1"}
```

**探针 v2 批量格式**（一个报文聚合为一条事件，业务时间取最早读数时间）
```json
{"ver":"2","dev":{"id":"dev-003","loc":"rack-7"},"reportId":"p-1",
 "data":[{"k":"temp","v":30.2,"ts":1727500002000},{"k":"humi","v":60,"ts":1727500002000}]}
```

### 拒绝原因（HTTP 200 体中的 `rejectReason`，绝不静默忽略）

| 原因 | 触发条件 |
|---|---|
| `MALFORMED_JSON` | 请求体为空或不是合法 JSON |
| `UNSUPPORTED_FORMAT` | 不是 JSON 对象 / 匹配不到任何受支持格式 / 探针 data 元素非对象 |
| `MISSING_DEVICE_ID` | 缺设备标识或为空 |
| `MISSING_TIMESTAMP` | 缺业务时间戳字段 |
| `MISSING_METRICS` | 缺 metrics、metrics 为空对象、探针 data 为空或缺指标名 |
| `INVALID_TIMESTAMP_FORMAT` | 时间戳既不是 epoch 毫秒也不是合法 ISO-8601 |
| `TIMESTAMP_OUT_OF_RANGE` | 时间戳 ≤ 0，或超过 `当前时间 + max-future-skew-millis`（默认 5 分钟） |
| `INVALID_METRIC_VALUE` | 指标值不是数值 |
| `BUFFER_FULL` | 本地缓冲已达容量上限（背压） |
| `WRITE_CONGESTION` | 并发写锁等待超过 `write-lock-timeout-millis`（背压） |

返回体形如：
```json
{"accepted":false,"duplicate":false,"idempotencyKey":null,"ingestSeq":-1,
 "rejectReason":"MISSING_TIMESTAMP","detail":"[standard] missing timestamp field, expected one of: ts/timestamp"}
```

## 3. 续传与去重规则

### 幂等键
1. 报文带 `reportId`（扁平格式为 `msgId`）：`deviceId + "#" + reportId`，**优先**；
2. 否则使用业务指纹：`deviceId|eventTime|按名排序后的指标键值`。
   同一设备、同一业务时间、同一组指标值的重复上报判重；仅属性差异不构成新数据点；
   指标值不同则视为不同事件。

重复上报返回 `accepted=true, duplicate=true` 及**首次入库的 seq**，不会二次落盘。

### 补发顺序
`loadPending()` 始终按 **业务时间 `eventTimeMillis` 升序**返回，业务时间相同按入库
`seq` 升序保证全序。乱序到达的报文在补发阶段自动重排。

### 位点（checkpoint）
- `frontier` = 已**连续确认**的最高入库序号，持久化于 `checkpoint` 文件（原子替换）；
- 每轮补发允许乱序成功：成功的 seq 先进入确认集合，只把从 `frontier+1` 开始连续的前缀
  一次性推进；中间有空洞时位点绝不越过（空洞对应的数据仍在本地，下轮继续发）；
- **位点回退明确拒绝**：`advanceCheckpoint(x)` 在 `x <= frontier` 时抛
  `CheckpointRollbackException`（HTTP 层映射为 409），不允许"从中间重新补发"造成重复；
- 断连时立即停止本轮：当前事件未确认、留在本地；恢复后从 `frontier+1` 继续。

### 崩溃一致性
追加写入顺序：**先 fsync `spool.log`，再 fsync `dedup.idx`，最后返回成功**。重启恢复时：
- 以 `spool.log` 为数据真相源，`seq > frontier` 的记录全部重新装载（不丢）；
- `dedup.idx` 中指向不存在 spool 记录的悬空条目直接丢弃（对应崩溃发生在 spool 之前）；
- spool 中存在但 dedup 缺失的记录，用 spool 重建索引（对应崩溃发生在两次写之间），
  恢复后旧数据仍可判重；
- `checkpoint` 通过临时文件 + 原子移动更新。

### 时钟回拨
入库 `seq` 来自持久化的递增计数器（启动时由落盘数据重建），**与墙上时钟无关**；
墙上时钟只作为 `wallClock` 排查字段落盘，不参与任何去重/覆盖/位点判定，时钟回拨或极早
时间戳的事件都只会拿到新的递增序号，不会覆盖任何数据。

## 4. 本地存储布局

目录由 `edge.gateway.data-dir` 指定（默认 `data/edge-gateway/`）：

| 文件 | 内容 | 维护方式 |
|---|---|---|
| `spool.log` | JSONL 每行一条待发记录（seq/key/deviceId/eventTime/wallClock/payload） | 追加写 + fsync；位点推进累计 256 条后重写压缩，仅留未发数据 |
| `dedup.idx` | JSONL 每行 `幂等键 -> seq` | 追加写 + fsync；压缩时重写但**不删键**（去重窗口覆盖本地生命周期） |
| `checkpoint` | 单行数字 frontier | 临时文件 + 原子替换 |

## 5. 缓冲与背压策略

- **有界**：缓冲容量 `edge.gateway.buffer-capacity`（默认 10000 条）。占满时新的上报立即
  以 `BUFFER_FULL` 拒绝并附带当前占用/容量，设备可在补发排空后重试——**不阻塞、不静默丢**。
- **并发**：所有结构性变更由一把**公平可超时** `ReentrantLock` 保护；写入是快速的顺序
  追加（fsync），临界区极短。
- **不阻塞采集主流程**：获取锁最多等待 `write-lock-timeout-millis`（默认 50ms），超时以
  `WRITE_CONGESTION` 降级拒绝并说明原因。
- **不丢不串**：被接受的写在锁内分配唯一 seq 并与缓冲记录一一对应；并发测试断言
  「接受数 = 不同 seq 数 = 缓冲记录数」「每次写都有明确结论（接受或拒绝）」。
- 补发释放槽位（frontier 前进）后缓冲自动恢复可写。

## 6. HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `POST /api/reports` | 上报，body 为原始报文 JSON，返回统一结论 |
| `POST /api/upstream/available` | 演示/测试用，`{"available":false}` 模拟断连 |
| `POST /api/relay/pump` | 立即执行一轮补发，返回 `attempted/delivered/interrupted/frontierAfter` |
| `GET /api/health` | `upstreamAvailable / pending / capacity / frontier / relayRunning` |

## 7. 验证方法

### 自动化测试（13 个用例，全部打印关键输入与判定依据）

```bash
mvn test
```

| 测试类 | 覆盖场景 |
|---|---|
| `NormalizationTest` | 三种格式归一到统一模型；11 类非法输入各自给出可区分拒绝原因 |
| `DedupAndOrderingTest` | reportId 去重、跨格式业务指纹去重；乱序到达按业务时间补发；断连/恢复零重复 |
| `BufferBackpressureTest` | 缓冲写满 BUFFER_FULL；8 线程并发写不丢不串、每次写都有结论；位点回退/重复确认被拒；锁超时 WRITE_CONGESTION |
| `RecoveryTest` | 重启后从磁盘恢复位点与未发数据、重放判重、回退拒绝；时钟回拨安全；崩溃在 spool/dedup 两次落盘之间的现场重建 |
| `ApplicationTests` | HTTP 端到端：断连期间乱序上报落本地（含重复与非法报文），恢复后后台按业务时间补发、云端无重复无丢失 |

用例中的 `System.out.printf` 会输出形如 `[去重] ...`、`[背压] ...`、`[恢复] ...` 的
关键输入与判定依据；也可查看
`target/surefire-reports/...*Output.txt` 或加 `-Dsurefire.useFile=false` 直接在控制台观察。

### 手工验证

```bash
mvn spring-boot:run

curl -s -XPOST localhost:8080/api/upstream/available -H 'Content-Type: application/json' -d '{"available":false}'
curl -s -XPOST localhost:8080/api/reports -H 'Content-Type: application/json' \
  -d '{"deviceId":"dev-1","ts":1727500003000,"reportId":"e3","metrics":{"v":3}}'
curl -s -XPOST localhost:8080/api/reports -H 'Content-Type: application/json' \
  -d '{"device":"dev-1","time":1727500001000,"v":1,"msgId":"e1"}'
curl -s localhost:8080/api/health          # pending=2, frontier=0
# 直接 kill 进程再重启：观察 data/edge-gateway/ 下文件被恢复
curl -s -XPOST localhost:8080/api/upstream/available -H 'Content-Type: application/json' -d '{"available":true}'
curl -s -XPOST localhost:8080/api/relay/pump   # 按 1000 -> 3000 业务时间补发
curl -s localhost:8080/api/health          # pending=0, frontier=2
```

## 8. 配置项

| 配置 | 默认值 | 说明 |
|---|---|---|
| `edge.gateway.data-dir` | `data/edge-gateway` | 本地缓冲目录 |
| `edge.gateway.buffer-capacity` | `10000` | 缓冲容量（条） |
| `edge.gateway.write-lock-timeout-millis` | `50` | 写锁最长等待，超时背压拒绝 |
| `edge.gateway.max-future-skew-millis` | `300000` | 允许的未来时间偏移 |
| `edge.gateway.relay-interval-millis` | `200` | 后台续传轮询间隔 |
