# Edge Ingest Gateway（边缘采集网关）

用本地文件模拟边缘网关的**采集缓冲**与**上报位点**，让设备上报在「并发、乱序、中断恢复」下得到一致、可解释的结果。

- 多格式接收与归一；非法上报不静默丢弃，返回可区分的拒绝原因码。
- 缓冲落盘 + 断线续传；重复、乱序、进程重启都不重不丢；位点回退明确拒绝。
- 有界缓冲背压；并发写不丢不串、不阻塞采集主流程；时钟回拨不错误覆盖。

---

## 1. 统一上报模型

所有接入格式归一为 `UnifiedReport`（`model/UnifiedReport.java`）：

| 字段 | 含义 |
| --- | --- |
| `deviceId` | 设备标识 |
| `metric` | 指标名 |
| `value` | 数值（拒绝 NaN/Infinity） |
| `businessTime` | 业务时间（`Instant`），毫秒精度，作为补发排序与去重依据 |
| `receivedAt` | 网关接收时间（由网关时钟产生） |
| `sourceFormat` | 来源格式：`json-v1` / `json-v2` / `line` |
| `dedupKey` | 去重键，见下 |

### 支持的三种上报格式

`POST /ingest/{format}`

- `json-v1`
  ```json
  {"deviceId":"d1","metric":"temp","value":21.5,"ts":1700000000000}
  ```
- `json-v2`（指标放在 `readings`，取第一个；时间为 ISO-8601）
  ```json
  {"dev_id":"d1","ts_iso":"2023-11-14T22:13:20Z","readings":{"temp":21.5}}
  ```
- `line`（设备,指标 空格 value=数字 空格 时间戳）
  ```
  d1,temp value=21.5 1700000000000
  ```

**时间戳规则**：纯数字按 epoch 解析，小于 `1e12` 按秒、否则按毫秒；非数字按 ISO-8601。
合法范围限定为 `2000-01-01 ~ 2100-01-01`，超范围与无法解析一律判为 `ILLEGAL_TIMESTAMP`。

### 拒绝原因（不静默丢弃，不抛栈给调用方）

| reasonCode | 含义 | HTTP |
| --- | --- | --- |
| `EMPTY_BODY` | 请求体为空 | 422 |
| `MALFORMED_FORMAT` | 报文无法解析 / 格式未知 / line 段数不对 | 422 |
| `MISSING_DEVICE_ID` | 缺 deviceId / dev_id | 422 |
| `MISSING_METRIC` | 缺 metric / readings | 422 |
| `MISSING_VALUE` | 缺 value | 422 |
| `ILLEGAL_VALUE` | value 不是合法数字（含 NaN/Infinity） | 422 |
| `MISSING_TIMESTAMP` | 缺业务时间戳 | 422 |
| `ILLEGAL_TIMESTAMP` | 时间戳无法解析或超出范围 | 422 |
| `DUPLICATE` | 重复上报，已按 dedupKey 去重 | 409 |
| `BUFFER_FULL` | 本地缓冲已满（背压），请稍后重试 | 503 |

正常接收返回 `202 Accepted`，响应体含分配的 `seq`。

---

## 2. 去重与续传规则

### 去重键

```
dedupKey = deviceId + "#" + metric + "#" + businessTime(毫秒)
```

同一设备、同一指标、同一毫秒的多次上报视为同一个点位，只保留第一次，后续判 `DUPLICATE`。
去重集合同时覆盖「待发送」与「已送达」两类数据，因此**进程重启后已送达的点仍会被去重**。

### 补发顺序

`UploadCoordinator.pumpOnce()` 每轮：
1. 从缓冲按 **(businessTime, seq) 升序** 读一批（只读，不推进位点）；
2. 整批投递远端；远端幂等，失败后整批重试不产生重复；
3. 投递成功才提交位点（写 `delivered.log` + 原子更新 `watermark.dat`）。

乱序到达的点在下一批次排序后按业务时间补发；晚到的点不会被丢弃。

### 不重不丢的崩溃窗口

「投递成功但提交位点前」崩溃：重启后位点未推进 → 重投整批 → 远端按 dedupKey 幂等去重 → 最终**不重不丢**。

### 位点（watermark）与回退保护

- 位点 = 已确认送达的最大序号，存于 `watermark.dat`，以 `tmp + 原子替换` 落盘。
- 位点**只能单调向前**：
  - 运行时请求回退 → `OffsetStoreException: 拒绝位点回退`；
  - 启动时若发现 `watermark < delivered.log 中的最大序号`（位点文件被改旧）→ **拒绝启动**，避免从旧位点继续造成重复；
  - 若 `watermark > 已分配最大序号`（位点文件损坏/超前）→ 同样拒绝启动。

---

## 3. 缓冲与背压策略

本地缓冲目录（默认 `build/spool`，可用 `edge.gateway.spool-dir` 覆盖）含三个文件：

| 文件 | 内容 | 写方式 |
| --- | --- | --- |
| `pending.log` | 每行一条未送达记录（行式 JSON：seq + 统一模型） | append + fsync |
| `delivered.log` | 每行 `seq\|dedupKey`，已确认送达 | append + fsync |
| `watermark.dat` | 当前位点（最大已送达序号） | tmp + 原子替换 |

- **序号 seq**：独立持久计数器，启动时由两个日志重放得出 `max(pendingMax, deliveredMax)` 继续递增；
  它与墙钟无关，**时钟回拨不会导致序号倒退或文件覆盖**。
- **并发写**：追加在实例锁内串行，单次仅一次顺序写 + fsync，耗时有界；写满立即抛 `BUFFER_FULL`，
  用**拒绝**表达背压而不是无限阻塞采集主流程。
- **有界**：容量由 `edge.gateway.capacity` 控制（默认 1000，按未送达条数计）。
- **时钟**：`SystemGatewayClock` 内置单调保护，物理时钟回拨时读数不倒退；
  生产网关时间仅用于 `receivedAt`，不参与序号与覆盖判定。
- **恢复**：构造时重放日志重建待发集合、去重集合、序号与位点并做一致性校验。

---

## 4. 配置（application.properties 前缀 `edge.gateway`）

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `edge.gateway.spool-dir` | `build/spool` | 缓冲目录 |
| `edge.gateway.capacity` | `1000` | 未送达条数上限 |
| `edge.gateway.batch-size` | `100` | 单次补发批大小 |
| `edge.gateway.pump-interval-millis` | `500` | 定时补发间隔 |

链路恢复后由 `PumpScheduler` 自动补发，无需人工触发。

---

## 5. 验证方法

```bash
mvn test
```

测试均在标准输出打印「关键输入 + 判定依据」，便于复现。共 35 个用例：

| 测试类 | 覆盖场景 |
| --- | --- |
| `NormalizerTest` | 三种格式正常归一；秒/毫秒自适应；空体、缺字段、非法 value、非法/越界时间戳、截断 JSON、line 缺段、未知格式等全部拒绝原因 |
| `SpoolBufferTest` | 重复判重；乱序按业务时间读取；缓冲写满 `BUFFER_FULL`；8 线程 ×50 并发写无丢失无串；时钟回拨序号仍单调；重启恢复 pending/去重/位点；运行时与位点文件被改旧两种回退都被拒绝；幂等提交；批次大小 |
| `ResumeIntegrationTest` | 在线直发 → 断线 4 条乱序只落本地 → 恢复按业务时间补发且不重不丢；断线期重复上报只送达一次；投递后崩溃重启重投被云端幂等去重；断线 pump 停止、恢复后 drain |
| `IngestControllerWebTest` | 202 / 409 / 422（含具体 reasonCode）/ 503 状态码与响应体 |

手工冒烟（可选）：

```bash
mvn spring-boot:run
# 正常
curl -i -X POST localhost:8080/ingest/json-v1 -H 'Content-Type: application/json' \
  -d '{"deviceId":"d1","metric":"temp","value":21.5,"ts":1700000000000}'
# 非法时间戳 -> 422 reasonCode=ILLEGAL_TIMESTAMP
curl -i -X POST localhost:8080/ingest/json-v1 -H 'Content-Type: application/json' \
  -d '{"deviceId":"d1","metric":"temp","value":1,"ts":"bad"}'
```
