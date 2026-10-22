# multiple-protocol-controller

Go 工业设备采集与控制服务，提供 Modbus RTU、OPC UA、MQTT、BACnet/IP 的采集与控制。当前 Modbus 使用 TCP 连接传输 RTU 帧；协议工厂注册 `MODBUS_RTU`，不应将连接管理中的 TCP 命名理解为已实现原生 Modbus TCP/MBAP。读取平台发布的 Redis 配置，写实时快照、网关健康状态、可选 InfluxDB 历史和 Judge Source。平台属性控制 HTTP 已由主后台桥接 Redis，MPC 不直接承担用户鉴权或页面功能。

工具链以 go.mod 为准：`go 1.23.0`，`toolchain go1.24.7`。运行前配置 `configs/config.toml` 或环境变量并保证 Redis 可达。

文档：[项目结构](PROJECT_SUMMARY.md)、[系统 MPC 架构](../documents/architecture/mpc.md)、[Judge Source 契约](docs/对接文档/MPC规则事件发布.md)、[部署指南](../documents/integration/server-deployment-guide.md)。跨仓库链接要求相邻检出。

GitLab 主源 `https://49.233.192.84:18443/new-iot/multiple-protocol-controller.git`，本地/GitLab 主分支 `master`；GitHub 备份主分支为 `main`（推送 `master:main`）。推送前先拉取并合并双端新增提交。

### 验证

```sh
go test ./...
```

测试不是实设备联调；真实读写、写后回读及 Judge 服务端需要单独验收。

### 配置刷新

后端写入 `IOT:DEVICE` 后，在 `CFG_CHANGE` 发布固定 payload `IOT:DEVICE`；当前订阅器每次收到该 payload 都重新加载，不能按 payload 相同跳过后续通知。Redis DB 也必须与平台一致，见[发布契约](../documents/integration/device-publish-redis.md)。

### 设备控制

MPC 在 `set_device_current_value` 接收控制命令，并在 `device_current_value_response` 发布结果。只有属性 `access` 支持写操作且存在有效协议写点位时，运行时才会生成控制映射；只读属性即使错误携带写点位，也不会被执行。

控制结果按属性返回：

- `verified`：协议写入成功，且可读写属性的回读值在超时前与目标值一致；
- `unverified`：指令已成功下发，但属性只写或协议无法可靠回读，设备最终状态不可验证；
- `failed`：写入、协议回执、回读或目标值验证失败。

Modbus、OPC UA 和 BACnet 的可读写属性会执行写后回读；MQTT 发布成功按 `unverified` 返回。回执保留 `control_command_result`，同时包含设备级 `verification_status` 与逐属性 `attributes`，已由平台后端消费。平台 HTTP 控制接口、用户权限、设备 scope 和操作记录由 `iot-platform-backend` 负责，操作说明见[设备控制手册](../documents/user-manual/device-control.md)。

### 点位类型与网关健康

四协议的每个读/写项可独立配置可选 `dataType`；缺失、null 或空值沿用属性 `type`。新显式类型执行对应协议的校验与转换，旧配置保留回退语义。属性枚举与实际编码类型分开，历史 `data_type` 仍记录业务类型；支持范围和字段位置见[MPC 架构](../documents/architecture/mpc.md#点位类型与属性类型)。

纯 Modbus 网关的在线状态由独立发布器维护 `gateway:status:{id}`，与子设备采集成败无关。Linux 使用不消费业务数据的 socket 检查；其他平台返回 Unknown，不应据此进行生产在线状态验收。目标 ID 与探测结果不一致或代次过期时丢弃本轮。格式、TTL、限制及配套顺序见[实时 Redis 对接](../documents/integration/realtime-data-redis.md)。

### How to run
- local
```
air
```
- production
```
go build -o ./multiple-protocol-controller ./cmd
./multiple-protocol-controller
```

### Jenkins Image Package

`scripts/package-image.sh` is the Jenkins image packaging script template. It supports amd64 and arm64 by environment variables.

```sh
# amd64
ARCH_NAME=linux-amd64 IMAGE_PLATFORM=linux/amd64 ./scripts/package-image.sh

# arm64
ARCH_NAME=linux-arm64 IMAGE_PLATFORM=linux/arm64 ./scripts/package-image.sh

# arm64 on amd64 Jenkins with buildx
ARCH_NAME=linux-arm64 IMAGE_PLATFORM=linux/arm64 USE_BUILDX=true ./scripts/package-image.sh
```

Output examples:

```text
new_mpc-basic_sys-linux-amd64.tar
new_mpc-basic_sys-linux-arm64.tar
```

Generated app image packages are uploaded to:

```text
NewFramework/apps/${ARCH_NAME}/
```

The script loads `alpine:3.23.4-${ARCH_NAME}` from `NewFramework/base-images/${ARCH_NAME}/` and verifies the loaded base image architecture before building.

### Device Params

`DeviceConfig.params` stores device-level special parameters as a list. Each item is matched by `key`, and `value` is converted according to the runtime use case.

```json
"params": [
  {
    "label": "从机ID",
    "key": "slaveID",
    "type": "string",
    "value": "1"
  },
  {
    "label": "超时时间",
    "key": "timeout",
    "type": "int",
    "value": 3000
  }
]
```

Current runtime keys:

| key | Purpose | Effect |
| --- | --- | --- |
| `slaveID` / `SlaveID` | Modbus slave ID | Device address in Modbus read/write frames |
| `timeout` / `responseTimeout` / `responseTimeoutMs` | Device response timeout in milliseconds | `DeviceRuntime.ResponseTimeoutMs`, used by collection and control waits |
| `deviceInstance` | BACnet device instance | Target device for BACnet read/write operations |
| `securityPolicy` | OPC UA security policy | OPC UA connection settings |
| `securityMode` | OPC UA security mode | OPC UA connection settings |
| `clientId` | MQTT client ID | MQTT connection |
| `username` | MQTT username, overrides device-level `Username` | MQTT connection |
| `password` | MQTT password, overrides device-level `Password` | MQTT connection |
| `qos` | MQTT default QoS, valid values are `0`-`2` | MQTT subscribe/publish default |
| `keepAlive` | MQTT keep alive in seconds | MQTT connection |
| `cleanSession` | MQTT clean session flag | MQTT connection |

Property-level protocol configuration is not read from `params`. It is read from each property's `protocol` field, such as Modbus function code and address, OPC UA `nodeId`, MQTT topic/path, and BACnet object configuration.

MQTT 采集按 broker/clientId 共享连接，每个 worker 独立登记订阅；同一 filter 可服务多个设备，配置重载只释放该 worker 的登记。Paho 负责主题匹配，每条订阅映射仍按自己的 path/dataType 写入。首次连接失败、订阅拒绝和断线后会恢复当前有效订阅；订阅等待超时会在没有进行中控制发布时重建连接。采集队列仍为 10 条，满队列保留原丢弃并记录日志的行为，不提供消息持久化或补发。

最后一个采集使用者退出时，连接在已有控制发布结束后释放；当前连接可用时会先尝试有界退订。`cleanSession=false` 且退出时离线或退订未确认，broker 的持久会话可能保留旧订阅，MPC 不提供跨退出清理补偿。相同 broker/clientId 的账号、keepAlive、cleanSession 必须一致；点位 QoS 为 0 时仍继承设备默认 QoS，共享 filter 使用有效订阅请求的最高 QoS。

共享连接更新参数时，新采集任务遇到仍在使用旧参数的连接，会每秒重试登记，直到旧任务和已有控制发布释放连接；停止任务会取消等待。只重试连接参数冲突，不抢占旧连接、不重发控制命令；若配置本身一直相互冲突，也不会强行覆盖其他任务的连接。

### History Data

MPC can write collected property values to InfluxDB for historical queries.

Configuration:

```toml
[influx]
enabled = false
url = "http://127.0.0.1:8086"
token = ""
org = "iot"
bucket = "device_history"
timeout_seconds = 3
batch_size = 100
flush_interval_ms = 500
queue_size = 10000
retry_count = 3
retry_interval_ms = 200
```

Environment overrides:

```sh
INFLUXDB_ENABLED=true
INFLUXDB_URL=http://iot-influxdb:8086
INFLUXDB_TOKEN=iot-influxdb-local-token
INFLUXDB_ORG=iot
INFLUXDB_BUCKET=device_history
INFLUXDB_TIMEOUT_SECONDS=3
INFLUXDB_BATCH_SIZE=100
INFLUXDB_FLUSH_INTERVAL_MS=500
INFLUXDB_QUEUE_SIZE=10000
INFLUXDB_RETRY_COUNT=3
INFLUXDB_RETRY_INTERVAL_MS=200
```

When enabled, each successful realtime Redis write enqueues one `device_history` point. A fixed background worker writes queued points to InfluxDB in batches. If InfluxDB is disabled or incomplete, MPC only writes Redis realtime data and skips history writes.

Batching behavior:

```text
collected value -> Redis realtime snapshot -> history queue -> batched InfluxDB write
```

The writer flushes when the batch reaches `batch_size` or `flush_interval_ms` elapses. Failed batches are retried up to `retry_count` times with `retry_interval_ms` between attempts. If `queue_size` is full, MPC drops new history points and logs the dropped count; realtime Redis writes are not blocked.

InfluxDB tags:

```text
device_id, property_key, serial_number, property_id, property_name, protocol, unit, data_type
```

InfluxDB fields:

```text
value_number, value_bool, value_string
```

### Judge Rule Events

MPC publishes collected property changes to the Judge Redis Pub/Sub channel `iot:judge:device-events`. Local configuration remains disabled by default, while `12_new_mpc.sh` enables the publisher unless `IOT_JUDGE_SOURCE_ENABLED=false` is set explicitly.

Both MPC and Judge deployment scripts read the shared `IOT_JUDGE_SOURCE_CHANNEL` from `project.sh` (default `iot:judge:device-events`). Set it once for both containers; changing only one side disconnects live evaluation. Judge's admission timeout is configured separately with `IOT_JUDGE_SOURCE_ADMISSION_TIMEOUT` (default `5s`).

MPC and Judge must use the same Redis address. The logical database still selects the realtime snapshot/device catalog keys, but Redis Pub/Sub itself is not isolated by DB number.

```toml
[judge_source]
enabled = false
channel = "iot:judge:device-events"
write_timeout_ms = 200
worker_count = 8
queue_size = 2048
queue_max_bytes = 16777216
device_queue_size = 64
device_queue_max_bytes = 1048576
max_event_bytes = 65536
```

Environment overrides:

```text
JUDGE_SOURCE_ENABLED
JUDGE_SOURCE_CHANNEL
JUDGE_SOURCE_WRITE_TIMEOUT_MS
JUDGE_SOURCE_WORKER_COUNT
JUDGE_SOURCE_QUEUE_SIZE
JUDGE_SOURCE_QUEUE_MAX_BYTES
JUDGE_SOURCE_DEVICE_QUEUE_SIZE
JUDGE_SOURCE_DEVICE_QUEUE_MAX_BYTES
JUDGE_SOURCE_MAX_EVENT_BYTES
REDIS_DB
```

The collector creates one complete JSON document with nested `values`, then tries a non-blocking enqueue. A fixed-size worker pool dynamically takes ready devices from one shared scheduler. Only one publication from a device may be in flight at a time, so that device stays ordered without being permanently pinned to a worker. After one publication, a device with more pending events returns to the end of the ready-device queue so other devices get a turn. A message gets at most one Redis `PUBLISH` attempt; both application retry and Redis client command retry are disabled, and pending events are not guaranteed to drain on shutdown.

The Judge publisher uses an isolated Redis client, so already-enqueued Source events do not wait for the snapshot client's connection pool or for a collector's `SET device:data:<device_id>` call to return. The collector still performs that `SET` synchronously; a slow `SET` can delay collection and the production of subsequent events. Both clients also share the configured Redis server. `worker_count` limits concurrent `PUBLISH` calls; it is not a device count or a permanent partition count.

Admission checks both global budgets (2,048 waiting items and 16 MiB of accepted-but-not-finished payloads by default) and per-device budgets (64 accepted-but-not-finished items and 1 MiB by default). Per-device counts and bytes include the in-flight publication and are released on every publication outcome. Exceeding any budget drops only the arriving Judge event; existing FIFO entries are not merged or overwritten. Nonpositive device limits use defaults; limits above the corresponding normalized global budget are clamped to it. These defaults bound a single hot device's footprint but do not guarantee capacity for every device under sustained aggregate overload. Docker overrides use `IOT_JUDGE_SOURCE_DEVICE_QUEUE_SIZE` and `IOT_JUDGE_SOURCE_DEVICE_QUEUE_MAX_BYTES`.

Judge client construction does not connect or `PING`; publisher workers connect on demand. Network failures drop the current event without stopping acquisition, and subsequent events can try normally without replaying failed events. Invalid publisher setup disables only the Judge branch and records `SOURCE_PUBLISHER_UNAVAILABLE`. The existing snapshot client's startup check is unchanged: this does not make acquisition independent of Redis itself.

`write_timeout_ms` (default 200, maximum 2,000) supplies one publication context deadline beginning when a worker takes an event, not at enqueue. The Judge client enables `ContextTimeoutEnabled`, so handshake and command I/O honor that deadline instead of obtaining a fresh full timeout at each stage; pool waits also observe the context. The pool's background dialing uses its own `DialTimeout` and need not stop at the exact moment a caller times out; it does not replay that caller's event. Per-stage read/write/pool timeout settings remain as fallback limits. This is a publication-call budget, not a hard real-time bound or a deadline for Judge rule completion. It does not alter the snapshot client's timeout, retry, or pool settings.

Queue full, byte limit, timeout, Redis error, zero subscribers, invalid payload, or oversize payload ends the current Judge event's attempt without retry. Callers update fixed-size counters and state under a short lock; logging runs on one independent background worker after releasing that lock. It reports the first failure, summarizes continuing failures about every 30 seconds, and reports recovery only after a successful operation in the corresponding stage. Recovery does not confirm every device's health or Judge processing. The worker never retains event payloads or builds a log-event queue. A slow log sink can delay reporting without holding the state lock or blocking publisher shutdown; pending counters may be lost on exit. Existing snapshot, history, and protocol logs may still be synchronous. See the [publication contract](docs/对接文档/MPC规则事件发布.md) for reason codes and log fields.

Configuration and message requirements are maintained in the [publication contract](docs/对接文档/MPC规则事件发布.md). The [collection boundary reference](docs/代码审查/Judge发布分支与采集链路边界审查.md) separates publisher guarantees, shared resource costs, and pre-existing acquisition behavior; it is not a performance or deployment acceptance report.

Pub/Sub is at-most-once: it has no Redis key, backlog, replay, acknowledgement, or offline retention. MPC never writes a fallback Stream, file, or database queue. Judge downtime or a broken subscription therefore loses messages during the gap by design.

Production activation:

1. Stop old MPC publication and deploy the Pub/Sub Judge first.
2. Wait until Judge has subscribed to `iot:judge:device-events` and is ready.
3. Deploy MPC with `12_new_mpc.sh`; Judge Source is enabled by default.
4. Verify that `PUBLISH` reports an active subscriber and that alarm SSE is visible.
5. Set `IOT_JUDGE_SOURCE_ENABLED=false` only when an explicit rollback requires MPC to stop producing Source events.
