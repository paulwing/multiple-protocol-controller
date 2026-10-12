# MPC 向 Judge 发布设备事件

## 对接结论

MPC 使用 Redis Pub/Sub 频道 `iot:judge:device-events` 向单实例 Judge 尽力发布设备完整快照。频道不是 Redis Key，没有 TTL、长度、积压、游标或历史查询。旧 `judge:source` Stream 不再写入。

- 发送约定：每条已入队消息最多发起一次 Redis `PUBLISH`；应用和发布客户端都不重试，不写备用 Stream、文件或数据库队列。
- 成功边界：订阅者数量大于 0，不等于 Judge 已完成判断或结果已保存。网络超时也不能确定 Redis 是否已接纳命令。
- 断线与停机：失败消息不补发，停机不保证排空内存队列；重新连接后只处理后续新消息。
- 采集影响：采集线程不等待 `PUBLISH` 或 Judge 判断，但仍要构造事件、入队和同步写实时快照。详细调用关系见[采集链路边界](../代码审查/Judge发布分支与采集链路边界审查.md)。

## 配置

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

| 环境变量 | 配置项 | 含义 |
|---|---|---|
| `JUDGE_SOURCE_ENABLED` | `judge_source.enabled` | 是否开启 Judge 发布 |
| `JUDGE_SOURCE_CHANNEL` | `judge_source.channel` | Pub/Sub 频道 |
| `JUDGE_SOURCE_WRITE_TIMEOUT_MS` | `judge_source.write_timeout_ms` | 一次发布的共同超时预算，默认 200ms，最大 2s；不含排队和 Judge 处理 |
| `JUDGE_SOURCE_WORKER_COUNT` | `judge_source.worker_count` | 共享发布 worker 数，即并发 `PUBLISH` 上限；默认 8，最大 64 |
| `JUDGE_SOURCE_QUEUE_SIZE` | `judge_source.queue_size` | 所有设备共享的最大等待消息数，默认 2048 |
| `JUDGE_SOURCE_QUEUE_MAX_BYTES` | `judge_source.queue_max_bytes` | 已接收但未完成的消息总字节上限，包含正在发布的消息；默认 16 MiB |
| `JUDGE_SOURCE_DEVICE_QUEUE_SIZE` | `judge_source.device_queue_size` | 单设备已接收但未完成的最大消息数，包含正在发布的一条；默认 64 |
| `JUDGE_SOURCE_DEVICE_QUEUE_MAX_BYTES` | `judge_source.device_queue_max_bytes` | 单设备已接收但未完成的最大 payload 字节，包含正在发布的一条；默认 1 MiB |
| `JUDGE_SOURCE_MAX_EVENT_BYTES` | `judge_source.max_event_bytes` | 单消息最大字节，上限 64 KiB |

本地配置默认关闭 Source，`12_new_mpc.sh` 部署脚本默认开启。Judge 应先启动并完成订阅，再启动 MPC，否则可能收到“无订阅者”失败。

两项单设备限制可通过脚本环境变量 `IOT_JUDGE_SOURCE_DEVICE_QUEUE_SIZE`、`IOT_JUDGE_SOURCE_DEVICE_QUEUE_MAX_BYTES` 覆盖。不填或非正数使用默认值；超过归一化后的对应全局容量时，收紧到全局容量。配置只属于 `judge_source`，不改变公共 Redis 地址、密码、DB，也不改变快照客户端、采集协议、历史存储或 Judge 服务自身配置。

Redis Pub/Sub 不按 DB0/DB1/DB2 隔离。`REDIS_DB` 仍用于 `device:data:*` 等 Key，但不能用来隔离测试和生产频道。

## 消息契约

每次 `PUBLISH` 的 payload 是一个 JSON 文档：

```json
{
  "event_id": "123e4567-e89b-42d3-a456-426614174000",
  "device_id": "device-001",
  "updated_point": "temperature",
  "collected_at": "2026-09-01T10:30:00.123Z",
  "values": {
    "temperature": 31.5,
    "humidity": 68,
    "power_state": true
  }
}
```

`values` 是嵌套 JSON 对象，不是 `"{\"temperature\":31.5}"` 这种转义字符串。Judge 因此只需对整个消息解码一次。

| 字段 | 含义与要求 |
| --- | --- |
| `event_id` | 本条事件编号，小写规范 UUIDv4 |
| `device_id` | 平台设备数据库 ID，不是设备编号 `serialNumber` |
| `updated_point` | 本次更新的属性标识，必须存在于 `values`，且对应值不为 `null` |
| `collected_at` | UTC 毫秒时间，固定为示例中的格式 |
| `values` | MPC 当时掌握的完整属性快照；值只允许 `null`、布尔、有限数字和字符串 |

顶层固定五个字段，单消息不超过配置的 `max_event_bytes`，最高 64 KiB。完整快照指“当时已收到的属性”，不代表设备所有属性都已上报。

## 性能与隔离

### 采集与发布

采集线程组装事件并尝试入队，不等待队列空位、Redis `PUBLISH` 或 Judge 处理。原有 `SET device:data:<device_id>` 仍同步执行，慢 `SET` 会影响后续采集和新事件产生。

发布器使用独立 Redis 客户端，已入队事件不用等待快照连接池或该次 `SET` 返回。两者仍共用 Redis 服务端及 MPC 的 CPU、内存，不能据此声称发布完全不影响采集。

### 顺序与容量

所有设备共用固定数量的发布 worker，不按设备创建 worker，也不把设备永久绑定到固定分片。同一设备最多有一条消息正在发布；每发完一条，还有消息的设备重新排到末尾，给其他设备执行机会。

| 限制范围 | 默认上限 | 统计口径 |
| --- | --- | --- |
| 全局等待条数 | 2048 条 | 只统计尚未交给 worker 的消息 |
| 全局消息字节 | 16 MiB | 包含等待和正在发布的消息 |
| 单设备条数 | 64 条 | 包含等待和正在发布的一条 |
| 单设备消息字节 | 1 MiB | 包含等待和正在发布的一条 |

任一额度不足，只丢弃当前新来的事件；不覆盖、不合并已接收消息，保留同设备的入队顺序。发布返回后释放占用，无论成功、超时、失败还是无订阅者；设备排空后删除其队列记录。字节额度只限制消息内容，不是整个进程的内存上限。

例如 A 已占满 64 个名额，新来的 A 事件会被丢弃；B 在自身和全局仍有额度时可以入队。多个热点设备仍可能共同用满全局容量；将单设备额度设得和全局一样，也会失去这层隔离。默认值是保护起点，不是压测得到的最优值，增大队列不能解决持续过载。

### 发布超时

worker 取出消息后创建默认 200ms 的 context。发布客户端启用 `ContextTimeoutEnabled`，连接池等待、握手和命令读写使用共同截止时间，不是每一步重新获得 200ms。内部后台拨号另有 `DialTimeout`，不保证与调用方同时取消，但不会补发已放弃的消息。

此预算不包含排队和 Judge 判断耗时；线程调度、垃圾回收等也会影响实际耗时，因此不是严格实时保证。快照 `SET` 客户端的超时、重试和连接池设置不受此配置影响。

## 失败与恢复日志

校验失败、额度不足和发布失败会结束当前事件的尝试，不触发补发，也不因这些失败主动停止后续采集。主要原因码如下：

| 原因码 | 含义 |
| --- | --- |
| `SOURCE_EVENT_INVALID` | 消息校验失败，包括内容或大小不符合要求 |
| `SOURCE_QUEUE_FULL` / `SOURCE_QUEUE_BYTES_FULL` | 全局条数或字节额度不足 |
| `SOURCE_DEVICE_QUEUE_FULL` / `SOURCE_DEVICE_QUEUE_BYTES_FULL` | 单设备条数或字节额度不足 |
| `SOURCE_NO_SUBSCRIBERS` | Redis 接纳了命令，但当时没有订阅者 |
| `REDIS_TIMEOUT`、其他 `REDIS_*` | 超时、连接、权限等 Redis 发布错误 |
| `SOURCE_PUBLISHER_UNAVAILABLE` | 发布器不可用 |
| `SOURCE_FAILURE_UNKNOWN` | 未知原因，归入固定计数，不新增无限分类 |

调用方只在短锁内更新固定大小的计数和状态、通知后台，不执行日志写入。一个独立后台协程处理首次失败、持续失败及恢复：

| 日志 `event` | 何时记录 | 如何理解 |
| --- | --- | --- |
| `first_failure` | 首次失败唤醒后台后输出 | 开始观察到这一类失败；不是采集线程同步打印 |
| `failure_summary` | 持续失败时，同一原因约每 30 秒汇总新增失败 | 后台每秒检查一次；日志输出慢时可能延后 |
| `recovered` | 对应操作阶段再次成功后输出 | 恢复范围为操作阶段，`recovery_scope=operation`，不代表所有设备恢复或 Judge 已完成判断 |

日志包含 `reason_code`、`event`、`failure_count`、`total_failure_count`。失败数量不等于确切丢失数量，例如超时后无法判断 Redis 是否已交付。成功入队不能算 Redis 发布恢复；发布返回订阅者数为 0 也不能算恢复。没有新错误但也没有成功操作时，不自动报恢复。

日志不保存事件内容、设备 ID、原始错误字符串或连接信息，不建立逐事件日志队列。写日志前释放状态锁，因此慢日志不会占着该锁阻塞采集或发布。发布器停止不等待日志输出，未输出计数可随退出丢失；原有快照、历史、协议和应用退出日志仍可能同步等待。

## 启动边界

Judge 独立客户端创建时不联网、不执行 `PING`，后台 worker 发送时才建立连接。发布器配置或构造无效时，只停用 Judge 支路、关闭其客户端并记录 `SOURCE_PUBLISHER_UNAVAILABLE`。快照客户端仍有原有的启动连接检查，Redis 不可用时采集启动仍可能失败。

## 联调顺序

1. 停止旧 MPC 发布；
2. 部署并启动新 Judge；
3. 确认 Judge 已订阅 `iot:judge:device-events` 且 ready；
4. 部署 MPC；
5. 发布一条正常数据，核对 MPC 无“0 订阅者”错误、Judge 完成判断、SSE 收到预期报警；
6. 再执行分阶段吞吐和延迟测试。
