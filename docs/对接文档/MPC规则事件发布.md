# MPC 向 Judge 发布设备事件

## 对接结论

MPC 使用 Redis Pub/Sub 频道 `iot:judge:device-events` 向单实例 Judge 尽力发布设备完整快照。频道不是 Redis Key，没有 TTL、长度、积压、游标或历史查询。旧 `judge:source` Stream 不再写入。

该链路采用“至多一次”：MPC 每条消息最多尝试一次 `PUBLISH`，失败后只累计失败计数并放弃，后台汇总日志，不重试、不补写。采集线程不等待发布网络请求或 Judge 判断；原有同步 SET 等边界见下文。停机时不保证排空待发消息。

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
| `JUDGE_SOURCE_WRITE_TIMEOUT_MS` | `judge_source.write_timeout_ms` | worker 取出消息后的一次发布 context 预算，默认 200ms、最大 2s；客户端读写启用共同截止时间，连接池等待也观察 context；不包含排队或 Judge 处理耗时 |
| `JUDGE_SOURCE_WORKER_COUNT` | `judge_source.worker_count` | 共享发布 worker 数，也就是同时执行的 `PUBLISH` 上限；不是设备数，也不是固定分片数 |
| `JUDGE_SOURCE_QUEUE_SIZE` | `judge_source.queue_size` | 所有设备共享的最大等待消息数；2048 条在 2 万条/秒时约等于 102 毫秒的缓冲 |
| `JUDGE_SOURCE_QUEUE_MAX_BYTES` | `judge_source.queue_max_bytes` | 已接收但尚未发布完成的消息最大总字节，包含正在执行的消息，防止大消息把内存吃满 |
| `JUDGE_SOURCE_DEVICE_QUEUE_SIZE` | `judge_source.device_queue_size` | 单设备已接收但未完成的最大消息数，包含正在发布的一条；默认 64 |
| `JUDGE_SOURCE_DEVICE_QUEUE_MAX_BYTES` | `judge_source.device_queue_max_bytes` | 单设备已接收但未完成的最大 payload 字节，包含正在发布的一条；默认 1 MiB |
| `JUDGE_SOURCE_MAX_EVENT_BYTES` | `judge_source.max_event_bytes` | 单消息最大字节，上限 64 KiB |

`12_new_mpc.sh` 默认开启 Source。Judge 必须先启动并完成订阅，再启动 MPC，否则 `PUBLISH` 返回订阅者数 0，该消息会被丢弃。

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

硬边界：

- `event_id` 是小写规范 UUIDv4；
- `collected_at` 是 UTC 毫秒时间；
- `values` 是 MPC 当时掌握的设备完整属性快照；
- `updated_point` 必须存在于 `values` 且不为 `null`；
- 属性只允许 `null`、布尔、有限数字和字符串；
- 顶层固定五个字段，单消息不超过 64 KiB。

## 性能与隔离

采集线程在 Judge 分支组装事件并尝试非阻塞入队，不同步等待 Redis `PUBLISH` 或 Judge 处理。原有快照 `SET device:data:<device_id>` 仍由采集线程同步执行；慢 `SET` 仍会影响后续采集和新事件产生。发布器使用独立 Redis Client，使已入队事件不必等待快照客户端连接池或该次 `SET` 返回；两者仍共用配置中的 Redis 服务端，并非完全资源隔离。

发布器由“共享 worker 池 + 动态设备调度器”组成。设备只有在存在待发消息时才进入就绪队列，不永久绑定某个 worker。任意空闲 worker 都可以接手一个就绪设备，但同一设备同时最多只能有一条消息正在发布，因此同设备消息仍按进入 MPC 的顺序发送。

worker 每次只发布某个设备的一条消息。如果该设备还有待发消息，它会重新排到就绪设备队列末尾，让其他设备先获得执行机会。这避免热点设备长期占住一个固定通道，也避免多个设备因哈希碰撞互相阻塞。`worker_count` 只控制 Redis `PUBLISH` 的最大并发数；它不随设备数量增长，因此 2 万个设备不会创建 2 万个 Publisher worker。

准入同时检查两层预算：所有设备合计默认最多等待 2,048 条、未完成 payload 最多 16 MiB；每台设备默认最多有 64 条、1 MiB 已接收但未完成的消息。单设备计数和字节都包含正在发布的一条，发布返回后才释放；成功、超时、命令失败、0 订阅者都释放额度，设备排空后移除其队列记录。这里限制的是 payload 字节，不是整个 Go 进程的精确内存上限。

任何预算不足都只丢弃当前新来的 Judge 事件，不等待空位、不覆盖旧消息、不合并完整快照，已经接收的消息继续保持 FIFO 顺序。例如 A 已占用 64 个单设备名额，再来的 A 事件会被丢弃；B 在自身和全局还有额度时仍能进入。64 条 / 1 MiB 是可调的初始保护值，并非压测得出的最优值；多个热点设备仍可能共同用满全局容量。如果把单设备上限调到与全局相同，也会失去容量隔离效果。

动态调度与单设备预算分别控制执行机会和容量占用，不能让系统吞吐超过 Redis/Judge 的持续处理能力，也不应通过无限扩大队列隐藏长时间过载。MPC 发布段压力页面提示本次暂不优化。

发布 worker 取出消息时创建默认 200ms 的 context，独立 Redis 客户端启用 `ContextTimeoutEnabled`，使握手、命令读写使用同一个截止时间，连接池等待也受 context 约束。连接池内部异步拨号由独立 `DialTimeout` 限制，不保证与调用方同时瞬间取消，但不会继续补发已放弃的消息；读、写、连接池等待仍保留各阶段超时兜底。不是“每一步都重新获得 200ms”，也不是“入队后 200ms 内 Judge 必须判断完”。调度、GC 等因素使其不能被宣传为严格实时总耗时保证。快照 SET 客户端的超时、重试及连接池配置完全不变。

## 失败语义

下列情况只丢弃当前 Judge 事件，不影响后续采集：

- JSON 协议校验失败或消息超限；
- 全局或单设备的发布条数/字节额度已满；
- Redis 超时、断网或命令失败；
- `PUBLISH` 返回当时订阅者数为 0。

失败调用方只更新固定原因码对应的原子计数器，不执行日志 I/O。一个独立后台协程每秒尝试汇总，输出 `judge source event failures`、`reason_code`、`failure_count`；首次失败也等待后台汇总，不再由采集线程立即打印。没有日志事件队列，不保留完整属性快照、设备 ID、原始错误字符串或连接信息；未知原因合并进固定的 `SOURCE_FAILURE_UNKNOWN` 桶，不会动态创建无限原因码。

`PUBLISH` 返回 0 个订阅者使用 `SOURCE_NO_SUBSCRIBERS`，与普通 Redis 命令错误区分。日志写入阻塞时，后续失败仍只更新计数，后台恢复后再汇总；汇总次数不等于精确丢失条数，例如网络超时不能确定 Redis 是否已交付消息。停止发布器不等待日志协程完成 I/O，因此停机时未输出的诊断计数允许丢失。该保证只针对 Judge 新增失败日志，不改变原有快照、历史、协议日志或应用关闭时的同步日志行为。

单设备条数/字节超限分别记录 `SOURCE_DEVICE_QUEUE_FULL`、`SOURCE_DEVICE_QUEUE_BYTES_FULL`；全局超限仍使用 `SOURCE_QUEUE_FULL`、`SOURCE_QUEUE_BYTES_FULL`。它们沿用上述后台汇总，不向采集调用栈加入日志 I/O。

启动时创建 Judge 独立客户端不联网、不执行 `PING`，由后台发布 worker 在需要发送时建立连接。网络失败只结束当前消息的尝试，后续消息正常尝试，不补发失败消息、不建立新的重试任务。如果发布器本身的配置或构造无效，只停用该发布支路、关闭其独立客户端并累计 `SOURCE_PUBLISHER_UNAVAILABLE`；不会关闭快照客户端或阻止采集启动。原快照客户端仍保留启动连接检查，因此 Redis 本身不可用时，原有采集启动仍可能失败。其余边界和归因见 [发布分支与采集链路审查](../代码审查/Judge发布分支与采集链路边界审查.md)。

MPC/Judge 断网、重启或订阅暂时失效期间的消息不保留；重连后从后续新消息重新判断。

## 联调顺序

1. 停止旧 MPC 发布；
2. 部署并启动新 Judge；
3. 确认 Judge 已订阅 `iot:judge:device-events` 且 ready；
4. 部署 MPC；
5. 发布一条正常数据，核对 MPC 无“0 订阅者”错误、Judge 完成判断、SSE 收到预期报警；
6. 再执行分阶段吞吐和延迟测试。
